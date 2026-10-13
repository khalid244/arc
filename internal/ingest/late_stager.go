package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// LateStager batches late-event parquet writes locally and uploads merged
// objects to the flat <measurement>_late/ sidecar. Per-pod component.
//
// Without this, every schema-change in the ingest stream produces its own
// events_late parquet — at ~120 files/min in prod that's ~7K objects/hour.
// The stager merges them via DuckDB COPY(union_by_name=true) so events_late
// gets about one object per pod per merge job regardless of how many distinct
// event-type schemas appeared.
//
// Durability note: Stage fsyncs the file before returning, and from then on
// the staged file is the ONLY copy of its rows until a drain uploads it — the
// buffer checkpoints their WAL entries on a nil Stage. The file survives a
// process crash on the stage volume (the next process uploads it) but not the
// loss of the volume (pod deletion of an emptyDir). Close reports files still
// staged after the final drain (ErrStagedNotUploaded) so shutdown keeps the
// WAL and alerts instead of reporting a clean drain.
//
// The merge-and-upload machinery is the shared stagerEngine.
type LateStager struct {
	*stagerEngine
}

// LateStagerConfig holds configuration for constructing a LateStager.
type LateStagerConfig struct {
	Storage  storage.Backend
	StageDir string
	FlushAge time.Duration
	Logger   zerolog.Logger
	// Options bounds and tunes the merge jobs; the zero value takes defaults.
	Options StagerOptions
}

// NewLateStager creates a stager and starts its background flush loop.
// Caller is responsible for calling Close() on shutdown so the final flush
// has a chance to drain remaining staged files.
func NewLateStager(cfg *LateStagerConfig) (*LateStager, error) {
	if cfg.Storage == nil {
		return nil, fmt.Errorf("late stager: storage backend is required")
	}
	if cfg.StageDir == "" {
		cfg.StageDir = "./data/ingest/late-stager"
	}
	e, err := newStagerEngine(lateStagerSpec(), cfg.Storage, cfg.StageDir, cfg.FlushAge, cfg.Logger, cfg.Options)
	if err != nil {
		return nil, err
	}
	e.start()
	return &LateStager{stagerEngine: e}, nil
}

func lateStagerSpec() stagerKindSpec {
	return stagerKindSpec{
		kind:    "late",
		marker:  "late_stage_",
		logName: "LateStager",
		merge:   mergeLateSingle,
		jobKey:  lateJobKey,
	}
}

// lateJobKey names a late job output in the flat sidecar:
// <db>/<m>_late/<m>_late_<jobID>_<YYYYMMDD>_<HHMMSS>_<nanos>.parquet. The tail
// is what the reorganizer's filenameTimeRE parses to bucket the file by hour;
// the job id (with the pod) makes the key unique.
func lateJobKey(db, measurement, _ string, jobID string, created time.Time) string {
	lateMeasurement := measurement + LateSuffix
	return fmt.Sprintf("%s/%s/%s_%s_%s_%09d.parquet", db, lateMeasurement, lateMeasurement, jobID,
		created.Format("20060102_150405"), created.Nanosecond())
}

// mergeLateSingle merges every source into one local parquet (union_by_name
// so schema differences resolve to one output schema with NULLs for missing
// columns).
func mergeLateSingle(ctx context.Context, conn *sql.Conn, _ string, paths []string, outDir string) ([]mergedOutput, error) {
	// Read through sqlutil so Hive partition inference stays off (#1005).
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = sqlutil.QuoteStringLiteral(p)
	}
	dst := filepath.Join(outDir, outputFileName("late"))
	q := fmt.Sprintf(`COPY (SELECT * FROM %s) TO %s (FORMAT PARQUET)`,
		sqlutil.ReadParquetList(quoted, "union_by_name=true"), sqlutil.QuoteStringLiteral(dst))
	if _, err := conn.ExecContext(ctx, q); err != nil {
		return nil, fmt.Errorf("duckdb merge: %w", err)
	}
	return []mergedOutput{{partition: "late", path: dst}}, nil
}

// encodeStagerSegment makes a db or measurement name safe AND reversible
// inside a stage file name <db>__<measurement>__<kind>_stage_<seq>.parquet.
//
// The previous sanitizer collapsed "__" to "_" and was not reversible: valid
// names (the API allows ^[a-zA-Z][a-zA-Z0-9_-]*$) such as "a__b" were uploaded
// under "a_b", and a trailing underscore ("m_" -> "db__m___fresh_stage_1")
// broke the "__" split so the file was never picked up at all — and since the
// merge with upstream a staged file's rows are already checkpointed out of the
// WAL, so that is loss, not a delay. Percent-encoding every '_' removes "__"
// from inside a segment entirely; '%', '/', '\\' and '.' are encoded too so a
// segment can never traverse or hide (leading dot) the file.
//
// Names staged by older binaries contain no '%' and decode to themselves.
func encodeStagerSegment(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '_', '%', '/', '\\', '.':
			fmt.Fprintf(&b, "%%%02X", c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// decodeStagerSegment reverses encodeStagerSegment. A segment that is not
// valid percent-encoding (a name staged by an older binary) is returned as is.
func decodeStagerSegment(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	out, err := url.PathUnescape(s)
	if err != nil {
		return s
	}
	return out
}

// LateStagerObjectKey returns the object key a LateStager job running on pod
// would give an output created at created, with the job id's random part set
// to jobHex. The late-event reorganizer must accept every key this produces;
// its tests build keys through here so the two cannot drift apart.
func LateStagerObjectKey(db, measurement, pod, jobHex string, created time.Time) string {
	return lateJobKey(db, measurement, "", sanitizeStagerPodID(pod)+"-"+jobHex, created)
}
