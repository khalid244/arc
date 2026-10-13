package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	_ "github.com/duckdb/duckdb-go/v2" // register database/sql driver
	"github.com/rs/zerolog"
)

// FreshStager mirrors LateStager but for the FRESH ingest path. Each
// schema-change-driven flush stages its parquet locally; periodically the
// stager merges staged files via DuckDB COPY ... union_by_name=true AND
// re-partitions the merged rows by event-time hour (Y/M/D/H) before
// uploading. The result: one upload per (pod, partition, merge job)
// regardless of how many distinct event schemas appeared.
//
// Compared to LateStager:
//   - Output goes to events/Y/M/D/H/ (the live query target), not the flat
//     events_late/ sidecar.
//   - The merge SQL adds PARTITION_BY (_y, _m, _d, _h) so the unified rows
//     are written to one partition-correct output file per hour.
//
// Durability semantics match LateStager: a staged file is fsynced before
// Stage returns, and from then on it is the ONLY copy of its rows — the
// buffer checkpoints their WAL entries on a nil Stage. It survives a process
// crash on the stage volume (the next process uploads it) but not the loss of
// the volume (pod deletion of an emptyDir). Close reports files that are still
// staged after the final drain (ErrStagedNotUploaded) so shutdown can keep the
// WAL and alert instead of reporting a clean drain.
//
// The merge-and-upload machinery is the shared stagerEngine.
type FreshStager struct {
	*stagerEngine
}

type FreshStagerConfig struct {
	Storage  storage.Backend
	StageDir string
	FlushAge time.Duration
	Logger   zerolog.Logger
	// Options bounds and tunes the merge jobs; the zero value takes defaults.
	Options StagerOptions
}

func NewFreshStager(cfg *FreshStagerConfig) (*FreshStager, error) {
	if cfg.Storage == nil {
		return nil, fmt.Errorf("fresh stager: storage backend required")
	}
	if cfg.StageDir == "" {
		cfg.StageDir = "./data/ingest/fresh-stager"
	}
	e, err := newStagerEngine(freshStagerSpec(), cfg.Storage, cfg.StageDir, cfg.FlushAge, cfg.Logger, cfg.Options)
	if err != nil {
		return nil, err
	}
	e.start()
	return &FreshStager{stagerEngine: e}, nil
}

func freshStagerSpec() stagerKindSpec {
	return stagerKindSpec{
		kind:    "fresh",
		marker:  "fresh_stage_",
		logName: "FreshStager",
		merge:   mergeFreshPartitioned,
		jobKey:  freshJobKey,
	}
}

// freshJobKey names a fresh job output in its hour partition:
// <db>/<m>/YYYY/MM/DD/HH/<m>_<jobID>_<YYYYMMDD>_<HHMMSS>_<nanos>.parquet.
// The job id carries the pod, so keys are unique across pods and jobs, and the
// tail keeps the <YYYYMMDD>_<HHMMSS>_<digits> shape the daily compactor and
// file-time pruning parse. The time is the job's creation, which is after
// every staged row's flush.
func freshJobKey(db, measurement, partition, jobID string, created time.Time) string {
	return fmt.Sprintf("%s/%s/%s/%s_%s_%s_%09d.parquet", db, measurement, partition, measurement, jobID,
		created.Format("20060102_150405"), created.Nanosecond())
}

// mergeFreshPartitioned merges sources into outDir partitioned by event-time
// hour and returns exactly one output file per hour. DuckDB may write more than
// one file for a partition (it closes and reopens files beyond
// partitioned_write_max_open_files); such files are merged into one so every
// partition is a single, deterministic unit of upload.
func mergeFreshPartitioned(ctx context.Context, conn *sql.Conn, measurement string, paths []string, outDir string) ([]mergedOutput, error) {
	rawDir := filepath.Join(outDir, "raw")
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = sqlutil.QuoteStringLiteral(p)
	}
	// Read through sqlutil so Hive partition inference stays off (#1005).
	source := sqlutil.ReadParquetList(quoted, "union_by_name=true")
	pattern := sqlutil.QuoteStringLiteral(fmt.Sprintf("%s_{uuid}", encodeStagerSegment(measurement)))
	q := fmt.Sprintf(`COPY (
  SELECT *,
         CAST(extract(year  FROM time) AS INTEGER) AS _y,
         CAST(extract(month FROM time) AS INTEGER) AS _m,
         CAST(extract(day   FROM time) AS INTEGER) AS _d,
         CAST(extract(hour  FROM time) AS INTEGER) AS _h
  FROM %s
  WHERE time IS NOT NULL
) TO %s (
  FORMAT PARQUET,
  PARTITION_BY (_y, _m, _d, _h),
  COMPRESSION ZSTD,
  OVERWRITE_OR_IGNORE,
  FILENAME_PATTERN %s
)`, source, sqlutil.QuoteStringLiteral(rawDir), pattern)
	if _, err := conn.ExecContext(ctx, q); err != nil {
		return nil, fmt.Errorf("duckdb merge: %w", err)
	}

	// rawDir/_y=YYYY/_m=M/_d=D/_h=H/<file>.parquet (DuckDB writes integers
	// without zero padding; partitions are zero-padded like generateStoragePath).
	byPartition := make(map[string][]string)
	walkErr := filepath.Walk(rawDir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return nil
		}
		rel, err := filepath.Rel(rawDir, p)
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 5 {
			return fmt.Errorf("fresh stager: unexpected merge output path %q", rel)
		}
		y, m, d, h, ok := parsePartitionDirs(parts[0], parts[1], parts[2], parts[3])
		if !ok {
			return fmt.Errorf("fresh stager: bad partition dirs in %q", rel)
		}
		part := fmt.Sprintf("%04d/%02d/%02d/%02d", y, m, d, h)
		byPartition[part] = append(byPartition[part], p)
		return nil
	})
	if walkErr != nil && !os.IsNotExist(walkErr) {
		return nil, walkErr
	}
	partitions := make([]string, 0, len(byPartition))
	for part := range byPartition {
		partitions = append(partitions, part)
	}
	sort.Strings(partitions)
	outputs := make([]mergedOutput, 0, len(partitions))
	for _, part := range partitions {
		files := byPartition[part]
		dst := filepath.Join(outDir, outputFileName(part))
		if len(files) == 1 {
			if err := os.Rename(files[0], dst); err != nil {
				return nil, fmt.Errorf("move merge output: %w", err)
			}
		} else {
			sort.Strings(files)
			lits := make([]string, len(files))
			for i, f := range files {
				lits[i] = sqlutil.QuoteStringLiteral(f)
			}
			cq := fmt.Sprintf(`COPY (SELECT * FROM %s) TO %s (FORMAT PARQUET, COMPRESSION ZSTD)`,
				sqlutil.ReadParquetList(lits, "union_by_name=true"), sqlutil.QuoteStringLiteral(dst))
			if _, err := conn.ExecContext(ctx, cq); err != nil {
				return nil, fmt.Errorf("duckdb merge of partition %s: %w", part, err)
			}
		}
		outputs = append(outputs, mergedOutput{partition: part, path: dst})
	}
	if err := os.RemoveAll(rawDir); err != nil {
		return nil, fmt.Errorf("remove raw merge output: %w", err)
	}
	return outputs, nil
}

// parsePartitionDirs accepts the four "_x=N" dir name components DuckDB
// emits and returns y,m,d,h as ints.
func parsePartitionDirs(y, m, d, h string) (int, int, int, int, bool) {
	yi, ok := parseKVInt(y, "_y=")
	if !ok {
		return 0, 0, 0, 0, false
	}
	mi, ok := parseKVInt(m, "_m=")
	if !ok {
		return 0, 0, 0, 0, false
	}
	di, ok := parseKVInt(d, "_d=")
	if !ok {
		return 0, 0, 0, 0, false
	}
	hi, ok := parseKVInt(h, "_h=")
	if !ok {
		return 0, 0, 0, 0, false
	}
	return yi, mi, di, hi, true
}

func parseKVInt(s, prefix string) (int, bool) {
	if !strings.HasPrefix(s, prefix) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(s, prefix))
	if err != nil {
		return 0, false
	}
	return n, true
}
