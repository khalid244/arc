package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Reorganizer drains the flat events_late sidecar measurements written by
// the ingest's late-routing hook and redistributes their rows back into the
// normal Y/M/D/H partition layout under the original measurement.
//
// The reorganizer is intentionally NOT a Tier — the existing Tier/Job
// framework assumes per-partition in-place compaction, while reorg does
// fan-out: one source file can contain rows spanning many target hour
// partitions. Implementing it as a sibling component avoids contorting the
// Job code for a different shape of work.
//
// Safety contract: source files are deleted ONLY after every target file
// has been written successfully AND the row-count audit (input ==
// non-null-time-output) passes. On crash before delete, the next run
// re-reads the same source — duplicate target files survive until daily
// compaction's tag-dedup folds them. No data loss; bounded duplication.
type Reorganizer struct {
	Backend       storage.Backend
	Databases     []string // databases to scan; empty = discover
	Measurements  []string // base measurement names opted into late-split (e.g. ["events"])
	MinAgeSeconds int      // skip source files whose hour bucket is fresher than this
	TempDirectory string   // local scratch dir for DuckDB + downloads
	MemoryLimit   string   // DuckDB memory limit; empty = unset
	MaxConcurrent int      // max parallel bucket workers; <= 0 falls back to 1
	// ManifestManager provides crash recovery. nil disables manifest tracking
	// (a partial-crash will leak duplicates that daily-tier dedup folds).
	ManifestManager *ReorgManifestManager
	// ClusterGate is the same Phase 4 role check the compaction scheduler
	// uses. When non-nil and CanCompact() reports false, Run() stays idle
	// (not an error — silent no-op so multi-pod deployments can deploy
	// reorg config to every replica and only the leader actually does work).
	// nil means "no check, run unconditionally" — used by OSS / single-pod
	// deployments and by existing tests that predate clustering.
	ClusterGate ClusterGate
	Logger      zerolog.Logger
}

// filenameTimeRE captures the YYYYMMDD_HHMMSS prefix in our standard filename
// pattern <measurement>_YYYYMMDD_HHMMSS_<nanos>.parquet. Anchored at start of
// filename (no `/`) so we don't accidentally match digits embedded in some
// other prefix that happens to share the date-time shape.
var filenameTimeRE = regexp.MustCompile(`^[^/]*?(\d{8})_(\d{6})_\d+\.parquet$`)

// Run executes one drain pass: for every (database, measurement) combo, bucket
// the source files in <db>/<measurement>_late/ by their filename hour and
// reorganize each closed-hour bucket. A bucket is "closed" when its hour is
// older than MinAgeSeconds, which guarantees ingest has stopped writing new
// files into that hour partition of the sidecar.
//
// If a ManifestManager is configured, recovery for stale in-flight buckets
// runs FIRST so the listing in step 2 reflects the cleaned-up state — no
// risk of double-processing source files that an earlier run already
// uploaded.
func (r *Reorganizer) Run(ctx context.Context) error {
	// Phase 4 role gate: same chokepoint pattern as compaction/scheduler.go.
	// A non-compactor node that's been deployed with reorg config (because
	// every replica gets the same TOML) silently no-ops here rather than
	// stamping `events_late/` from multiple pods at once. Logged at debug
	// to avoid flooding logs on every cron tick.
	if r.ClusterGate != nil && !r.ClusterGate.CanCompact() {
		r.Logger.Debug().
			Str("role", r.ClusterGate.Role()).
			Msg("Reorg: gated by ClusterGate; this node is not the compactor — skipping cycle")
		return nil
	}
	if r.ManifestManager != nil {
		if _, err := r.ManifestManager.RecoverOrphanedReorgManifests(ctx); err != nil {
			r.Logger.Warn().Err(err).Msg("Reorg recovery: continuing despite error; pending manifests will be retried next cycle")
		}
	}

	minAge := time.Duration(r.MinAgeSeconds) * time.Second
	if minAge <= 0 {
		minAge = time.Hour
	}

	databases := r.Databases
	if len(databases) == 0 {
		if dl, ok := r.Backend.(storage.DirectoryLister); ok {
			dirs, err := dl.ListDirectories(ctx, "")
			if err != nil {
				return fmt.Errorf("list databases: %w", err)
			}
			for _, d := range dirs {
				if d != "" && !strings.HasPrefix(d, ".") {
					databases = append(databases, d)
				}
			}
		}
	}

	for _, db := range databases {
		for _, measurement := range r.Measurements {
			m := strings.TrimSpace(measurement)
			if m == "" {
				continue
			}
			lateName := m + ingest.LateSuffix
			if err := r.runOne(ctx, db, m, lateName, minAge); err != nil {
				r.Logger.Error().Err(err).
					Str("database", db).
					Str("late_measurement", lateName).
					Msg("Reorganize failed")
			}
		}
	}
	return nil
}

// runOne drains a single (db, late_measurement) sidecar.
func (r *Reorganizer) runOne(ctx context.Context, db, measurement, lateName string, minAge time.Duration) error {
	prefix := db + "/" + lateName + "/"
	keys, err := r.Backend.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("list %s: %w", prefix, err)
	}
	if len(keys) == 0 {
		return nil
	}

	// Bucket source files by their filename's hour (== ingest hour, since
	// late routing uses now.UTC() to name files). Only buckets that are
	// fully closed get processed this cycle.
	cutoff := time.Now().UTC().Add(-minAge)
	buckets := make(map[time.Time][]string)
	for _, key := range keys {
		ft, ok := parseFilenameTime(key)
		if !ok {
			r.Logger.Debug().Str("key", key).Msg("Reorg: skipping file with unparseable name")
			continue
		}
		hour := ft.Truncate(time.Hour)
		if hour.After(cutoff) {
			continue
		}
		buckets[hour] = append(buckets[hour], key)
	}

	if len(buckets) == 0 {
		return nil
	}

	r.Logger.Info().
		Str("database", db).
		Str("late_measurement", lateName).
		Int("closed_buckets", len(buckets)).
		Int("total_source_files", len(keys)).
		Msg("Reorganizer starting drain")

	// Bucket-level parallelism mirrors the compactor's per-partition
	// semaphore + waitgroup pattern in compaction/manager.go. Each bucket
	// is independent (disjoint source files, disjoint downloads, disjoint
	// DuckDB processes) so the only shared resource is the storage backend.
	//
	// Default MaxConcurrent=1: DuckDB runs in-process today and a second
	// concurrent bucket would double the memory pressure on the parent
	// process. Raising above 1 should wait until reorg moves to subprocess
	// isolation (same pattern as compaction/subprocess.go).
	maxConcurrent := r.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	for hour, files := range buckets {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		default:
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(h time.Time, fs []string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := r.processBucket(ctx, db, measurement, lateName, h, fs); err != nil {
				metrics.Get().IncReorgBucketsFailed()
				r.Logger.Error().Err(err).
					Time("hour", h).
					Int("files", len(fs)).
					Msg("Reorg bucket failed")
			} else {
				metrics.Get().IncReorgBucketsSuccess()
			}
		}(hour, files)
	}
	wg.Wait()
	return nil
}

// processBucket handles all source files written in one ingest hour. The
// flow is the same two-phase commit the compactor uses (see
// compaction/manifest.go and compaction/job.go's deleteOldFiles dance):
//
//  1. Download sources to a local scratch dir
//  2. DuckDB COPY ... PARTITION_BY produces one parquet per target hour
//     (filtered to WHERE time IS NOT NULL — see step 2.5)
//  2.5 Row-count audit: count NULL-time rows separately, sum output rows,
//     refuse to delete sources if the math doesn't match.
//  3. Enumerate outputs with deterministic JobID-bound paths (so a crashed
//     run's partials are distinguishable from a re-run's outputs)
//  4. Write manifest in "pending" state — past this point recovery can
//     finish or roll back the bucket without local scratch data
//  5. Upload every output
//  6. Mark manifest "uploaded" — recovery now only has to retry the
//     source-delete step
//  7. Delete sources
//  8. Delete manifest
//
// Failure in steps 4–8 leaves a recoverable manifest. Failure in 1–3 is
// pre-manifest: the bucket's source files stay in events_late/ and the
// next cycle re-runs it from scratch.
func (r *Reorganizer) processBucket(ctx context.Context, db, measurement, lateName string, hour time.Time, sources []string) error {
	scratch, err := os.MkdirTemp(r.TempDirectory, fmt.Sprintf("reorg_%s_%s_*", lateName, hour.Format("20060102T150405")))
	if err != nil {
		return fmt.Errorf("mkdir scratch: %w", err)
	}
	defer os.RemoveAll(scratch)

	srcDir := filepath.Join(scratch, "sources")
	outDir := filepath.Join(scratch, "out")
	duckdbTmp := filepath.Join(scratch, "duckdb-tmp")
	if err := os.MkdirAll(srcDir, 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(duckdbTmp, 0700); err != nil {
		return err
	}

	// Download sources. Each file is small (~tens of KB to a few MB) so we
	// stay serial — but the loop checks ctx between iterations so SIGTERM
	// is observed within one file's download latency.
	var localPaths []string
	for _, key := range sources {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		local := filepath.Join(srcDir, filepath.Base(key))
		f, err := os.Create(local)
		if err != nil {
			return fmt.Errorf("create %s: %w", local, err)
		}
		if err := r.Backend.ReadTo(ctx, key, f); err != nil {
			f.Close()
			return fmt.Errorf("download %s: %w", key, err)
		}
		f.Close()
		localPaths = append(localPaths, local)
	}

	// One DuckDB instance per bucket; closed before the next bucket starts
	// so jemalloc fully releases memory between buckets.
	d, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer d.Close()
	if r.MemoryLimit != "" {
		if _, err := d.ExecContext(ctx, "SET memory_limit='"+escapeSQLString(r.MemoryLimit)+"'"); err != nil {
			r.Logger.Warn().Err(err).Msg("Reorg: failed to set DuckDB memory_limit")
		}
	}
	// Per-bucket spill directory. Without this, concurrent buckets on the
	// same pod default to <cwd>/.tmp/ and race on identical spill filenames
	// — the same fix subprocess.go:138-145 documents for the compactor.
	if _, err := d.ExecContext(ctx, "SET temp_directory='"+escapeSQLPath(duckdbTmp)+"'"); err != nil {
		r.Logger.Warn().Err(err).Str("dir", duckdbTmp).Msg("Reorg: failed to set DuckDB temp_directory")
	}

	// Build the file list as a DuckDB array literal.
	fileList := "["
	for i, p := range localPaths {
		if i > 0 {
			fileList += ", "
		}
		fileList += "'" + escapeSQLPath(p) + "'"
	}
	fileList += "]"

	// Guard: confirm the `time` column is a TIMESTAMP variant before issuing
	// the partition-by COPY. Without this an `events_late` written from a
	// measurement that stores `time` as e.g. BIGINT nanos would fail the
	// COPY with an opaque DuckDB cast error mid-query.
	timeType, err := r.sniffTimeColumn(ctx, d, fileList)
	if err != nil {
		return fmt.Errorf("sniff time column: %w", err)
	}
	if !isTimestampType(timeType) {
		r.Logger.Warn().
			Str("late_measurement", lateName).
			Time("bucket_hour", hour).
			Str("time_type", timeType).
			Int("sources", len(sources)).
			Msg("Reorg: skipping bucket — `time` column is not a TIMESTAMP variant; reorg partition derivation requires TIMESTAMP")
		return nil
	}

	// Count total rows AND null-time rows in sources up front. Used to
	// audit the COPY: input_rows = output_rows + null_time_rows. Any
	// mismatch means DuckDB silently dropped data and we MUST NOT delete
	// the sources.
	inputRows, nullTimeRows, err := r.countRows(ctx, d, fileList)
	if err != nil {
		return fmt.Errorf("count input rows: %w", err)
	}
	if nullTimeRows > 0 {
		r.Logger.Warn().
			Str("late_measurement", lateName).
			Time("bucket_hour", hour).
			Int64("null_time_rows", nullTimeRows).
			Int64("input_rows", inputRows).
			Msg("Reorg: rows with NULL time will be dropped (cannot derive partition); operator should investigate ingest source")
		metrics.Get().IncReorgRowsDroppedNullTS(nullTimeRows)
	}

	// COPY ... PARTITION_BY: DuckDB strips the partition columns from the
	// output files automatically, so adding y/m/d/h via SELECT * is safe —
	// only `time` and the original payload columns end up in each parquet.
	// EXTRACT(... FROM time AT TIME ZONE 'UTC') coerces TIMESTAMP_TZ to
	// TIMESTAMP in UTC so the partition layout matches Arc's UTC convention.
	// WHERE time IS NOT NULL prevents DuckDB from writing rows into a
	// _y=__HIVE_DEFAULT_PARTITION__/ directory that our path parser
	// silently skips (see parseHivePartitions).
	query := fmt.Sprintf(`
COPY (
  SELECT *,
    EXTRACT(YEAR  FROM time AT TIME ZONE 'UTC')::INT AS _y,
    EXTRACT(MONTH FROM time AT TIME ZONE 'UTC')::INT AS _m,
    EXTRACT(DAY   FROM time AT TIME ZONE 'UTC')::INT AS _d,
    EXTRACT(HOUR  FROM time AT TIME ZONE 'UTC')::INT AS _h
  FROM read_parquet(%s)
  WHERE time IS NOT NULL
) TO '%s' (
  FORMAT PARQUET,
  PARTITION_BY (_y, _m, _d, _h),
  OVERWRITE_OR_IGNORE
)`, fileList, escapeSQLPath(outDir))

	if _, err := d.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("duckdb COPY: %w", err)
	}

	// JobID disambiguates this attempt's outputs from any prior crashed
	// attempt's partials.
	jobID := fmt.Sprintf("%d", time.Now().UnixNano())

	planned, err := r.enumerateOutputs(outDir, db, measurement, jobID)
	if err != nil {
		return fmt.Errorf("enumerate outputs: %w", err)
	}

	// Row-count audit: sum row counts across enumerated output files and
	// require it to equal input_rows - null_time_rows. Any other shortfall
	// means DuckDB dropped data we didn't predict (HIVE_DEFAULT_PARTITION
	// from some other column, schema-evolution edge case, etc.) — refuse
	// to delete sources so an operator can investigate.
	outputRows, err := r.countOutputRows(ctx, d, planned)
	if err != nil {
		return fmt.Errorf("count output rows: %w", err)
	}
	expected := inputRows - nullTimeRows
	if outputRows != expected {
		r.Logger.Error().
			Str("late_measurement", lateName).
			Time("bucket_hour", hour).
			Int64("input_rows", inputRows).
			Int64("null_time_rows", nullTimeRows).
			Int64("expected_output_rows", expected).
			Int64("actual_output_rows", outputRows).
			Int("planned_outputs", len(planned)).
			Msg("Reorg: ROW COUNT MISMATCH — refusing to delete sources to prevent data loss; bucket will be retried next cycle once root cause is fixed")
		return fmt.Errorf("row count mismatch: input=%d null_time=%d expected_output=%d actual_output=%d",
			inputRows, nullTimeRows, expected, outputRows)
	}

	if len(planned) == 0 {
		// Audit passed (expected == 0), so all input was NULL-time. The
		// dropped count is already logged + metricized above. Safe to
		// delete sources — they hold only data we can never partition.
		if err := r.Backend.DeleteBatch(ctx, sources); err != nil {
			return fmt.Errorf("delete null-only sources: %w", err)
		}
		metrics.Get().IncReorgSourcesDrained(int64(len(sources)))
		return nil
	}

	manifestPlanned := make([]PlannedReorgOutput, 0, len(planned))
	for _, p := range planned {
		manifestPlanned = append(manifestPlanned, PlannedReorgOutput{Path: p.targetKey, Size: p.size})
	}

	var manifestKey string
	if r.ManifestManager != nil {
		manifest := &ReorgManifest{
			JobID:          jobID,
			Database:       db,
			Measurement:    measurement,
			LateName:       lateName,
			BucketHour:     hour,
			SourceFiles:    sources,
			PlannedOutputs: manifestPlanned,
			Status:         ReorgStatusPending,
		}
		manifestKey, err = r.ManifestManager.Write(ctx, manifest)
		if err != nil {
			return fmt.Errorf("write reorg manifest: %w", err)
		}
	}

	// Upload every output. ctx is checked between files so SIGTERM during
	// a long upload sequence is observed within one file's upload latency.
	for _, p := range planned {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := r.uploadOne(ctx, p); err != nil {
			return fmt.Errorf("upload %s: %w", p.targetKey, err)
		}
	}

	if r.ManifestManager != nil && manifestKey != "" {
		if err := r.ManifestManager.MarkUploaded(ctx, manifestKey); err != nil {
			return fmt.Errorf("mark manifest uploaded: %w", err)
		}
	}

	if err := r.Backend.DeleteBatch(ctx, sources); err != nil {
		r.Logger.Warn().Err(err).
			Int("sources", len(sources)).
			Msg("Reorg: delete sources failed; recovery will complete on next cycle")
		return nil
	}

	if r.ManifestManager != nil && manifestKey != "" {
		if err := r.ManifestManager.Delete(ctx, manifestKey); err != nil {
			r.Logger.Warn().Err(err).Str("manifest", manifestKey).Msg("Reorg: failed to delete manifest after successful drain")
		}
	}

	metrics.Get().IncReorgSourcesDrained(int64(len(sources)))
	metrics.Get().IncReorgOutputsWritten(int64(len(planned)))

	r.Logger.Info().
		Str("database", db).
		Str("measurement", measurement).
		Time("source_hour", hour).
		Str("job_id", jobID).
		Int("sources", len(sources)).
		Int("outputs", len(planned)).
		Int64("input_rows", inputRows).
		Int64("output_rows", outputRows).
		Int64("dropped_null_time", nullTimeRows).
		Msg("Reorg bucket drained")
	return nil
}

// reorgOutput is the local-scratch + target-storage pairing for one DuckDB
// output file. Kept private to the package — manifest persistence uses
// PlannedReorgOutput which has just the (target, size) bits.
type reorgOutput struct {
	localPath string
	targetKey string
	size      int64
}

// enumerateOutputs walks the DuckDB output directory and builds the list of
// reorgOutput entries the bucket will upload. Target keys embed jobID so
// rollback during recovery can selectively delete this attempt's partials
// without touching files written by earlier successful runs of the same
// target partition.
func (r *Reorganizer) enumerateOutputs(outDir, db, measurement, jobID string) ([]reorgOutput, error) {
	var outputs []reorgOutput
	seq := 0
	err := filepath.WalkDir(outDir, func(path string, info os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".parquet") {
			return nil
		}
		rel, err := filepath.Rel(outDir, path)
		if err != nil {
			return err
		}
		y, m, d, h, ok := parseHivePartitions(rel)
		if !ok {
			r.Logger.Warn().Str("rel", rel).Msg("Reorg: unparseable DuckDB output path; skipping")
			return nil
		}
		st, err := os.Stat(path)
		if err != nil {
			return err
		}
		targetKey := fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d/%s_reorg_%s_%d.parquet",
			db, measurement, y, m, d, h,
			measurement, jobID, seq,
		)
		outputs = append(outputs, reorgOutput{
			localPath: path,
			targetKey: targetKey,
			size:      st.Size(),
		})
		seq++
		return nil
	})
	return outputs, err
}

// uploadOne streams a single output file from local scratch to the storage
// backend.
func (r *Reorganizer) uploadOne(ctx context.Context, o reorgOutput) error {
	f, err := os.Open(o.localPath)
	if err != nil {
		return fmt.Errorf("open output %s: %w", o.localPath, err)
	}
	defer f.Close()
	if err := r.Backend.WriteReader(ctx, o.targetKey, f, o.size); err != nil {
		return fmt.Errorf("upload %s: %w", o.targetKey, err)
	}
	return nil
}

// countRows returns (total_rows, null_time_rows) for the source files.
// Parquet's footer metadata gives row counts at zero data-scan cost; the
// NULL count requires a scan but for the small late files that's cheap.
func (r *Reorganizer) countRows(ctx context.Context, d *sql.DB, fileList string) (int64, int64, error) {
	q := fmt.Sprintf(`
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE time IS NULL) AS null_time
FROM read_parquet(%s)`, fileList)
	row := d.QueryRowContext(ctx, q)
	var total, nullTime int64
	if err := row.Scan(&total, &nullTime); err != nil {
		return 0, 0, err
	}
	return total, nullTime, nil
}

// countOutputRows sums row counts across every enumerated output file. Uses
// parquet metadata only (no data scan) so it's cheap regardless of file size.
func (r *Reorganizer) countOutputRows(ctx context.Context, d *sql.DB, planned []reorgOutput) (int64, error) {
	if len(planned) == 0 {
		return 0, nil
	}
	fileList := "["
	for i, p := range planned {
		if i > 0 {
			fileList += ", "
		}
		fileList += "'" + escapeSQLPath(p.localPath) + "'"
	}
	fileList += "]"
	q := fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet(%s)`, fileList)
	row := d.QueryRowContext(ctx, q)
	var total int64
	if err := row.Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func parseFilenameTime(key string) (time.Time, bool) {
	m := filenameTimeRE.FindStringSubmatch(filepath.Base(key))
	if m == nil {
		return time.Time{}, false
	}
	t, err := time.Parse("20060102_150405", m[1]+"_"+m[2])
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// sniffTimeColumn returns the DuckDB column_type of the `time` column for the
// given list of parquet files. Returns an error if the column is absent or the
// describe query fails; returns the type string otherwise.
func (r *Reorganizer) sniffTimeColumn(ctx context.Context, d *sql.DB, fileList string) (string, error) {
	q := fmt.Sprintf(`SELECT column_type FROM (DESCRIBE SELECT * FROM read_parquet(%s)) WHERE column_name = 'time'`, fileList)
	row := d.QueryRowContext(ctx, q)
	var t string
	if err := row.Scan(&t); err != nil {
		return "", err
	}
	return t, nil
}

// isTimestampType reports whether a DuckDB column type can drive Arc's
// year/month/day/hour partition derivation. We accept the four parquet
// TIMESTAMP variants explicitly so adding e.g. BIGINT support later is a
// deliberate change to this allowlist rather than an accidental match.
func isTimestampType(t string) bool {
	u := strings.ToUpper(strings.TrimSpace(t))
	return u == "TIMESTAMP" ||
		u == "TIMESTAMP WITH TIME ZONE" ||
		u == "TIMESTAMP_MS" ||
		u == "TIMESTAMP_NS" ||
		u == "TIMESTAMP_S"
}

var hivePartitionRE = regexp.MustCompile(`_y=(\d+)/_m=(\d+)/_d=(\d+)/_h=(\d+)/`)

func parseHivePartitions(rel string) (year, month, day, hour int, ok bool) {
	m := hivePartitionRE.FindStringSubmatch(rel)
	if m == nil {
		return 0, 0, 0, 0, false
	}
	fmt.Sscanf(m[1], "%d", &year)
	fmt.Sscanf(m[2], "%d", &month)
	fmt.Sscanf(m[3], "%d", &day)
	fmt.Sscanf(m[4], "%d", &hour)
	return year, month, day, hour, true
}
