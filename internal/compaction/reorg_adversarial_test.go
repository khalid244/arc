package compaction

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// jsonMarshalIndent mirrors the manifest manager's serialization so a test can
// write a manifest with a controlled (aged) UpdatedAt.
func jsonMarshalIndent(v interface{}) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

// ============================================================================
// Shared adversarial-test helpers
// ============================================================================

// faultBackend wraps a *storage.LocalBackend and fails the Nth WriteReader
// call (1-indexed) with a synthetic error, delegating everything else. Used to
// simulate a partial-upload crash mid-bucket. Thread-safe: WriteReader is
// called concurrently by uploadOutputs' worker pool.
//
// It embeds the CONCRETE *LocalBackend (not the interface) so the optional
// interfaces the reorg/manifest code type-asserts — storage.ObjectLister
// (probeOutputs, size-based partial detection) and storage.DirectoryLister
// (db discovery) — pass through. Only WriteReader and DeleteBatch are
// overridden for fault injection.
type faultBackend struct {
	*storage.LocalBackend
	failOnWriteN int32 // fail the Nth WriteReader (atomic counter); 0 = never
	writeN       int32
	// failDeleteBatch, when true, makes DeleteBatch fail (simulate crash
	// between MarkUploaded and source-delete).
	failDeleteBatch atomic.Bool
	writes          atomic.Int64 // count of successful WriteReader calls
}

func (f *faultBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	n := atomic.AddInt32(&f.writeN, 1)
	if f.failOnWriteN > 0 && n == f.failOnWriteN {
		// Drain the reader so we don't leave a half-read body, then fail.
		_, _ = io.Copy(io.Discard, r)
		return fmt.Errorf("injected WriteReader failure on call %d (path=%s)", n, path)
	}
	if err := f.LocalBackend.WriteReader(ctx, path, r, size); err != nil {
		return err
	}
	f.writes.Add(1)
	return nil
}

func (f *faultBackend) DeleteBatch(ctx context.Context, paths []string) error {
	if f.failDeleteBatch.Load() {
		return fmt.Errorf("injected DeleteBatch failure (%d paths)", len(paths))
	}
	return f.LocalBackend.DeleteBatch(ctx, paths)
}

// faultBackend must still satisfy the optional interfaces the reorg code
// type-asserts (ObjectLister for probeOutputs, DirectoryLister for db
// discovery). Embedding the LocalBackend gives us those methods for free,
// but Go interface satisfaction via embedding only works if the concrete
// embedded type implements them — LocalBackend does, so the assertions in
// reorg/manifest code will see the embedded methods. Verified below.
var _ storage.Backend = (*faultBackend)(nil)

// writeLateParquet writes a synthetic late-event parquet file at lateDir using
// DuckDB. selectBody must be a SELECT producing at least a `time` column.
func writeLateParquet(t *testing.T, db *sql.DB, lateDir, filename, selectBody string) {
	t.Helper()
	path := filepath.Join(lateDir, filename)
	q := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET)`, selectBody, escapeSQLPath(path))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write late parquet %s: %v", filename, err)
	}
}

// newReorg builds a Reorganizer with a manifest manager over the given backend.
func newReorg(backend storage.Backend, tmp string, logger zerolog.Logger) *Reorganizer {
	return &Reorganizer{
		Backend:          backend,
		Databases:        []string{"posthog"},
		Measurements:     []string{"events"},
		MinAgeSeconds:    3600,
		TempDirectory:    tmp,
		MaxConcurrent:    1,
		MaxFilesPerBatch: 2000,
		DownloadWorkers:  4,
		ManifestManager:  NewReorgManifestManager(backend, logger),
		Logger:           logger,
	}
}

// outputRowsByDay reads every parquet under db/meas event tree and returns
// (totalRows, distinctDayPartitions, perDayRowCounts keyed by "Y/M/D").
func outputRowsByDay(t *testing.T, eventsDir string) (int64, map[string]int64) {
	t.Helper()
	perDay := map[string]int64{}
	var total int64
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	_ = filepath.Walk(eventsDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(p) != ".parquet" {
			return nil
		}
		// path: <eventsDir>/Y/M/D/file.parquet
		rel, _ := filepath.Rel(eventsDir, p)
		parts := strings.Split(rel, string(os.PathSeparator))
		var key string
		if len(parts) >= 4 {
			key = parts[0] + "/" + parts[1] + "/" + parts[2]
		}
		var n int64
		if e := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s')`, escapeSQLPath(p))).Scan(&n); e != nil {
			t.Fatalf("count rows %s: %v", p, e)
		}
		perDay[key] += n
		total += n
		return nil
	})
	return total, perDay
}

// ============================================================================
// PROPERTY 1: Losslessness / row-count audit across data shapes
// ============================================================================

// TestReorg_NullTimeRows_DroppedAndAudited: rows with NULL time must be dropped,
// the audit (input-null == output) must pass, and sources must still be deleted.
// Also covers the null-only path (all rows NULL => planned outputs == 0).
func TestReorg_NullTimeRows_DroppedAndAudited(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, err := storage.NewLocalBackend(tmp, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	if err := os.MkdirAll(lateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	// File 0: mix of 10 non-null + 4 NULL-time rows.
	writeLateParquet(t, db, lateDir, "events_late_20260520_030000_0.parquet", `
		SELECT TIMESTAMP '2026-04-01 12:00:00' + (i * INTERVAL 1 DAY) AS time, ('h'||i)::VARCHAR AS host, i::BIGINT AS value
		FROM range(0,10) t(i)
		UNION ALL
		SELECT NULL::TIMESTAMP AS time, ('n'||i)::VARCHAR AS host, i::BIGINT AS value
		FROM range(0,4) t(i)`)
	db.Close()

	reorg := newReorg(backend, filepath.Join(tmp, "scratch"), logger)
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run: %v", err)
	}
	// Sources deleted (drain committed) — the audit passed (10 == 14-4).
	if remaining := countParquetFlat(t, lateDir); remaining != 0 {
		t.Errorf("expected sources drained, %d remain", remaining)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	total, perDay := outputRowsByDay(t, eventsDir)
	if total != 10 {
		t.Errorf("expected 10 non-null output rows, got %d", total)
	}
	if len(perDay) != 10 {
		t.Errorf("expected 10 day partitions (one per row's day), got %d (%v)", len(perDay), perDay)
	}
}

// TestReorg_AllNullTime_NullOnlyPath: every row has NULL time. planned==0,
// audit passes (expected==0), sources deleted via the null-only branch, and
// NO output files are written.
func TestReorg_AllNullTime_NullOnlyPath(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, err := storage.NewLocalBackend(tmp, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	_ = os.MkdirAll(lateDir, 0o755)
	db, _ := sql.Open("duckdb", "")
	writeLateParquet(t, db, lateDir, "events_late_20260520_030000_0.parquet", `
		SELECT NULL::TIMESTAMP AS time, ('n'||i)::VARCHAR AS host, i::BIGINT AS value FROM range(0,7) t(i)`)
	db.Close()

	reorg := newReorg(backend, filepath.Join(tmp, "scratch"), logger)
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run: %v", err)
	}
	if remaining := countParquetFlat(t, lateDir); remaining != 0 {
		t.Errorf("null-only path must still delete sources, %d remain", remaining)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	if n := countParquetTree(t, eventsDir); n != 0 {
		t.Errorf("null-only bucket must write 0 outputs, got %d", n)
	}
}

// TestReorg_SchemaDrift_UnionByName: input files have differing column sets.
// union_by_name must merge them with no abort and no row loss.
func TestReorg_SchemaDrift_UnionByName(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, _ := storage.NewLocalBackend(tmp, logger)
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	_ = os.MkdirAll(lateDir, 0o755)
	db, _ := sql.Open("duckdb", "")
	// File 0: time, host, value (3 cols)
	writeLateParquet(t, db, lateDir, "events_late_20260520_030000_0.parquet", `
		SELECT TIMESTAMP '2026-04-01 12:00:00' + (i * INTERVAL 1 DAY) AS time, ('h'||i)::VARCHAR AS host, i::BIGINT AS value
		FROM range(0,5) t(i)`)
	// File 1: time, host, value, extra (4 cols — schema evolution)
	writeLateParquet(t, db, lateDir, "events_late_20260520_030000_1.parquet", `
		SELECT TIMESTAMP '2026-04-10 12:00:00' + (i * INTERVAL 1 DAY) AS time, ('h'||i)::VARCHAR AS host, i::BIGINT AS value, ('x'||i)::VARCHAR AS input_file_extension
		FROM range(0,5) t(i)`)
	db.Close()

	reorg := newReorg(backend, filepath.Join(tmp, "scratch"), logger)
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run (schema drift must not abort): %v", err)
	}
	if remaining := countParquetFlat(t, lateDir); remaining != 0 {
		t.Errorf("expected drained, %d remain", remaining)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	total, _ := outputRowsByDay(t, eventsDir)
	if total != 10 {
		t.Errorf("schema drift lost rows: expected 10, got %d", total)
	}
}

// TestReorg_SingleRow: degenerate single-row bucket.
func TestReorg_SingleRow(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, _ := storage.NewLocalBackend(tmp, logger)
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	_ = os.MkdirAll(lateDir, 0o755)
	db, _ := sql.Open("duckdb", "")
	writeLateParquet(t, db, lateDir, "events_late_20260520_030000_0.parquet", `
		SELECT TIMESTAMP '2026-04-01 12:34:56' AS time, 'solo'::VARCHAR AS host, 42::BIGINT AS value`)
	db.Close()
	reorg := newReorg(backend, filepath.Join(tmp, "scratch"), logger)
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run: %v", err)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	total, perDay := outputRowsByDay(t, eventsDir)
	if total != 1 || len(perDay) != 1 {
		t.Errorf("single row: expected 1 row / 1 day, got %d rows / %d days", total, len(perDay))
	}
}

// TestReorg_ManyDaysManyFiles: a single bucket spanning 400 days across several
// files. Verify lossless, no dup, no phantom, exactly one output per day, and
// the readback equals the non-null input set.
func TestReorg_ManyDaysManyFiles(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, _ := storage.NewLocalBackend(tmp, logger)
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	_ = os.MkdirAll(lateDir, 0o755)
	db, _ := sql.Open("duckdb", "")
	const numFiles = 8
	const daysPerFile = 50 // 400 distinct days total
	for f := 0; f < numFiles; f++ {
		// midday rows so TZ can't move them across a day boundary
		writeLateParquet(t, db, lateDir, fmt.Sprintf("events_late_20260520_030000_%d.parquet", f), fmt.Sprintf(`
			SELECT TIMESTAMP '2025-01-01 12:00:00' + (d * INTERVAL 1 DAY) AS time, ('h'||d)::VARCHAR AS host, d::BIGINT AS value
			FROM range(%d,%d) days(d)`, f*daysPerFile, (f+1)*daysPerFile))
	}
	db.Close()
	reorg := newReorg(backend, filepath.Join(tmp, "scratch"), logger)
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run: %v", err)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	total, perDay := outputRowsByDay(t, eventsDir)
	wantRows := int64(numFiles * daysPerFile)
	if total != wantRows {
		t.Errorf("expected %d rows, got %d", wantRows, total)
	}
	if len(perDay) != numFiles*daysPerFile {
		t.Errorf("expected %d day partitions, got %d", numFiles*daysPerFile, len(perDay))
	}
	for k, v := range perDay {
		if v != 1 {
			t.Errorf("day %s has %d rows, expected exactly 1 (dup/phantom!)", k, v)
		}
	}
}

// ============================================================================
// PROPERTY 3: Fix B — parallel-upload 2-phase commit under failure
// ============================================================================

// makeBucketManyDays writes one late file spanning `days` distinct event-days
// (midday rows, TZ-safe) into lateDir using the given duckdb handle.
func makeBucketManyDays(t *testing.T, db *sql.DB, lateDir, filename string, days int) {
	t.Helper()
	writeLateParquet(t, db, lateDir, filename, fmt.Sprintf(`
		SELECT TIMESTAMP '2026-04-01 12:00:00' + (d * INTERVAL 1 DAY) AS time, ('h'||d)::VARCHAR AS host, d::BIGINT AS value
		FROM range(0,%d) days(d)`, days))
}

// TestReorg_UploadFailure_NoCommit_ThenLosslessRetry injects a failure on the
// 3rd WriteReader of a multi-output bucket and asserts the 2-phase commit holds:
//   - reorg.Run surfaces no error at Run level (errors are logged per-bucket),
//     but the bucket did NOT commit: sources remain, manifest is NOT uploaded.
//   - the manifest is left "pending" (not "uploaded").
//   - a second run (recovery) converges losslessly with NO duplicate rows.
func TestReorg_UploadFailure_NoCommit_ThenLosslessRetry(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	local, err := storage.NewLocalBackend(tmp, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	fb := &faultBackend{LocalBackend: local, failOnWriteN: 3}

	lateDir := filepath.Join(tmp, "posthog", "events_late")
	_ = os.MkdirAll(lateDir, 0o755)
	db, _ := sql.Open("duckdb", "")
	const days = 6 // 6 outputs -> failing on the 3rd guarantees a partial set
	makeBucketManyDays(t, db, lateDir, "events_late_20260520_030000_0.parquet", days)
	db.Close()

	reorg := newReorg(fb, filepath.Join(tmp, "scratch"), logger)
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)

	// Run 1: the injected fault must abort the bucket. Run() itself logs and
	// returns nil (per-bucket errors don't bubble), so assert on the SIDE
	// EFFECTS: sources untouched, manifest still pending.
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run (run1) unexpected top-level error: %v", err)
	}
	// Sources must remain — the safety contract forbids deleting on upload fail.
	if remaining := countParquetFlat(t, lateDir); remaining != 1 {
		t.Fatalf("DATA SAFETY VIOLATION: source deleted despite upload failure (remaining=%d, want 1)", remaining)
	}
	// Manifest must exist and be pending (not uploaded).
	mm := reorg.ManifestManager
	keys, _ := mm.List(ctx)
	if len(keys) != 1 {
		t.Fatalf("expected 1 pending manifest after upload failure, got %d (%v)", len(keys), keys)
	}
	man, err := mm.Read(ctx, keys[0])
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if man.Status != ReorgStatusPending {
		t.Errorf("manifest must be pending after upload failure, got %q", man.Status)
	}

	// Disarm the fault and re-run. Recovery is age-gated (5 min) so the young
	// pending manifest is NOT auto-recovered this cycle; instead the source
	// filter (SourceFilesInManifests) skips the still-owned source — meaning no
	// progress until recovery's age gate elapses. To exercise the lossless
	// RETRY path deterministically without sleeping 5 minutes, we roll the
	// manifest back exactly as recovery's "partial upload" branch would (delete
	// partials + drop the manifest), then re-run.
	fb.failOnWriteN = 0
	// Identify and delete this attempt's partial outputs by their jobID path.
	for _, po := range man.PlannedOutputs {
		_ = fb.Delete(ctx, po.Path) // best-effort; some were never written
	}
	if err := mm.Delete(ctx, keys[0]); err != nil {
		t.Fatalf("manual rollback delete manifest: %v", err)
	}

	// Run 2: clean retry. Must be lossless with no dup.
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run (run2) error: %v", err)
	}
	if remaining := countParquetFlat(t, lateDir); remaining != 0 {
		t.Errorf("after recovery retry, expected sources drained, %d remain", remaining)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	total, perDay := outputRowsByDay(t, eventsDir)
	if total != int64(days) {
		t.Errorf("retry not lossless: expected %d rows, got %d", days, total)
	}
	for k, v := range perDay {
		if v != 1 {
			t.Errorf("DUPLICATE after retry: day %s has %d rows (want 1)", k, v)
		}
	}
}

// TestReorg_UploadFailure_RecoveryRollback drives the REAL recovery path
// (RecoverOrphanedReorgManifests) rather than a manual rollback, by aging the
// manifest's UpdatedAt past ReorgManifestMinRecoveryAge. Asserts recovery
// rolls back the partials and the next run is lossless with no dup.
func TestReorg_UploadFailure_RecoveryRollback(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	local, _ := storage.NewLocalBackend(tmp, logger)
	fb := &faultBackend{LocalBackend: local, failOnWriteN: 2}

	lateDir := filepath.Join(tmp, "posthog", "events_late")
	_ = os.MkdirAll(lateDir, 0o755)
	db, _ := sql.Open("duckdb", "")
	const days = 5
	makeBucketManyDays(t, db, lateDir, "events_late_20260520_030000_0.parquet", days)
	db.Close()

	reorg := newReorg(fb, filepath.Join(tmp, "scratch"), logger)
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)

	// Run 1 fails mid-upload.
	_ = reorg.Run(ctx)
	mm := reorg.ManifestManager
	keys, _ := mm.List(ctx)
	if len(keys) != 1 {
		t.Fatalf("expected 1 manifest, got %d", len(keys))
	}
	// Age the manifest so recovery treats it as orphaned. Rewrite with old
	// UpdatedAt by reading, mutating, and writing the JSON directly via backend.
	man, _ := mm.Read(ctx, keys[0])
	man.UpdatedAt = man.UpdatedAt.Add(-2 * ReorgManifestMinRecoveryAge)
	man.CreatedAt = man.CreatedAt.Add(-2 * ReorgManifestMinRecoveryAge)
	// Re-marshal through Write would reset UpdatedAt; instead overwrite raw.
	if err := writeRawManifest(ctx, fb, keys[0], man); err != nil {
		t.Fatalf("age manifest: %v", err)
	}

	// Snapshot how many outputs run-1 successfully uploaded BEFORE the fault.
	// With the parallel pool, writes #1,#3,#4,#5 can land before #2 fails — so
	// some target files already exist when recovery runs.
	preRecoveryWrites := fb.writes.Load()
	t.Logf("run1 uploaded %d/%d outputs before the injected fault", preRecoveryWrites, days)

	// Disarm fault, run recovery via a fresh Run (Run calls RecoverOrphaned... first).
	fb.failOnWriteN = 0
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run (recovery) error: %v", err)
	}

	// CONTRACT (documented on Reorganizer): "No data loss; bounded duplication.
	// duplicate target files survive until daily compaction's tag-dedup folds
	// them." Recovery's probeOutputs only deletes WRONG-SIZED partials — a
	// fully-uploaded output from run-1 is preserved, and the re-run writes a
	// second copy under a new jobID. So we assert the SAFETY contract, not
	// zero-dup:
	//   (1) NO DATA LOSS: every event-day present at least once.
	//   (2) DUPLICATES ARE EXACT-ROW: SELECT DISTINCT folds the partition back
	//       to exactly `days` rows (proves daily-tier dedup will converge).
	eventsDir := filepath.Join(tmp, "posthog", "events")
	total, perDay := outputRowsByDay(t, eventsDir)
	t.Logf("post-recovery raw rows=%d across %d day partitions (duplication is contract-permitted)", total, len(perDay))
	if len(perDay) != days {
		t.Errorf("DATA LOSS: expected all %d event-days present, got %d", days, len(perDay))
	}
	for k, v := range perDay {
		if v < 1 {
			t.Errorf("DATA LOSS: day %s has %d rows", k, v)
		}
	}
	// Prove the duplicates are exact-row (dedup-foldable): DISTINCT over the
	// whole tree must collapse to exactly `days` unique rows.
	distinct := distinctRowsTree(t, eventsDir)
	if distinct != int64(days) {
		t.Errorf("duplicates are NOT exact-row (dedup would NOT converge): DISTINCT=%d, want %d", distinct, days)
	}
	// Sources drained and manifest cleaned up (the bucket made forward progress).
	if remaining := countParquetFlat(t, lateDir); remaining != 0 {
		t.Errorf("expected sources drained after recovery, %d remain", remaining)
	}
	leftover, _ := mm.List(ctx)
	if len(leftover) != 0 {
		t.Errorf("expected no leftover manifests, got %d (%v)", len(leftover), leftover)
	}
}

// distinctRowsTree returns COUNT over SELECT DISTINCT * across the parquet tree
// (mirrors compaction's no-tag-metadata dedup path: buildCompactionQuery uses
// SELECT DISTINCT * when no arc:tags metadata is present).
func distinctRowsTree(t *testing.T, dir string) int64 {
	t.Helper()
	db, _ := sql.Open("duckdb", "")
	defer db.Close()
	glob := escapeSQLPath(filepath.Join(dir, "**", "*.parquet"))
	var n int64
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM (SELECT DISTINCT * FROM read_parquet('%s', union_by_name=true))`, glob)).Scan(&n); err != nil {
		t.Fatalf("distinct count: %v", err)
	}
	return n
}

// writeRawManifest overwrites a manifest object with the given struct WITHOUT
// touching UpdatedAt (unlike ManifestManager.Write). Used by tests to age a
// manifest into recovery eligibility.
func writeRawManifest(ctx context.Context, b storage.Backend, key string, m *ReorgManifest) error {
	data, err := jsonMarshalIndent(m)
	if err != nil {
		return err
	}
	return b.Write(ctx, key, data)
}

// ============================================================================
// PROPERTY 4: Crash / idempotency — fail DeleteBatch AFTER MarkUploaded
// ============================================================================

// TestReorg_CrashAfterMarkUploaded_BeforeDelete simulates a crash between
// MarkUploaded and the source delete: the manifest is "uploaded", outputs are
// live, sources still present. processBucket's DeleteBatch failure is swallowed
// (logged, returns nil) leaving the manifest for recovery. The next cycle's
// recovery must:
//   - finish the source delete (idempotent),
//   - NOT re-upload (outputs already exist; no new duplicates from THIS path),
//   - converge losslessly.
func TestReorg_CrashAfterMarkUploaded_BeforeDelete(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	local, _ := storage.NewLocalBackend(tmp, logger)
	fb := &faultBackend{LocalBackend: local}
	fb.failDeleteBatch.Store(true) // crash at the source-delete step

	lateDir := filepath.Join(tmp, "posthog", "events_late")
	_ = os.MkdirAll(lateDir, 0o755)
	db, _ := sql.Open("duckdb", "")
	const days = 4
	makeBucketManyDays(t, db, lateDir, "events_late_20260520_030000_0.parquet", days)
	db.Close()

	reorg := newReorg(fb, filepath.Join(tmp, "scratch"), logger)
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)

	// Run 1: all uploads succeed, MarkUploaded succeeds, DeleteBatch fails.
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("run1: %v", err)
	}
	// Outputs are live; sources still present (delete failed).
	if remaining := countParquetFlat(t, lateDir); remaining != 1 {
		t.Fatalf("expected source still present after delete-fail, got %d", remaining)
	}
	mm := reorg.ManifestManager
	keys, _ := mm.List(ctx)
	if len(keys) != 1 {
		t.Fatalf("expected 1 uploaded manifest, got %d", len(keys))
	}
	man, _ := mm.Read(ctx, keys[0])
	if man.Status != ReorgStatusUploaded {
		t.Errorf("manifest must be 'uploaded' after upload+mark succeed, got %q", man.Status)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	if total, _ := outputRowsByDay(t, eventsDir); total != days {
		t.Fatalf("run1 outputs wrong: got %d want %d", total, days)
	}

	// Age the manifest into recovery eligibility, disarm the delete fault.
	man.UpdatedAt = man.UpdatedAt.Add(-2 * ReorgManifestMinRecoveryAge)
	_ = writeRawManifest(ctx, fb, keys[0], man)
	fb.failDeleteBatch.Store(false)

	// Run 2: recovery sees Status=uploaded -> finishDelete (delete sources +
	// manifest). It must NOT re-process or re-upload.
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("run2 (recovery): %v", err)
	}
	if remaining := countParquetFlat(t, lateDir); remaining != 0 {
		t.Errorf("recovery must delete sources, %d remain", remaining)
	}
	if leftover, _ := mm.List(ctx); len(leftover) != 0 {
		t.Errorf("recovery must delete manifest, %d remain", len(leftover))
	}
	// Lossless and NO duplication from the uploaded-status path (no re-upload).
	total, perDay := outputRowsByDay(t, eventsDir)
	if total != days || len(perDay) != days {
		t.Errorf("after recovery: got %d rows / %d days, want %d / %d", total, len(perDay), days, days)
	}
	for k, v := range perDay {
		if v != 1 {
			t.Errorf("UNEXPECTED dup on uploaded-status recovery: day %s has %d rows", k, v)
		}
	}
}

// TestReorg_SourceFilterPreventsDoubleProcess: with a young (not-yet-recoverable)
// uploaded manifest still owning the sources, a re-run must NOT re-read those
// sources (SourceFilesInManifests filter) — otherwise it would write a duplicate
// set into the already-uploaded partitions.
func TestReorg_SourceFilterPreventsDoubleProcess(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	local, _ := storage.NewLocalBackend(tmp, logger)
	fb := &faultBackend{LocalBackend: local}
	fb.failDeleteBatch.Store(true)

	lateDir := filepath.Join(tmp, "posthog", "events_late")
	_ = os.MkdirAll(lateDir, 0o755)
	db, _ := sql.Open("duckdb", "")
	const days = 3
	makeBucketManyDays(t, db, lateDir, "events_late_20260520_030000_0.parquet", days)
	db.Close()

	reorg := newReorg(fb, filepath.Join(tmp, "scratch"), logger)
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)

	// Run 1: uploaded, delete fails, manifest left (YOUNG -> not recoverable yet).
	_ = reorg.Run(ctx)
	eventsDir := filepath.Join(tmp, "posthog", "events")
	total1, _ := outputRowsByDay(t, eventsDir)

	// Run 2 immediately (manifest still young): recovery skips it (too young),
	// and the source filter must skip the still-owned source -> NO new outputs.
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("run2: %v", err)
	}
	total2, perDay := outputRowsByDay(t, eventsDir)
	if total2 != total1 {
		t.Errorf("source filter failed: re-run produced duplicates (%d -> %d rows)", total1, total2)
	}
	for k, v := range perDay {
		if v != 1 {
			t.Errorf("duplicate from double-process: day %s has %d rows", k, v)
		}
	}
}

// ============================================================================
// PROPERTY 5: Case-4 for DAILY reorg files (the reconciliation)
// ============================================================================

// writeParquetAt writes a parquet at the given storage key (relative to tmp
// root) by writing to a local path via DuckDB then placing it. Returns nothing.
func writeParquetAt(t *testing.T, tmpRoot, key, selectBody string) {
	t.Helper()
	full := filepath.Join(tmpRoot, key)
	_ = os.MkdirAll(filepath.Dir(full), 0o755)
	db, _ := sql.Open("duckdb", "")
	defer db.Close()
	q := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET)`, selectBody, escapeSQLPath(full))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write parquet %s: %v", key, err)
	}
}

// TestCase4_RealCompaction_FoldsDuplicateReorgFiles drives a REAL daily Job over
// a partition holding a sealed _daily file + TWO identical _reorg_ files
// (crash-induced exact-row duplicate). It asserts: ShouldCompact fires (Case 4),
// the compaction folds the duplicate so the final partition has NO double-count,
// and the output carries no _reorg_ marker (self-terminating).
func TestCase4_RealCompaction_FoldsDuplicateReorgFiles(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, _ := storage.NewLocalBackend(tmp, logger)

	// Partition posthog/events/2026/02/18 with:
	//   - a sealed _daily file (3 rows: days... actually rows within the day)
	//   - two IDENTICAL reorg files (2 rows each, same content) = the crash dup.
	dayPrefix := "posthog/events/2026/02/18"
	dailyBody := `SELECT TIMESTAMP '2026-02-18 01:00:00' + (i*INTERVAL 1 HOUR) AS time, ('h'||i)::VARCHAR AS host, i::BIGINT AS value FROM range(0,3) t(i)`
	reorgBody := `SELECT TIMESTAMP '2026-02-18 10:00:00' + (i*INTERVAL 1 HOUR) AS time, ('r'||i)::VARCHAR AS host, (100+i)::BIGINT AS value FROM range(0,2) t(i)`
	writeParquetAt(t, tmp, dayPrefix+"/events_20260218_daily.parquet", dailyBody)
	writeParquetAt(t, tmp, dayPrefix+"/events_reorg_1717252801111_0.parquet", reorgBody)
	writeParquetAt(t, tmp, dayPrefix+"/events_reorg_1717252899222_0.parquet", reorgBody) // identical dup

	// Sanity: raw rows = 3 + 2 + 2 = 7; distinct = 3 + 2 = 5.
	eventsDir := filepath.Join(tmp, "posthog", "events")
	if raw, _ := outputRowsByDay(t, eventsDir); raw != 7 {
		t.Fatalf("setup: expected 7 raw rows, got %d", raw)
	}

	// Build a daily tier and confirm Case 4 fires despite file count (3) < MinFiles (12).
	tier := NewDailyTier(&DailyTierConfig{
		StorageBackend: backend,
		MinFiles:       12,
		Enabled:        true,
		Logger:         logger,
	})
	files := []string{
		dayPrefix + "/events_20260218_daily.parquet",
		dayPrefix + "/events_reorg_1717252801111_0.parquet",
		dayPrefix + "/events_reorg_1717252899222_0.parquet",
	}
	if !tier.ShouldCompact(files, time.Date(2026, 2, 18, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("Case 4 did NOT fire: reorg files on sealed partition under MinFiles must trigger compaction")
	}

	// Now run a REAL daily compaction Job over the partition.
	shared, _ := sql.Open("duckdb", "")
	defer shared.Close()
	job := NewJob(&JobConfig{
		Measurement:    "events",
		PartitionPath:  dayPrefix,
		Files:          files,
		StorageBackend: backend,
		Database:       "posthog",
		Tier:           "daily",
		TempDirectory:  filepath.Join(tmp, "compact-scratch"),
		SortKeys:       []string{"time"},
		Logger:         logger,
		DB:             shared,
	})
	if err := job.Run(ctx); err != nil {
		t.Fatalf("daily compaction Job.Run: %v", err)
	}

	// After compaction: the sources are deleted, a single _daily output remains,
	// and the dup is folded -> exactly 5 distinct rows, 5 total (NO double-count).
	total, _ := outputRowsByDay(t, eventsDir)
	if total != 5 {
		t.Errorf("DOUBLE-COUNT: expected 5 rows after dedup-fold, got %d", total)
	}
	// Self-termination: no _reorg_ file remains, output is _daily.
	var reorgLeft, dailyOut int
	_ = filepath.Walk(eventsDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(p) != ".parquet" {
			return nil
		}
		if isReorgFile(p) {
			reorgLeft++
		}
		if strings.HasSuffix(p, "_daily.parquet") {
			dailyOut++
		}
		return nil
	})
	if reorgLeft != 0 {
		t.Errorf("self-termination broken: %d _reorg_ files remain after fold", reorgLeft)
	}
	if dailyOut != 1 {
		t.Errorf("expected exactly 1 _daily output, got %d", dailyOut)
	}

	// And the folded output must NOT re-trigger Case 4 (no reorg marker left).
	remaining := []string{}
	objs, _ := backend.List(ctx, dayPrefix+"/")
	remaining = append(remaining, objs...)
	if tier.ShouldCompact(remaining, time.Date(2026, 2, 18, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("LOOP RISK: compacted output (no _reorg_) re-triggered Case 4: files=%v", remaining)
	}
}

// TestCase4_Narrowness_OrdinaryLateFileDoesNotTrigger: an ordinary sub-MinFiles
// late hourly file (NOT a reorg file) next to a sealed daily file must NOT
// trigger Case 4 — it waits for MinFiles like before.
func TestCase4_Narrowness_OrdinaryLateFileDoesNotTrigger(t *testing.T) {
	logger := zerolog.Nop()
	tier := NewDailyTier(&DailyTierConfig{MinFiles: 12, Enabled: true, Logger: logger})
	files := []string{
		"posthog/events/2026/02/18/events_20260218_daily.parquet",
		// ordinary 7-part hourly late file (valid input, but not reorg)
		"posthog/events/2026/02/18/14/events_20260218_141500_222.parquet",
	}
	if tier.ShouldCompact(files, time.Date(2026, 2, 18, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("Case 4 over-fired: ordinary sub-MinFiles late file must NOT trigger immediate fold")
	}
}

// TestCase4_Discovery_FindsReorgOnlyPartition verifies the daily tier's
// candidate DISCOVERY actually surfaces a partition whose only "new" file is a
// reorg file. The freshness filter (extractNewestFileTime) cannot parse a reorg
// filename, so it returns zero time and does NOT exclude the partition. Uses the
// flat (no-cache) discovery path (tier built without a PartitionCache).
func TestCase4_Discovery_FindsReorgOnlyPartition(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, _ := storage.NewLocalBackend(tmp, logger)

	// An OLD day partition (well past MinAgeHours and SkipFileAgeCheckDays) with
	// a sealed _daily file and a single late reorg file.
	dayPrefix := "posthog/events/2025/01/10"
	writeParquetAt(t, tmp, dayPrefix+"/events_20250110_daily.parquet",
		`SELECT TIMESTAMP '2025-01-10 01:00:00' AS time, 'a'::VARCHAR AS host, 1::BIGINT AS value`)
	writeParquetAt(t, tmp, dayPrefix+"/events_reorg_1717252801111_0.parquet",
		`SELECT TIMESTAMP '2025-01-10 10:00:00' AS time, 'r'::VARCHAR AS host, 100::BIGINT AS value`)

	tier := NewDailyTier(&DailyTierConfig{
		StorageBackend: backend,
		MinFiles:       12,
		Enabled:        true,
		Logger:         logger,
	})
	cands, err := tier.FindCandidates(ctx, "posthog", "events")
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	found := false
	for _, c := range cands {
		if strings.Contains(c.PartitionPath, "2025/01/10") {
			found = true
		}
	}
	if !found {
		t.Errorf("DISCOVERY GAP: daily tier did not surface the reorg-only partition as a candidate (cands=%+v)", cands)
	}
}

// ============================================================================
// PROPERTY 6: A — memory/scale (single-glob day COPY on a large bucket)
// ============================================================================

// TestReorg_Scale_5000FilesAcross300Days confirms the single-glob, day-
// partitioned COPY completes and is lossless on a large bucket: 5,000 tiny
// source files whose rows span 300 distinct event-days. Day partitioning keeps
// the open-partition-writer count bounded (<=300) vs the old hourly path
// (<=300*24=7200). Marked with a generous body but skipped under -short.
func TestReorg_Scale_5000FilesAcross300Days(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test; skipped under -short")
	}
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, _ := storage.NewLocalBackend(tmp, logger)
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	_ = os.MkdirAll(lateDir, 0o755)

	const numFiles = 5000
	const numDays = 300
	// Each file holds 1 row at a day determined by its index (round-robin over
	// 300 days, midday). Total rows = numFiles; distinct days = numDays.
	db, _ := sql.Open("duckdb", "")
	for f := 0; f < numFiles; f++ {
		day := f % numDays
		writeLateParquet(t, db, lateDir, fmt.Sprintf("events_late_20260520_030000_%d.parquet", f), fmt.Sprintf(`
			SELECT TIMESTAMP '2025-01-01 12:00:00' + (%d * INTERVAL 1 DAY) AS time, ('h'||%d)::VARCHAR AS host, %d::BIGINT AS value`,
			day, f, f))
	}
	db.Close()
	if got := countParquetFlat(t, lateDir); got != numFiles {
		t.Fatalf("setup: expected %d files, got %d", numFiles, got)
	}

	reorg := newReorg(backend, filepath.Join(tmp, "scratch"), logger)
	reorg.DownloadWorkers = 16
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("large-bucket reorg.Run: %v", err)
	}

	// Drained.
	if remaining := countParquetFlat(t, lateDir); remaining != 0 {
		t.Errorf("expected events_late drained, %d remain", remaining)
	}
	// Lossless: exactly numFiles rows; exactly numDays distinct day partitions.
	eventsDir := filepath.Join(tmp, "posthog", "events")
	total, perDay := outputRowsByDay(t, eventsDir)
	if total != int64(numFiles) {
		t.Errorf("scale loss/dup: expected %d rows, got %d", numFiles, total)
	}
	if len(perDay) != numDays {
		t.Errorf("expected %d distinct day partitions, got %d", numDays, len(perDay))
	}
	// NOTE: DuckDB's parallel PARTITION_BY can emit MORE than one file per day
	// partition at scale (each writer thread flushes its own file per open
	// partition; the open-partition-writer count is bounded by day, not hour).
	// So we do NOT assert exactly numDays files — we assert the output is
	// (a) bounded well below the OLD hourly worst case (numDays*24), and
	// (b) every output file lives in a valid Y/M/D day dir (6-part key, no
	// stray hour level). The downstream Case-4 daily compaction then folds the
	// per-day fragments into one _daily file.
	outFiles := countParquetTree(t, eventsDir)
	hourlyWorstCase := numDays * 24
	if outFiles >= hourlyWorstCase {
		t.Errorf("day partitioning did not reduce output count: got %d, hourly worst-case was %d", outFiles, hourlyWorstCase)
	}
	// Every output must be at day granularity (db/meas/Y/M/D/file = 6 parts),
	// never an hour subdir.
	_ = filepath.Walk(eventsDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(p) != ".parquet" {
			return nil
		}
		rel, _ := filepath.Rel(tmp, p) // tmp is the storage root
		if n := strings.Count(rel, string(os.PathSeparator)); n != 5 {
			t.Errorf("output not at day granularity: %s has %d path separators (want 5 = db/meas/Y/M/D/file)", rel, n)
		}
		return nil
	})
	t.Logf("SCALE: %d source files (300 days) -> %d day-granularity outputs (hourly worst-case %d), %d rows, lossless",
		numFiles, outFiles, hourlyWorstCase, total)
}
