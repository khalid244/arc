package rollup

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// BatchResult is returned by BuildWindowBatch. PerSpec is keyed by spec.Name.
// Success/failure are per-spec: the scheduler advances watermarks only for
// specs whose entry has OK=true; the rest keep their manifests for Recover().
type BatchResult struct {
	PerSpec map[string]SpecOutcome
}

// SpecOutcome captures a single spec's result inside a batched build.
type SpecOutcome struct {
	OK           bool
	OutputKey    string
	BytesWritten int
	Err          string
}

// Builder runs one window's aggregation and persists the result.
type Builder struct {
	db      *sql.DB
	backend storage.Backend
	wmStore WMWriter
	logger  zerolog.Logger

	// InProcess controls whether BuildWindow runs DuckDB in-process (true)
	// or spawns an isolated subprocess (false, the production default).
	// Set to true in tests to avoid depending on the compiled binary.
	InProcess bool

	// MemoryLimit, when non-empty, is passed to the rollup-build subprocess
	// which applies `SET memory_limit = '<value>'` on its DuckDB. Without
	// it, the subprocess auto-detects from the host (ignoring cgroup limits)
	// and frequently OOM-kills the pod alongside the parent.
	MemoryLimit string

	// ThreadCount, when > 0, is passed to the rollup-build subprocess which
	// applies `SET threads = N` on its DuckDB. Without it, DuckDB picks the
	// host's nproc — on a 2-core pod sharing a 12-CPU node that means 12
	// threads competing for a 2-core CFS quota, which throttles aggregation
	// to near-serial. Set this to the pod's cgroup CPU limit.
	ThreadCount int

	manifests *ManifestStore
}

// NewBuilder wires a Builder. db is a *sql.DB pointing at DuckDB with the
// datasketches extension already loaded (handled by internal/database/duckdb.go).
// backend is the same storage backend ingest uses; rollup data lands there.
// wm can be a *WatermarkStore or a *WatermarkCache (which writes through
// to the backing store and also invalidates its read cache).
func NewBuilder(db *sql.DB, backend storage.Backend, wm WMWriter, logger zerolog.Logger) *Builder {
	return &Builder{
		db:        db,
		backend:   backend,
		wmStore:   wm,
		logger:    logger.With().Str("component", "rollup-builder").Logger(),
		manifests: NewManifestStore(backend, logger),
	}
}

// BuildWindow runs one [windowStart, windowEnd) build for spec, reading from
// fromTable. The caller (typically Scheduler) is responsible for choosing what
// fromTable should be: a bare DuckDB table name in tests, or a read_parquet(...)
// expression in production. Result is written to a deterministic key on the
// storage backend; same key is overwritten safely on rebuild (idempotent).
//
// Crash safety protocol (Phase 1 + 2):
//  1. Write a WindowManifest to storage BEFORE the build.
//  2. Run the build (in-process or subprocess depending on InProcess).
//  3. Advance the watermark.
//  4. Delete the manifest.
//
// If Arc dies between steps 1 and 4, Recover() will finish or clean up on restart.
func (b *Builder) BuildWindow(ctx context.Context, spec RollupSpec, fromTable string, windowStart, windowEnd time.Time) error {
	relKey := windowParquetPath(spec, windowStart, windowEnd)
	storagePath := spec.StoragePath()

	// Step 1: write crash-recovery manifest.
	manifest := WindowManifest{
		RollupName:  spec.Name,
		StoragePath: storagePath,
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		OutputKey:   relKey,
		CreatedAt:   time.Now().UTC(),
	}
	if err := b.manifests.Write(ctx, manifest); err != nil {
		return fmt.Errorf("write window manifest: %w", err)
	}

	b.logger.Debug().
		Str("rollup", spec.Name).
		Time("start", windowStart).
		Time("end", windowEnd).
		Str("key", relKey).
		Bool("in_process", b.InProcess).
		Msg("building rollup window")

	started := time.Now()

	// Step 2: run the build.
	var buildErr error
	if b.InProcess {
		buildErr = b.buildInProcess(ctx, spec, fromTable, windowStart, windowEnd, relKey)
	} else {
		buildErr = b.buildSubprocess(ctx, spec, fromTable, windowStart, windowEnd, relKey)
	}
	if buildErr != nil {
		// Manifest remains; Recover() will clean it up on next restart.
		return buildErr
	}

	// Step 3: advance watermark. Stamp the current spec fingerprint so the
	// scheduler's drift check can detect future shape changes.
	wm := Watermark{
		Rollup:               spec.Name,
		StoragePath:          storagePath,
		BucketInterval:       spec.BucketInterval,
		Watermark:            windowEnd,
		LastBuildCompletedAt: time.Now().UTC(),
		LastBuildWindowStart: windowStart,
		LastBuildWindowEnd:   windowEnd,
		SpecFingerprint:      spec.Fingerprint(),
	}
	if err := b.wmStore.Put(ctx, wm); err != nil {
		// Manifest remains; Recover() will re-advance watermark on next restart.
		return fmt.Errorf("update watermark: %w", err)
	}

	// Step 4: remove manifest (best-effort — not-found is fine).
	if err := b.manifests.Delete(ctx, storagePath, windowStart, windowEnd); err != nil {
		b.logger.Warn().Err(err).Str("rollup", spec.Name).Msg("failed to delete window manifest after build (non-fatal)")
	}

	b.logger.Info().
		Str("rollup", spec.Name).
		Str("key", relKey).
		Dur("duration", time.Since(started)).
		Msg("rollup window built")
	return nil
}

// BuildWindowBatch runs a single subprocess (or in-process build) that reads
// fromTable ONCE and emits one parquet per spec in `specs`. All specs MUST
// share fromTable, windowStart, windowEnd, and BucketColumn (caller's job
// to group them).
//
// Crash protocol mirrors BuildWindow's per-spec contract but multiplied by N:
//  1. Write N WindowManifests (one per spec) BEFORE the build.
//  2. Run the build, capturing per-spec success/failure.
//  3. For each spec that succeeded: advance its watermark, delete its manifest.
//  4. Specs that failed: leave their manifest for Recover() to clean up on
//     the next pod start. The scheduler will retry them on the next tick.
//
// Single-spec batches are valid and equivalent to BuildWindow.
func (b *Builder) BuildWindowBatch(
	ctx context.Context,
	specs []RollupSpec,
	fromTable string,
	windowStart, windowEnd time.Time,
) (BatchResult, error) {
	res := BatchResult{PerSpec: make(map[string]SpecOutcome, len(specs))}
	if len(specs) == 0 {
		return res, fmt.Errorf("BuildWindowBatch: empty specs")
	}

	// Step 1: write a manifest per spec.
	relKeys := make(map[string]string, len(specs))
	for _, spec := range specs {
		relKey := windowParquetPath(spec, windowStart, windowEnd)
		relKeys[spec.Name] = relKey
		manifest := WindowManifest{
			RollupName:  spec.Name,
			StoragePath: spec.StoragePath(),
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
			OutputKey:   relKey,
			CreatedAt:   time.Now().UTC(),
		}
		if err := b.manifests.Write(ctx, manifest); err != nil {
			// If we cannot write a manifest we cannot crash-recover this spec.
			// Mark it failed and continue with the rest of the batch (so the
			// shared scan still pays for itself across the remaining specs).
			res.PerSpec[spec.Name] = SpecOutcome{OK: false, OutputKey: relKey, Err: fmt.Sprintf("write window manifest: %v", err)}
			b.logger.Warn().Err(err).Str("rollup", spec.Name).Msg("failed to write window manifest; skipping spec")
		}
	}

	// Filter out specs whose manifest write failed.
	runnable := make([]RollupSpec, 0, len(specs))
	for _, spec := range specs {
		if _, failed := res.PerSpec[spec.Name]; !failed {
			runnable = append(runnable, spec)
		}
	}
	if len(runnable) == 0 {
		return res, nil
	}

	started := time.Now()

	// Step 2: run the build.
	var perSpec map[string]SpecSubprocessOutcome
	var buildErr error
	if b.InProcess {
		perSpec, buildErr = b.buildBatchInProcess(ctx, runnable, fromTable, windowStart, windowEnd, relKeys)
	} else {
		perSpec, buildErr = b.buildBatchSubprocess(ctx, runnable, fromTable, windowStart, windowEnd, relKeys)
	}
	if buildErr != nil {
		// Fatal error: no spec made it through. Manifests for runnable specs
		// remain on disk so Recover() can clean them up on next start.
		for _, spec := range runnable {
			res.PerSpec[spec.Name] = SpecOutcome{OK: false, OutputKey: relKeys[spec.Name], Err: buildErr.Error()}
		}
		return res, buildErr
	}

	// Step 3 + 4: advance watermarks and delete manifests for successes.
	for _, spec := range runnable {
		out, ok := perSpec[spec.Name]
		if !ok || !out.Success {
			errMsg := ""
			if ok {
				errMsg = out.Error
			} else {
				errMsg = "subprocess returned no outcome for spec"
			}
			res.PerSpec[spec.Name] = SpecOutcome{OK: false, OutputKey: relKeys[spec.Name], Err: errMsg}
			b.logger.Warn().Str("rollup", spec.Name).Str("error", errMsg).Msg("rollup spec failed in batch")
			continue
		}

		wm := Watermark{
			Rollup:               spec.Name,
			StoragePath:          spec.StoragePath(),
			BucketInterval:       spec.BucketInterval,
			Watermark:            windowEnd,
			LastBuildCompletedAt: time.Now().UTC(),
			LastBuildWindowStart: windowStart,
			LastBuildWindowEnd:   windowEnd,
			SpecFingerprint:      spec.Fingerprint(),
		}
		if err := b.wmStore.Put(ctx, wm); err != nil {
			// Manifest still on disk; Recover() will re-advance on restart.
			res.PerSpec[spec.Name] = SpecOutcome{OK: false, OutputKey: relKeys[spec.Name], BytesWritten: out.BytesWritten, Err: fmt.Sprintf("update watermark: %v", err)}
			b.logger.Warn().Err(err).Str("rollup", spec.Name).Msg("failed to advance watermark; leaving manifest for recovery")
			continue
		}

		if err := b.manifests.Delete(ctx, spec.StoragePath(), windowStart, windowEnd); err != nil {
			// Output is in place and watermark advanced; stale manifest is
			// harmless (Recover() handles it). Just warn.
			b.logger.Warn().Err(err).Str("rollup", spec.Name).Msg("failed to delete window manifest after batch build (non-fatal)")
		}

		res.PerSpec[spec.Name] = SpecOutcome{
			OK:           true,
			OutputKey:    relKeys[spec.Name],
			BytesWritten: out.BytesWritten,
		}
	}

	b.logger.Info().
		Int("specs", len(runnable)).
		Time("window_start", windowStart).
		Time("window_end", windowEnd).
		Dur("duration", time.Since(started)).
		Msg("rollup batch built")
	return res, nil
}

// buildBatchInProcess runs the shared-scan path in the current process.
// Used by tests; production goes through buildBatchSubprocess.
func (b *Builder) buildBatchInProcess(
	ctx context.Context,
	specs []RollupSpec,
	fromTable string,
	windowStart, windowEnd time.Time,
	relKeys map[string]string,
) (map[string]SpecSubprocessOutcome, error) {
	bucketCol := specs[0].BucketColumn
	for i, s := range specs {
		if s.BucketColumn != bucketCol {
			return nil, fmt.Errorf("batched specs disagree on bucket_column: %q vs %q (at %d)", bucketCol, s.BucketColumn, i)
		}
	}

	tmpDir, err := os.MkdirTemp("", "arc-rollup-batch-")
	if err != nil {
		return nil, fmt.Errorf("temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	createStmt := fmt.Sprintf(
		"CREATE TEMP TABLE day_src AS SELECT * FROM %s WHERE %s >= TIMESTAMP '%s' AND %s < TIMESTAMP '%s'",
		fromTable,
		bucketCol,
		windowStart.UTC().Format("2006-01-02 15:04:05"),
		bucketCol,
		windowEnd.UTC().Format("2006-01-02 15:04:05"),
	)
	if _, err := b.db.ExecContext(ctx, createStmt); err != nil {
		return nil, fmt.Errorf("create temp table: %w", err)
	}
	defer func() { _, _ = b.db.Exec("DROP TABLE IF EXISTS day_src") }()

	outcomes := make(map[string]SpecSubprocessOutcome, len(specs))
	for _, spec := range specs {
		relKey := relKeys[spec.Name]
		selectSQL, err := BuildWindowSQL(spec, "day_src", windowStart, windowEnd)
		if err != nil {
			outcomes[spec.Name] = SpecSubprocessOutcome{Success: false, Error: err.Error()}
			continue
		}
		safeName := strings.ReplaceAll(spec.Name, "/", "_")
		tmpFile := filepath.Join(tmpDir, "window_"+safeName+".parquet")
		copyStmt := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", selectSQL, strings.ReplaceAll(tmpFile, "'", "''"))
		if _, err := b.db.ExecContext(ctx, copyStmt); err != nil {
			outcomes[spec.Name] = SpecSubprocessOutcome{Success: false, Error: err.Error()}
			continue
		}
		data, err := os.ReadFile(tmpFile)
		if err != nil {
			outcomes[spec.Name] = SpecSubprocessOutcome{Success: false, Error: err.Error()}
			continue
		}
		if err := b.backend.Write(ctx, relKey, data); err != nil {
			outcomes[spec.Name] = SpecSubprocessOutcome{Success: false, Error: err.Error()}
			continue
		}
		outcomes[spec.Name] = SpecSubprocessOutcome{Success: true, BytesWritten: len(data)}
	}
	return outcomes, nil
}

// buildBatchSubprocess sends a SubprocessConfig with SpecBatch populated to
// the rollup-build subprocess and returns the per-spec outcomes from its
// SubprocessResult.PerSpec map.
func (b *Builder) buildBatchSubprocess(
	ctx context.Context,
	specs []RollupSpec,
	fromTable string,
	windowStart, windowEnd time.Time,
	relKeys map[string]string,
) (map[string]SpecSubprocessOutcome, error) {
	batch := make([]BatchedSpec, 0, len(specs))
	for _, spec := range specs {
		batch = append(batch, BatchedSpec{Spec: spec, OutputKey: relKeys[spec.Name]})
	}
	cfg := &SubprocessConfig{
		SpecBatch:     batch,
		FromTable:     fromTable,
		WindowStart:   windowStart,
		WindowEnd:     windowEnd,
		StorageType:   b.backend.Type(),
		StorageConfig: b.backend.ConfigJSON(),
		MemoryLimit:   b.MemoryLimit,
		ThreadCount:   b.ThreadCount,
	}
	result, err := RunBuildSubprocess(ctx, cfg, b.logger)
	if err != nil {
		return nil, fmt.Errorf("rollup batch subprocess: %w", err)
	}
	if !result.Success {
		return nil, fmt.Errorf("rollup batch subprocess reported failure: %s", result.Error)
	}
	if result.PerSpec == nil {
		return nil, fmt.Errorf("rollup batch subprocess returned no per-spec outcomes")
	}
	return result.PerSpec, nil
}

// buildInProcess runs DuckDB COPY and backend upload in the current process.
// Used by tests (InProcess: true) and as a fallback when the binary is unavailable.
func (b *Builder) buildInProcess(ctx context.Context, spec RollupSpec, fromTable string, windowStart, windowEnd time.Time, relKey string) error {
	selectSQL, err := BuildWindowSQL(spec, fromTable, windowStart, windowEnd)
	if err != nil {
		return fmt.Errorf("build sql: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "arc-rollup-")
	if err != nil {
		return fmt.Errorf("temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	tmpFile := filepath.Join(tmpDir, "window.parquet")

	copyStmt := fmt.Sprintf(
		"COPY (%s) TO '%s' (FORMAT PARQUET)",
		selectSQL,
		strings.ReplaceAll(tmpFile, "'", "''"),
	)

	if _, err := b.db.ExecContext(ctx, copyStmt); err != nil {
		return fmt.Errorf("execute copy: %w", err)
	}

	data, err := os.ReadFile(tmpFile)
	if err != nil {
		return fmt.Errorf("read temp parquet: %w", err)
	}
	if err := b.backend.Write(ctx, relKey, data); err != nil {
		return fmt.Errorf("write to backend: %w", err)
	}
	return nil
}

// buildSubprocess serializes the build config and runs it in an isolated child
// process to contain any cgo crashes from DuckDB's datasketches aggregator.
func (b *Builder) buildSubprocess(ctx context.Context, spec RollupSpec, fromTable string, windowStart, windowEnd time.Time, relKey string) error {
	cfg := &SubprocessConfig{
		Spec:          spec,
		FromTable:     fromTable,
		WindowStart:   windowStart,
		WindowEnd:     windowEnd,
		OutputKey:     relKey,
		StorageType:   b.backend.Type(),
		StorageConfig: b.backend.ConfigJSON(),
		MemoryLimit:   b.MemoryLimit,
		ThreadCount:   b.ThreadCount,
	}

	result, err := RunBuildSubprocess(ctx, cfg, b.logger)
	if err != nil {
		return fmt.Errorf("rollup build subprocess: %w", err)
	}
	if !result.Success {
		return fmt.Errorf("rollup build subprocess reported failure: %s", result.Error)
	}
	return nil
}

// windowParquetPath returns the deterministic relative key for a window.
// Format: _arc/rollup/<db>/<source_table>/<kind>/[<dim>/]<interval>/dt=YYYY-MM-DD/window_*.parquet
func windowParquetPath(spec RollupSpec, windowStart, windowEnd time.Time) string {
	day := windowStart.UTC().Format("2006-01-02")
	startStr := windowStart.UTC().Format("20060102-150405")
	endStr := windowEnd.UTC().Format("150405")
	return fmt.Sprintf("_arc/rollup/%s/dt=%s/window_%s-%s.parquet",
		spec.StoragePath(), day, startStr, endStr)
}

// ReadParquetFromTable returns a read_parquet(...) expression for spec's
// source table, anchored at the given storage backend. Production-side
// resolver; tests can pass a bare table name instead.
//
// Deprecated: use ReadParquetFromTableWindow so rollup builds scope their
// read_parquet glob to the window's day/hour partition. The full-bucket
// `**/*.parquet` glob this returns forces DuckDB to LIST every key in the
// source table prefix (~tens of thousands of objects in production) for
// every window build, which made historical backfills take ~90 min per
// day. Kept for tests that bypass the windowed path.
func ReadParquetFromTable(backend storage.Backend, spec RollupSpec) string {
	path := storage.GetStoragePath(backend, spec.Database, spec.SourceTable)
	return fmt.Sprintf("read_parquet('%s', union_by_name=true)",
		strings.ReplaceAll(path, "'", "''"))
}

// ReadParquetFromTableWindow returns a read_parquet(...) expression scoped
// to the partition path the window's start falls in. For BucketInterval ≥
// 24h, returns the day-level glob (<db>/<table>/YYYY/MM/DD/**/*.parquet);
// for sub-day intervals, returns the hour-level glob. Arc's storage layout
// is YYYY/MM/DD/HH/*.parquet — and historical compaction also writes
// YYYY/MM/DD/*_daily.parquet at the day level — so the day-level glob
// covers both shapes.
func ReadParquetFromTableWindow(backend storage.Backend, spec RollupSpec, windowStart time.Time) string {
	t := windowStart.UTC()
	var partitionKey string
	if spec.BucketInterval >= 24*time.Hour {
		partitionKey = fmt.Sprintf("%s/%s/%04d/%02d/%02d",
			spec.Database, spec.SourceTable, t.Year(), int(t.Month()), t.Day())
	} else {
		partitionKey = fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d",
			spec.Database, spec.SourceTable, t.Year(), int(t.Month()), t.Day(), t.Hour())
	}
	glob := windowedGlob(backend, partitionKey, spec.BucketInterval >= 24*time.Hour)
	return fmt.Sprintf("read_parquet('%s', union_by_name=true)",
		strings.ReplaceAll(glob, "'", "''"))
}

// windowedGlob returns the full read_parquet path for a partition prefix.
// dayLevel=true uses recursive `**/*.parquet` so both `HH/*.parquet` and
// day-level `*_daily.parquet` files match; dayLevel=false uses a single
// `*.parquet` glob suitable for an hour-level directory.
func windowedGlob(backend storage.Backend, partitionKey string, dayLevel bool) string {
	switch b := unwrapBackendForResolver(backend).(type) {
	case *storage.S3Backend:
		base := "s3://" + b.GetBucket() + "/" + b.GetPrefix() + partitionKey
		if dayLevel {
			return base + "/**/*.parquet"
		}
		return base + "/*.parquet"
	case *storage.LocalBackend:
		base := b.GetBasePath() + "/" + partitionKey
		if dayLevel {
			return base + "/**/*.parquet"
		}
		return base + "/*.parquet"
	default:
		if dayLevel {
			return "./data/" + partitionKey + "/**/*.parquet"
		}
		return "./data/" + partitionKey + "/*.parquet"
	}
}

// unwrapBackendForResolver mirrors storage.unwrapBackend (which is package-private).
// Returns the concrete backend, unwrapping ResilientBackend.
func unwrapBackendForResolver(backend storage.Backend) storage.Backend {
	if rb, ok := backend.(*storage.ResilientBackend); ok {
		return rb.Unwrap()
	}
	return backend
}
