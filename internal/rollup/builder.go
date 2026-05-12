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

	// Step 1: write crash-recovery manifest.
	manifest := WindowManifest{
		RollupName:  spec.Name,
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
	if err := b.manifests.Delete(ctx, spec.Name, windowStart, windowEnd); err != nil {
		b.logger.Warn().Err(err).Str("rollup", spec.Name).Msg("failed to delete window manifest after build (non-fatal)")
	}

	b.logger.Info().
		Str("rollup", spec.Name).
		Str("key", relKey).
		Dur("duration", time.Since(started)).
		Msg("rollup window built")
	return nil
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
// Format: <database>/<rollup_table>/dt=YYYY-MM-DD/window_YYYYMMDD-HHMMSS-HHMMSS.parquet
func windowParquetPath(spec RollupSpec, windowStart, windowEnd time.Time) string {
	day := windowStart.UTC().Format("2006-01-02")
	startStr := windowStart.UTC().Format("20060102-150405")
	endStr := windowEnd.UTC().Format("150405")
	return fmt.Sprintf("%s/%s/dt=%s/window_%s-%s.parquet",
		spec.Database, spec.RollupTableName(), day, startStr, endStr)
}

// ReadParquetFromTable returns a read_parquet(...) expression for spec's
// source table, anchored at the given storage backend. Production-side
// resolver; tests can pass a bare table name instead.
func ReadParquetFromTable(backend storage.Backend, spec RollupSpec) string {
	path := storage.GetStoragePath(backend, spec.Database, spec.SourceTable)
	return fmt.Sprintf("read_parquet('%s', union_by_name=true)",
		strings.ReplaceAll(path, "'", "''"))
}
