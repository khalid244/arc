package rollup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestRunBuildJob_InProcess exercises RunBuildJob directly (no subprocess exec)
// using a local backend and an in-process DuckDB. This is the canonical way to
// test the build logic without needing the compiled binary.
func TestRunBuildJob_InProcess(t *testing.T) {
	// datasketches is needed for sketch aggregations; skip gracefully if unavailable.
	_ = openDuckDBWithDataSketches(t)

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}

	// Build a small parquet source file via DuckDB that the subprocess will read.
	srcDB := openDuckDBWithDataSketches(t)
	defer srcDB.Close()
	mustExec(t, srcDB, `CREATE TABLE events (ts TIMESTAMP, service VARCHAR, v DOUBLE)`)
	mustExec(t, srcDB, `INSERT INTO events VALUES
		('2026-05-10 12:00:00', 'api', 10),
		('2026-05-10 12:30:00', 'api', 20),
		('2026-05-10 12:55:00', 'web', 30)`)

	// Export to parquet so the subprocess can read it.
	srcParquet := filepath.Join(dir, "src.parquet")
	mustExec(t, srcDB, `COPY events TO '`+srcParquet+`' (FORMAT PARQUET)`)

	spec := RollupSpec{
		Name:           "events__1h",
		Database:       "main",
		SourceTable:    "events",
		BucketColumn:   "ts",
		BucketInterval: time.Hour,
		KeepDimensions: []string{"service"},
		Aggregations: []Aggregation{
			{SourceColumn: "v", Functions: []AggFunction{AggSum, AggMin, AggMax}},
		},
	}

	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	outputKey := windowParquetPath(spec, start, end)

	fromTable := "read_parquet('" + filepath.ToSlash(srcParquet) + "', union_by_name=true)"

	cfg := &SubprocessConfig{
		Spec:          spec,
		FromTable:     fromTable,
		WindowStart:   start,
		WindowEnd:     end,
		OutputKey:     outputKey,
		StorageType:   backend.Type(),
		StorageConfig: backend.ConfigJSON(),
	}

	result, err := RunBuildJob(cfg)
	if err != nil {
		t.Fatalf("RunBuildJob: %v", err)
	}
	if !result.Success {
		t.Errorf("result.Success=false, error=%q", result.Error)
	}
	if result.BytesWritten == 0 {
		t.Errorf("BytesWritten should be > 0")
	}

	// Verify parquet landed on storage.
	exists, err := backend.Exists(context.Background(), outputKey)
	if err != nil {
		t.Fatalf("backend.Exists: %v", err)
	}
	if !exists {
		t.Errorf("output parquet not found at %q", outputKey)
	}
}

// TestRunBuildSubprocess_SkipIfNotBinary skips unless the test binary happens to
// be the real arc binary. This guard prevents false failures in plain `go test`.
func TestRunBuildSubprocess_SkipIfNotBinary(t *testing.T) {
	execPath, err := os.Executable()
	if err != nil {
		t.Skip("cannot determine executable path")
	}
	// The test binary is named something like "rollup.test" — not the real arc binary.
	// Skip unless explicitly enabled via env var ARC_TEST_SUBPROCESS=1.
	if os.Getenv("ARC_TEST_SUBPROCESS") != "1" {
		t.Skipf("skipping subprocess test (set ARC_TEST_SUBPROCESS=1 to enable, using binary %s)", execPath)
	}

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}

	spec := RollupSpec{
		Name:           "events__1h",
		Database:       "main",
		SourceTable:    "events",
		BucketColumn:   "ts",
		BucketInterval: time.Hour,
		Aggregations: []Aggregation{
			{SourceColumn: "v", Functions: []AggFunction{AggSum}},
		},
	}

	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	outputKey := windowParquetPath(spec, start, end)

	cfg := &SubprocessConfig{
		Spec:          spec,
		FromTable:     "events",
		WindowStart:   start,
		WindowEnd:     end,
		OutputKey:     outputKey,
		StorageType:   backend.Type(),
		StorageConfig: backend.ConfigJSON(),
	}

	result, err := RunBuildSubprocess(context.Background(), cfg, zerolog.Nop())
	if err != nil {
		t.Fatalf("RunBuildSubprocess: %v", err)
	}
	if !result.Success {
		t.Errorf("expected success, got error: %q", result.Error)
	}
}
