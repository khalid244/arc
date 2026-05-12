package rollup

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestBuilder_BuildOneWindowFromSource(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	db := openDuckDBWithDataSketches(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE events (
		ts TIMESTAMP, service VARCHAR, latency_ms DOUBLE
	)`)
	mustExec(t, db, `INSERT INTO events VALUES
		('2026-05-10 12:00:01', 'api', 10),
		('2026-05-10 12:30:00', 'api', 20),
		('2026-05-10 12:45:00', 'web', 30),
		('2026-05-10 13:01:00', 'api', 40)
	`)

	spec := RollupSpec{
		Name:           "events__1h",
		Database:       "main",
		SourceTable:    "events",
		BucketColumn:   "ts",
		BucketInterval: time.Hour,
		KeepDimensions: []string{"service"},
		Aggregations: []Aggregation{
			{SourceColumn: "latency_ms", Functions: []AggFunction{AggSum, AggMin, AggMax}},
		},
	}

	logger := zerolog.Nop()
	b := NewBuilder(db, backend, NewWatermarkStore(backend), logger)
	b.InProcess = true
	windowStart := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 5, 10, 13, 0, 0, 0, time.UTC)
	if err := b.BuildWindow(context.Background(), spec, "events", windowStart, windowEnd); err != nil {
		t.Fatalf("BuildWindow: %v", err)
	}

	// Watermark advanced
	wm, err := NewWatermarkStore(backend).Get(context.Background(), spec.Name)
	if err != nil {
		t.Fatalf("watermark get: %v", err)
	}
	if !wm.Watermark.Equal(windowEnd) {
		t.Errorf("watermark: got %v want %v", wm.Watermark, windowEnd)
	}

	// Parquet file exists at the deterministic key (relative to backend root)
	relKey := "main/events__1h/dt=2026-05-10/window_20260510-120000-130000.parquet"
	exists, err := backend.Exists(context.Background(), relKey)
	if err != nil {
		t.Fatalf("backend.Exists: %v", err)
	}
	if !exists {
		t.Fatalf("expected parquet at backend key %q", relKey)
	}

	// Query the file back via DuckDB read_parquet on the local-backed file path
	absPath := filepath.Join(dir, relKey)
	verifyDB := openDuckDBWithDataSketches(t)
	defer verifyDB.Close()
	var apiSum, apiMin, apiMax sql.NullFloat64
	var rowCount sql.NullInt64
	row := verifyDB.QueryRowContext(context.Background(),
		`SELECT __row_count, latency_ms__sum, latency_ms__min, latency_ms__max
		 FROM read_parquet(?)
		 WHERE service = 'api'`, absPath)
	if err := row.Scan(&rowCount, &apiSum, &apiMin, &apiMax); err != nil {
		t.Fatalf("verify query: %v", err)
	}
	if rowCount.Int64 != 2 {
		t.Errorf("api rows: %d want 2", rowCount.Int64)
	}
	if apiSum.Float64 != 30 {
		t.Errorf("api latency sum: %f want 30", apiSum.Float64)
	}
}

func TestBuilder_RebuildSameWindowOverwrites(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	db := openDuckDBWithDataSketches(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE events (ts TIMESTAMP, v DOUBLE)`)
	mustExec(t, db, `INSERT INTO events VALUES ('2026-05-10 12:00:00', 1)`)

	spec := RollupSpec{
		Name: "events__1h", Database: "main", SourceTable: "events",
		BucketColumn: "ts", BucketInterval: time.Hour,
		Aggregations: []Aggregation{{SourceColumn: "v", Functions: []AggFunction{AggSum}}},
	}

	b := NewBuilder(db, backend, NewWatermarkStore(backend), zerolog.Nop())
	b.InProcess = true
	wsStart := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	wsEnd := wsStart.Add(time.Hour)

	if err := b.BuildWindow(context.Background(), spec, "events", wsStart, wsEnd); err != nil {
		t.Fatalf("first build: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES ('2026-05-10 12:30:00', 99)`)
	if err := b.BuildWindow(context.Background(), spec, "events", wsStart, wsEnd); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	relKey := "main/events__1h/dt=2026-05-10/window_20260510-120000-130000.parquet"
	absPath := filepath.Join(dir, relKey)

	verifyDB := openDuckDBWithDataSketches(t)
	defer verifyDB.Close()
	var sum sql.NullFloat64
	row := verifyDB.QueryRowContext(context.Background(),
		`SELECT v__sum FROM read_parquet(?)`, absPath)
	if err := row.Scan(&sum); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if sum.Float64 != 100 {
		t.Errorf("sum after rebuild: %f want 100 (1+99); previous-build leakage suspected", sum.Float64)
	}
}
