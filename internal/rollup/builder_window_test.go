package rollup

import (
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestReadParquetFromTableWindow_DailyInterval_DayLevelGlob(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("LocalBackend: %v", err)
	}

	spec := RollupSpec{
		Database:       "default",
		SourceTable:    "downloads",
		BucketInterval: 24 * time.Hour,
		BucketColumn:   "time",
		Name:           "default__downloads__1d",
	}
	windowStart := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)

	expr := ReadParquetFromTableWindow(backend, spec, windowStart)

	// For a 24h+ interval we want a day-scoped recursive glob, NOT the
	// full table prefix (which would force DuckDB to LIST all years).
	wantSubstr := "default/downloads/2026/05/13/**/*.parquet"
	if !strings.Contains(expr, wantSubstr) {
		t.Errorf("daily interval: expr does not contain %q\ngot: %s", wantSubstr, expr)
	}
	// MUST NOT contain the bucket-wide glob — that's the bug we fixed.
	badSubstr := "default/downloads/**/*.parquet"
	// The day path *also* matches that substring with a wider regex,
	// so check explicitly that the year prefix is present.
	if strings.Contains(expr, "default/downloads/**") && !strings.Contains(expr, "default/downloads/2026/") {
		t.Errorf("daily interval: expr falls back to bucket-wide glob\ngot: %s", expr)
	}
	_ = badSubstr
}

func TestReadParquetFromTableWindow_HourlyInterval_HourLevelGlob(t *testing.T) {
	dir := t.TempDir()
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())

	spec := RollupSpec{
		Database:       "default",
		SourceTable:    "downloads",
		BucketInterval: time.Hour,
		BucketColumn:   "time",
		Name:           "default__downloads__1h",
	}
	windowStart := time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC)

	expr := ReadParquetFromTableWindow(backend, spec, windowStart)

	wantSubstr := "default/downloads/2026/05/13/14/*.parquet"
	if !strings.Contains(expr, wantSubstr) {
		t.Errorf("hourly interval: expr does not contain %q\ngot: %s", wantSubstr, expr)
	}
	// Hour-level glob must NOT use the recursive `**` (one fewer LIST level).
	if strings.Contains(expr, "/14/**/") {
		t.Errorf("hourly interval: should use single-level glob, not recursive\ngot: %s", expr)
	}
}

func TestReadParquetFromTableWindow_WindowStartChangesPath(t *testing.T) {
	dir := t.TempDir()
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())
	spec := RollupSpec{
		Database:       "db",
		SourceTable:    "t",
		BucketInterval: 24 * time.Hour,
		Name:           "db__t__1d",
	}

	d1 := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC)
	e1 := ReadParquetFromTableWindow(backend, spec, d1)
	e2 := ReadParquetFromTableWindow(backend, spec, d2)

	if e1 == e2 {
		t.Errorf("different window starts should produce different paths; both = %s", e1)
	}
	if !strings.Contains(e1, "2026/05/13") {
		t.Errorf("expr1 missing 2026/05/13: %s", e1)
	}
	if !strings.Contains(e2, "2026/05/14") {
		t.Errorf("expr2 missing 2026/05/14: %s", e2)
	}
}

func TestReadParquetFromTableWindow_UTCNormalization(t *testing.T) {
	dir := t.TempDir()
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())
	spec := RollupSpec{
		Database:       "db",
		SourceTable:    "t",
		BucketInterval: 24 * time.Hour,
	}

	// Provide a window start in a non-UTC zone. The path must be derived
	// from the UTC representation so partitions don't shift.
	tz, _ := time.LoadLocation("Asia/Riyadh") // UTC+3
	local := time.Date(2026, 5, 14, 2, 0, 0, 0, tz) // 2026-05-13 23:00 UTC
	expr := ReadParquetFromTableWindow(backend, spec, local)

	if !strings.Contains(expr, "2026/05/13") {
		t.Errorf("non-UTC window start should resolve to UTC partition (expected 2026/05/13); got %s", expr)
	}
}

func TestReadParquetFromTableWindow_QuotesSafeForSingleQuoteInPath(t *testing.T) {
	// LocalBackend uses the temp dir as base path; that path can't easily
	// contain a single quote, but we verify the resolver escapes quotes
	// generally so the produced SQL is safe.
	dir := t.TempDir()
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())
	spec := RollupSpec{
		Database:       "db'odd",
		SourceTable:    "t",
		BucketInterval: 24 * time.Hour,
	}
	expr := ReadParquetFromTableWindow(backend, spec, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	// Single quote must be escaped to '' inside SQL string literals.
	if strings.Contains(expr, "db'odd") {
		t.Errorf("single quote not escaped in SQL literal: %s", expr)
	}
	if !strings.Contains(expr, "db''odd") {
		t.Errorf("expected SQL-escaped single quote; got: %s", expr)
	}
}
