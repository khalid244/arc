package rollup

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2"
)

// openLocalDuck opens a plain in-memory DuckDB (no httpfs/S3) for tests that read
// local Parquet files — no MinIO corpus required, so these always run.
func openLocalDuck(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	if _, err := db.Exec("SET TimeZone='UTC'"); err != nil {
		db.Close()
		t.Fatalf("set tz: %v", err)
	}
	return db
}

// TestBuildMonthAdaptsToSchemaDrift reproduces the production failure where a
// whole-month cube build references a dimension column that did not exist that
// month (a newer, sparse event property), and pins the fix:
//   - the bare BuildRange over an absent dimension fails with a DuckDB Binder Error
//     (the bug: the month path used to retry this forever every tick);
//   - globColumns reports the month's real columns;
//   - prunedToColumns skips the absent-dimension cube (errMonthDimAbsent territory);
//   - a present-dimension cube still builds after pruning.
func TestBuildMonthAdaptsToSchemaDrift(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	dir := t.TempDir()

	// An "April" month whose schema has time + site, but NOT email (added later).
	src := filepath.Join(dir, "apr.parquet")
	copySrc := fmt.Sprintf(
		`COPY (SELECT TIMESTAMPTZ '2026-04-10 01:30:00' AS "time", 'web' AS site
		       UNION ALL SELECT TIMESTAMPTZ '2026-04-10 02:15:00', 'ios') TO '%s' (FORMAT PARQUET)`, src)
	if _, err := db.Exec(copySrc); err != nil {
		t.Fatalf("write source parquet: %v", err)
	}
	glob := "['" + src + "']"

	m := &Manager{db: db, cfg: Config{}.withDefaults(), log: zerolog.New(io.Discard)}

	cols, err := m.globColumns(glob)
	if err != nil {
		t.Fatalf("globColumns: %v", err)
	}
	if !cols["site"] || !cols["time"] || cols["email"] {
		t.Fatalf("month columns = %v, want {time,site} and no email", cols)
	}

	lo, hi := "2026-04-01 00:00:00", "2026-05-01 00:00:00"

	// (bug repro) a by-email cube over a month lacking "email" Binder-errors.
	emailSpec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"email"}, Aggs: []Aggregate{{Kind: AggCount}}}
	if _, err := BuildRange(db, emailSpec, glob, "time", "2026-04", lo, hi, filepath.Join(dir, "email.parquet")); err == nil {
		t.Fatal("repro: expected a Binder error building by-email over a month with no 'email' column")
	}
	// (fix) prune skips that cube for the month instead of erroring forever.
	if _, ok := emailSpec.prunedToColumns(cols); ok {
		t.Fatal("by-email cube should be skipped (ok=false) when 'email' is absent")
	}

	// (fix) a by-site cube still builds after pruning.
	siteSpec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}}}
	pruned, ok := siteSpec.prunedToColumns(cols)
	if !ok {
		t.Fatal("by-site cube should build (ok=true) when 'site' is present")
	}
	entry, err := BuildRange(db, pruned, glob, "time", "2026-04", lo, hi, filepath.Join(dir, "site.parquet"))
	if err != nil {
		t.Fatalf("by-site month build failed: %v", err)
	}
	if entry.Rows == 0 {
		t.Fatal("by-site cube should have rows")
	}
}

// TestBuildMonthSkipsAbsentDim pins that buildMonth signals errMonthDimAbsent —
// without touching the DB or building anything — when the cube's dimension is not
// among the month's columns. (The skip decision returns before BuildRange.)
func TestBuildMonthSkipsAbsentDim(t *testing.T) {
	m := &Manager{} // no DB needed: the skip path returns before any build
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"email"}, Aggs: []Aggregate{{Kind: AggCount}}}
	monthCols := map[string]bool{"time": true, "site": true} // no "email"
	_, err := m.buildMonth(spec, "default.events", "2026-04", nil, monthCols)
	if !errors.Is(err, errMonthDimAbsent) {
		t.Fatalf("buildMonth over absent dim: err = %v, want errMonthDimAbsent", err)
	}
}

// TestMarkMonthResolved pins the bookkeeping that stops the retry-storm: once a
// cube's dimension is known absent for a month, that month is removed from the
// pending month-build set and its sealed days are marked built so the day phase
// skips them too — without writing any cube file.
func TestMarkMonthResolved(t *testing.T) {
	cb := &cubeBuild{
		spec:       CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"email"}},
		built:      map[string]bool{},
		monthBuild: map[string]bool{"2026-04": true, "2026-05": true},
	}
	apr := []time.Time{
		time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), // different month — must be untouched
	}
	markMonthResolved(cb, "2026-04", apr)

	if cb.monthBuild["2026-04"] {
		t.Fatal("2026-04 should be removed from monthBuild")
	}
	if !cb.monthBuild["2026-05"] {
		t.Fatal("2026-05 should remain pending")
	}
	if !cb.built["2026-04-10"] || !cb.built["2026-04-11"] {
		t.Fatalf("April sealed days should be marked built, got %v", cb.built)
	}
	if cb.built["2026-05-01"] {
		t.Fatal("a May day must not be marked built when resolving April")
	}
}
