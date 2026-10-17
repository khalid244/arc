package rollup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The day temp table holds only the source columns the day's cubes read, not
// SELECT * (posthog.events: ~250 columns, ~65% of a day's bytes in columns no
// exact cube reads, so the full-width table spilled ~30GB a day).

func TestSourceColsCoversEveryBuildReference(t *testing.T) {
	s := CubeSpec{
		Source: "default.events",
		Grain:  "hour",
		Dims:   []string{"Site", "status"},
		Aggs: []Aggregate{
			{Kind: AggCount},
			{Kind: AggSum, Col: "bytes"},
			{Kind: AggAvg, Col: "latency"},
			{Kind: AggCountDistinct, Col: "user_id"},
			{Kind: AggCondSum, Cond: `"status" = 200`, CondCols: []string{"status"}, ThenCol: "body_size"},
		},
	}
	got := map[string]bool{}
	s.sourceCols("time", got)
	var names []string
	for n := range got {
		names = append(names, n)
	}
	sort.Strings(names)
	want := []string{"body_size", "bytes", "latency", "site", "status", "time", "user_id"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("sourceCols = %v, want %v", names, want)
	}
	// Every column the build SELECT names must be in the set.
	sel := s.buildSelectFrom("t", "time", "2026-01-01 00:00:00+00", "2026-01-02 00:00:00+00", nil)
	for _, n := range names {
		if !strings.Contains(strings.ToLower(sel), `"`+n+`"`) {
			t.Errorf("build SELECT does not reference %q: %s", n, sel)
		}
	}
}

func TestDayProjectionReadsOnlyCubeColumns(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 1)
	d := dates[0]
	addSourceFile(t, m, stg.root, d, "wide.parquet", fmt.Sprintf(
		`SELECT TIMESTAMPTZ '%s 11:00:00' AS "time", 'tv' AS site, repeat('x', 100) AS uuid, 42 AS session_ms`, d))
	cubes := heldBuilds(t, m, specs)
	ds := m.listDaySource(context.Background(), freshSrc, day(d))
	if !ds.ok {
		t.Fatal("day did not list cleanly")
	}
	got := m.dayProjection(cubes, ds.expr)
	if got != `"time", "site"` {
		t.Fatalf("projection = %s, want only the cube columns \"time\", \"site\"", got)
	}
}

func TestDayProjectionOmitsCubeColumnAbsentFromDay(t *testing.T) {
	m, _, specs, dates := passFixture(t, 1)
	cubes := heldBuilds(t, m, specs)
	cubes[0].spec.Dims = append(cubes[0].spec.Dims, "os_name") // a sparse property this day never carried
	ds := m.listDaySource(context.Background(), freshSrc, day(dates[0]))
	got := m.dayProjection(cubes, ds.expr)
	if strings.Contains(got, "os_name") {
		t.Fatalf("projection %s names a column the day lacks (the CREATE would fail to bind)", got)
	}
}

func TestDayProjectionFallsBackToStarWhenSchemaUnreadable(t *testing.T) {
	m, stg, specs, _ := passFixture(t, 1)
	cubes := heldBuilds(t, m, specs)
	missing := "['" + filepath.Join(stg.root, "nope.parquet") + "']"
	if got := m.dayProjection(cubes, missing); got != "*" {
		t.Fatalf("projection = %s, want * when the source schema cannot be read", got)
	}
}

// A pass over a day with columns no cube reads builds the same cubes as before.
func TestPassWithUnreadColumnsBuildsFullCounts(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 1)
	d := dates[0]
	addSourceFile(t, m, stg.root, d, "wide.parquet", fmt.Sprintf(
		`SELECT TIMESTAMPTZ '%s 11:00:00' AS "time", 'tv' AS site, repeat('x', 100) AS uuid`, d))
	pass(t, m, stg, dates)
	want := sourceCount(t, m, stg.root, d)
	for _, s := range specs {
		if got := cubeCount(t, m, s, d); got != want {
			t.Errorf("%s: cube has %d rows, want %d", CubeID(s), got, want)
		}
	}
}

// The day's source is held in a table of an attached DuckDB file (compressed
// row groups), not a TEMP table (uncompressed, spilled to temp_directory).

func TestDayTableLivesInDayDatabase(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 2)
	pass(t, m, stg, dates)
	if !m.dayDBAttached {
		t.Fatal("day database was not attached")
	}
	if _, err := os.Stat(m.dayDBPath); err != nil {
		t.Fatalf("day database file: %v", err)
	}
	var n int
	if err := m.db.QueryRow(`SELECT count(*) FROM duckdb_tables() WHERE table_name = '_rollup_day'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d _rollup_day tables left after the pass, want 0", n)
	}
	for _, s := range specs {
		for _, d := range dates {
			if got, want := cubeCount(t, m, s, d), sourceCount(t, m, stg.root, d); got != want {
				t.Errorf("%s@%s: cube has %d rows, want %d", CubeID(s), d, got, want)
			}
		}
	}
}

func TestDayDatabaseStaleFileReplaced(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 1)
	if err := os.WriteFile(m.dayDBPath, []byte("left by a crashed process"), 0o644); err != nil {
		t.Fatal(err)
	}
	pass(t, m, stg, dates)
	if !m.dayDBAttached {
		t.Fatal("stale file blocked the day database")
	}
	if got, want := cubeCount(t, m, specs[0], dates[0]), sourceCount(t, m, stg.root, dates[0]); got != want {
		t.Fatalf("cube has %d rows, want %d", got, want)
	}
}

func TestDayDatabaseUnavailableFallsBackToTempTable(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 1)
	m.dayDBPath = filepath.Join(stg.root, "no-such-dir", "rollup_day.duckdb")
	pass(t, m, stg, dates)
	if m.dayDBAttached {
		t.Fatal("attached a day database at an unwritable path")
	}
	for _, s := range specs {
		if got, want := cubeCount(t, m, s, dates[0]), sourceCount(t, m, stg.root, dates[0]); got != want {
			t.Errorf("%s: cube has %d rows, want %d", CubeID(s), got, want)
		}
	}
}

// Each day's table is dropped and checkpointed, so the file is reused instead
// of growing by one day's data per day.
func TestDayDatabaseFileReused(t *testing.T) {
	m, stg, _, dates := passFixture(t, 1)
	big := fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 00:00:00' + to_seconds(i) AS "time", 'tv' AS site FROM range(300000) t(i)`, dates[0])
	addSourceFile(t, m, stg.root, dates[0], "big.parquet", big)
	tbl, _ := m.dayTable()
	size := func() int64 {
		fi, err := os.Stat(m.dayDBPath)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Size()
	}
	src := readParquetFrom("'" + filepath.Join(dayDir(stg.root, dates[0]), "*.parquet") + "'")
	var sizes []int64
	for i := 0; i < 4; i++ {
		mustExec(t, m.db, "CREATE OR REPLACE TABLE "+tbl+" AS SELECT * FROM "+src)
		m.dropDayTable()
		sizes = append(sizes, size())
	}
	if sizes[3] > sizes[1] {
		t.Fatalf("day database grew across days: %v bytes", sizes)
	}
}
