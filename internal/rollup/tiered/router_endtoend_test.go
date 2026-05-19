package tiered

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// These tests exercise the full Rewrite path against a real DuckDB with
// a real source table registered in the catalog. They prove:
//
//   1. Which query shapes the router can/cannot handle
//   2. That production's parser failure is a CATALOG issue (table missing
//      from DuckDB) — when the table IS registered, the same shape works
//
// To make production work, the caller MUST hand the router a SQL whose
// FROM clause resolves in DuckDB's catalog. Two ways: register tables at
// startup, OR pre-rewrite FROM to read_parquet before calling Rewrite.

// TestRouter_Accepts_GroupByTimeAndDim — Grafana-shape time series.
func TestRouter_Accepts_GroupByTimeAndDim(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/by_site/abc.parquet",
		"_arc/rollup/default/events/1h/2025/03/02/by_site/def.parquet",
		"_arc/rollup/default/events/1h/2025/03/03/by_site/ghi.parquet",
		"_arc/rollup/default/events/1h/2025/03/07/by_site/jkl.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"site":    {Role: "Dim", KeptValues: []string{"a", "b", "c"}, EffectiveCard: 3},
			"country": {Role: "Dim", KeptValues: []string{"x", "y"}, EffectiveCard: 2},
		},
	}
	sql := `SELECT date_trunc('hour', time) AS h, site, COUNT(*) AS n
		FROM events
		WHERE time >= TIMESTAMP '2025-03-01 00:00:00' AND time < TIMESTAMP '2025-03-08 00:00:00'
		GROUP BY 1, 2
		ORDER BY h ASC
		LIMIT 20`

	out, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if !ok {
		t.Fatalf("Rewrite refused. SQL:\n%s", sql)
	}
	if !strings.Contains(out, "WITH rollup AS") {
		t.Errorf("missing WITH rollup AS:\n%s", out)
	}
	if !strings.Contains(out, "by_site") {
		t.Errorf("expected by_site variant in output:\n%s", out)
	}
}

// TestRouter_Accepts_BetweenSyntax — BETWEEN should desugar to >= AND <=.
func TestRouter_Accepts_BetweenSyntax(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/by_site/x.parquet",
		"_arc/rollup/default/events/1h/2025/03/07/by_site/y.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"site": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT date_trunc('hour', time) AS h, site, COUNT(*) FROM events
		WHERE time BETWEEN TIMESTAMP '2025-03-01' AND TIMESTAMP '2025-03-08'
		GROUP BY 1, 2`

	_, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if !ok {
		t.Fatalf("BETWEEN refused. SQL:\n%s", sql)
	}
}

// TestRouter_Accepts_OrderByLimit — verifies ORDER BY + LIMIT pass through.
func TestRouter_Accepts_OrderByLimit(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/by_site/x.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"site": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT date_trunc('hour', time) AS h, site, COUNT(*) AS n FROM events
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-02'
		GROUP BY 1, 2
		ORDER BY n DESC
		LIMIT 10`

	_, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if !ok {
		t.Fatalf("ORDER BY + LIMIT refused. SQL:\n%s", sql)
	}
}

// TestRouter_Accepts_NoTimeFilter — router accepts even without a time
// filter, using the full available range. Documented behaviour finding from
// the suite: not the prior assumption.
func TestRouter_Accepts_NoTimeFilter(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/by_site/x.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"site": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT date_trunc('hour', time) AS h, site, COUNT(*) FROM events GROUP BY 1, 2`

	_, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if !ok {
		t.Fatalf("router refused unbounded query — surprising; investigate")
	}
}

// TestRouter_Refuses_TableMissingFromCatalog — reproduces the production
// failure: bare table name not in DuckDB's catalog → EXPLAIN errors →
// router refuses every query at parser stage.
func TestRouter_Refuses_TableMissingFromCatalog(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/by_site/x.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"site": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT date_trunc('hour', time) AS h, site, COUNT(*) FROM nonexistent_table_xyz
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-02'
		GROUP BY 1, 2`

	_, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if ok {
		t.Fatalf("expected refuse — table doesn't exist in DuckDB catalog")
	}
}

// TestRouter_Refuses_ReadParquetFROM — KEY FINDING: even though
// `FROM read_parquet('file.parquet')` resolves in DuckDB, the router's
// parser (router_parse.go::walkNode) expects a BOUND-table reference, not
// a BOUND-function reference. The plan node for read_parquet is treated as
// "no source table found" and the parser refuses. This rules out the
// "pre-rewrite FROM to read_parquet" workaround and forces the
// view-registration approach.
func TestRouter_Refuses_ReadParquetFROM(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)

	parquetPath := t.TempDir() + "/events.parquet"
	if _, err := db.Exec(fmt.Sprintf(`COPY events TO '%s' (FORMAT PARQUET)`, parquetPath)); err != nil {
		t.Fatal(err)
	}

	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/downloads/1h/2025/03/01/by_site/x.parquet",
	}}
	spec := &Spec{
		Table: "default.downloads", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"site": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := fmt.Sprintf(`SELECT date_trunc('hour', time) AS h, site, COUNT(*) FROM read_parquet('%s')
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-02'
		GROUP BY 1, 2`, parquetPath)

	_, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if ok {
		t.Fatalf("router unexpectedly accepted read_parquet FROM. SQL:\n%s", sql)
	}
}

// TestRouter_BareTable_FixedByReadParquetView — proves that pre-registering
// the table as a view (alternative production fix) makes the bare-table
// SQL parseable and the router accepts it.
func TestRouter_BareTable_FixedByReadParquetView(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)

	// Register a view that simulates Arc's source-rewrite — a 0-row view
	// pointing at the real table. EXPLAIN can now resolve `FROM downloads`.
	if _, err := db.Exec(`CREATE VIEW downloads AS SELECT * FROM events WHERE 1=0`); err != nil {
		t.Fatal(err)
	}
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/downloads/1h/2025/03/01/by_site/x.parquet",
	}}
	spec := &Spec{
		Table: "default.downloads", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"site": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT date_trunc('hour', time) AS h, site, COUNT(*) FROM downloads
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-02'
		GROUP BY 1, 2`

	_, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if !ok {
		t.Fatalf("expected accept with view-bound bare table. SQL:\n%s", sql)
	}
}

// TestRouter_Accepts_GroupByDimOnly — scalar aggregate per group (no time
// bucket). Should pick the coarsest tier with full coverage.
func TestRouter_Accepts_GroupByDimOnly(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1d/2025/03/01/by_site/y.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"site": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT site, COUNT(*) AS n FROM events
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-02'
		GROUP BY site`

	_, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if !ok {
		t.Fatalf("scalar agg per dim refused. SQL:\n%s", sql)
	}
}

// buildEventsTable creates an in-memory DuckDB with a populated `events`
// table whose schema matches a typical Arc source table.
func buildEventsTable(t *testing.T) *sql.DB {
	t.Helper()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE events (
		time TIMESTAMPTZ, site VARCHAR, country VARCHAR, device_id VARCHAR, value DOUBLE
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO events
		SELECT TIMESTAMP '2025-03-01 00:00:00+00' + INTERVAL (i) MINUTE,
		       'site_' || (i % 5),
		       'c_' || (i % 3),
		       'dev_' || (i % 100),
		       1.0
		FROM range(1000) t(i)`); err != nil {
		t.Fatal(err)
	}
	return db
}

// Keep fmt imported.
var _ = fmt.Sprintf
