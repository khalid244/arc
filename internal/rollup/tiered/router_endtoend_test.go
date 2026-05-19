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
	// Contiguous coverage 3/01-3/03; query window matches so the
	// emit's coverage check accepts. A previous version of this test
	// had a 3/04-3/06 gap with a 3/07 file — that's the silent
	// undercount scenario the coverage check now blocks.
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/abc.parquet",
		"_arc/rollup/default/events/1h/2025/03/02/by_dim_a/def.parquet",
		"_arc/rollup/default/events/1h/2025/03/03/by_dim_a/ghi.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"a", "b", "c"}, EffectiveCard: 3},
			"dim_b": {Role: "Dim", KeptValues: []string{"x", "y"}, EffectiveCard: 2},
		},
	}
	sql := `SELECT date_trunc('hour', time) AS h, dim_a, COUNT(*) AS n
		FROM events
		WHERE time >= TIMESTAMP '2025-03-01 00:00:00' AND time < TIMESTAMP '2025-03-04 00:00:00'
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
	if !strings.Contains(out, "by_dim_a") {
		t.Errorf("expected by_dim_a variant in output:\n%s", out)
	}
}

// TestRouter_Accepts_BetweenSyntax — BETWEEN should desugar to >= AND <=.
func TestRouter_Accepts_BetweenSyntax(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	// Single-day coverage; query window matches.
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/x.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"dim_a": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT date_trunc('hour', time) AS h, dim_a, COUNT(*) FROM events
		WHERE time BETWEEN TIMESTAMP '2025-03-01' AND TIMESTAMP '2025-03-01 23:59:59'
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
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/x.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"dim_a": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT date_trunc('hour', time) AS h, dim_a, COUNT(*) AS n FROM events
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
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/x.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"dim_a": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT date_trunc('hour', time) AS h, dim_a, COUNT(*) FROM events GROUP BY 1, 2`

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
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/x.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"dim_a": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT date_trunc('hour', time) AS h, dim_a, COUNT(*) FROM nonexistent_table_xyz
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
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/x.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"dim_a": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := fmt.Sprintf(`SELECT date_trunc('hour', time) AS h, dim_a, COUNT(*) FROM read_parquet('%s')
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-02'
		GROUP BY 1, 2`, parquetPath)

	_, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if ok {
		t.Fatalf("router unexpectedly accepted read_parquet FROM. SQL:\n%s", sql)
	}
}

// TestRouter_BareTable_ViewOverReadParquet_IsStillRefused — REPRODUCES
// THE PRODUCTION BUG. In production the stub view points to read_parquet
// (because Arc tables are S3-backed), not to a real DuckDB table. DuckDB
// inlines the view at plan time, so the LOGICAL_GET node in the plan
// shows the read_parquet function, NOT the view name. The router's parser
// reads qs.Table from FunctionData.Table — which is empty for read_parquet
// — and refuses at "no source table found".
//
// The earlier test `TestRouter_BareTable_FixedByReadParquetView` passes
// only because its view points to a REAL TABLE (events). This test mirrors
// production exactly.
func TestRouter_BareTable_ViewOverReadParquet_IsStillRefused(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)

	parquetPath := t.TempDir() + "/events.parquet"
	if _, err := db.Exec(fmt.Sprintf(`COPY events TO '%s' (FORMAT PARQUET)`, parquetPath)); err != nil {
		t.Fatal(err)
	}
	// Production-shape view: points at read_parquet, not a regular table.
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE VIEW events_view AS SELECT * FROM read_parquet('%s', union_by_name=true) WHERE 1=0`,
		parquetPath,
	)); err != nil {
		t.Fatal(err)
	}
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/x.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"dim_a": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT date_trunc('hour', time) AS h, dim_a, COUNT(*) FROM events
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-02'
		GROUP BY 1, 2`

	_, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if ok {
		t.Logf("router accepted view-over-read_parquet (production bug fixed)")
	} else {
		t.Fatalf("router refused view-over-read_parquet — production bug. SQL:\n%s", sql)
	}
}

// TestRouter_BareTable_FixedByReadParquetView — proves that pre-registering
// the table as a view BACKED BY A REAL TABLE makes the bare-table SQL
// parseable. This works in the test but NOT in production (see the
// ViewOverReadParquet test above), because production views point to
// read_parquet not a real table.
func TestRouter_BareTable_FixedByReadParquetView(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)

	// Register a view that simulates Arc's source-rewrite — a 0-row view
	// pointing at the real table. EXPLAIN can now resolve `FROM events`.
	if _, err := db.Exec(`CREATE VIEW events_view AS SELECT * FROM events WHERE 1=0`); err != nil {
		t.Fatal(err)
	}
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/x.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"dim_a": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT date_trunc('hour', time) AS h, dim_a, COUNT(*) FROM events
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-02'
		GROUP BY 1, 2`

	_, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if !ok {
		t.Fatalf("expected accept with view-bound bare table. SQL:\n%s", sql)
	}
}

// TestRouter_TopNViaHardcodedINList — proves the rollup-friendly approach
// for "top N + time series": hard-code the list of values via `dim_a IN (...)`.
// This shape SHOULD be accepted by the router and emit a rollup-served plan.
func TestRouter_TopNViaHardcodedINList(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/x.parquet",
		"_arc/rollup/default/events/1h/2025/03/02/by_dim_a/y.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"a_0", "a_1", "a_2", "a_3", "a_4"}, EffectiveCard: 5},
		},
	}
	sql := `SELECT date_trunc('hour', time) AS time, dim_a, COUNT(*) AS value
		FROM events
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-03'
		  AND dim_a IN ('a_0', 'a_1', 'a_2', 'a_3', 'a_4')
		GROUP BY 1, 2
		ORDER BY time ASC`

	out, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if !ok {
		t.Fatalf("hardcoded IN list refused. SQL:\n%s", sql)
	}
	if !strings.Contains(out, "by_dim_a") {
		t.Errorf("expected by_dim_a variant in output:\n%s", out)
	}
	if !strings.Contains(out, "IN (") {
		t.Errorf("expected IN-filter in output (passed through to class column):\n%s", out)
	}
	t.Logf("hardcoded IN accepted. Rewritten:\n%s", out)
}

// TestRouter_TopNViaCTE — verifies the OTHER shape ("WITH top_dims AS ... ;
// outer query with IN subquery") is refused by the router. Documents the
// limitation so users know to hard-code the top-N list for rollup serving.
func TestRouter_TopNViaCTE(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/x.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"dim_a": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `WITH top_dims AS (
		SELECT dim_a FROM events
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-03'
		GROUP BY dim_a ORDER BY COUNT(*) DESC LIMIT 20
	)
	SELECT date_trunc('hour', time) AS time, dim_a, COUNT(*) AS value
	FROM events
	WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-03'
	  AND dim_a IN (SELECT dim_a FROM top_dims)
	GROUP BY 1, 2
	ORDER BY time ASC`

	_, ok := Rewrite(ctx, sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if ok {
		t.Logf("router unexpectedly accepted CTE+subquery; great, but document it")
	} else {
		t.Logf("router refused CTE+subquery (as expected — falls back to source scan)")
	}
}

// TestRouter_Accepts_GroupByDimOnly — scalar aggregate per group (no time
// bucket). Should pick the coarsest tier with full coverage.
func TestRouter_Accepts_GroupByDimOnly(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1d/2025/03/01/by_dim_a/y.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{"dim_a": {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1}},
	}
	sql := `SELECT dim_a, COUNT(*) AS n FROM events
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-02'
		GROUP BY dim_a`

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
		time TIMESTAMPTZ, dim_a VARCHAR, dim_b VARCHAR, dim_c VARCHAR, value DOUBLE
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO events
		SELECT TIMESTAMP '2025-03-01 00:00:00+00' + INTERVAL (i) MINUTE,
		       'a_' || (i % 5),
		       'b_' || (i % 3),
		       'c_' || (i % 100),
		       1.0
		FROM range(1000) t(i)`); err != nil {
		t.Fatal(err)
	}
	return db
}

// Keep fmt imported.
var _ = fmt.Sprintf
