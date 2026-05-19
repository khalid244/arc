package tiered

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// ── unit: extractSelectAlias regex helper ───────────────────────────────

func TestExtractSelectAlias_BasicDateTrunc(t *testing.T) {
	sql := `SELECT date_trunc('hour', time) AS time, site, COUNT(*) AS value FROM downloads`
	if got := extractSelectAlias(sql, "hour"); got != "time" {
		t.Errorf("got %q want time", got)
	}
}

func TestExtractSelectAlias_ToTimestampForm(t *testing.T) {
	sql := `SELECT to_timestamp((epoch_ns(time)//1000000000//3600)*3600) AS hour, COUNT(*) FROM downloads`
	if got := extractSelectAlias(sql, "hour"); got != "hour" {
		t.Errorf("got %q want hour", got)
	}
}

func TestExtractSelectAlias_NoAlias(t *testing.T) {
	sql := `SELECT date_trunc('hour', time), COUNT(*) FROM downloads`
	if got := extractSelectAlias(sql, "hour"); got != "" {
		t.Errorf("got %q want empty (no AS clause)", got)
	}
}

func TestExtractSelectAlias_CommaInsideFunctionDoesNotConfuse(t *testing.T) {
	// First projection contains a comma inside date_trunc(); must not be
	// treated as the projection-list separator.
	sql := `SELECT date_trunc('day', time, 'UTC') AS t, COUNT(*) FROM downloads`
	if got := extractSelectAlias(sql, "day"); got != "t" {
		t.Errorf("got %q want t", got)
	}
}

func TestExtractSelectAlias_MultilineSQL(t *testing.T) {
	sql := `
		SELECT $__timeGroup(time, $__interval) AS time,
		       site,
		       COUNT(*) AS value
		FROM downloads`
	if got := extractSelectAlias(sql, ""); got != "time" {
		t.Errorf("got %q want time", got)
	}
}

func TestExtractSelectAlias_CaseInsensitiveAs(t *testing.T) {
	sql := `SELECT date_trunc('hour', time) as hour, COUNT(*) FROM downloads`
	if got := extractSelectAlias(sql, "hour"); got != "hour" {
		t.Errorf("got %q want hour", got)
	}
}

func TestExtractSelectAlias_NoSelect(t *testing.T) {
	if got := extractSelectAlias("INVALID SQL", ""); got != "" {
		t.Errorf("got %q want empty", got)
	}
}

func TestExtractSelectAlias_NoFrom(t *testing.T) {
	if got := extractSelectAlias("SELECT 1 AS x", ""); got != "" {
		t.Errorf("got %q want empty (no FROM clause)", got)
	}
}

// ── unit: firstTopLevelProjection splits on top-level commas only ───────

func TestFirstTopLevelProjection(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"a, b, c", "a"},
		{"date_trunc('hour', time) AS t, site", "date_trunc('hour', time) AS t"},
		{"COUNT(*), site", "COUNT(*)"},
		{"a", "a"},
		{"f(g(x, y), z)", "f(g(x, y), z)"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := firstTopLevelProjection(tc.in); got != tc.want {
				t.Errorf("got %q want %q", got, tc.want)
			}
		})
	}
}

// ── integration: BucketAlias + OutputAlias survive the parser ───────────

func TestExtractShape_CapturesBucketAndAggregateAliases(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	sql := `SELECT date_trunc('hour', time) AS t, site, COUNT(*) AS v FROM events
	        WHERE time >= TIMESTAMP '2026-05-01' AND time < TIMESTAMP '2026-05-02'
	        GROUP BY 1, 2 ORDER BY 1`
	qs, err := ExtractQueryShape(ctx, db, sql)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.BucketAlias != "t" {
		t.Errorf("BucketAlias=%q want t", qs.BucketAlias)
	}
	if len(qs.Aggregates) != 1 || qs.Aggregates[0].OutputAlias != "v" {
		t.Errorf("Aggregates[0].OutputAlias=%q want v (got %+v)", qs.Aggregates[0].OutputAlias, qs.Aggregates)
	}
}

func TestExtractShape_OmittedAliasesYieldEmptyBucketAlias(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	sql := `SELECT date_trunc('hour', time), COUNT(*) FROM events
	        WHERE time >= TIMESTAMP '2026-05-01' AND time < TIMESTAMP '2026-05-02'
	        GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, sql)
	if err != nil {
		t.Fatal(err)
	}
	if qs.BucketAlias != "" {
		t.Errorf("BucketAlias=%q want empty (no AS)", qs.BucketAlias)
	}
}

// ── emit uses the alias in the outer SELECT ─────────────────────────────

func TestEmit_UsesBucketAliasInOuterSelect(t *testing.T) {
	timeHi := mustTime("2026-05-15")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/05/01/sketch/f.parquet"})
	shape := &QueryShape{
		Table:       "db.events",
		TimeColumn:  "time",
		TimeLo:      mustTime("2026-05-01"),
		TimeHi:      timeHi,
		BucketArg:   "hour",
		BucketAlias: "time", // user wrote AS time
		Aggregates: []Aggregate{{
			Kind:        AggCountStar,
			OutputAlias: "value", // user wrote AS value
		}},
	}
	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape: shape, Tier: Tier1h, TailLo: timeHi, Variant: "sketch",
		Files: idx, Spec: makeSpec("UTC", nil), SkipCoverageCheck: true,
	})
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if !strings.Contains(sql, "_bkt AS time") {
		t.Errorf("expected `_bkt AS time` (user's bucket alias):\n%s", sql)
	}
	if !strings.Contains(sql, "AS value") {
		t.Errorf("expected `AS value` on aggregate:\n%s", sql)
	}
	if strings.Contains(sql, "AS hour") {
		t.Errorf("must not leak BucketArg literal `hour` as alias when user said AS time:\n%s", sql)
	}
}

// Property: rollup-served column names match source-served column names
// for the same user SQL. This is the parity that was broken in the user's
// CSV exports (source "time"/"value", rollup "hour"/"CAST(sum(_agg_0)...)").
func TestProperty_ColumnNames_RollupMatchesSource(t *testing.T) {
	ctx := context.Background()
	keptSites := []string{"youtu.be", "www.instagram.com"}
	db, spec, idx, tmp := setupKept3DayDataset(t, ctx, keptSites, 1)

	userSQL := `SELECT date_trunc('hour', time) AS time,
	                  site,
	                  COUNT(*) AS value
	            FROM events
	            WHERE time >= TIMESTAMP '2026-05-14 00:00:00+00'
	              AND time <  TIMESTAMP '2026-05-15 00:00:00+00'
	              AND site IN ('youtu.be', 'www.instagram.com')
	            GROUP BY 1, 2 ORDER BY time ASC`

	srcCols := queryColumnNames(t, db, userSQL)
	rewritten := mustRewriteAccept(t, ctx, db, idx, spec, tmp, userSQL)
	rolCols := queryColumnNames(t, db, rewritten)

	if len(srcCols) != len(rolCols) {
		t.Fatalf("column count diverged: src=%v rol=%v\nrewrite:\n%s", srcCols, rolCols, rewritten)
	}
	for i := range srcCols {
		if srcCols[i] != rolCols[i] {
			t.Errorf("column %d diverged: src=%q rol=%q\nfull source: %v\nfull rollup: %v\nrewrite:\n%s",
				i, srcCols[i], rolCols[i], srcCols, rolCols, rewritten)
		}
	}
}

// queryColumnNames executes the SQL (splitting any preamble SET TimeZone
// statements) and returns the result-set column names.
func queryColumnNames(t *testing.T, db *databaseSQL, sql string) []string {
	t.Helper()
	stmts := splitTopLevelStmts(sql)
	for i := 0; i < len(stmts)-1; i++ {
		if strings.TrimSpace(stmts[i]) == "" {
			continue
		}
		if _, err := db.Exec(stmts[i]); err != nil {
			t.Fatalf("exec setup stmt %q: %v", stmts[i], err)
		}
	}
	last := stmts[len(stmts)-1]
	rows, err := db.Query(last)
	if err != nil {
		t.Fatalf("query %q: %v", last, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	return cols
}

// databaseSQL aliases *sql.DB so the test can name the param type without
// importing the package directly in this short file (db is already used
// across other tests).
type databaseSQL = sql.DB

// When the user omits aliases the emit falls back to the BucketArg literal
// (current behaviour for backward compat).
func TestEmit_FallsBackToBucketArgWhenAliasMissing(t *testing.T) {
	timeHi := mustTime("2026-05-15")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/05/01/sketch/f.parquet"})
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     mustTime("2026-05-01"),
		TimeHi:     timeHi,
		BucketArg:  "hour",
		// no BucketAlias, no OutputAlias
		Aggregates: []Aggregate{{Kind: AggCountStar}},
	}
	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape: shape, Tier: Tier1h, TailLo: timeHi, Variant: "sketch",
		Files: idx, Spec: makeSpec("UTC", nil), SkipCoverageCheck: true,
	})
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if !strings.Contains(sql, "_bkt AS hour") {
		t.Errorf("expected fallback to BucketArg literal `_bkt AS hour`:\n%s", sql)
	}
}
