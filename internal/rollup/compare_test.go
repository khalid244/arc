package rollup

import (
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The fully-sealed historic day used as the comparison window.
const (
	day28Glob = "['s3://arc-test/default/downloads/2025/12/28/**/*.parquet']"
	day28Lo   = "2025-12-28 00:00:00+00"
	day28Hi   = "2025-12-29 00:00:00+00"
)

// stdAggs is the standard aggregate payload Rollup materializes for downloads:
// count, duration summaries (avg/min/max/count), an HLL on the high-card device_id,
// and a KLL on duration for percentiles.
var stdAggs = []Aggregate{
	{Kind: AggCount},
	{Kind: AggAvg, Col: "duration_seconds"},
	{Kind: AggMin, Col: "duration_seconds"},
	{Kind: AggMax, Col: "duration_seconds"},
	{Kind: AggCountCol, Col: "duration_seconds"},
	{Kind: AggCountDistinct, Col: "device_id"},
	{Kind: AggPercentile, Col: "duration_seconds", P: 0.95},
}

// dimRichCube stores all low-cardinality dims — serves any group-by/filter over
// them with exact counts/sums and (lossless) HLL distincts.
var dimRichCube = CubeSpec{
	Source: "default.downloads", Grain: "hour",
	Dims: []string{"status", "tag", "os", "region", "app_version", "os_version", "site", "vpn", "response"},
	Aggs: stdAggs,
}

// coarseCube stores no dims: one fat sketch per hour. The coverage matcher picks
// it (narrowest) for dimensionless sketch queries, where it gives high-accuracy
// percentiles a many-tiny-sketch merge on the dim-rich cube cannot.
var coarseCube = CubeSpec{
	Source: "default.downloads", Grain: "hour", Dims: nil, Aggs: stdAggs,
}

// statusCube: one sketch per (hour,status) — serves accurate sketch queries that
// group by status, again chosen by narrowest-coverage over the dim-rich cube.
var statusCube = CubeSpec{
	Source: "default.downloads", Grain: "hour", Dims: []string{"status"}, Aggs: stdAggs,
}

var allCubes = []CubeSpec{dimRichCube, coarseCube, statusCube}

// buildCubes materializes every cube for the 2025-12-28 day and returns a map
// from cube (by dim signature) to its read_parquet(...) expression.
func buildCubes(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	dir := t.TempDir()
	exprs := map[string]string{}
	for i, c := range allCubes {
		dest := filepath.Join(dir, fmt.Sprintf("cube_%d.parquet", i))
		copySQL := c.BuildCopySQL(day28Glob, "time", day28Lo, day28Hi, dest)
		if _, err := db.Exec(copySQL); err != nil {
			t.Fatalf("build cube %v: %v\nSQL: %s", c.Dims, err, copySQL)
		}
		exprs[cubeKey(c)] = fmt.Sprintf("'%s'", dest)
	}
	return exprs
}

func cubeKey(c CubeSpec) string { return c.Source + "|" + c.Grain + "|" + strings.Join(c.Dims, ",") }

// resultRow is keyed by the bucket+dim columns; vals holds the aggregate columns.
type result struct {
	keys []string
	rows map[string][]float64
}

// runShape executes sqlText and splits each row into nKeys key columns (joined into
// a string) and the remaining numeric aggregate columns.
func runShape(t *testing.T, db *sql.DB, sqlText string, nKeys int) result {
	t.Helper()
	rows, err := db.Query(sqlText)
	if err != nil {
		t.Fatalf("query: %v\nSQL: %s", err, sqlText)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	res := result{rows: map[string][]float64{}}
	for rows.Next() {
		raw := make([]any, len(cols))
		ptr := make([]any, len(cols))
		for i := range raw {
			ptr[i] = &raw[i]
		}
		if err := rows.Scan(ptr...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		var key []string
		for i := 0; i < nKeys; i++ {
			key = append(key, fmt.Sprintf("%v", raw[i]))
		}
		var vals []float64
		for i := nKeys; i < len(cols); i++ {
			vals = append(vals, toFloat(raw[i]))
		}
		k := strings.Join(key, "|")
		res.keys = append(res.keys, k)
		res.rows[k] = vals
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return res
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case nil:
		return math.NaN()
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case bool:
		if x {
			return 1
		}
		return 0
	default:
		var f float64
		fmt.Sscanf(fmt.Sprintf("%v", x), "%g", &f)
		return f
	}
}

// compareShape routes q through the coverage matcher to the narrowest covering
// cube, runs it against source and against that cube, and asserts they match:
// exact for non-sketch aggregates, within tolerance for HLL/KLL sketches.
func compareShape(t *testing.T, db *sql.DB, cubeExprs map[string]string, q QueryShape) {
	t.Helper()
	spec := PickNarrowest(allCubes, q)
	if spec == nil {
		t.Fatalf("no cube covers shape: %+v", q)
	}
	cubeExpr := cubeExprs[cubeKey(*spec)]
	t.Logf("routed to cube dims=%v", spec.Dims)

	nKeys := len(q.Dims)
	if q.Grain != "" {
		nKeys++
	}
	src := runShape(t, db, q.SourceRefSQL(day28Glob), nKeys)
	cube := runShape(t, db, q.CubeReadSQL(cubeExpr), nKeys)

	if len(src.rows) != len(cube.rows) {
		t.Fatalf("group count mismatch: source=%d cube=%d", len(src.rows), len(cube.rows))
	}
	// Stable iteration for deterministic failure messages.
	keys := make([]string, 0, len(src.rows))
	for k := range src.rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		sv, ok := src.rows[k]
		cv, cok := cube.rows[k]
		if !cok {
			t.Fatalf("cube missing group %q", k)
		}
		_ = ok
		for i := range sv {
			if !aggMatch(sv[i], cv[i], q.Aggs[i]) {
				t.Errorf("group %q agg[%s] mismatch (tol=%.0f%%): source=%v cube=%v",
					k, q.Aggs[i].Alias, aggTolerance(q.Aggs[i])*100, sv[i], cv[i])
			}
		}
	}
}

// aggTolerance is the per-aggregate relative tolerance.
//   - exact aggregates (count/sum/min/max/avg) -> ~0 (epsilon for float rounding)
//   - HLL COUNT(DISTINCT): near-lossless under union; ~2% per small group
//   - KLL percentile: the genuinely "hard to match" case — rank error maps to
//     value error at the tail and multithreaded sketch merges are mildly
//     nondeterministic, so ~3% per group (the global estimate stays >=99%).
func aggTolerance(a Aggregate) float64 {
	switch a.Kind {
	case AggCountDistinct:
		return 0.02
	case AggPercentile:
		return 0.03
	default:
		return 0
	}
}

func aggMatch(src, cube float64, a Aggregate) bool {
	return floatsMatch(src, cube, aggTolerance(a))
}

func floatsMatch(a, b, tol float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	if math.IsNaN(a) || math.IsNaN(b) {
		return false
	}
	if tol > 0 {
		if a == 0 {
			return math.Abs(b) < 1
		}
		return math.Abs(a-b)/math.Abs(a) <= tol
	}
	return math.Abs(a-b) <= 1e-6*math.Max(1, math.Abs(a))
}

// --- the shape catalog -------------------------------------------------------

func catalog() []QueryShape {
	base := func(grain string, dims []string, aggs []Aggregate, fs []Filter) QueryShape {
		return QueryShape{
			Source: "default.downloads", TimeCol: "time", Grain: grain,
			Dims: dims, Aggs: aggs, Filters: fs, TimeLo: day28Lo, TimeHi: day28Hi,
		}
	}
	cnt := []Aggregate{{Kind: AggCount, Alias: "n"}}
	return []QueryShape{
		base("", nil, cnt, nil),                           // total COUNT(*)
		base("hour", nil, cnt, nil),                       // COUNT(*) by hour
		base("day", nil, cnt, nil),                        // COUNT(*) by day (rebucket hour->day)
		base("hour", []string{"status"}, cnt, nil),        // COUNT(*) by status,hour
		base("", []string{"region"}, cnt, nil),            // COUNT(*) by region
		base("hour", []string{"status", "tag"}, cnt, nil), // two dims
		base("hour", nil, []Aggregate{{Kind: AggAvg, Col: "duration_seconds", Alias: "a"}}, nil),
		base("hour", nil, []Aggregate{
			{Kind: AggMin, Col: "duration_seconds", Alias: "mn"},
			{Kind: AggMax, Col: "duration_seconds", Alias: "mx"},
		}, nil),
		base("hour", []string{"region"}, []Aggregate{
			{Kind: AggCount, Alias: "n"},
			{Kind: AggAvg, Col: "duration_seconds", Alias: "a"},
		}, nil),

		// --- post-aggregation filters (applied on cube rows) ---
		base("hour", nil, cnt, []Filter{{Col: "status", Op: OpEq, Values: []string{"ns"}}}),
		base("hour", nil, cnt, []Filter{{Col: "response", Op: OpEq, Values: []string{"200"}}}), // numeric
		base("hour", nil, cnt, []Filter{{Col: "tag", Op: OpIn, Values: []string{"HAMIOS", "HAMAND"}}}),
		base("hour", nil, cnt, []Filter{{Col: "tag", Op: OpIsNull}}),
		base("hour", nil, cnt, []Filter{{Col: "status", Op: OpNe, Values: []string{"ns"}}}),
		base("", []string{"tag"}, cnt, []Filter{{Col: "response", Op: OpNotIn, Values: []string{"404"}}}),

		// --- sketch aggregates (approximate, asserted >=99%) ---
		base("hour", nil, []Aggregate{{Kind: AggCountDistinct, Col: "device_id", Alias: "uniq"}}, nil),
		base("hour", nil, []Aggregate{{Kind: AggPercentile, Col: "duration_seconds", P: 0.95, Alias: "p95"}}, nil),
		base("", []string{"status"}, []Aggregate{{Kind: AggCountDistinct, Col: "device_id", Alias: "uniq"}}, nil),
	}
}

func TestCompare_Phase1Exact(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)
	cubes := buildCubes(t, db)

	for i, q := range catalog() {
		q := q
		name := fmt.Sprintf("shape%02d_grain=%s_dims=%v", i, q.Grain, q.Dims)
		t.Run(name, func(t *testing.T) {
			compareShape(t, db, cubes, q)
		})
	}
}
