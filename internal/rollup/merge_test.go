package rollup

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

const watermark28 = "2025-12-28 18:00:00+00" // cube sealed here; [18:00,24:00) is fresh

// buildSealedCubes materializes each cube over [day28Lo, watermark28) only — the
// sealed portion — so merge-on-read must patch the [watermark,hi) tail from source.
func buildSealedCubes(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	dir := t.TempDir()
	exprs := map[string]string{}
	for i, c := range allCubes {
		dest := filepath.Join(dir, fmt.Sprintf("sealed_%d.parquet", i))
		copySQL := c.BuildCopySQL(day28Glob, "time", day28Lo, watermark28, dest)
		if _, err := db.Exec(copySQL); err != nil {
			t.Fatalf("build sealed cube %v: %v", c.Dims, err)
		}
		exprs[cubeKey(c)] = fmt.Sprintf("'%s'", dest)
	}
	return exprs
}

// compareMerge routes q to a cube, builds the merge-on-read SQL (cube + fresh +
// head) and compares it against the full-source reference over [lo,hi).
func compareMerge(t *testing.T, db *sql.DB, cubeExprs map[string]string, q QueryShape) {
	t.Helper()
	spec := PickNarrowest(allCubes, q)
	if spec == nil {
		t.Fatalf("no cube covers shape: %+v", q)
	}
	cubeExpr := cubeExprs[cubeKey(*spec)]
	mergeSQL, ok := q.MergeReadSQL(*spec, cubeExpr, StaticGlob(day28Glob), watermark28)
	if !ok {
		t.Fatalf("merge emit failed for %+v", q)
	}
	nKeys := len(q.Dims)
	if q.Grain != "" {
		nKeys++
	}
	src := runShape(t, db, q.SourceRefSQL(day28Glob), nKeys)
	cube := runShape(t, db, mergeSQL, nKeys)

	if len(src.rows) != len(cube.rows) {
		t.Fatalf("group count mismatch: source=%d merge=%d\nSQL: %s", len(src.rows), len(cube.rows), mergeSQL)
	}
	for k, sv := range src.rows {
		cv, ok := cube.rows[k]
		if !ok {
			t.Fatalf("merge missing group %q", k)
		}
		for i := range sv {
			if !aggMatch(sv[i], cv[i], q.Aggs[i]) {
				t.Errorf("group %q agg[%s] mismatch: source=%v merge=%v", k, q.Aggs[i].Alias, sv[i], cv[i])
			}
		}
	}
}

func mergeCatalog() []QueryShape {
	mk := func(lo, hi, grain string, dims []string, aggs []Aggregate, fs []Filter) QueryShape {
		return QueryShape{Source: "default.downloads", TimeCol: "time", Grain: grain,
			Dims: dims, Aggs: aggs, Filters: fs, TimeLo: lo, TimeHi: hi}
	}
	cnt := []Aggregate{{Kind: AggCount, Alias: "n"}}
	full := func(grain string, dims []string, aggs []Aggregate, fs []Filter) QueryShape {
		return mk(day28Lo, day28Hi, grain, dims, aggs, fs)
	}
	return []QueryShape{
		full("hour", nil, cnt, nil),                // stored[00,18)+fresh[18,24) by hour
		full("day", nil, cnt, nil),                 // 1 day bucket spanning the boundary
		full("hour", []string{"status"}, cnt, nil), // dims across boundary
		full("", nil, cnt, nil),                    // grand total across boundary
		full("hour", []string{"region"}, cnt, []Filter{{Col: "status", Op: OpEq, Values: []string{"ns"}}}),
		full("hour", nil, []Aggregate{{Kind: AggAvg, Col: "duration_seconds", Alias: "a"}}, nil),
		full("hour", nil, []Aggregate{
			{Kind: AggMin, Col: "duration_seconds", Alias: "mn"},
			{Kind: AggMax, Col: "duration_seconds", Alias: "mx"},
		}, nil),
		full("hour", nil, []Aggregate{{Kind: AggCountDistinct, Col: "device_id", Alias: "u"}}, nil),              // Theta across boundary
		full("hour", nil, []Aggregate{{Kind: AggPercentile, Col: "duration_seconds", P: 0.95, Alias: "p"}}, nil), // KLL across boundary
		// mid-bucket leading edge -> head patch [00:30,01:00) from source
		mk("2025-12-28 00:30:00+00", day28Hi, "hour", nil, cnt, nil),
		mk("2025-12-28 00:30:00+00", day28Hi, "", nil, cnt, nil),
	}
}

func TestCompare_MergeOnRead(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)
	cubes := buildSealedCubes(t, db)

	for i, q := range mergeCatalog() {
		q := q
		name := fmt.Sprintf("merge%02d_%s..%s_grain=%s_dims=%v", i, q.TimeLo[11:16], q.TimeHi[11:16], q.Grain, q.Dims)
		t.Run(name, func(t *testing.T) {
			compareMerge(t, db, cubes, q)
		})
	}
}
