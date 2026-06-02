package rollup

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

// realCard probes the actual distinct cardinality of a dimension over the test
// day, cached. This is the production-style signal: cardinality measured from the
// data the cube is built from, not pre-sampled heuristics.
func realCard(t *testing.T, db *sql.DB) CardinalityFunc {
	cache := map[string]int{}
	return func(_, dim string) int {
		if v, ok := cache[dim]; ok {
			return v
		}
		var n int
		q := fmt.Sprintf("SELECT count(DISTINCT %q) FROM read_parquet(%s, union_by_name=true) WHERE %q >= TIMESTAMPTZ '%s' AND %q < TIMESTAMPTZ '%s'",
			dim, day28Glob, "time", day28Lo, "time", day28Hi)
		if err := db.QueryRow(q).Scan(&n); err != nil {
			t.Fatalf("cardinality probe %s: %v", dim, err)
		}
		cache[dim] = n
		return n
	}
}

// TestPlan_CoversCatalogEndToEnd is the whole thesis in one test: feed the real
// query catalog as observed workload, let the planner choose cubes with ZERO
// configuration, materialize exactly those cubes, then prove every shape is both
// (a) covered by a planned cube and (b) numerically correct vs source.
func TestPlan_CoversCatalogEndToEnd(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)

	// 1. Observe the workload.
	w := NewWorkload()
	shapes := catalog()
	for _, q := range shapes {
		w.Record(q)
	}

	// 2. Plan cubes with no configuration (defaults only), real cardinalities.
	cubes := Plan(w, realCard(t, db), PlanConfig{})
	t.Logf("planner chose %d cubes from %d shapes:", len(cubes), len(shapes))
	for _, c := range cubes {
		t.Logf("  cube grain=%s dims=%v aggs=%d", c.Grain, c.Dims, len(c.Aggs))
	}

	// 3. Materialize exactly the planned cubes.
	dir := t.TempDir()
	exprs := map[string]string{}
	for i, c := range cubes {
		dest := filepath.Join(dir, fmt.Sprintf("planned_%d.parquet", i))
		if _, err := db.Exec(c.BuildCopySQL(day28Glob, "time", day28Lo, day28Hi, dest)); err != nil {
			t.Fatalf("build planned cube %v: %v", c.Dims, err)
		}
		exprs[cubeKeyOf(c)] = fmt.Sprintf("'%s'", dest)
	}

	// 4. Every shape must route to a planned cube and match source.
	covered := 0
	for i, q := range shapes {
		q := q
		t.Run(fmt.Sprintf("shape%02d", i), func(t *testing.T) {
			spec := PickNarrowest(cubes, q)
			if spec == nil {
				t.Fatalf("planner produced no cube covering shape %+v", q)
			}
			covered++
			cubeExpr := exprs[cubeKeyOf(*spec)]
			nKeys := len(q.Dims)
			if q.Grain != "" {
				nKeys++
			}
			src := runShape(t, db, q.SourceRefSQL(day28Glob), nKeys)
			cube := runShape(t, db, q.CubeReadSQL(cubeExpr), nKeys)
			if len(src.rows) != len(cube.rows) {
				t.Fatalf("group count mismatch: source=%d cube=%d", len(src.rows), len(cube.rows))
			}
			for k, sv := range src.rows {
				cv, ok := cube.rows[k]
				if !ok {
					t.Fatalf("cube missing group %q", k)
				}
				for j := range sv {
					if !aggMatch(sv[j], cv[j], q.Aggs[j]) {
						t.Errorf("group %q agg[%s]: source=%v cube=%v", k, q.Aggs[j].Alias, sv[j], cv[j])
					}
				}
			}
		})
	}
	t.Logf("coverage: %d/%d shapes served by planned cubes", covered, len(shapes))
}
