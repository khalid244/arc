package rollup

import "testing"

// Dimension-expression fixes from the perf/cube-router review: an integer
// constant at or beyond 2^53 is refused (the float64 JSON round trip would
// re-emit it rounded), and the special-syntax built-ins DuckDB parses with
// schema "main" (substring(.. FROM ..), trim(BOTH .. FROM ..), position(.. IN
// ..), LIKE .. ESCAPE) are accepted. Each case compares routed vs raw.

const dxTF = "time >= '2026-06-01T00:00:00Z' AND time < '2026-06-03T00:00:00Z'"

var dxCases = map[string]struct {
	sql       string
	mustServe bool
}{
	"constant beyond 2^53 in a key": {"SELECT status + 9007199254740993 AS big, count(*) AS n FROM events WHERE " + dxTF +
		" GROUP BY 1 ORDER BY 1", false},
	"negative constant beyond 2^53": {"SELECT status - 9007199254740993 AS big, count(*) AS n FROM events WHERE " + dxTF +
		" GROUP BY 1 ORDER BY 1", false},
	"substring / trim / position keys": {"SELECT substring(app_version, 1, 1) AS a, trim(BOTH '.' FROM app_version) AS b, position('.' IN app_version) AS c, count(*) AS n FROM events WHERE " + dxTF +
		" GROUP BY 1, 2, 3 ORDER BY 1, 2, 3", true},
	"LIKE ... ESCAPE filter": {"SELECT region, count(*) AS n FROM events WHERE " + dxTF + " AND app_version LIKE '1!.%' ESCAPE '!' GROUP BY 1 ORDER BY 1", true},
}

func TestDimExprReview_SameAsSource(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"app_version", "region", "status", "tag"},
		Aggs: []Aggregate{{Kind: AggCount}}}
	for wmName, wm := range map[string]string{"sealed": "", "merge": "2026-06-02 12:00:00+00"} {
		f := newQRFixture(t, spec, wm)
		f.r.DimExprs = true
		for name, c := range dxCases {
			t.Run(wmName+"/"+name, func(t *testing.T) {
				d := f.requireSameAsRaw(t, c.sql)
				if c.mustServe && !d.Served {
					t.Fatalf("want served from the cube, declined: %s", d.Reason)
				}
			})
		}
	}
}

// exactConst must refuse ±(2^53+1): it rounds to 2^53 in float64.
func TestReviewFixes_ExactConstBoundary(t *testing.T) {
	for _, c := range []struct {
		v    float64
		want bool
	}{{9007199254740991, true}, {9007199254740992, false}, {9007199254740993, false}, {-9007199254740993, false}} {
		n := map[string]any{"value": map[string]any{"value": c.v}}
		if got := exactConst(n); got != c.want {
			t.Errorf("exactConst(%v) = %v, want %v", c.v, got, c.want)
		}
	}
}
