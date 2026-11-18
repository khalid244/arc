package rollup

import "testing"

// Regressions from the perf/cube-router review. Every case compares the routed
// result with the source result: column names and order, column types, rows,
// and row order when the query orders (requireSameAsRaw). A case may be served
// or declined; a declined case runs from source and so always matches. Cases in
// mustServe must also be served.

const rvTF = "time >= '2026-06-01T00:00:00Z' AND time < '2026-06-03T00:00:00Z'"

func rvSpec() CubeSpec {
	return CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"app_version", "region", "status", "tag"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggCountDistinct, Col: "bytes"}}}
}

type rvCase struct {
	sql       string
	dimExprs  bool
	mustServe bool
}

var rvCases = map[string]rvCase{
	// 1. a computed key before a plain dim: positional ORDER BY and the output
	// column order follow the user's select list.
	"computed key first, positional order, limit": {"SELECT LEFT(app_version, 1) AS major, region, count(*) AS n FROM events WHERE " + rvTF +
		" GROUP BY 1, 2 ORDER BY 1 DESC, 2 LIMIT 2", true, true},
	"computed key first, full order": {"SELECT LEFT(app_version, 1) AS major, region, count(*) AS n FROM events WHERE " + rvTF +
		" GROUP BY 1, 2 ORDER BY 1 DESC, 2", true, true},
	// 6. an aggregate before a dim (flag off).
	"aggregate first, positional order, limit": {"SELECT count(*) AS n, app_version FROM events WHERE " + rvTF +
		" GROUP BY 2 ORDER BY 2 DESC LIMIT 1", false, true},
	"aggregate first, column order": {"SELECT count(DISTINCT bytes) AS u, count(*) AS n, region FROM events WHERE " + rvTF +
		" GROUP BY 3 ORDER BY 3", false, true},
	// 2. a dim aliased to another selected dim's column name (flag off).
	"alias equal to another dim's column": {"SELECT tag AS region, region AS x, count(*) AS n FROM events WHERE " + rvTF +
		" GROUP BY 1, 2 ORDER BY region DESC, x LIMIT 2", false, false},
	"order by an unclaimed underlying column": {"SELECT region AS r, count(*) AS n FROM events WHERE " + rvTF +
		" GROUP BY 1 ORDER BY region DESC LIMIT 2", false, true},
	// 4. a grouped column that is not selected, hidden by two identical keys.
	"identical keys plus an unselected group column": {"SELECT upper(region) AS a, upper(region) AS b, count(*) AS n FROM events WHERE " + rvTF +
		" GROUP BY 1, tag", true, false},
	"duplicate dims plus an unselected group column": {"SELECT region, region AS r2, count(*) AS n FROM events WHERE " + rvTF +
		" GROUP BY 1, tag", false, false},
	"duplicate dims, grouped once": {"SELECT region, region AS r2, count(*) AS n FROM events WHERE " + rvTF +
		" GROUP BY 1 ORDER BY 1", false, true},
	// 5. grouping sets keep their subtotal rows.
	"rollup":        {"SELECT region, tag, count(*) AS n FROM events WHERE " + rvTF + " GROUP BY ROLLUP (region, tag) ORDER BY 1, 2, 3", false, false},
	"cube":          {"SELECT region, tag, count(*) AS n FROM events WHERE " + rvTF + " GROUP BY CUBE (region, tag) ORDER BY 1, 2, 3", false, false},
	"grouping sets": {"SELECT region, count(*) AS n FROM events WHERE " + rvTF + " GROUP BY GROUPING SETS ((region), ()) ORDER BY 1, 2", false, false},
	// 7. ORDER BY a conditional sum must pick the aggregate with the same branches.
	"order by the second of two conditional sums": {"SELECT app_version, sum(CASE WHEN tag = 'x' THEN 1 ELSE 0 END) AS a, sum(CASE WHEN tag = 'x' THEN 0 ELSE 1 END) AS b FROM events WHERE " + rvTF +
		" GROUP BY 1 ORDER BY sum(CASE WHEN tag = 'x' THEN 0 ELSE 1 END) DESC, app_version LIMIT 1", false, true},
	// GROUP BY a name binds to a same-named column before an alias at source.
	"group by a renamed dim's alias": {"SELECT region AS r, count(*) AS n FROM events WHERE " + rvTF + " GROUP BY r", false, false},
	// NULLS FIRST / LAST is part of the ordering.
	"explicit nulls first": {"SELECT tag, count(*) AS n FROM events WHERE " + rvTF + " GROUP BY 1 ORDER BY 1 NULLS FIRST", false, true},
	"explicit nulls last":  {"SELECT tag, count(*) AS n FROM events WHERE " + rvTF + " GROUP BY 1 ORDER BY 1 DESC NULLS LAST", false, true},
	// An ORDER BY the cube read cannot reproduce declines even without LIMIT:
	// row order is part of the result.
	"order by an unmappable expression": {"SELECT region, count(*) AS n FROM events WHERE " + rvTF + " GROUP BY 1 ORDER BY length(region), 1", false, false},
}

func TestReviewFixes_SameAsSource(t *testing.T) {
	for wmName, wm := range map[string]string{"sealed": "", "merge": "2026-06-02 12:00:00+00"} {
		for _, on := range []bool{false, true} {
			f := newQRFixture(t, rvSpec(), wm)
			f.r.DimExprs = on
			for name, c := range rvCases {
				if c.dimExprs && !on {
					continue
				}
				t.Run(wmName+"/"+map[bool]string{false: "off", true: "on"}[on]+"/"+name, func(t *testing.T) {
					d := f.requireSameAsRaw(t, c.sql)
					if c.mustServe && !d.Served {
						t.Fatalf("want served from the cube, declined: %s", d.Reason)
					}
				})
			}
		}
	}
}

// `region AS tag ... GROUP BY tag` groups by the COLUMN tag at source, where it
// is a Binder Error (region is not grouped); the cube read must not answer it.
func TestReviewFixes_GroupByNameBindsToTheColumn(t *testing.T) {
	f := newQRFixture(t, rvSpec(), "")
	for _, on := range []bool{false, true} {
		f.r.DimExprs = on
		q := "SELECT region AS tag, count(*) AS n FROM events WHERE " + rvTF + " GROUP BY tag"
		if d := f.r.Route(q); d.Served {
			t.Fatalf("dim_expressions=%v: served a query source rejects:\n%s", on, d.SQL)
		}
	}
}
