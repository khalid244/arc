package rollup

import "testing"

// A grouped column renamed in the select list (`event AS tool`) must keep its
// output name in the cube read. Before the fix the cube read emitted the bare
// column, so a top-level query returned the column under the wrong name and a
// base CTE whose outer SQL reads the alias failed with a Binder Error.
func TestDimAlias_KeepsOutputName(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region", "tag"}, Aggs: []Aggregate{{Kind: AggCount}}}
	tf := "time >= '2026-06-01T00:00:00Z' AND time < '2026-06-03T00:00:00Z'"
	cases := map[string]string{
		"top level, positional group":  "SELECT region AS r, count(*) AS n FROM events WHERE " + tf + " GROUP BY 1",
		"top level, group by column":   "SELECT region AS r, count(*) AS n FROM events WHERE " + tf + " GROUP BY region",
		"top level, quoted alias":      `SELECT region AS "Region name", count(*) AS n FROM events WHERE ` + tf + " GROUP BY 1",
		"alias next to a plain dim":    "SELECT tag, region AS r, count(*) AS n FROM events WHERE " + tf + " GROUP BY 1, 2",
		"order by alias under a limit": "SELECT region AS r, count(*) AS n FROM events WHERE " + tf + " GROUP BY 1 ORDER BY r DESC LIMIT 2",
		"filter on the renamed column": "SELECT region AS r, count(*) AS n FROM events WHERE " + tf + " AND region <> 'us' GROUP BY 1",
		"base CTE read by its alias":   "WITH b AS (SELECT region AS r, count(*) AS n FROM events WHERE " + tf + " GROUP BY 1) SELECT r, n FROM b",
		"base CTE with an hour bucket": "WITH b AS (SELECT date_trunc('hour', time) AS t, region AS r, count(*) AS n FROM events WHERE " + tf + " GROUP BY 1, 2) SELECT r, sum(n) AS n FROM b GROUP BY 1",
		"alias equal to the column":    "SELECT region AS region, count(*) AS n FROM events WHERE " + tf + " GROUP BY 1",
		"alias, filter on another dim": "SELECT region AS a, count(*) AS n FROM events WHERE " + tf + " AND tag IS NOT NULL GROUP BY 1",
	}
	// GROUP BY <alias> declines: DuckDB binds a GROUP BY name to a same-named
	// column first, so the cube cannot tell an alias from a column (see
	// TestReviewFixes_GroupByNameBindsToTheColumn); it then runs from source.
	declined := "SELECT region AS r, count(*) AS n FROM events WHERE " + tf + " GROUP BY r"
	for wmName, wm := range map[string]string{"sealed": "", "merge": "2026-06-02 12:00:00+00"} {
		f := newQRFixture(t, spec, wm)
		if d := f.requireSameAsRaw(t, declined); d.Served {
			t.Fatalf("GROUP BY alias served:\n%s", d.SQL)
		}
		for name, q := range cases {
			t.Run(wmName+"/"+name, func(t *testing.T) {
				if d := f.requireSameAsRaw(t, q); !d.Served {
					t.Fatalf("want served from the cube, declined: %s", d.Reason)
				}
			})
		}
	}
}
