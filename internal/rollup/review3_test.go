package rollup

import (
	"fmt"
	"testing"
)

// Round-3 review regressions: the Grafana epoch bucket is recognised in the
// parse tree (the SQL text is never rewritten, so the same text inside a quoted
// identifier, a $$ string or a comment is left alone), and forms the source
// rejects are declined. Each case must be served (and equal source) or declined.

const epochText = "to_timestamp((epoch_ns(time) // 1000000000 // 3600) * 3600)"

var r3Cases = map[string]r2Case{
	"epoch bucket text in a quoted alias":  {`SELECT tag AS "` + epochText + `", count(*) AS n FROM events WHERE ` + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"epoch bucket text in a comment":       {"SELECT tag, count(*) AS n FROM events WHERE " + r2TF + " -- " + epochText + "\nGROUP BY 1 ORDER BY 1", true, false},
	"epoch bucket text in a $$ string":     {"SELECT tag, count(*) AS n FROM events WHERE " + r2TF + " AND msg IS DISTINCT FROM $$" + epochText + "$$ GROUP BY 1 ORDER BY 1", true, false},
	"epoch bucket text in a block comment": {"SELECT tag, /* " + epochText + " */ count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"epoch bucket, aliased":                {"SELECT " + epochText + " AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"epoch bucket, unaliased":              {"SELECT " + epochText + ", count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"two epoch widths":                     {"SELECT " + epochText + " AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1, to_timestamp((epoch_ns(time) // 1000000000 // 7200) * 7200) ORDER BY 1", false, false},
	"time_bucket interval":                 {"SELECT time_bucket(INTERVAL '1 hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"time_bucket varchar":                  {"SELECT time_bucket('1 day'::VARCHAR, time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"time_bucket unit only":                {"SELECT time_bucket(INTERVAL 'hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"order by a printed name":              {`SELECT region, count(*) FROM events WHERE ` + r2TF + ` GROUP BY 1 ORDER BY "count_star()" DESC LIMIT 1`, false, false},
}

func TestReview3_ServeOrDecline(t *testing.T) {
	for wmName, wm := range map[string]string{"sealed": "", "merge": "2026-06-02 12:00:00+00"} {
		f := newQRFixtureBody(t, untiedSpec(), wm, untiedSourceBody)
		for name, c := range r3Cases {
			t.Run(fmt.Sprintf("%s/%s", wmName, name), func(t *testing.T) {
				if !c.serve {
					if d := f.r.Route(c.sql); d.Served {
						t.Fatalf("want declined, served from %s:\n%s", d.Cube, d.SQL)
					}
					return
				}
				if d := f.requireSameAsRaw(t, c.sql); !d.Served {
					t.Fatalf("want served, declined: %s", d.Reason)
				}
			})
		}
	}
}
