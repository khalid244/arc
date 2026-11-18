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
	"epoch bucket text in a $$ string":     {"SELECT tag, count(*) AS n FROM events WHERE " + r2TF + " AND msg < $$" + epochText + "$$ GROUP BY 1 ORDER BY 1", true, true},
	"epoch bucket text in a block comment": {"SELECT tag, /* " + epochText + " */ count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"epoch bucket, aliased":                {"SELECT " + epochText + " AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"epoch bucket, unaliased":              {"SELECT " + epochText + ", count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"two epoch widths":                     {"SELECT " + epochText + " AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1, to_timestamp((epoch_ns(time) // 1000000000 // 7200) * 7200) ORDER BY 1", false, false},
	"time_bucket interval":                 {"SELECT time_bucket(INTERVAL '1 hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"time_bucket varchar":                  {"SELECT time_bucket('1 day'::VARCHAR, time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"time_bucket unit only":                {"SELECT time_bucket(INTERVAL 'hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	// DISTINCT inside a bucket expression is an error at source.
	"to_timestamp(DISTINCT ...)": {"SELECT to_timestamp(DISTINCT (epoch_ns(time) // 1000000000 // 3600) * 3600) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"epoch_ns(DISTINCT time)":    {"SELECT to_timestamp((epoch_ns(DISTINCT time) // 1000000000 // 3600) * 3600) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"date_trunc(DISTINCT ...)":   {"SELECT date_trunc(DISTINCT 'hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"time_bucket(DISTINCT ...)":  {"SELECT time_bucket(DISTINCT INTERVAL '1 hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	// A repeated bucket must be the same source text: INTERVAL '1 hour' and
	// '1 hour'::INTERVAL parse and print alike but do not bind as the same
	// expression at source.
	"group by the bucket spelled differently": {"SELECT time_bucket(INTERVAL '1 hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY time_bucket('1 hour'::INTERVAL, time) ORDER BY 1", false, false},
	"order by the bucket spelled differently": {"SELECT time_bucket(INTERVAL '1 hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY time_bucket('1 hour'::INTERVAL, time)", false, false},
	"group by the same bucket text":           {"SELECT time_bucket(INTERVAL '1 hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY time_bucket(INTERVAL '1 hour', time) ORDER BY 1", true, false},
	"group and order by the same date_trunc":  {"SELECT date_trunc('hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY date_trunc('hour', time) ORDER BY date_trunc('hour', time) DESC LIMIT 3", true, false},
	"order by a printed name":                 {`SELECT region, count(*) FROM events WHERE ` + r2TF + ` GROUP BY 1 ORDER BY "count_star()" DESC LIMIT 1`, false, false},
}

func TestReview3_ServeOrDecline(t *testing.T) {
	for wmName, wm := range map[string]string{"sealed": "", "merge": "2026-06-02 12:00:00+00"} {
		for _, on := range []bool{false, true} {
			f := newQRFixtureBody(t, untiedSpec(), wm, untiedSourceBody)
			f.r.DimExprs = on
			for name, c := range r3Cases {
				if c.dimExprs && !on {
					continue
				}
				t.Run(fmt.Sprintf("%s/dimexprs=%v/%s", wmName, on, name), func(t *testing.T) {
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
}
