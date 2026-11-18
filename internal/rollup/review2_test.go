package rollup

import (
	"fmt"
	"testing"
)

// Round-2 review regressions. Every case states whether it must be SERVED (and
// then equal the source: columns, column order, types, rows, row order) or
// DECLINED (it then runs from source; many of these are rejected by the source
// itself, so serving them would answer a query that should fail).

// untiedSourceBody gives every region and every tag a different row count, so
// ORDER BY <aggregate> and LIMIT select one well-defined row, and holds NULL,
// ”, mixed-case and multibyte values for the string functions.
func untiedSourceBody(day string) string {
	return fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 00:00:00+00' + to_hours(i %% 24) + to_minutes(i %% 60) + to_seconds(i %% 37) AS "time",
		CASE WHEN i %% 12 < 6 THEN 'eu' WHEN i %% 12 < 9 THEN 'us' WHEN i %% 12 < 11 THEN 'ünï' ELSE 'EU' END AS region,
		CASE WHEN i %% 10 < 5 THEN 'x' WHEN i %% 10 < 8 THEN 'y' WHEN i %% 10 < 9 THEN '' ELSE NULL END AS tag,
		['  pad  ','100%%','a_b','日本語テキスト','ÀÉÎ','','émoji😀x', NULL][1 + (i // 3) %% 8] AS msg,
		(i * 10)::BIGINT AS bytes,
		(i %% 7)::DOUBLE / 4 AS lat,
		[200, 404, 500][1 + (i // 5) %% 3]::INTEGER AS status,
		'u' || (i %% 50) AS uid
		FROM range(0, 240) t(i)`, day)
}

func untiedSpec() CubeSpec {
	return CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"msg", "region", "status", "tag"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Col: "bytes"}, {Kind: AggAvg, Col: "bytes"}, {Kind: AggMin, Col: "bytes"},
			{Kind: AggMax, Col: "bytes"}, {Kind: AggCountCol, Col: "bytes"}, {Kind: AggSum, Col: "lat"}, {Kind: AggCountDistinct, Col: "uid"},
			{Kind: AggPercentile, Col: "bytes", P: 0.5}, {Kind: AggPercentile, Col: "lat", P: 0.5}}}
}

const r2TF = "time >= '2026-06-01T00:00:00Z' AND time < '2026-06-03T00:00:00Z'"

type r2Case struct {
	sql      string
	serve    bool // true: must be served and equal source; false: must decline
	dimExprs bool // run only with dim_expressions on
}

var r2Cases = map[string]r2Case{
	// 1. two buckets of different grain / function
	"group by two bucket grains":   {"SELECT date_trunc('day', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY date_trunc('day', time), date_trunc('hour', time) ORDER BY 1, 2", false, false},
	"group by bucket and finer":    {"SELECT date_trunc('day', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1, time_bucket(INTERVAL '1 hour', time) ORDER BY 1, 2", false, false},
	"week vs time_bucket week":     {"SELECT date_trunc('week', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY time_bucket(INTERVAL '1 week', time) ORDER BY 1", false, false},
	"order by another grain":       {"SELECT date_trunc('hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY date_trunc('day', time), 2", false, false},
	"order by the same bucket":     {"SELECT date_trunc('hour', time) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY date_trunc('hour', time) DESC LIMIT 3", true, false},
	"epoch bucket":                 {"SELECT to_timestamp((epoch_ns(time) // 1000000000 // 7200) * 7200) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"epoch bucket, bad multiplier": {"SELECT to_timestamp((epoch_ns(time) // 1000000000 // 7200) * 3600) AS t, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"epoch bucket in a literal":    {"SELECT tag, count(*) AS n FROM events WHERE " + r2TF + " AND msg = 'to_timestamp((epoch_ns(time) // 1000000000 // 3600) * 3600)' GROUP BY 1 ORDER BY 1", true, false},
	// 2. ORDER BY names are case-insensitive
	"alias differs by case": {"SELECT region AS x, tag AS REGION, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1, 2 ORDER BY region, 1", true, false},
	"agg alias shadows dim": {"SELECT tag AS r, count(*) AS Tag FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY tag DESC LIMIT 1", true, false},
	"two names by case":     {"SELECT region AS a, tag AS A, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1, 2 ORDER BY a LIMIT 1", false, false},
	// 3. a DOUBLE constant is not re-emitted as a DECIMAL
	"double constant":  {"SELECT region, count(*) AS n FROM events WHERE " + r2TF + " AND status * 1.1e0 = 220 GROUP BY 1 ORDER BY 1", false, true},
	"decimal constant": {"SELECT region, count(*) AS n FROM events WHERE " + r2TF + " AND status * 1.1 = 220 GROUP BY 1 ORDER BY 1", true, true},
	// 4. LIMIT / OFFSET forms
	"limit 0":           {"SELECT region, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1 LIMIT 0", false, false},
	"limit offset":      {"SELECT region, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1 LIMIT 2 OFFSET 2", false, false},
	"offset":            {"SELECT region, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1 OFFSET 2", false, false},
	"limit percent":     {"SELECT region, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1 LIMIT 50%", false, false},
	"limit expression":  {"SELECT region, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1 LIMIT 1+1", false, false},
	"limit decimal":     {"SELECT region, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 2 DESC LIMIT 2.6", false, false},
	"limit by agg (ok)": {"SELECT region, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 2 DESC LIMIT 2", true, false},
	// 5. DISTINCT
	"distinct on":     {"SELECT DISTINCT ON (region) region, tag, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1, 2 ORDER BY 1, 2", false, false},
	"select distinct": {"SELECT DISTINCT region, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	// 6. aggregate DISTINCT and arity
	"sum distinct":            {"SELECT region, sum(DISTINCT bytes) AS s FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"avg distinct":            {"SELECT region, avg(DISTINCT bytes) AS s FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"max two args":            {"SELECT region, max(bytes, 2) AS s FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"count distinct two args": {"SELECT region, count(DISTINCT uid, tag) AS s FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"order by sum distinct":   {"SELECT region, sum(bytes) AS s FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY sum(DISTINCT bytes) DESC, 1", false, false},
	"min / max / count(col)":  {"SELECT region, min(bytes) AS mn, max(bytes) AS mx, count(bytes) AS c FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	// 7. sum(CASE) without ELSE
	"sum case no else, none": {"SELECT region, sum(CASE WHEN tag = 'zzz' THEN 1 END) AS s FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"sum case else null":     {"SELECT region, sum(CASE WHEN tag = 'x' THEN 1 ELSE NULL END) AS s FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"sum case col, no else":  {"SELECT region, sum(CASE WHEN tag = 'zzz' THEN bytes END) AS s FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"sum case else 0":        {"SELECT region, sum(CASE WHEN tag = 'zzz' THEN 1 ELSE 0 END) AS s FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"order by else-null sum": {"SELECT region, sum(CASE WHEN tag = 'x' THEN 1 END) AS a, sum(CASE WHEN tag = 'x' THEN 1 ELSE 0 END) AS b FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY sum(CASE WHEN tag = 'x' THEN 1 END) DESC LIMIT 2", true, false},
	// 8. percentiles of any kind run from source
	"quantile over bigint":  {"SELECT region, quantile_cont(bytes, 0.5) AS p FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"quantile over double":  {"SELECT region, quantile_cont(lat, 0.95) AS p FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"plain percentile_cont": {"SELECT region, percentile_cont(lat, 0.5) AS p FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"quantile string p":     {"SELECT region, quantile_cont(lat, '0.5') AS p FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"percentile in a CTE":   {"WITH b AS (SELECT region, quantile_cont(lat, 0.5) AS p FROM events WHERE " + r2TF + " GROUP BY 1) SELECT * FROM b ORDER BY 1", false, false},
	"approx_quantile":       {"SELECT region, approx_quantile(bytes, 0.5) AS p FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	// 9. TABLESAMPLE and column aliases on the table
	"tablesample":          {"SELECT region, count(*) AS n FROM events TABLESAMPLE RESERVOIR(10 ROWS) WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"tablesample percent":  {"SELECT region, sum(bytes) AS s FROM events TABLESAMPLE 50 PERCENT (bernoulli) WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	"table column aliases": {"SELECT region, count(*) AS n FROM events AS e(region, time) WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", false, false},
	// 10. unaliased items keep DuckDB's names
	"unaliased aggregates": {"SELECT region, count(*), sum(bytes), avg(bytes) FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"unaliased bucket":     {"SELECT date_trunc('day', time), count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 1", true, false},
	"unaliased in a CTE":   {"WITH base AS (SELECT region, count(*) FROM events WHERE " + r2TF + " GROUP BY 1) SELECT region, \"count_star()\" AS n FROM base ORDER BY 1", true, false},
	// 12. positions must be integer constants
	"order by a string": {"SELECT region, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY '2', 1", false, false},
	"order by 0.2":      {"SELECT region, count(*) AS n FROM events WHERE " + r2TF + " GROUP BY 1 ORDER BY 0.2 DESC", false, false},
	// string functions over NULL, '' and multibyte values
	"string keys": {"SELECT LEFT(msg, 3) AS l, upper(msg) AS u, length(msg) AS len, substring(msg, 2, 2) AS s, trim(msg) AS tr, count(*) AS n FROM events WHERE " + r2TF +
		" GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 2, 3, 4, 5", true, true},
	"string filters": {"SELECT msg, count(*) AS n FROM events WHERE " + r2TF + " AND (msg ILIKE '%À%' OR msg LIKE '100!%' ESCAPE '!' OR msg = '' OR msg IS NULL) GROUP BY 1 ORDER BY 1", true, true},
}

func TestReview2_ServeOrDecline(t *testing.T) {
	for wmName, wm := range map[string]string{"sealed": "", "merge": "2026-06-02 12:00:00+00"} {
		for _, on := range []bool{false, true} {
			f := newQRFixtureBody(t, untiedSpec(), wm, untiedSourceBody)
			f.r.DimExprs = on
			for name, c := range r2Cases {
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

// Unaligned ranges with a production-like watermark (now - grace, past every
// cube day) and with a fully sealed router: the partial edge buckets come
// from source, the whole buckets from the cube, and the result equals source.
func TestReview2_UnalignedRanges(t *testing.T) {
	ranges := [][2]string{
		{"2026-06-01T05:20:00Z", "2026-06-02T12:30:00Z"},
		{"2026-06-01T00:00:00Z", "2026-06-02T12:30:00Z"},
		{"2026-06-01T05:20:00Z", "2026-06-02T12:00:00Z"},
		{"2026-06-01T05:20:00Z", "2026-06-01T05:40:00Z"},
	}
	for wmName, wm := range map[string]string{"sealed": "", "prod-like": "2026-06-03 01:00:00+00"} {
		f := newQRFixtureBody(t, untiedSpec(), wm, untiedSourceBody)
		for _, rg := range ranges {
			tf := fmt.Sprintf("time >= '%s' AND time < '%s'", rg[0], rg[1])
			for _, q := range []string{
				"SELECT region, count(*) AS n, sum(bytes) AS s, count(DISTINCT uid) AS u FROM events WHERE " + tf + " GROUP BY 1 ORDER BY 1",
				"SELECT date_trunc('hour', time) AS t, count(*) AS n FROM events WHERE " + tf + " GROUP BY 1 ORDER BY 1",
				"SELECT count(*) AS n FROM events WHERE " + tf,
			} {
				t.Run(wmName+"/"+rg[0]+"/"+rg[1]+"/"+q[:30], func(t *testing.T) {
					if d := f.requireSameAsRaw(t, q); !d.Served {
						t.Fatalf("want served, declined: %s", d.Reason)
					}
				})
			}
		}
	}
}
