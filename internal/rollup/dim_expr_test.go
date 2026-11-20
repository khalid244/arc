package rollup

import (
	"strings"
	"testing"
)

const dimExprTF = "time >= '2026-06-01T00:00:00Z' AND time < '2026-06-03T00:00:00Z'"

// dimExprSpec is a cube storing three dims, the row count and a Theta sketch of
// bytes (240 distinct values a day, far below the sketch's exact-mode capacity,
// so COUNT(DISTINCT) compares exactly against source).
func dimExprSpec() CubeSpec {
	return CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"app_version", "region", "tag"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggCountDistinct, Col: "bytes"}}}
}

// dimExprServed are aggregates whose select list, GROUP BY or WHERE hold scalar
// expressions over stored dims: the dashboard shapes (CASE over ILIKE classes,
// LEFT/regexp_replace message keys, `lib = 'x' AS server`, a CASE of constants
// compared to a dim, NOT(COALESCE(CAST(..)) IN ..), an OR across dims). Each must
// be served from the cube and return exactly the source result.
var dimExprServed = map[string]string{
	"CASE over ILIKE in select": "SELECT CASE WHEN tag ILIKE 'x%' THEN 'X' WHEN tag IS NULL THEN 'none' ELSE 'other' END AS k, count(*) AS n, count(DISTINCT bytes) AS u FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"LEFT + regexp_replace key": "SELECT region, regexp_replace(LEFT(app_version, 3), '[0-9]$', '#', 'g') AS v, count(*) AS n FROM events WHERE " + dimExprTF + " GROUP BY 1, 2",
	"boolean key":               "SELECT region = 'eu' AS is_eu, count(DISTINCT bytes) AS u FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"group by the expression":   "SELECT upper(region) AS r, count(*) AS n FROM events WHERE " + dimExprTF + " GROUP BY upper(region)",
	"unaliased expression":      "SELECT upper(region), count(*) AS n FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"with an hour bucket":       "SELECT date_trunc('hour', time) AS t, lower(region) AS r, count(*) AS n FROM events WHERE " + dimExprTF + " GROUP BY 1, 2",
	"order by alias, limit":     "SELECT LEFT(app_version, 1) AS major, count(*) AS n FROM events WHERE " + dimExprTF + " GROUP BY 1 ORDER BY n DESC, major LIMIT 2",
	"ILIKE in WHERE":            "SELECT region, count(*) AS n FROM events WHERE " + dimExprTF + " AND tag ILIKE '%X%' GROUP BY 1",
	"NOT ILIKE in WHERE":        "SELECT region, count(*) AS n FROM events WHERE " + dimExprTF + " AND tag NOT ILIKE '%X%' GROUP BY 1",
	"CASE of constants in WHERE": "SELECT tag, count(*) AS n FROM events WHERE " + dimExprTF +
		" AND region = CASE 'posthog-ios' WHEN 'posthog-ios' THEN 'eu' ELSE 'us' END GROUP BY 1",
	"NOT COALESCE CAST IN": "SELECT region, count(DISTINCT bytes) AS u FROM events WHERE " + dimExprTF +
		" AND NOT (COALESCE(CAST(tag AS VARCHAR),'') IN ('x','1')) GROUP BY 1",
	"OR across dims in WHERE": "SELECT region, count(*) AS n FROM events WHERE " + dimExprTF + " AND (region = 'eu' OR tag = 'y') GROUP BY 1",
	"starts_with in WHERE":    "SELECT region, count(*) AS n FROM events WHERE " + dimExprTF + " AND NOT starts_with(app_version, '1.') GROUP BY 1",
	"expression and plain filter": "SELECT CASE WHEN app_version LIKE '1.%' THEN 'one' ELSE 'other' END AS line, count(*) AS n FROM events WHERE " + dimExprTF +
		" AND region IN ('eu','ap') AND tag ILIKE 'y' GROUP BY 1",
	"grand total, expression filter": "SELECT count(*) AS n, count(DISTINCT bytes) AS u FROM events WHERE " + dimExprTF + " AND lower(region) <> 'us'",
	"base CTE, outer reads the alias": "WITH base AS (SELECT CASE WHEN tag ILIKE 'x' THEN 'x' ELSE 'rest' END AS cls, count(DISTINCT bytes) AS users, count(*) AS events FROM events WHERE " + dimExprTF +
		" AND region <> 'ap' GROUP BY 1) SELECT cls, users, events, round(1.0*events/users, 2) AS per FROM base",
	// COUNT(DISTINCT CASE WHEN <dim predicate> THEN col END): the predicate has one
	// value per cube row, so the sketch union of the rows where it holds is the
	// distinct count of the source rows where it holds (crash-free panels).
	"conditional distinct":          "SELECT region, count(DISTINCT bytes) AS u, count(DISTINCT CASE WHEN tag = 'x' AND app_version = '7' THEN bytes END) AS ux FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"conditional distinct, ILIKE":   "SELECT region, count(DISTINCT CASE WHEN tag ILIKE 'X' THEN bytes ELSE NULL END) AS ux FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"conditional distinct, no rows": "SELECT region, count(DISTINCT CASE WHEN tag = 'nope' THEN bytes END) AS ux FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"crash-free ratio in a base CTE": "WITH base AS (SELECT app_version, count(DISTINCT bytes) AS users, count(DISTINCT CASE WHEN region = 'eu' AND tag IS NOT NULL THEN bytes END) AS crashed FROM events WHERE " + dimExprTF +
		" GROUP BY 1) SELECT app_version, users, crashed, round(100.0*(1 - 1.0*crashed/users), 3) AS pct FROM base",
	"base CTE, hour bucket + ILIKE filter": "WITH base AS (SELECT date_trunc('hour', time) AS time, region, count(DISTINCT bytes) AS users FROM events WHERE " + dimExprTF +
		" AND (tag ILIKE '%x%' OR app_version IN ('7','007')) GROUP BY 1, 2) SELECT time, region, users FROM base",
}

// dimExprDeclined are shapes the expression path must still refuse: an expression
// over the time column, a function outside the deterministic allow-list, a
// subquery, an aggregate nested in a key, and a column the cube does not store.
var dimExprDeclined = map[string]string{
	"time column in a key":           "SELECT CASE WHEN time >= TIMESTAMPTZ '2026-06-02 00:00:00+00' THEN 'late' ELSE 'early' END AS w, count(*) AS n FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"volatile function":              "SELECT region, count(*) AS n FROM events WHERE " + dimExprTF + " AND random() < 2 GROUP BY 1",
	"now() in a key":                 "SELECT region || CAST(now() AS VARCHAR) AS k, count(*) AS n FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"subquery in WHERE":              "SELECT region, count(*) AS n FROM events WHERE " + dimExprTF + " AND region IN (SELECT 'eu') GROUP BY 1",
	"column not stored":              "SELECT region, count(*) AS n FROM events WHERE " + dimExprTF + " AND lower(resp) = '5' GROUP BY 1",
	"key over a non-dim":             "SELECT LEFT(resp, 1) AS r, count(*) AS n FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"aggregate inside a key":         "SELECT region, count(*) + 1 AS n FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"distinct of an expression":      "SELECT region, count(DISTINCT CASE WHEN tag = 'x' THEN bytes + 1 END) AS u FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"distinct with a non-NULL ELSE":  "SELECT region, count(DISTINCT CASE WHEN tag = 'x' THEN bytes ELSE 0 END) AS u FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"distinct predicate on a metric": "SELECT region, count(DISTINCT CASE WHEN bytes > 10 THEN bytes END) AS u FROM events WHERE " + dimExprTF + " GROUP BY 1",
	"ungrouped expression key":       "SELECT upper(region) AS r, count(*) AS n FROM events WHERE " + dimExprTF + " GROUP BY region",
	// DuckDB binds a GROUP BY name to a same-named table column before an
	// alias, so a computed key must be grouped by position or by repeating it.
	"key grouped by its alias": "SELECT upper(region) AS r, count(*) AS n FROM events WHERE " + dimExprTF + " GROUP BY r",
	"alias shadows a column":   "SELECT LEFT(region, 1) AS region, count(*) AS n FROM events WHERE " + dimExprTF + " GROUP BY region",
}

func TestDimExpr_ServedSameAsSource(t *testing.T) {
	for wmName, wm := range map[string]string{"sealed": "", "merge": "2026-06-02 12:00:00+00"} {
		f := newQRFixture(t, dimExprSpec(), wm)
		f.r.DimExprs = true
		for name, q := range dimExprServed {
			t.Run(wmName+"/"+name, func(t *testing.T) {
				if d := f.requireSameAsRaw(t, q); !d.Served {
					t.Fatalf("want served from the cube, declined: %s", d.Reason)
				}
			})
		}
	}
}

func TestDimExpr_Declined(t *testing.T) {
	f := newQRFixture(t, dimExprSpec(), "")
	f.r.DimExprs = true
	for name, q := range dimExprDeclined {
		t.Run(name, func(t *testing.T) {
			if d := f.r.Route(q); d.Served {
				t.Fatalf("want declined, served from %s:\n%s", d.Cube, d.SQL)
			}
		})
	}
}

// The expression path is opt-in: with DimExprs off (the default), every shape
// that needs it still falls through to source exactly as before.
func TestDimExpr_OffByDefault(t *testing.T) {
	f := newQRFixture(t, dimExprSpec(), "")
	if f.r.DimExprs {
		t.Fatal("DimExprs must default to false")
	}
	for name, q := range dimExprServed {
		t.Run(name, func(t *testing.T) {
			if d := f.r.Route(q); d.Served {
				t.Fatalf("served with DimExprs off:\n%s", d.SQL)
			}
		})
	}
}

// Rendered expressions are re-emitted by DuckDB from the parse tree, never
// copied from the request text: a literal holding quotes, a comment marker and a
// semicolon stays one string literal in the cube read.
func TestDimExpr_LiteralsStayLiterals(t *testing.T) {
	f := newQRFixture(t, dimExprSpec(), "")
	f.r.DimExprs = true
	q := "SELECT region, count(*) AS n FROM events WHERE " + dimExprTF + " AND tag NOT ILIKE '%''; DROP TABLE x; --%' GROUP BY 1"
	d := f.requireSameAsRaw(t, q)
	if !d.Served {
		t.Fatalf("want served, declined: %s", d.Reason)
	}
	if !strings.Contains(d.SQL, "'%''; DROP TABLE x; --%'") {
		t.Fatalf("literal not re-emitted as one escaped string:\n%s", d.SQL)
	}
}
