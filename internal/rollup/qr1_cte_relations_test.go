package rollup

import (
	"strings"
	"testing"
)

// QR-1: the CTE-base rewrite replaces only the base CTE's body with cube SQL and
// the executor runs the result as-is (no storage-path conversion). Any other
// relation that names a real table — a second CTE, a JOIN, a subquery — kept the
// bare table name and failed with a Catalog Error (500, no fallback). The rewrite
// must decline unless every relation outside the base body is a CTE in scope.

const qr1Window = "time >= TIMESTAMPTZ '2026-06-01 00:00:00+00' AND time < TIMESTAMPTZ '2026-06-03 00:00:00+00'"

const qr1Base = "WITH base AS (SELECT date_trunc('day', time) AS d, region, count(*) AS n FROM events WHERE " +
	qr1Window + " GROUP BY 1, 2)"

func qr1Spec() CubeSpec {
	return CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount}}}
}

func TestQR1_CTEBaseWithOtherTableRelationsDeclines(t *testing.T) {
	cases := map[string]string{
		"second CTE reads the same table": qr1Base + `, tot AS (SELECT count(*) AS total FROM events WHERE ` + qr1Window + `)
			SELECT d, region, n, round(n * 100.0 / total, 4) AS pct FROM base, tot`,
		"second CTE reads another table": qr1Base + `, s AS (SELECT region, count(*) AS c FROM sessions GROUP BY 1)
			SELECT base.d, base.region, base.n, s.c FROM base JOIN s USING (region)`,
		"outer JOIN on a table": qr1Base + `
			SELECT base.d, base.region, base.n, count(*) AS c FROM base JOIN sessions s ON s.region = base.region GROUP BY 1, 2, 3`,
		"scalar subquery on a table": qr1Base + `
			SELECT d, region, n, (SELECT count(*) FROM events WHERE ` + qr1Window + `) AS total FROM base`,
		"IN subquery on a table": qr1Base + `
			SELECT d, region, n FROM base WHERE region IN (SELECT region FROM sessions WHERE app_version = '7')`,
		"table function over a table subquery": qr1Base + `
			SELECT d, region, n, x FROM base, unnest((SELECT list(DISTINCT status) FROM sessions)) u(x)`,
	}
	for _, wm := range []string{"", "2026-06-02 12:00:00+00"} {
		f := newQRFixture(t, qr1Spec(), wm)
		mustExec(t, f.raw, "CREATE VIEW sessions AS SELECT * FROM events WHERE status <> 500")
		for name, q := range cases {
			t.Run(wm+"/"+name, func(t *testing.T) {
				d := f.requireSameAsRaw(t, q)
				if d.Served {
					t.Fatalf("served %q, want declined (another relation reads a real table)\nSQL: %s", d.Cube, d.SQL)
				}
				if !strings.HasPrefix(d.Reason, "cte base: other relations") {
					t.Fatalf("reason = %q, want \"cte base: other relations ...\"", d.Reason)
				}
			})
		}
	}
}

// Relations that are all CTEs (or relation-free table functions) keep being
// served, with results equal to source.
func TestQR1_CTEBaseWithOnlyCTERelationsStillServed(t *testing.T) {
	cases := map[string]string{
		"single base": qr1Base + ` SELECT d, region, n FROM base`,
		"CTE chain over base": qr1Base + `, ranked AS (SELECT *, row_number() OVER (PARTITION BY d ORDER BY n DESC, region) AS rk FROM base)
			SELECT d, region, n, rk FROM ranked WHERE rk <= 2`,
		"self-join of CTEs": qr1Base + `, tot AS (SELECT d, sum(n) AS total FROM base GROUP BY 1)
			SELECT base.d, base.region, round(base.n * 100.0 / tot.total, 4) AS pct FROM base JOIN tot USING (d)`,
		"subquery over a CTE": qr1Base + `
			SELECT d, region, n, (SELECT max(n) FROM base) AS top FROM base`,
		"CTE scoped in a subquery": qr1Base + `
			SELECT d, region, n FROM base WHERE region IN (WITH keep AS (SELECT 'eu' AS r UNION ALL SELECT 'ap') SELECT r FROM keep)`,
		"generate_series": qr1Base + `
			SELECT g.x, base.d, base.region, base.n FROM generate_series(1, 2) g(x) JOIN base ON true WHERE base.region = 'eu'`,
		"CTE named like the source table": `WITH events AS (SELECT date_trunc('day', time) AS d, region, count(*) AS n FROM events WHERE ` +
			qr1Window + ` GROUP BY 1, 2) SELECT d, region, n FROM events`,
	}
	for _, wm := range []string{"", "2026-06-02 12:00:00+00"} {
		f := newQRFixture(t, qr1Spec(), wm)
		for name, q := range cases {
			t.Run(wm+"/"+name, func(t *testing.T) {
				if d := f.requireSameAsRaw(t, q); !d.Served {
					t.Fatalf("want served from the cube, declined: %s", d.Reason)
				}
			})
		}
	}
}

// Scoping: a CTE defined inside a subquery does not cover a reference outside
// it, and a schema-qualified name is never a CTE.
func TestQR1_CTEScopeAndQualifiedNames(t *testing.T) {
	f := newQRFixture(t, qr1Spec(), "")
	for name, q := range map[string]string{
		"CTE name used outside its subquery scope": qr1Base + `
			SELECT * FROM base, (WITH tot AS (SELECT 1 AS one) SELECT one FROM tot) x, tot`,
		"qualified name shadowing a CTE": qr1Base + `, tot AS (SELECT 1 AS one)
			SELECT * FROM base, main.tot`,
		"later sibling CTE reads a table": qr1Base + `, a AS (SELECT * FROM base), b AS (SELECT * FROM a, events) SELECT * FROM b`,
	} {
		if d := f.r.Route(q); d.Served || !strings.HasPrefix(d.Reason, "cte base: other relations") {
			t.Errorf("%s: served=%v reason=%q, want declined with \"cte base: other relations ...\"", name, d.Served, d.Reason)
		}
	}
}
