package rollup

import (
	"strings"
	"testing"
)

// QR-2: a constant keeps its type from the parse to the emitted SQL. A string
// constant is always single-quoted, even when it looks like a number; only a
// real numeric constant is emitted bare. Emitting '1.10' as 1.10 made DuckDB
// cast the whole VARCHAR column to a number: a 500 (Conversion Error) when any
// value is not numeric ('2.0.1'), and '1.1' = '1.10', '5' = '05' = '5.0' when
// all values are.

func qr2Spec() CubeSpec {
	return CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"app_version", "resp", "status"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Col: "bytes"}}}
}

func qr2Window(pred string) string {
	return "time >= TIMESTAMPTZ '2026-06-01 00:00:00+00' AND time < TIMESTAMPTZ '2026-06-03 00:00:00+00' AND " + pred
}

// Filters: every served result equals the source result, on the sealed cube
// read and on the merge-on-read path. The served SQL must actually be the cube
// (not a decline) for the string cases.
func TestQR2_StringConstantsStayStrings_Filters(t *testing.T) {
	cases := []struct {
		pred      string
		mustServe bool
	}{
		{`app_version = '1.1'`, true},                        // must not also match '1.10'
		{`app_version = '1.10'`, true},                       // data holds '2.0.1': must not 500
		{`app_version IN ('1.1', '2.0.1')`, true},            // mixed numeric-looking + not
		{`app_version <> '7'`, true},                         // must not drop '007' / '7.0'
		{`(app_version = '7' OR app_version = '7.0')`, true}, // OR -> IN keeps both as strings
		{`resp = '5'`, true},                                 // all-numeric column: no '05'/'5.0' overcount
		{`resp NOT IN ('5', '4')`, true},
		{`status = 404`, true}, // numeric constant on a numeric column still served
		{`status IN (200, 500)`, true},
		{`resp = 5`, true},      // numeric constant on VARCHAR: same cast semantics as source
		{`resp = 5.0`, true},    // DECIMAL keeps its scale
		{`status = NULL`, true}, // NULL stays NULL (never true), not ''
		{`resp = 5e0`, true},    // DOUBLE stays DOUBLE
	}
	for _, wm := range []string{"", "2026-06-02 12:00:00+00"} {
		f := newQRFixture(t, qr2Spec(), wm)
		for _, c := range cases {
			q := "SELECT date_trunc('day', time) AS d, app_version, count(*) AS n, sum(bytes) AS b FROM events WHERE " +
				qr2Window(c.pred) + " GROUP BY 1, 2"
			t.Run(wm+"/"+c.pred, func(t *testing.T) {
				d := f.requireSameAsRaw(t, q)
				if c.mustServe && !d.Served {
					t.Fatalf("want served from the cube, declined: %s", d.Reason)
				}
			})
		}
	}
}

// Conditional aggregates: the CASE predicate is rendered with the same typed
// literals (it is evaluated on cube rows at read time).
func TestQR2_StringConstantsStayStrings_CondAgg(t *testing.T) {
	aggs := []string{
		`count(CASE WHEN app_version = '1.1' THEN 1 END) AS c1`,
		`sum(CASE WHEN app_version IN ('1.10', '2.0.1') THEN 1 ELSE 0 END) AS c2`,
		`sum(CASE WHEN resp = '5' THEN bytes ELSE 0 END) AS c3`,
		`sum(CASE WHEN '7' = app_version THEN 2 ELSE 0 END) AS c4`,
		`count(CASE WHEN status = 404 THEN 1 END) AS c5`,
		`sum(CASE WHEN resp <> '05' AND status >= 404 THEN 1.5 ELSE 0.5 END) AS c6`,
	}
	for _, wm := range []string{"", "2026-06-02 12:00:00+00"} {
		f := newQRFixture(t, qr2Spec(), wm)
		for _, a := range aggs {
			q := "SELECT date_trunc('day', time) AS d, " + a + " FROM events WHERE " +
				qr2Window("TRUE = TRUE") + " GROUP BY 1"
			q = strings.Replace(q, " AND TRUE = TRUE", "", 1)
			t.Run(wm+"/"+a, func(t *testing.T) {
				if d := f.requireSameAsRaw(t, q); !d.Served {
					t.Fatalf("want served from the cube, declined: %s", d.Reason)
				}
			})
		}
	}
}

// Constants whose value cannot be carried exactly are declined (the query runs
// against source) instead of being rendered as something else: integers beyond
// 2^53 and wide DECIMALs (decoded through float64 or not numbers at all in the
// serialized tree — the old code rendered the latter as ”).
func TestQR2_InexactConstantsDecline(t *testing.T) {
	for _, pred := range []string{
		`bytes = 9007199254740993`,
		`bytes = 0.1234567890123456789`,
		`bytes = 99999999999999999999`,
		`count(CASE WHEN bytes = 9007199254740993 THEN 1 END)`,
	} {
		var q string
		if strings.HasPrefix(pred, "count(") {
			q = "SELECT " + pred + " AS n FROM events WHERE " + strings.TrimSuffix(qr2Window("x"), " AND x")
		} else {
			q = "SELECT count(*) AS n FROM events WHERE " + qr2Window(pred)
		}
		if s, ok, _ := Parse(q, "time"); ok {
			t.Errorf("%s: parsed as servable (filters=%+v aggs=%+v); want declined", pred, s.Filters, s.Aggs)
		}
	}
}

// Only the read path carries the predicate: cube build SQL holds no filter and
// no CASE predicate (filters and CondSum predicates are applied post-aggregation
// over stored dims), so typed literals change no cube contents.
func TestQR2_BuildSQLHasNoPredicates(t *testing.T) {
	q, ok, why := Parse("SELECT count(CASE WHEN app_version = '1.10' THEN 1 END) AS c, sum(CASE WHEN resp = '5' THEN bytes ELSE 0 END) AS s FROM events WHERE "+qr2Window("app_version = '2.0.1'"), "time")
	if !ok {
		t.Fatalf("parse: %s", why)
	}
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"app_version", "resp"}, Aggs: q.Aggs}
	build := spec.BuildSelect("'x.parquet'", "time", "2026-06-01 00:00:00+00", "2026-06-02 00:00:00+00")
	for _, frag := range []string{"1.10", "'5'", "2.0.1", "CASE"} {
		if strings.Contains(build, frag) {
			t.Errorf("build SQL contains %q (predicates must stay read-side):\n%s", frag, build)
		}
	}
}
