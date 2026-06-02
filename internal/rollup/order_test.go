package rollup

import (
	"strings"
	"testing"
)

// TestOrderBy_TopN pins that ORDER BY <agg> LIMIT n is captured and reproduced in
// the cube read by select-list position — the fix for TopN returning wrong rows.
func TestOrderBy_TopN(t *testing.T) {
	q, ok, reason := Parse(
		`SELECT site, count(*) AS total FROM downloads `+
			`WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-23' `+
			`GROUP BY site ORDER BY total DESC LIMIT 20`, "time")
	if !ok {
		t.Fatalf("ok=false: %s", reason)
	}
	if q.Limit != 20 {
		t.Errorf("Limit=%d want 20", q.Limit)
	}
	// select list: site(1), total(2) -> ORDER BY total => position 2 DESC
	if len(q.OrderBy) != 1 || q.OrderBy[0].Pos != 2 || !q.OrderBy[0].Desc {
		t.Fatalf("OrderBy=%+v want [{2 true}]", q.OrderBy)
	}
	sql := q.CubeReadSQL("'cube'")
	if !strings.Contains(sql, "ORDER BY 2 DESC") {
		t.Errorf("cube SQL missing 'ORDER BY 2 DESC':\n%s", sql)
	}
	if !strings.Contains(sql, "LIMIT 20") {
		t.Errorf("cube SQL missing LIMIT 20:\n%s", sql)
	}
	// The grouping must NOT be reordered away — GROUP BY stays positional.
	if !strings.Contains(sql, "GROUP BY 1") {
		t.Errorf("cube SQL missing GROUP BY 1:\n%s", sql)
	}
}

// TestOrderBy_Positional_And_Bucket covers positional ORDER BY and ordering by the
// time bucket alias.
func TestOrderBy_Positional_And_Bucket(t *testing.T) {
	q, ok, _ := Parse(
		`SELECT date_trunc('hour', time) AS t, status, count(*) AS n FROM downloads `+
			`WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-23' `+
			`GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 5`, "time")
	if !ok {
		t.Fatal("parse failed")
	}
	// layout: t(1), status(2), n(3)
	if len(q.OrderBy) != 1 || q.OrderBy[0].Pos != 3 || !q.OrderBy[0].Desc {
		t.Fatalf("OrderBy=%+v want [{3 true}]", q.OrderBy)
	}

	q2, ok2, _ := Parse(
		`SELECT date_trunc('hour', time) AS t, count(*) AS n FROM downloads `+
			`WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-23' `+
			`GROUP BY 1 ORDER BY t LIMIT 100`, "time")
	if !ok2 {
		t.Fatal("parse2 failed")
	}
	if len(q2.OrderBy) != 1 || q2.OrderBy[0].Pos != 1 || q2.OrderBy[0].Desc {
		t.Fatalf("OrderBy=%+v want [{1 false}]", q2.OrderBy)
	}
}

// TestOrderBy_RejectWhenLimitedAndUnreproducible guards fidelity: an ORDER BY we
// can't map plus a LIMIT must reject (else TopN returns wrong rows). The same
// ORDER BY without a LIMIT is fine (row order doesn't change the value set).
func TestOrderBy_RejectWhenLimitedAndUnreproducible(t *testing.T) {
	// ORDER BY an expression not in the select list, with LIMIT -> reject.
	bad := `SELECT site, count(*) AS total FROM downloads ` +
		`WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-23' ` +
		`GROUP BY site ORDER BY length(site) DESC LIMIT 10`
	if _, ok, _ := Parse(bad, "time"); ok {
		t.Error("expected reject for unreproducible ORDER BY under LIMIT")
	}
	// Same ORDER BY without LIMIT -> accepted (ordering is cosmetic).
	okSQL := `SELECT site, count(*) AS total FROM downloads ` +
		`WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-23' ` +
		`GROUP BY site ORDER BY length(site) DESC`
	if _, ok, reason := Parse(okSQL, "time"); !ok {
		t.Errorf("expected accept without LIMIT, got: %s", reason)
	}
}
