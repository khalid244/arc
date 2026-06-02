package rollup

import "testing"

// findAgg returns the parsed aggregate with the given alias.
func findAgg(q QueryShape, alias string) (Aggregate, bool) {
	for _, a := range q.Aggs {
		if a.Alias == alias {
			return a, true
		}
	}
	return Aggregate{}, false
}

// TestCondAgg_Shape pins the structural decoding of a SUM(CASE…)/COUNT(*) success
// rate base aggregation: the conditional aggregate becomes an AggCondSum carrying
// the rendered predicate and the dimension it references. No corpus needed —
// Parse uses an in-memory DuckDB front-end.
func TestCondAgg_Shape(t *testing.T) {
	q, ok, reason := Parse(
		`SELECT date_trunc('hour', time) AS t, site, `+
			`sum(CASE WHEN response = 200 THEN 1 ELSE 0 END) AS ok, `+
			`count(*) AS total `+
			`FROM downloads WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-23' `+
			`GROUP BY 1, site`, "time")
	if !ok {
		t.Fatalf("expected supported, got ok=false: %s", reason)
	}
	if len(q.Dims) != 1 || q.Dims[0] != "site" {
		t.Fatalf("dims = %v, want [site]", q.Dims)
	}
	ok1, found := findAgg(q, "ok")
	if !found {
		t.Fatalf("no agg aliased ok; aggs=%+v", q.Aggs)
	}
	if ok1.Kind != AggCondSum {
		t.Errorf("ok.Kind = %v, want AggCondSum", ok1.Kind)
	}
	if ok1.Cond != `"response" = 200` {
		t.Errorf("ok.Cond = %q, want \"response\" = 200", ok1.Cond)
	}
	if len(ok1.CondCols) != 1 || ok1.CondCols[0] != "response" {
		t.Errorf("ok.CondCols = %v, want [response]", ok1.CondCols)
	}
	if ok1.ThenK != "1" || ok1.ElseK != "0" || ok1.ThenCol != "" {
		t.Errorf("ok then/else = %q/%q col=%q", ok1.ThenK, ok1.ElseK, ok1.ThenCol)
	}
	// requiredDims must include both the group dim and the predicate dim, so only a
	// cube storing site AND response can serve it.
	req := q.requiredDims()
	if !contains(req, "site") || !contains(req, "response") {
		t.Errorf("requiredDims = %v, want site+response", req)
	}
}

// TestCondAgg_FinalAndOrig pins the emitted SQL on both sides of the equivalence:
// the cube-read scales constant branches by _cnt; the source reference reproduces
// the user's CASE verbatim.
func TestCondAgg_FinalAndOrig(t *testing.T) {
	cases := []struct {
		sql        string
		alias      string
		wantFinal  string
		wantOrig   string
		wantCond   string
		wantThenCl string // ThenCol
	}{
		{
			sql:       `sum(CASE WHEN response = 200 THEN 1 ELSE 0 END)`,
			wantFinal: `sum(CASE WHEN "response" = 200 THEN _cnt ELSE 0 END)`,
			wantOrig:  `sum(CASE WHEN "response" = 200 THEN 1 ELSE 0 END)`,
			wantCond:  `"response" = 200`,
		},
		{ // COUNT(CASE … END) with implicit NULL else => count of matching rows (BIGINT)
			sql:       `count(CASE WHEN status = 'ok' THEN 1 END)`,
			wantFinal: `sum(CASE WHEN "status" = 'ok' THEN _cnt ELSE 0 END)::BIGINT`,
			wantOrig:  `sum(CASE WHEN "status" = 'ok' THEN 1 ELSE 0 END)`,
			wantCond:  `"status" = 'ok'`,
		},
		{ // success class as a range, AND of two comparisons
			sql:       `sum(CASE WHEN response >= 200 AND response < 300 THEN 1 ELSE 0 END)`,
			wantFinal: `sum(CASE WHEN ("response" >= 200 AND "response" < 300) THEN _cnt ELSE 0 END)`,
			wantOrig:  `sum(CASE WHEN ("response" >= 200 AND "response" < 300) THEN 1 ELSE 0 END)`,
			wantCond:  `("response" >= 200 AND "response" < 300)`,
		},
		{ // metric-weighted conditional sum -> uses the summed store column
			sql:        `sum(CASE WHEN response = 200 THEN duration_seconds ELSE 0 END)`,
			wantFinal:  `sum(CASE WHEN "response" = 200 THEN _sum_duration_seconds ELSE 0 END)`,
			wantOrig:   `sum(CASE WHEN "response" = 200 THEN "duration_seconds" ELSE 0 END)`,
			wantCond:   `"response" = 200`,
			wantThenCl: "duration_seconds",
		},
		{ // IN-list predicate
			sql:       `sum(CASE WHEN response IN (200, 206) THEN 1 ELSE 0 END)`,
			wantFinal: `sum(CASE WHEN "response" IN (200, 206) THEN _cnt ELSE 0 END)`,
			wantOrig:  `sum(CASE WHEN "response" IN (200, 206) THEN 1 ELSE 0 END)`,
			wantCond:  `"response" IN (200, 206)`,
		},
	}
	for _, c := range cases {
		full := `SELECT ` + c.sql + ` AS v FROM downloads WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-23'`
		q, ok, reason := Parse(full, "time")
		if !ok {
			t.Errorf("%s: ok=false (%s)", c.sql, reason)
			continue
		}
		a, found := findAgg(q, "v")
		if !found || a.Kind != AggCondSum {
			t.Errorf("%s: not an AggCondSum (%+v)", c.sql, q.Aggs)
			continue
		}
		if a.Cond != c.wantCond {
			t.Errorf("%s: Cond=%q want %q", c.sql, a.Cond, c.wantCond)
		}
		if a.ThenCol != c.wantThenCl {
			t.Errorf("%s: ThenCol=%q want %q", c.sql, a.ThenCol, c.wantThenCl)
		}
		if got := a.finalExpr(); got != c.wantFinal {
			t.Errorf("%s: finalExpr=%q want %q", c.sql, got, c.wantFinal)
		}
		if got := a.origExpr(); got != c.wantOrig {
			t.Errorf("%s: origExpr=%q want %q", c.sql, got, c.wantOrig)
		}
	}
}

// TestCondAgg_Coverage proves routing: a cube must store both the group dim and
// the predicate dim to serve a conditional-aggregate query.
func TestCondAgg_Coverage(t *testing.T) {
	q, ok, _ := Parse(
		`SELECT site, sum(CASE WHEN response = 200 THEN 1 ELSE 0 END) AS ok, count(*) AS total `+
			`FROM downloads WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-23' `+
			`GROUP BY site`, "time")
	if !ok {
		t.Fatal("parse failed")
	}
	base := []Aggregate{{Kind: AggCount, Alias: "total"}, {Kind: AggCondSum, Alias: "ok", Cond: `"response" = 200`, CondCols: []string{"response"}, ThenK: "1", ElseK: "0"}}

	siteOnly := CubeSpec{Source: "default.downloads", Grain: "hour", Dims: []string{"site"}, Aggs: base}
	if cov, _ := siteOnly.Covers(q); cov {
		t.Error("site-only cube must NOT cover a site×response conditional query")
	}
	dimRich := CubeSpec{Source: "default.downloads", Grain: "hour", Dims: []string{"site", "response", "status"}, Aggs: base}
	if cov, reason := dimRich.Covers(q); !cov {
		t.Errorf("dim-rich cube must cover the conditional query: %s", reason)
	}
	if pick := PickNarrowest([]CubeSpec{siteOnly, dimRich}, q); pick == nil || len(pick.Dims) != 3 {
		t.Errorf("PickNarrowest should choose the dim-rich cube, got %+v", pick)
	}
}

// TestCondAgg_Reject guards the fidelity boundary: shapes we can't prove equal
// must fall through (ok=false), never silently mis-serve.
func TestCondAgg_Reject(t *testing.T) {
	reject := []string{
		// multiple WHEN branches — not a single predicate
		`SELECT sum(CASE WHEN response = 200 THEN 1 WHEN response = 404 THEN 2 ELSE 0 END) AS v FROM downloads WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-23'`,
		// COUNT(CASE) with a non-NULL ELSE would count all rows, not the matches
		`SELECT count(CASE WHEN response = 200 THEN 1 ELSE 0 END) AS v FROM downloads WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-23'`,
		// SUM(CASE) THEN a non-numeric constant
		`SELECT sum(CASE WHEN response = 200 THEN 'x' ELSE 'y' END) AS v FROM downloads WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-23'`,
	}
	for _, s := range reject {
		if _, ok, _ := Parse(s, "time"); ok {
			t.Errorf("expected reject (ok=false) for:\n%s", s)
		}
	}
}

func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}
