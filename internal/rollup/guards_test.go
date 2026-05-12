package rollup

import (
	"testing"

	pgquery "github.com/pganalyze/pg_query_go/v6"
)

func parseSelect(t *testing.T, sql string) *pgquery.SelectStmt {
	t.Helper()
	tree, err := pgquery.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(tree.Stmts) != 1 {
		t.Fatalf("got %d stmts", len(tree.Stmts))
	}
	sel, ok := tree.Stmts[0].Stmt.GetNode().(*pgquery.Node_SelectStmt)
	if !ok {
		t.Fatalf("not a SELECT")
	}
	return sel.SelectStmt
}

func TestHasTimeFilter_BothBoundsPresent(t *testing.T) {
	sel := parseSelect(t, "SELECT 1 FROM t WHERE time >= '2026-01-01' AND time < '2026-02-01' AND status = 'ok'")
	tr, ok := HasTimeFilter(sel, "time")
	if !ok {
		t.Fatalf("expected hasTimeFilter=true for top-level AND with both bounds")
	}
	if tr.Lo.IsZero() || tr.Hi.IsZero() {
		t.Errorf("expected both bounds set, got Lo=%v Hi=%v", tr.Lo, tr.Hi)
	}
}

func TestHasTimeFilter_OneSidedRefused(t *testing.T) {
	// v2 contract: rewrite needs both endpoints. Open-ended queries fall back.
	for _, sql := range []string{
		"SELECT 1 FROM t WHERE time >= '2026-01-01'",
		"SELECT 1 FROM t WHERE time < '2026-02-01'",
	} {
		sel := parseSelect(t, sql)
		if _, ok := HasTimeFilter(sel, "time"); ok {
			t.Errorf("expected hasTimeFilter=false for open-ended %q", sql)
		}
	}
}

func TestHasTimeFilter_OnlyInsideOR_Refuses(t *testing.T) {
	sel := parseSelect(t, "SELECT 1 FROM t WHERE status = 'ok' OR time >= '2026-01-01'")
	if _, ok := HasTimeFilter(sel, "time"); ok {
		t.Error("expected hasTimeFilter=false when time only in OR branch")
	}
}

func TestHasTimeFilter_Missing(t *testing.T) {
	sel := parseSelect(t, "SELECT 1 FROM t WHERE status = 'ok'")
	if _, ok := HasTimeFilter(sel, "time"); ok {
		t.Error("expected hasTimeFilter=false when WHERE has no time predicate")
	}
}

// TestHasTimeFilter_OperatorInclusivityFolded pins the v2 contract: `>` is
// folded into Lo and `<=` into Hi without preserving the operator. Boundary-
// instant ambiguity is acknowledged in the function's godoc.
func TestHasTimeFilter_OperatorInclusivityFolded(t *testing.T) {
	sel := parseSelect(t, "SELECT 1 FROM t WHERE time > '2026-01-01' AND time <= '2026-02-01'")
	tr, ok := HasTimeFilter(sel, "time")
	if !ok {
		t.Fatalf("expected hasTimeFilter=true even with > / <= operators")
	}
	if tr.Lo.IsZero() || tr.Hi.IsZero() {
		t.Errorf("expected both bounds set, got Lo=%v Hi=%v", tr.Lo, tr.Hi)
	}
}

// TestHasTimeFilter_RejectsWrongTypecast pins the typecast target check.
// A predicate like `time >= '2026-01-01'::interval` must not be parsed as
// a timestamp bound just because the inner string matches a timestamp
// layout — the cast target is interval, so the resulting value is a
// duration, not a point in time. The rewriter must refuse.
func TestHasTimeFilter_RejectsWrongTypecast(t *testing.T) {
	sel := parseSelect(t, "SELECT 1 FROM t WHERE time >= '2026-01-01'::interval AND time < TIMESTAMP '2026-02-01'")
	if _, ok := HasTimeFilter(sel, "time"); ok {
		t.Error("expected hasTimeFilter=false when one bound uses a non-timestamp cast")
	}
}

func TestAggregateTranslatable_KnownOps(t *testing.T) {
	for _, sql := range []string{
		"SELECT COUNT(*) FROM t",
		"SELECT SUM(v) FROM t",
		"SELECT AVG(v) FROM t",
		"SELECT MIN(v), MAX(v) FROM t",
		"SELECT COUNT(DISTINCT user_id) FROM t",
		"SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) FROM t",
	} {
		sel := parseSelect(t, sql)
		if !AllAggregatesTranslatable(sel) {
			t.Errorf("expected translatable: %s", sql)
		}
	}
}

func TestAggregateTranslatable_UnknownOp_Refuses(t *testing.T) {
	sel := parseSelect(t, "SELECT array_agg(country) FROM t WHERE time >= '2026-01-01'")
	if AllAggregatesTranslatable(sel) {
		t.Error("array_agg should not be translatable")
	}
}

// TestAggregateTranslatable_OrderBySketchWithLimit_Allowed: the rank-flip
// protection was removed (see comment on AllAggregatesTranslatable). Top-N
// by HLL/t-digest is the common dashboard case; ~1.6% error effectively
// doesn't change top-5/10/20 ordering, and 40× speedup matters more.
func TestAggregateTranslatable_OrderBySketchWithLimit_Allowed(t *testing.T) {
	sql := "SELECT country, COUNT(DISTINCT device_id) AS uniq FROM t WHERE time >= '2026-01-01' GROUP BY country ORDER BY uniq DESC LIMIT 10"
	sel := parseSelect(t, sql)
	if !AllAggregatesTranslatable(sel) {
		t.Error("ORDER BY sketch + LIMIT should be translatable now")
	}
}
