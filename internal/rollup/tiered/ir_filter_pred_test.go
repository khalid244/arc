package tiered

import (
	"testing"
)

// filterPredToExpr is the bridge between FilterPredicate (parsed user
// WHERE clause) and the IR. Every supported operator must render to the
// shape the rollup parquet expects.
func TestFilterPredToExpr_AllOps(t *testing.T) {
	cases := []struct {
		name string
		col  string
		fp   FilterPredicate
		want string
		mode ColMode
	}{
		{"eq", "site_class", FilterPredicate{Op: "=", Values: []string{"a"}}, "site_class = 'a'", RollupMode},
		{"eq with quote", "site_class", FilterPredicate{Op: "=", Values: []string{"a'b"}}, "site_class = 'a''b'", RollupMode},
		{"in single", "site_class", FilterPredicate{Op: "IN", Values: []string{"a"}}, "site_class IN ('a')", RollupMode},
		{"in many", "site_class", FilterPredicate{Op: "IN", Values: []string{"a", "b", "c"}}, "site_class IN ('a', 'b', 'c')", RollupMode},
		{"in with quote", "site_class", FilterPredicate{Op: "IN", Values: []string{"a", "b'c"}}, "site_class IN ('a', 'b''c')", RollupMode},
		{"not in", "site_class", FilterPredicate{Op: "NOT IN", Values: []string{"x"}}, "site_class NOT IN ('x')", RollupMode},
		{"is null", "site_class", FilterPredicate{Op: "IS NULL"}, "site_class = '_null_'", RollupMode},
		{"is not null", "site_class", FilterPredicate{Op: "IS NOT NULL"}, "site_class <> '_null_'", RollupMode},
		// source-mode dims use the raw column name (no _class).
		{"source eq", "site", FilterPredicate{Op: "=", Values: []string{"a"}}, "site = 'a'", SourceMode},
		{"source in", "site", FilterPredicate{Op: "IN", Values: []string{"a", "b"}}, "site IN ('a', 'b')", SourceMode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := filterPredToExpr(tc.col, tc.fp)
			got, err := e.sql(tc.mode)
			if err != nil {
				t.Fatalf("sql err: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q\nwant %q", got, tc.want)
			}
		})
	}
}

// IN over a rollup-only column from a source-mode context must surface
// the mode error rather than silently emitting bad SQL.
func TestFilterPredToExpr_SourceModeRejectsRollupClass(t *testing.T) {
	e := filterPredToExpr("site_class", FilterPredicate{Op: "IN", Values: []string{"a"}})
	if _, err := e.sql(SourceMode); err == nil {
		t.Fatalf("expected mode error for site_class IN SourceMode")
	}
}

// Unknown op renders empty (mirrors old buildFilterExpr behaviour) so the
// caller can still proceed but produces a no-op filter.
func TestFilterPredToExpr_UnknownOp(t *testing.T) {
	e := filterPredToExpr("site_class", FilterPredicate{Op: "wat", Values: []string{"a"}})
	got, err := e.sql(RollupMode)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "" {
		t.Fatalf("expected empty for unknown op, got %q", got)
	}
}
