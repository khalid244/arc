package rollup

import "testing"

func TestFirstCTEBody(t *testing.T) {
	cases := []struct {
		name             string
		sql              string
		ok               bool
		body, tailPrefix string
	}{
		{"simple", "WITH base AS (SELECT 1) SELECT * FROM base", true, "SELECT 1", ") SELECT"},
		{"nested parens (COUNT(*))", "WITH b AS (SELECT count(*) c FROM t GROUP BY 1) SELECT * FROM b", true, "SELECT count(*) c FROM t GROUP BY 1", ") SELECT"},
		{"paren inside string literal", "WITH b AS (SELECT '(' AS x) SELECT * FROM b", true, "SELECT '(' AS x", ") SELECT"},
		{"paren inside line comment", "WITH b AS (SELECT 1 -- )(\n) SELECT * FROM b", true, "SELECT 1 -- )(\n", ") SELECT"},
		{"multiple CTEs keeps rest in tail", "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT *", true, "SELECT 1", "), b AS"},
		{"leading whitespace", "  \n WITH base AS (SELECT 1) SELECT *", true, "SELECT 1", ") SELECT"},
		{"not a WITH query", "SELECT 1 FROM t", false, "", ""},
		{"unbalanced", "WITH b AS (SELECT 1 SELECT *", false, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			head, body, tail, ok := firstCTEBody(c.sql)
			if ok != c.ok {
				t.Fatalf("ok=%v want %v", ok, c.ok)
			}
			if !ok {
				return
			}
			if body != c.body {
				t.Errorf("body=%q want %q", body, c.body)
			}
			if tail[:min2(len(tail), len(c.tailPrefix))] != c.tailPrefix {
				t.Errorf("tail=%q want prefix %q", tail, c.tailPrefix)
			}
			// Reassembly must reproduce the original SQL exactly.
			if head+body+tail != c.sql {
				t.Errorf("head+body+tail != original:\n  got  %q\n  want %q", head+body+tail, c.sql)
			}
		})
	}
}

func min2(a, b int) int {
	if a < b {
		return a
	}
	return b
}
