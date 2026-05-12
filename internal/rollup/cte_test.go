package rollup

import (
	"strings"
	"testing"
)

func TestRewriteCTEs_SingleCTE(t *testing.T) {
	calls := 0
	originalSQL := `WITH x AS (SELECT a FROM downloads WHERE t >= '2026-01-01' AND t < '2026-02-01')
SELECT * FROM x`
	out := RewriteCTEs(originalSQL, func(inner string) (string, bool) {
		calls++
		if !strings.Contains(inner, "FROM downloads") {
			t.Errorf("recurse fn received unexpected body: %q", inner)
		}
		return "SELECT REWRITTEN", true
	})
	if calls != 1 {
		t.Errorf("expected 1 recurse call, got %d", calls)
	}
	if !strings.Contains(out, "SELECT REWRITTEN") {
		t.Errorf("rewritten body missing from output:\n%s", out)
	}
	if !strings.Contains(out, "WITH x AS") {
		t.Errorf("WITH clause stripped, expected to survive:\n%s", out)
	}
}

func TestRewriteCTEs_MultipleCTEs(t *testing.T) {
	calls := []string{}
	originalSQL := `WITH a AS (SELECT 1 FROM downloads), b AS (SELECT 2 FROM downloads) SELECT * FROM a, b`
	out := RewriteCTEs(originalSQL, func(inner string) (string, bool) {
		calls = append(calls, strings.TrimSpace(inner))
		return "REW(" + strings.TrimSpace(inner) + ")", true
	})
	if len(calls) != 2 {
		t.Fatalf("expected 2 recurse calls, got %d: %v", len(calls), calls)
	}
	if !strings.Contains(out, "REW(SELECT 1 FROM downloads)") {
		t.Errorf("first CTE body not rewritten in output:\n%s", out)
	}
	if !strings.Contains(out, "REW(SELECT 2 FROM downloads)") {
		t.Errorf("second CTE body not rewritten in output:\n%s", out)
	}
}

func TestRewriteCTEs_NoCTE(t *testing.T) {
	originalSQL := `SELECT * FROM downloads`
	out := RewriteCTEs(originalSQL, func(inner string) (string, bool) {
		t.Errorf("recurse fn should not be called for non-CTE query")
		return inner, false
	})
	if out != originalSQL {
		t.Errorf("non-CTE query should return unchanged, got:\n%s", out)
	}
}

func TestRewriteCTEs_RecurseFnRefuses(t *testing.T) {
	originalSQL := `WITH x AS (SELECT 1 FROM downloads) SELECT * FROM x`
	out := RewriteCTEs(originalSQL, func(inner string) (string, bool) {
		return inner, false
	})
	if out != originalSQL {
		t.Errorf("when recurse fn refuses, output should be unchanged, got:\n%s", out)
	}
}
