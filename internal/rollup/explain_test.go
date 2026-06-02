package rollup

import (
	"strings"
	"testing"
)

// TestExplain_NoWorkloadSideEffect proves the pre-run check is read-only: Explain
// returns the routing decision but must NOT fire OnQuery (which drives cube
// selection) — otherwise editor keystrokes would nudge what gets materialized.
func TestExplain_NoWorkloadSideEffect(t *testing.T) {
	calls := 0
	r := &Router{TimeCol: "time", OnQuery: func(QueryShape) { calls++ }}
	sql := `SELECT site, count(*) FROM downloads ` +
		`WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-02' GROUP BY site`

	d := r.Explain(sql)
	if calls != 0 {
		t.Errorf("Explain must not record; OnQuery fired %d times", calls)
	}
	if d.Served {
		t.Error("no cubes registered, expected not served")
	}
	if d.Reason == "" {
		t.Error("expected a non-empty reason when not served")
	}

	// Route, by contrast, records exactly once.
	r.Route(sql)
	if calls != 1 {
		t.Errorf("Route should record once, got %d", calls)
	}

	// ExplainHTTP returns the humanized reason and never records.
	served, _, reason := r.ExplainHTTP(sql, "default")
	if served || reason == "" {
		t.Errorf("ExplainHTTP = served:%v reason:%q", served, reason)
	}
	if calls != 1 {
		t.Errorf("ExplainHTTP recorded the query (calls=%d)", calls)
	}
}

// TestHumanizeReason maps terse codes to editor-friendly text.
func TestHumanizeReason(t *testing.T) {
	cases := map[string]string{
		"":                           "",
		"no_covering_cube":           "no cube stores",
		"parse:CTE not supported":    "CTE not supported",
		"cte base: no_covering_cube": "base CTE is not rollup-able",
	}
	for code, want := range cases {
		got := humanizeReason(code)
		if want == "" {
			if got != "" {
				t.Errorf("humanizeReason(%q) = %q, want empty", code, got)
			}
			continue
		}
		if !strings.Contains(got, want) {
			t.Errorf("humanizeReason(%q) = %q, want it to contain %q", code, got, want)
		}
	}
}
