package metrics

import (
	"strings"
	"testing"
)

// Review 2026-11-10 N1: tail-fallback merges abandoned past their deadline and
// merges not started because the abandoned cap was full are counted, so a
// storage stall that degrades dashboards shows on /metrics, not only in logs.
func TestMetrics_QueryTailMerges_CountedAndExported(t *testing.T) {
	m := &Metrics{}
	m.IncQueryTailMergeAbandoned()
	m.IncQueryTailMergeAbandoned()
	m.IncQueryTailMergeSkipped()
	snap := m.Snapshot()
	if got := snap["query_tail_merges_abandoned_total"]; got != int64(2) {
		t.Fatalf("snapshot query_tail_merges_abandoned_total = %v, want 2", got)
	}
	if got := snap["query_tail_merges_skipped_total"]; got != int64(1) {
		t.Fatalf("snapshot query_tail_merges_skipped_total = %v, want 1", got)
	}
	out := m.PrometheusFormat()
	for _, want := range []string{
		"# HELP arc_query_tail_merges_abandoned_total ",
		"# TYPE arc_query_tail_merges_abandoned_total counter",
		"arc_query_tail_merges_abandoned_total 2",
		"# HELP arc_query_tail_merges_skipped_total ",
		"# TYPE arc_query_tail_merges_skipped_total counter",
		"arc_query_tail_merges_skipped_total 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q from /metrics output", want)
		}
	}
}
