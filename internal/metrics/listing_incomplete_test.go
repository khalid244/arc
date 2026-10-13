package metrics

import (
	"strings"
	"testing"
)

// QR-6: queries refused with a 503 because partition pruning could not verify
// its storage listing (failed or timed out), or because DuckDB found no files
// on a partition a listing said existed, are counted so an S3 slow period is
// visible instead of reading as silently empty dashboards.
func TestMetrics_QueryListingIncomplete_CountedAndExported(t *testing.T) {
	m := &Metrics{}
	m.IncQueryListingIncomplete()
	m.IncQueryListingIncomplete()
	if got := m.Snapshot()["query_listing_incomplete_total"]; got != int64(2) {
		t.Fatalf("snapshot query_listing_incomplete_total = %v, want 2", got)
	}
	out := m.PrometheusFormat()
	for _, want := range []string{
		"# HELP arc_query_listing_incomplete_total ",
		"# TYPE arc_query_listing_incomplete_total counter",
		"arc_query_listing_incomplete_total 2",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q from /metrics output", want)
		}
	}
}
