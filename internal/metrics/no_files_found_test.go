package metrics

import (
	"strings"
	"testing"
)

// The "no files found" swallow path in query_arrow_json.go and query.go
// returns a silent empty-success when DuckDB reports the glob matched
// zero parquet files. That's correct for genuinely empty measurements
// but masks stale-cache bugs that cause the same error against existing
// data. Surface a counter so monitoring catches recurrences.

func TestMetrics_IncQueryNoFilesFound_CountsCalls(t *testing.T) {
	m := &Metrics{}
	if m.QueryNoFilesFound.Load() != 0 {
		t.Fatalf("expected initial 0, got %d", m.QueryNoFilesFound.Load())
	}
	m.IncQueryNoFilesFound()
	m.IncQueryNoFilesFound()
	m.IncQueryNoFilesFound()
	if got := m.QueryNoFilesFound.Load(); got != 3 {
		t.Errorf("expected 3 after 3 increments, got %d", got)
	}
}

func TestMetrics_QueryNoFilesFound_ExportedAsPrometheus(t *testing.T) {
	m := &Metrics{}
	m.IncQueryNoFilesFound()
	m.IncQueryNoFilesFound()
	out := m.PrometheusFormat()
	if !strings.Contains(out, "arc_query_no_files_found_total") {
		t.Errorf("metric name missing from /metrics output:\n%s", out)
	}
	if !strings.Contains(out, "arc_query_no_files_found_total 2") {
		t.Errorf("counter value not exported as 2:\n%s", out)
	}
	// HELP and TYPE lines for grafana auto-discovery.
	if !strings.Contains(out, "# HELP arc_query_no_files_found_total") {
		t.Errorf("HELP line missing")
	}
	if !strings.Contains(out, "# TYPE arc_query_no_files_found_total counter") {
		t.Errorf("TYPE line missing")
	}
}
