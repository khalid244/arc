package metrics

import (
	"strings"
	"testing"
)

// The two counters manifest recovery moves when it keeps an output it would
// once have deleted reach both operator surfaces.
func TestCompactionRecoveryOutputCountersAreExposed(t *testing.T) {
	m := Get()
	for _, tc := range []struct {
		inc      func()
		snapshot string
		prom     string
	}{
		{m.IncCompactionRecoveryOutputsParked, "compaction_outputs_parked", "arc_compaction_recovery_outputs_parked_total"},
		{m.IncCompactionRecoveryOutputsQuarantined, "compaction_outputs_quarantined", "arc_compaction_recovery_outputs_quarantined_total"},
	} {
		before, ok := m.Snapshot()[tc.snapshot].(int64)
		if !ok {
			t.Fatalf("%s missing from the JSON snapshot", tc.snapshot)
		}
		tc.inc()
		if after := m.Snapshot()[tc.snapshot].(int64); after != before+1 {
			t.Errorf("%s = %d, want %d", tc.snapshot, after, before+1)
		}
		prom := m.PrometheusFormat()
		for _, want := range []string{"# HELP " + tc.prom + " ", "# TYPE " + tc.prom + " counter", "\n" + tc.prom + " "} {
			if !strings.Contains(prom, want) {
				t.Errorf("Prometheus output is missing %q", want)
			}
		}
	}
}
