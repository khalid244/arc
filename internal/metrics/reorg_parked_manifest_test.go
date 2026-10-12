package metrics

import (
	"strings"
	"testing"
)

// A parked reorg manifest stops holding its sources back, so its counter must
// reach both operator surfaces.
func TestReorgManifestParkedUnparseableIsExposedOnBothSurfaces(t *testing.T) {
	m := Get()
	before := m.Snapshot()["reorg_manifests_parked_unparseable_total"].(int64)

	m.IncReorgManifestParkedUnparseable()

	after, ok := m.Snapshot()["reorg_manifests_parked_unparseable_total"].(int64)
	if !ok {
		t.Fatal("reorg_manifests_parked_unparseable_total is missing from the JSON snapshot")
	}
	if after != before+1 {
		t.Errorf("counter = %d, want %d", after, before+1)
	}
	prom := m.PrometheusFormat()
	for _, want := range []string{
		"# HELP arc_reorg_manifests_parked_unparseable_total",
		"# TYPE arc_reorg_manifests_parked_unparseable_total counter",
		"arc_reorg_manifests_parked_unparseable_total ",
	} {
		if !strings.Contains(prom, want) {
			t.Errorf("Prometheus output is missing %q", want)
		}
	}
}
