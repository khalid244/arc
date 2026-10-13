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

func TestReorgPhantomKeysIsExposedOnBothSurfaces(t *testing.T) {
	m := Get()
	before := m.Snapshot()["reorg_phantom_keys_total"].(int64)
	m.IncReorgPhantomKeys(2)
	after, ok := m.Snapshot()["reorg_phantom_keys_total"].(int64)
	if !ok {
		t.Fatal("reorg_phantom_keys_total is missing from the JSON snapshot")
	}
	if after != before+2 {
		t.Errorf("counter = %d, want %d", after, before+2)
	}
	prom := m.PrometheusFormat()
	for _, want := range []string{
		"# HELP arc_reorg_phantom_keys_total",
		"# TYPE arc_reorg_phantom_keys_total counter",
		"arc_reorg_phantom_keys_total ",
	} {
		if !strings.Contains(prom, want) {
			t.Errorf("Prometheus output is missing %q", want)
		}
	}
}

// A flat late-sidecar key whose name the drain does not recognise is never
// moved; the counter is what makes a writer/reorg naming drift visible.
func TestReorgSidecarKeysUnrecognizedIsExposedOnBothSurfaces(t *testing.T) {
	m := Get()
	before, _ := m.Snapshot()["reorg_sidecar_keys_unrecognized_total"].(int64)
	m.IncReorgSidecarKeysUnrecognized(3)
	after, ok := m.Snapshot()["reorg_sidecar_keys_unrecognized_total"].(int64)
	if !ok {
		t.Fatal("reorg_sidecar_keys_unrecognized_total is missing from the JSON snapshot")
	}
	if after != before+3 {
		t.Errorf("counter = %d, want %d", after, before+3)
	}
	prom := m.PrometheusFormat()
	for _, want := range []string{
		"# HELP arc_reorg_sidecar_keys_unrecognized_total",
		"# TYPE arc_reorg_sidecar_keys_unrecognized_total counter",
		"arc_reorg_sidecar_keys_unrecognized_total ",
	} {
		if !strings.Contains(prom, want) {
			t.Errorf("Prometheus output is missing %q", want)
		}
	}
}
