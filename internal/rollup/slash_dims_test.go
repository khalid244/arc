package rollup

import (
	"strings"
	"testing"
)

// slashFixture writes one source day with a normal dim (site) and a dim whose
// name has a '/' (like PostHog's "$feature/<flag>" properties), and returns a
// Manager over it.
func slashFixture(t *testing.T) *Manager {
	t.Helper()
	db := openLocalDuck(t)
	t.Cleanup(func() { db.Close() })
	root := t.TempDir()
	// "feature/x" has 1 distinct value and site has 2, so with equal (zero)
	// workload the planner orders feature/x first (lower cardinality).
	writeSourceDay(t, db, root, "2026/06/01",
		`SELECT TIMESTAMPTZ '2026-06-01 10:00:00' AS "time", 'web' AS site, 'on' AS "feature/x"
		 UNION ALL SELECT TIMESTAMPTZ '2026-06-01 11:00:00', 'ios', 'on'`)
	stg := &localListStorage{fakeStorage: newFakeStorage(), root: root}
	return driftManager(t, db, root, stg)
}

func specDims(specs []CubeSpec) []string {
	var out []string
	for _, s := range specs {
		out = append(out, strings.Join(s.Dims, ","))
	}
	return out
}

func hasDim(specs []CubeSpec, dim string) bool {
	for _, s := range specs {
		for _, d := range s.Dims {
			if d == dim {
				return true
			}
		}
	}
	return false
}

// TestPlanSpecsSkipsSlashDims: no cube is planned on a '/'-named dim (its folder
// would get an extra path level the router never loads), the normal dim still
// gets its per-dim cube.
func TestPlanSpecsSkipsSlashDims(t *testing.T) {
	m := slashFixture(t)
	specs, err := m.planSpecs(freshSrc)
	if err != nil {
		t.Fatalf("planSpecs: %v", err)
	}
	if hasDim(specs, "feature/x") {
		t.Fatalf("planned a cube on a '/' dim: %v", specDims(specs))
	}
	if !hasDim(specs, "site") {
		t.Fatalf("by_site missing: %v", specDims(specs))
	}
	for _, s := range specs {
		if strings.Count(cubeDir(s), "/") != 2 {
			t.Fatalf("cube dir %q is not <db>/<table>/<kind>", cubeDir(s))
		}
	}
}

// TestPlanSpecsSlashDimKeepsItsSlot pins that the skip happens AFTER the MaxDims
// cut: the skipped dim still uses its slot, so no other dim's cube appears in
// its place (in prod that would start a full-history backfill of a new cube).
func TestPlanSpecsSlashDimKeepsItsSlot(t *testing.T) {
	m := slashFixture(t)
	m.cfg.MaxDims = 1 // only the first candidate (feature/x, lower cardinality) fits
	specs, err := m.planSpecs(freshSrc)
	if err != nil {
		t.Fatalf("planSpecs: %v", err)
	}
	if hasDim(specs, "feature/x") || hasDim(specs, "site") {
		t.Fatalf("want only the coarse cube (feature/x skipped in its slot, site past the cap), got %v", specDims(specs))
	}
	if len(specs) != 1 || len(specs[0].Dims) != 0 {
		t.Fatalf("want exactly the coarse cube, got %v", specDims(specs))
	}
}

// TestDimRichAndTargetedSkipSlashDims: the dim-rich union drops '/' dims and a
// targeted cube that names one is rejected (caller warns and skips it).
func TestDimRichAndTargetedSkipSlashDims(t *testing.T) {
	m := slashFixture(t)
	m.cfg.DimRich = true
	specs, err := m.planSpecs(freshSrc)
	if err != nil {
		t.Fatalf("planSpecs: %v", err)
	}
	if hasDim(specs, "feature/x") {
		t.Fatalf("dim-rich or per-dim cube kept a '/' dim: %v", specDims(specs))
	}
	p, err := m.ensureProfile(freshSrc)
	if err != nil {
		t.Fatalf("ensureProfile: %v", err)
	}
	if _, ok := p.DimCard["feature/x"]; !ok {
		t.Fatalf("setup: feature/x should be a profiled dim, got %v", p.DimCard)
	}
	if _, ok := p.targetedSpec([]string{"site", "feature/x"}, nil); ok {
		t.Fatal("targeted cube with a '/' dim was accepted")
	}
	if _, ok := p.targetedSpec([]string{"site"}, nil); !ok {
		t.Fatal("targeted cube on site was rejected")
	}
}

func TestPathSafeDims(t *testing.T) {
	got := pathSafeDims([]string{"a", "feature/x", "b", "survey_dismissed/0d27"})
	if strings.Join(got, ",") != "a,b" {
		t.Fatalf("pathSafeDims = %v", got)
	}
	in := []string{"a", "b/c"}
	_ = pathSafeDims(in)
	if in[0] != "a" || in[1] != "b/c" {
		t.Fatalf("pathSafeDims modified its input: %v", in)
	}
}
