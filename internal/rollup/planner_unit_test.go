package rollup

import (
	"sort"
	"strings"
	"testing"
)

// fakeCard: device_id/ip/url/title are high-cardinality; everything else low.
func fakeCard(_, dim string) int {
	switch dim {
	case "device_id", "ip", "url", "title":
		return 1_000_000
	default:
		return 100
	}
}

func dimsOf(cubes []CubeSpec) []string {
	var out []string
	for _, c := range cubes {
		out = append(out, "["+strings.Join(c.Dims, ",")+"]")
	}
	sort.Strings(out)
	return out
}

func TestPlan_SharedDimRichPlusSpecialized(t *testing.T) {
	w := NewWorkload()
	// Low-card group-bys -> fold into shared dim-rich cube.
	w.Record(shape("hour", []string{"status"}, []Aggregate{{Kind: AggCount}}, nil))
	w.Record(shape("hour", []string{"region"}, []Aggregate{{Kind: AggCount}}, nil))
	w.Record(shape("hour", []string{"status", "tag"}, []Aggregate{{Kind: AggSum, Col: "duration_seconds"}}, nil))
	// High-card group-by -> dedicated narrow cube.
	w.Record(shape("hour", []string{"ip"}, []Aggregate{{Kind: AggCount}}, nil))
	// Percentile by hour (no dims) -> dedicated coarse cube for accuracy.
	w.Record(shape("hour", nil, []Aggregate{{Kind: AggPercentile, Col: "duration_seconds", P: 0.95}}, nil))

	cubes := Plan(w, fakeCard, PlanConfig{})

	got := dimsOf(cubes)
	// Expect: shared dim-rich [region,status,tag]; coarse []; per-dim [ip].
	want := []string{"[]", "[ip]", "[region,status,tag]"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("cube dim-sets = %v, want %v", got, want)
	}

	// The shared dim-rich cube must NOT contain the high-card ip dim.
	for _, c := range cubes {
		for _, d := range c.Dims {
			if d == "ip" && len(c.Dims) > 1 {
				t.Fatalf("high-card ip leaked into shared cube %v", c.Dims)
			}
		}
	}
}

func TestPlan_NeverMaterializesUnqueried(t *testing.T) {
	w := NewWorkload()
	w.Record(shape("hour", []string{"status"}, []Aggregate{{Kind: AggCount}}, nil))
	cubes := Plan(w, fakeCard, PlanConfig{})
	// Only one shape queried -> exactly one (shared) cube; nothing speculative.
	if len(cubes) != 1 {
		t.Fatalf("expected 1 cube for 1 shape, got %d: %v", len(cubes), dimsOf(cubes))
	}
	if strings.Join(cubes[0].Dims, ",") != "status" {
		t.Fatalf("expected status cube, got %v", cubes[0].Dims)
	}
}

func TestPlan_MinCountGate(t *testing.T) {
	w := NewWorkload()
	w.Record(shape("hour", []string{"status"}, []Aggregate{{Kind: AggCount}}, nil)) // count 1
	if cubes := Plan(w, fakeCard, PlanConfig{MinCount: 2}); len(cubes) != 0 {
		t.Fatalf("MinCount=2 must drop single-shot shapes, got %v", dimsOf(cubes))
	}
}
