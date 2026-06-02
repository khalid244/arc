package rollup

import (
	"reflect"
	"testing"
)

// TestClassifyColumn pins the column-role rules: continuous floats are always
// metrics (their sampled cardinality under-counts), integers split metric/dim by
// cardinality, strings split dim/sketch, and all-NULL columns are skipped.
func TestClassifyColumn(t *testing.T) {
	cfg := ClassifyConfig{MaxDimCard: 1024, MaxPerDimCard: 50000}
	cases := []struct {
		typ  string
		card int
		want colClass
	}{
		{"DOUBLE", 5, classMetric},           // continuous, even at low sampled card -> metric (the duration_seconds bug)
		{"DOUBLE", 200000, classMetric},      // continuous, high card -> metric
		{"DECIMAL(18,2)", 300, classMetric},  // decimal money -> metric
		{"FLOAT", 900, classMetric},          // float -> metric
		{"BIGINT", 500, classDim},            // low-card integer (e.g. status code) -> dimension
		{"INTEGER", 5000, classMetric},       // high-card integer -> metric
		{"VARCHAR", 800, classDim},           // low-card string -> dimension
		{"VARCHAR", 3319, classDim},          // medium-card string (like site) -> still a dimension
		{"VARCHAR", 100000, classSketch},     // very-high-card string -> HLL sketch
		{"VARCHAR", 0, classSkip},            // all-NULL in sample -> skip
		{"DOUBLE", 0, classSkip},             // all-NULL continuous -> skip (card==0 wins)
	}
	for _, c := range cases {
		if got := classifyColumn(c.typ, c.card, cfg); got != c.want {
			t.Errorf("classifyColumn(%q, %d) = %d, want %d", c.typ, c.card, got, c.want)
		}
	}
}

// TestDimRichSpecExcludesMediumCard pins that the dim-rich cube unions only low-card
// dims, so a medium-card dim cannot explode the cross-product toward source size.
func TestDimRichSpecExcludesMediumCard(t *testing.T) {
	p := TableProfile{
		Source: "default.downloads", Grain: "hour",
		DimCard: map[string]int{"site": 3319, "vpn": 2, "tag": 40, "region": 200},
	}
	spec, ok := p.DimRichSpec(12, 1024)
	if !ok {
		t.Fatal("expected a dim-rich cube from the low-card dims")
	}
	want := []string{"region", "tag", "vpn"} // site (3319 > 1024) excluded
	if !reflect.DeepEqual(spec.Dims, want) {
		t.Fatalf("dim-rich dims = %v, want %v (site must be excluded)", spec.Dims, want)
	}

	// Fewer than 2 low-card dims => no dim-rich cube.
	p2 := TableProfile{Source: "s", Grain: "hour", DimCard: map[string]int{"a": 10, "big": 99999}}
	if _, ok := p2.DimRichSpec(12, 1024); ok {
		t.Fatal("expected ok=false with only one low-card dim")
	}

	// More low-card dims than maxDims => skipped.
	p3 := TableProfile{Source: "s", Grain: "hour", DimCard: map[string]int{"a": 1, "b": 1, "c": 1}}
	if _, ok := p3.DimRichSpec(2, 1024); ok {
		t.Fatal("expected ok=false when low-card dims exceed maxDims")
	}
}
