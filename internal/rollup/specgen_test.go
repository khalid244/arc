package rollup

import (
	"testing"
)

func specNames(specs []RollupSpec) []string {
	out := make([]string, 0, len(specs))
	for _, s := range specs {
		out = append(out, s.Name)
	}
	return out
}

// TestGenerateSpecs_Shape verifies the fixed three-kind output:
//
//	__1d (dim-rich) + __sketch_1d (no-dim) + __by_<dim>__1d (one per RoleDim)
//
// when there is at least one sketched column or t-digest metric.
func TestGenerateSpecs_Shape(t *testing.T) {
	schema := InferredSchema{
		TimeColumn: "time",
		Columns: []ClassifiedColumn{
			{Name: "time", Role: RoleTime},
			{Name: "country", Role: RoleDim},
			{Name: "status", Role: RoleDim},
			{Name: "duration_seconds", Role: RoleMetric, TDigest: true},
			{Name: "device_id", Role: RoleSketch, HLL: true},
		},
	}
	specs := GenerateSpecs("default", "downloads", schema)
	want := []string{
		"default__downloads__1d",
		"default__downloads__sketch_1d",
		"default__downloads__by_country__1d",
		"default__downloads__by_status__1d",
	}
	if len(specs) != len(want) {
		t.Fatalf("expected %d specs, got %d (%v)", len(want), len(specs), specNames(specs))
	}
	got := map[string]bool{}
	for _, s := range specs {
		got[s.Name] = true
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("missing spec: %s", w)
		}
	}
}

// TestGenerateSpecs_NoSketchesNoSketchVariant verifies sketch_1d and per-dim
// variants are omitted when there are no sketched columns and no t-digest
// metric — the dim-rich __1d alone is sufficient.
func TestGenerateSpecs_NoSketchesNoSketchVariant(t *testing.T) {
	schema := InferredSchema{
		TimeColumn: "time",
		Columns: []ClassifiedColumn{
			{Name: "time", Role: RoleTime},
			{Name: "country", Role: RoleDim},
			{Name: "status_code", Role: RoleMetric}, // no TDigest
		},
	}
	specs := GenerateSpecs("default", "downloads", schema)
	if len(specs) != 1 || specs[0].Name != "default__downloads__1d" {
		t.Errorf("expected only __1d, got %v", specNames(specs))
	}
}

// TestGenerateSpecs_PerDimEmittedForTDigestOnly verifies a TDigest metric (no
// id-like column) still produces per-dim variants — needed for
// "percentile_cont(...) GROUP BY day, dim" queries.
func TestGenerateSpecs_PerDimEmittedForTDigestOnly(t *testing.T) {
	schema := InferredSchema{
		TimeColumn: "time",
		Columns: []ClassifiedColumn{
			{Name: "time", Role: RoleTime},
			{Name: "country", Role: RoleDim},
			{Name: "duration_seconds", Role: RoleMetric, TDigest: true},
		},
	}
	specs := GenerateSpecs("default", "downloads", schema)
	found := false
	for _, s := range specs {
		if s.Name == "default__downloads__by_country__1d" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected by_country__1d for TDigest-only table; got %v", specNames(specs))
	}
}

func TestGenerateSpecs_PopulatesSketchConfig(t *testing.T) {
	schema := InferredSchema{
		TimeColumn: "time",
		Columns: []ClassifiedColumn{
			{Name: "time", Role: RoleTime},
			{Name: "country", Role: RoleDim},
			{Name: "duration_seconds", Role: RoleMetric, TDigest: true},
			{Name: "device_id", Role: RoleSketch, HLL: true},
		},
	}
	specs := GenerateSpecs("d", "t", schema)
	for _, s := range specs {
		for _, agg := range s.Aggregations {
			needsSketch := false
			for _, f := range agg.Functions {
				if f == AggHLL || f == AggTDigest {
					needsSketch = true
				}
			}
			if needsSketch && agg.SketchConfig == nil {
				t.Errorf("%s: aggregation on %q (funcs=%v) missing SketchConfig", s.Name, agg.SourceColumn, agg.Functions)
			}
		}
		if err := s.Validate(); err != nil {
			t.Errorf("%s: Validate() = %v", s.Name, err)
		}
	}
}

// TestGenerateSpecs_HighCardKeepColumns verifies that a force-kept high-card
// column (via TableConfig.KeepColumns → HighCard=true) gets its own
// by_<col>__1d variant but is excluded from the dim-rich __1d cross-product.
func TestGenerateSpecs_HighCardKeepColumns(t *testing.T) {
	schema := InferredSchema{
		TimeColumn: "time",
		Columns: []ClassifiedColumn{
			{Name: "time", Role: RoleTime},
			{Name: "country", Role: RoleDim},
			{Name: "site", Role: RoleDim, HighCard: true},
			{Name: "device_id", Role: RoleSketch, HLL: true},
		},
	}
	specs := GenerateSpecs("default", "downloads", schema)
	byName := map[string]RollupSpec{}
	for _, s := range specs {
		byName[s.Name] = s
	}
	dr, ok := byName["default__downloads__1d"]
	if !ok {
		t.Fatalf("missing dim-rich __1d spec; got %v", specNames(specs))
	}
	if len(dr.KeepDimensions) != 1 || dr.KeepDimensions[0] != "country" {
		t.Errorf("dim-rich KeepDimensions = %v, want [country] (site is HighCard, must be excluded)", dr.KeepDimensions)
	}
	if _, ok := byName["default__downloads__by_site__1d"]; !ok {
		t.Errorf("missing by_site__1d spec for high-card site; got %v", specNames(specs))
	}
	if _, ok := byName["default__downloads__by_country__1d"]; !ok {
		t.Errorf("missing by_country__1d spec; got %v", specNames(specs))
	}
}
