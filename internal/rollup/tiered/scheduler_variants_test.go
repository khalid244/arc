package tiered

import (
	"reflect"
	"testing"
)

func TestVariantsForSpec_EmptySpecHasOnlySketch(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{}}
	got := variantsForSpec(spec, 100)
	want := []variantPlan{{Variant: "sketch"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func TestVariantsForSpec_OneDimAddsAllAndByDim(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{
		"dim_a": {Role: "Dim", KeptValues: []string{"x"}, EffectiveCard: 1},
	}}
	got := variantsForSpec(spec, 100)
	want := []variantPlan{
		{Variant: "sketch"},
		{Variant: "by_dim_a", Dim: "dim_a"},
		{Variant: "all"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func TestVariantsForSpec_PerDimOnlyNoAll(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{
		"dim_a": {Role: "PerDim", KeptValues: []string{"x"}, EffectiveCard: 500},
	}}
	got := variantsForSpec(spec, 100)
	want := []variantPlan{
		{Variant: "sketch"},
		{Variant: "by_dim_a", Dim: "dim_a"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func TestVariantsForSpec_DimAboveCapNoAll(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{
		"dim_a": {Role: "Dim", KeptValues: []string{"x"}, EffectiveCard: 500}, // > cap=100
	}}
	got := variantsForSpec(spec, 100)
	want := []variantPlan{
		{Variant: "sketch"},
		{Variant: "by_dim_a", Dim: "dim_a"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func TestVariantsForSpec_EmptyKeptValuesSkipped(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{
		"dim_a": {Role: "Dim", KeptValues: []string{}},
	}}
	got := variantsForSpec(spec, 100)
	want := []variantPlan{
		{Variant: "sketch"},
		{Variant: "all"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func TestVariantsForSpec_DeterministicDimOrder(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{
		"dim_z": {Role: "Dim", KeptValues: []string{"x"}, EffectiveCard: 1},
		"dim_a": {Role: "Dim", KeptValues: []string{"x"}, EffectiveCard: 1},
		"dim_m": {Role: "Dim", KeptValues: []string{"x"}, EffectiveCard: 1},
	}}
	got := variantsForSpec(spec, 100)
	if len(got) != 5 {
		t.Fatalf("expected 5 entries, got %d: %+v", len(got), got)
	}
	if got[1].Variant != "by_dim_a" || got[2].Variant != "by_dim_m" || got[3].Variant != "by_dim_z" {
		t.Errorf("dim order not sorted: %+v", got)
	}
}
