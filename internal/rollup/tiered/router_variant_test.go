package tiered

import "testing"

func dim_aSpec() DimSpec {
	return DimSpec{Role: "Dim", KeptValues: []string{"val_a", "val_b", "val_c"}, EffectiveCard: 3}
}

func dim_dSpec() DimSpec {
	return DimSpec{Role: "Dim", KeptValues: []string{"yes", "no"}, EffectiveCard: 2}
}

func dim_eSpec() DimSpec {
	return DimSpec{Role: "Dim", KeptValues: []string{"tag_a", "tag_b", "tag_c"}, EffectiveCard: 3}
}

func dim_bSpec() DimSpec {
	return DimSpec{Role: "PerDim", KeptValues: []string{"kept_1", "kept_2"}, EffectiveCard: 2}
}

func dim_b_classSpec() DimSpec {
	return DimSpec{Role: "Dim", KeptValues: []string{"cat_a", "cat_b", "cat_c"}, EffectiveCard: 3}
}

func dim_cSpec() DimSpec {
	return DimSpec{Role: "PerDim", KeptValues: []string{"kept_1", "kept_2"}, EffectiveCard: 2}
}

func highCardSpec() DimSpec {
	return DimSpec{Role: "Dim", KeptValues: nil, EffectiveCard: 5000}
}

func baseSpec(dims map[string]DimSpec) *Spec {
	return &Spec{Dims: dims}
}

func TestPickVariant_SketchWhenNoDims(t *testing.T) {
	shape := &QueryShape{}
	spec := baseSpec(map[string]DimSpec{"dim_a": dim_aSpec()})
	got := PickVariant(shape, spec, 100)
	if got != "sketch" {
		t.Fatalf("expected sketch, got %q", got)
	}
}

func TestPickVariant_BySiteForSingleSiteGroupBy(t *testing.T) {
	shape := &QueryShape{GroupDims: []string{"dim_b_class"}}
	spec := baseSpec(map[string]DimSpec{"dim_b_class": dim_b_classSpec()})
	got := PickVariant(shape, spec, 100)
	if got != "by_dim_b_class" {
		t.Fatalf("expected by_dim_b_class, got %q", got)
	}
}

func TestPickVariant_ByCountryForSingleCountryFilter(t *testing.T) {
	shape := &QueryShape{
		Filters: map[string]FilterPredicate{
			"dim_a": {Op: "=", Values: []string{"val_a"}},
		},
	}
	spec := baseSpec(map[string]DimSpec{"dim_a": dim_aSpec()})
	got := PickVariant(shape, spec, 100)
	if got != "by_dim_a" {
		t.Fatalf("expected by_dim_a, got %q", got)
	}
}

func TestPickVariant_AllForMultipleDimRichDims(t *testing.T) {
	shape := &QueryShape{
		Filters: map[string]FilterPredicate{
			"dim_a": {Op: "=", Values: []string{"val_a"}},
			"dim_d":     {Op: "=", Values: []string{"no"}},
			"dim_e":     {Op: "=", Values: []string{"tag_a"}},
		},
	}
	spec := baseSpec(map[string]DimSpec{
		"dim_a": dim_aSpec(),
		"dim_d":     dim_dSpec(),
		"dim_e":     dim_eSpec(),
	})
	got := PickVariant(shape, spec, 100)
	if got != "all" {
		t.Fatalf("expected all, got %q", got)
	}
}

func TestPickVariant_FallbackOnNonKeptFilterValue(t *testing.T) {
	shape := &QueryShape{
		Filters: map[string]FilterPredicate{
			"dim_a": {Op: "=", Values: []string{"ZZ"}},
		},
	}
	spec := baseSpec(map[string]DimSpec{"dim_a": dim_aSpec()})
	got := PickVariant(shape, spec, 100)
	if got != "" {
		t.Fatalf("expected empty fallback, got %q", got)
	}
}

func TestPickVariant_FallbackOnNicheSiteFilter(t *testing.T) {
	shape := &QueryShape{
		Filters: map[string]FilterPredicate{
			"dim_b": {Op: "=", Values: []string{"unknown_value"}},
		},
	}
	spec := baseSpec(map[string]DimSpec{"dim_b": dim_bSpec()})
	got := PickVariant(shape, spec, 100)
	if got != "" {
		t.Fatalf("expected empty fallback, got %q", got)
	}
}

func TestPickVariant_FallbackOnHighCardDimFilter(t *testing.T) {
	shape := &QueryShape{
		Filters: map[string]FilterPredicate{
			"user_id": {Op: "=", Values: []string{"some-uuid"}},
		},
	}
	spec := baseSpec(map[string]DimSpec{"user_id": {Role: "Drop"}})
	got := PickVariant(shape, spec, 100)
	if got != "" {
		t.Fatalf("expected empty fallback, got %q", got)
	}
}

func TestPickVariant_AllRefusedWhenOneDimIsPerDim(t *testing.T) {
	shape := &QueryShape{
		Filters: map[string]FilterPredicate{
			"dim_a": {Op: "=", Values: []string{"val_a"}},
			"dim_c":    {Op: "=", Values: []string{"kept_1"}},
		},
	}
	spec := baseSpec(map[string]DimSpec{
		"dim_a": dim_aSpec(),
		"dim_c":    dim_cSpec(),
	})
	got := PickVariant(shape, spec, 100)
	if got != "" {
		t.Fatalf("expected empty fallback (PerDim mixed in multi-dim), got %q", got)
	}
}

func TestPickVariant_AcceptIsNotNull(t *testing.T) {
	shape := &QueryShape{
		Filters: map[string]FilterPredicate{
			"dim_a": {Op: "IS NOT NULL"},
		},
	}
	spec := baseSpec(map[string]DimSpec{"dim_a": dim_aSpec()})
	got := PickVariant(shape, spec, 100)
	if got != "by_dim_a" {
		t.Fatalf("expected by_dim_a, got %q", got)
	}
}

func TestPickVariant_AcceptInWithAllKeptValues(t *testing.T) {
	shape := &QueryShape{
		Filters: map[string]FilterPredicate{
			"dim_a": {Op: "IN", Values: []string{"val_a", "val_b"}},
		},
	}
	spec := baseSpec(map[string]DimSpec{"dim_a": dim_aSpec()})
	got := PickVariant(shape, spec, 100)
	if got != "by_dim_a" {
		t.Fatalf("expected by_dim_a, got %q", got)
	}
}

func TestPickVariant_FallbackOnInWithMissingKeptValue(t *testing.T) {
	shape := &QueryShape{
		Filters: map[string]FilterPredicate{
			"dim_a": {Op: "IN", Values: []string{"val_a", "ZZ"}},
		},
	}
	spec := baseSpec(map[string]DimSpec{"dim_a": dim_aSpec()})
	got := PickVariant(shape, spec, 100)
	if got != "" {
		t.Fatalf("expected empty fallback, got %q", got)
	}
}

func TestPickVariant_GroupByDimNotInSpec(t *testing.T) {
	shape := &QueryShape{GroupDims: []string{"unknown_dim"}}
	spec := baseSpec(map[string]DimSpec{"dim_a": dim_aSpec()})
	got := PickVariant(shape, spec, 100)
	if got != "" {
		t.Fatalf("expected empty fallback, got %q", got)
	}
}
