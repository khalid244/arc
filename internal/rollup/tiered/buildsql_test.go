package tiered

import (
	"strings"
	"testing"
)

func TestBuildSQL_Sketch1h(t *testing.T) {
	sql := BuildSketchVariantSQL(BuildArgs{
		Tier:           Tier1h,
		Source:         "read_parquet('/tmp/foo/**/*.parquet')",
		MetricCols:     []MetricCol{{Name: "duration_seconds", Numeric: true}, {Name: "response", Numeric: true}},
		HLLCols:        []string{"device_id", "ip", "url", "title"},
		KLLCols:        []string{"duration_seconds", "response"},
		HLLLgK:         14,
		KLLk:           200,
	})
	want := []string{
		"date_trunc('hour', time) AS bucket",
		"COUNT(*) AS cnt",
		"SUM(duration_seconds) AS sum_duration_seconds",
		"SUM(duration_seconds * duration_seconds) AS sum_sq_duration_seconds",
		"MIN(duration_seconds) AS min_duration_seconds",
		"MAX(duration_seconds) AS max_duration_seconds",
		"datasketch_hll(14, device_id) AS hll_device_id",
		"datasketch_kll(200, duration_seconds) AS kll_duration_seconds",
		"GROUP BY 1",
	}
	for _, w := range want {
		if !strings.Contains(sql, w) {
			t.Errorf("missing %q in generated SQL:\n%s", w, sql)
		}
	}
}

func TestBuildSQL_PerDim(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{"site": {Role: "Dim", KeptValues: []string{"a", "b"}}}}
	sql := BuildPerDimVariantSQL(BuildArgs{
		Tier:       Tier1h,
		Source:     "read_parquet('/tmp/x')",
		MetricCols: []MetricCol{{Name: "x", Numeric: true}},
		HLLCols:    []string{"id"},
		HLLLgK:     14,
	}, spec, "site")
	want := []string{
		"date_trunc('hour', time) AS bucket",
		"CASE WHEN COALESCE(site, '_null_') IN ('a', 'b')",
		"AS site_class",
		"GROUP BY 1, 2",
	}
	for _, w := range want {
		if !strings.Contains(sql, w) {
			t.Errorf("missing %q in:\n%s", w, sql)
		}
	}
}

func TestBuildSQL_DimRich(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{
		"site":    {Role: "Dim", KeptValues: []string{"a", "b"}, EffectiveCard: 2},
		"country": {Role: "Dim", KeptValues: []string{"SA", "EG"}, EffectiveCard: 2},
		"city":    {Role: "PerDim", KeptValues: []string{"x", "y"}, EffectiveCard: 1500}, // > cap
	}}
	sql := BuildDimRichVariantSQL(BuildArgs{
		Tier:       Tier1h,
		Source:     "read_parquet('/tmp/x')",
		MetricCols: []MetricCol{{Name: "x", Numeric: true}},
	}, spec, 100)
	if !strings.Contains(sql, "site_class") {
		t.Error("site_class column missing")
	}
	if !strings.Contains(sql, "country_class") {
		t.Error("country_class column missing")
	}
	if strings.Contains(sql, "city_class") {
		t.Error("city should NOT be in dim-rich (over dim_rich_cap)")
	}
	if strings.Contains(sql, "datasketch_hll") {
		t.Error("dim-rich must NOT contain sketches (storage bloat)")
	}
}

func TestBuildAllVariantsSQL_HasGroupingSetsForAllVariants(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{
		"country": {Role: "Dim", KeptValues: []string{"SA", "EG"}, EffectiveCard: 2},
		"site":    {Role: "Dim", KeptValues: []string{"a", "b"}, EffectiveCard: 2},
		"city":    {Role: "PerDim", KeptValues: []string{"x", "y"}, EffectiveCard: 50},
		"os":      {Role: "Sketch"},
	}}
	sql := BuildAllVariantsSQL(BuildArgs{
		Tier:       Tier1h,
		Source:     "src",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
		HLLCols:    []string{"id"},
		KLLCols:    []string{"m"},
		HLLLgK:     14,
		KLLk:       200,
	}, spec, 100)

	// Must contain GROUPING SETS
	if !strings.Contains(sql, "GROUPING SETS") {
		t.Error("missing GROUPING SETS in generated SQL")
	}

	// Must contain sketch grouping set (bucket only)
	if !strings.Contains(sql, "date_trunc('hour', time)") {
		t.Error("bucket expression missing")
	}

	// Must contain per-dim grouping sets for country, site, city
	for _, dim := range []string{"country", "site", "city"} {
		if !strings.Contains(sql, dim+"_class") {
			t.Errorf("missing %s_class in SQL", dim)
		}
	}

	// Must NOT include the Sketch-role dim
	if strings.Contains(sql, "os_class") {
		t.Error("os (Sketch role) should not appear as a _class column")
	}

	// Must contain GROUPING_ID discriminator
	if !strings.Contains(sql, "GROUPING_ID") {
		t.Error("missing GROUPING_ID in generated SQL")
	}

	// Must contain sketch aggregates
	if !strings.Contains(sql, "datasketch_hll(14, id)") {
		t.Error("missing HLL sketch in generated SQL")
	}
	if !strings.Contains(sql, "datasketch_kll(200, m)") {
		t.Error("missing KLL sketch in generated SQL")
	}

	// Count the number of grouping sets: sketch(1) + per-dim(3: country,site,city) + all(1) = 5
	setCount := strings.Count(sql, "date_trunc('hour', time)")
	// Each grouping set line references the bucket expr, plus the outer SELECT:
	// SELECT has 1, GROUP BY has 5 sets = 6 total occurrences
	if setCount < 5 {
		t.Errorf("expected at least 5 occurrences of bucket expr (1 SELECT + 5 grouping sets), got %d\nSQL:\n%s", setCount, sql)
	}
}

func TestVariantGroupingID_Sketch(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{
		"country": {Role: "Dim", KeptValues: []string{"SA"}, EffectiveCard: 1},
		"site":    {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1},
	}}
	allDims := allVariantDims(spec, 100)
	n := len(allDims)
	if n != 2 {
		t.Fatalf("expected 2 dims, got %d: %v", n, allDims)
	}
	sketchID := VariantGroupingID("sketch", allDims, spec, 100)
	want := (1 << n) - 1
	if sketchID != want {
		t.Errorf("sketch variant_id = %d, want %d", sketchID, want)
	}
}

func TestVariantGroupingID_PerDim(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{
		"country": {Role: "Dim", KeptValues: []string{"SA"}, EffectiveCard: 1},
		"site":    {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1},
	}}
	allDims := allVariantDims(spec, 100)
	// allDims sorted: ["country", "site"] (alphabetical)
	// by_country: country is grouped (GROUPING=0), site is not (GROUPING=1)
	// GROUPING_ID(country_class, site_class) = 0*2^1 + 1*2^0 = 1
	id := VariantGroupingID("by_country", allDims, spec, 100)
	if id != 1 {
		t.Errorf("by_country variant_id = %d, want 1 (allDims=%v)", id, allDims)
	}
	// by_site: site is grouped (GROUPING=0), country is not (GROUPING=1)
	// GROUPING_ID(country_class, site_class) = 1*2^1 + 0*2^0 = 2
	id2 := VariantGroupingID("by_site", allDims, spec, 100)
	if id2 != 2 {
		t.Errorf("by_site variant_id = %d, want 2 (allDims=%v)", id2, allDims)
	}
}

func TestVariantGroupingID_All(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{
		"country": {Role: "Dim", KeptValues: []string{"SA"}, EffectiveCard: 1},
		"site":    {Role: "Dim", KeptValues: []string{"a"}, EffectiveCard: 1},
	}}
	allDims := allVariantDims(spec, 100)
	// all: both dims grouped → GROUPING_ID = 0
	id := VariantGroupingID("all", allDims, spec, 100)
	if id != 0 {
		t.Errorf("all variant_id = %d, want 0 (allDims=%v)", id, allDims)
	}
}

