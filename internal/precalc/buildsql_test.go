package precalc

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

func TestBuildSQL_Rollup_DayFromHour(t *testing.T) {
	sql := BuildRollupSketchSQL(RollupArgs{
		TargetTier: Tier1d,
		SourcePath: "/tmp/foo/1h.parquet",
		MetricCols: []MetricCol{{Name: "x", Numeric: true}},
		HLLCols:    []string{"id"},
		KLLCols:    []string{"x"},
		HLLLgK:     14,
		KLLk:       200,
	})
	want := []string{
		"date_trunc('day', bucket) AS bucket",
		"SUM(cnt) AS cnt",
		"SUM(sum_x) AS sum_x",
		"MIN(min_x) AS min_x",
		"MAX(max_x) AS max_x",
		"datasketch_hll(14, CAST(hll_id AS sketch_hll)) AS hll_id",
		"datasketch_kll(200, CAST(kll_x AS sketch_kll_double)) AS kll_x",
	}
	for _, w := range want {
		if !strings.Contains(sql, w) {
			t.Errorf("missing %q in:\n%s", w, sql)
		}
	}
}
