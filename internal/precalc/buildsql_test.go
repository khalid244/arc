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
