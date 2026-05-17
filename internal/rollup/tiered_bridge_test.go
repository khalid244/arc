package rollup

import (
	"testing"
	"time"
)

func TestConvertConfig_RoundTrip(t *testing.T) {
	src := Config{
		Enabled:           true,
		TZ:                "Asia/Riyadh",
		Builder:           true,
		Tiers:             []string{"1h", "1d", "1w", "1mo"},
		GraceWindow:       6 * time.Hour,
		CoverageThreshold: 0.99,
		DimRichCap:        100,
		HLLLgK:            14,
		KLLk:              200,
		ObsoleteGrace:     7 * 24 * time.Hour,
		Tables: map[string]TableOverride{
			"default.events": {
				TimeColumn:  "ts",
				ForceKeep:   []string{"region"},
				ForceSketch: []string{"user_id"},
				IgnoreCols:  []string{"url_addr"},
			},
		},
	}
	got := ConvertConfig(src)
	if !got.Enabled || got.TZ != "Asia/Riyadh" {
		t.Errorf("converted: Enabled=%v TZ=%q", got.Enabled, got.TZ)
	}
	if len(got.Tiers) != 4 || got.Tiers[3] != "1mo" {
		t.Errorf("Tiers = %v", got.Tiers)
	}
	if got.GraceWindow != 6*time.Hour || got.HLLLgK != 14 {
		t.Errorf("GraceWindow/HLLLgK mismatch: %+v", got)
	}
	tbl, ok := got.Tables["default.events"]
	if !ok || tbl.TimeColumn != "ts" || len(tbl.ForceKeep) != 1 || tbl.ForceKeep[0] != "region" {
		t.Errorf("table override mismatch: %+v", tbl)
	}

	// After Defaults() + Validate() the config is operational.
	got.Defaults()
	if err := got.Validate(); err != nil {
		t.Errorf("Validate after convert+defaults: %v", err)
	}

	// Sanity: convert should be invariant of zero-valued Tables map.
	src.Tables = nil
	got2 := ConvertConfig(src)
	if got2.Tables != nil && len(got2.Tables) != 0 {
		t.Errorf("nil Tables in source should produce empty/nil in target, got %+v", got2.Tables)
	}
}
