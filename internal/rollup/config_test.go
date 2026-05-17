package rollup

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestParseConfig_Enabled(t *testing.T) {
	v := viper.New()
	v.SetConfigType("toml")
	v.ReadConfig(strings.NewReader(`[rollup]
enabled = true
`))
	cfg, err := ParseConfig(v)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !cfg.Enabled {
		t.Error("expected Enabled=true")
	}
}

// TestParseConfig_DefaultsDisabled pins the production-safety contract:
// upgrading to a binary that ships rollups must NOT trigger a backfill on
// first restart unless the operator opts in. Absent [rollup] section → off.
func TestParseConfig_DefaultsDisabled(t *testing.T) {
	v := viper.New()
	v.SetConfigType("toml")
	v.ReadConfig(strings.NewReader(`[server]
host = "0.0.0.0"
`))
	cfg, err := ParseConfig(v)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Enabled {
		t.Error("expected Enabled=false when [rollup] is absent")
	}
}

func TestParseConfig_TableKeyIsCaseInsensitive(t *testing.T) {
	v := viper.New()
	v.SetConfigType("toml")
	v.ReadConfig(strings.NewReader(`[rollup]
enabled = true
[rollup.tables."DB.Tbl"]
sketch_columns = ["x"]
`))
	cfg, err := ParseConfig(v)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	key := cfg.TableKey("DB", "Tbl")
	if _, ok := cfg.Tables[key]; !ok {
		t.Errorf("TableKey(%q,%q)=%q not found in Tables (keys: %v)", "DB", "Tbl", key, mapKeys(cfg.Tables))
	}
}

func mapKeys(m map[string]TableConfig) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestConfig_TieredEnableViaNestedTOML(t *testing.T) {
	v := viper.New()
	v.SetConfigType("toml")
	if err := v.ReadConfig(strings.NewReader(`
[rollup]
enabled = true

[rollup.tiered]
enabled = true
tz      = "Asia/Riyadh"
builder = true
tiers   = ["1h", "1d", "1w", "1mo"]
grace_window       = "6h"
coverage_threshold = 0.99
dim_rich_cap       = 100
hll_lg_k           = 14
kll_k              = 200

[rollup.tiered.tables."default.events"]
time_column = "ts"
force_keep  = ["region"]
ignore_cols = ["url"]
`)); err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := v.UnmarshalKey("rollup", &cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.Tiered.Enabled {
		t.Error("Tiered.Enabled should be true")
	}
	if cfg.Tiered.TZ != "Asia/Riyadh" {
		t.Errorf("Tiered.TZ = %q, want Asia/Riyadh", cfg.Tiered.TZ)
	}
	if len(cfg.Tiered.Tiers) != 4 {
		t.Errorf("Tiered.Tiers = %v, want 4 entries", cfg.Tiered.Tiers)
	}
	if cfg.Tiered.GraceWindow != 6*time.Hour {
		t.Errorf("Tiered.GraceWindow = %v, want 6h", cfg.Tiered.GraceWindow)
	}
	if cfg.Tiered.CoverageThreshold != 0.99 {
		t.Errorf("Tiered.CoverageThreshold = %v, want 0.99", cfg.Tiered.CoverageThreshold)
	}
	tbl, ok := cfg.Tiered.Tables["default.events"]
	if !ok {
		t.Fatal("Tiered.Tables['default.events'] missing")
	}
	if tbl.TimeColumn != "ts" || len(tbl.ForceKeep) != 1 || tbl.ForceKeep[0] != "region" {
		t.Errorf("table override mismatch: %+v", tbl)
	}
}

func TestParseConfig_AcceptsEscapeHatches(t *testing.T) {
	v := viper.New()
	v.SetConfigType("toml")
	v.ReadConfig(strings.NewReader(`[rollup]
enabled = true
[rollup.tables."default.downloads"]
sketch_columns   = ["device_id"]
ignore_columns   = ["raw_payload"]
quantile_columns = ["duration_seconds"]
keep_columns     = ["site"]
time_column      = "event_ts"
`))
	cfg, err := ParseConfig(v)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	tbl := cfg.Tables["default.downloads"]
	if len(tbl.SketchColumns) != 1 || tbl.SketchColumns[0] != "device_id" {
		t.Errorf("SketchColumns = %v", tbl.SketchColumns)
	}
	if tbl.TimeColumn != "event_ts" {
		t.Errorf("TimeColumn = %q", tbl.TimeColumn)
	}
	if len(tbl.KeepColumns) != 1 || tbl.KeepColumns[0] != "site" {
		t.Errorf("KeepColumns = %v", tbl.KeepColumns)
	}
}
