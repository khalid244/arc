package rollup

import (
	"strings"
	"testing"

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
