package rollup

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config is the rollup configuration. See docs/rollups.md for the
// operator-facing reference.
type Config struct {
	Enabled           bool          `mapstructure:"enabled"`
	TZ                string        `mapstructure:"tz"`
	Builder           bool          `mapstructure:"builder"`
	Tiers             []string      `mapstructure:"tiers"`
	GraceWindow       time.Duration `mapstructure:"grace_window"`
	CoverageThreshold float64       `mapstructure:"coverage_threshold"`
	DimRichCap        int           `mapstructure:"dim_rich_cap"`
	HLLLgK            int           `mapstructure:"hll_lg_k"`
	KLLk              int           `mapstructure:"kll_k"`
	ObsoleteGrace     time.Duration `mapstructure:"obsolete_grace"`

	// Tables is the per-table override map. Maps "db.table" → overrides.
	Tables map[string]TableOverride `mapstructure:"tables"`
}

// TableOverride is `[rollup.tables."db.table"]` in arc.toml.
type TableOverride struct {
	TimeColumn  string   `mapstructure:"time_column"`
	ForceKeep   []string `mapstructure:"force_keep"`
	ForceSketch []string `mapstructure:"force_sketch"`
	IgnoreCols  []string `mapstructure:"ignore_cols"`
}

// ParseConfig reads the [rollup] section from a viper instance.
func ParseConfig(v *viper.Viper) (Config, error) {
	var cfg Config
	if err := v.UnmarshalKey("rollup", &cfg); err != nil {
		return cfg, fmt.Errorf("rollup config: %w", err)
	}
	return cfg, nil
}
