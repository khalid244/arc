package tiered

import (
	"fmt"
	"time"
)

// Config is the precalc configuration. Operator-facing reference: see
// docs/precalc.md (forthcoming). The minimum-viable config is:
//
//	[precalc]
//	enabled = true
//	tz      = "Asia/Riyadh"   # REQUIRED — bucket alignment timezone
//	builder = true             # one node per cluster materializes
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

	Tables map[string]TableOverride `mapstructure:"tables"`
}

// TableOverride is `[precalc.tables."db.table"]` in arc.toml.
type TableOverride struct {
	TimeColumn  string   `mapstructure:"time_column"`
	Source      string   `mapstructure:"source"`
	DimColumns  []string `mapstructure:"dim_columns"`
	ForceKeep   []string `mapstructure:"force_keep"`
	ForceSketch []string `mapstructure:"force_sketch"`
	IgnoreCols  []string `mapstructure:"ignore_cols"`
}

// Defaults fills zero values with documented defaults. Call after viper unmarshal.
func (c *Config) Defaults() {
	if len(c.Tiers) == 0 {
		c.Tiers = []string{"1h", "1d", "1w", "1mo"}
	}
	if c.GraceWindow == 0 {
		c.GraceWindow = 15 * time.Minute
	}
	if c.CoverageThreshold == 0 {
		c.CoverageThreshold = 0.99
	}
	if c.DimRichCap == 0 {
		c.DimRichCap = 100
	}
	if c.HLLLgK == 0 {
		c.HLLLgK = 14
	}
	if c.KLLk == 0 {
		c.KLLk = 200
	}
	if c.ObsoleteGrace == 0 {
		c.ObsoleteGrace = 7 * 24 * time.Hour
	}
}

// Validate returns an error if required fields are missing or inconsistent.
// Must be called before any builder or router uses the config.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.TZ == "" {
		return fmt.Errorf("precalc.tz is required when precalc.enabled = true")
	}
	if _, err := time.LoadLocation(c.TZ); err != nil {
		return fmt.Errorf("precalc.tz %q is not a valid timezone: %w", c.TZ, err)
	}
	if c.CoverageThreshold <= 0 || c.CoverageThreshold > 1 {
		return fmt.Errorf("precalc.coverage_threshold must be in (0, 1], got %v", c.CoverageThreshold)
	}
	if c.HLLLgK < 4 || c.HLLLgK > 21 {
		return fmt.Errorf("precalc.hll_lg_k must be in [4, 21], got %d", c.HLLLgK)
	}
	return nil
}
