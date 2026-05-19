package tiered

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// Config is the rollup configuration. Operator-facing reference: see
// docs/rollups.md. The minimum-viable config is:
//
//	[rollup]
//	enabled = true
//	tz      = "Asia/Riyadh"   # REQUIRED — bucket alignment timezone
//	builder = true             # one node per cluster materializes
type Config struct {
	Enabled           bool          `mapstructure:"enabled"`
	TZ                string        `mapstructure:"tz"`
	Builder           bool          `mapstructure:"builder"`
	GraceWindow       time.Duration `mapstructure:"grace_window"`
	RecentGrace       time.Duration `mapstructure:"recent_grace"`
	CoverageThreshold float64       `mapstructure:"coverage_threshold"`
	DimRichCap        int           `mapstructure:"dim_rich_cap"`
	HLLLgK            int           `mapstructure:"hll_lg_k"`
	KLLk              int           `mapstructure:"kll_k"`
	ObsoleteGrace     time.Duration `mapstructure:"obsolete_grace"`
	RebuildHorizon    time.Duration `mapstructure:"rebuild_horizon"`

	Tables map[string]TableOverride `mapstructure:"tables"`

	// ExcludeTables filters out table names that match any of these glob
	// patterns (e.g. `*_late`, `*_test`). Patterns are checked against the
	// unqualified table name (after the dot in `db.table`). Defaults to
	// ["*_late"] so delayed/late-arriving variants of base tables are
	// skipped — rolling them up would double-count their data on top of
	// the base table's rollup.
	ExcludeTables []string `mapstructure:"exclude_tables"`
}

// TableOverride is `[rollup.tables."db.table"]` in arc.toml.
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
	if c.GraceWindow == 0 {
		c.GraceWindow = 15 * time.Minute
	}
	if c.RecentGrace == 0 {
		c.RecentGrace = 48 * time.Hour
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
	// `*_late` is ALWAYS excluded — rolling up late-arriving variants on top
	// of their base table would double-count the data. User-supplied patterns
	// add to this baseline; they don't replace it.
	hasLate := false
	for _, p := range c.ExcludeTables {
		if p == "*_late" {
			hasLate = true
			break
		}
	}
	if !hasLate {
		c.ExcludeTables = append(c.ExcludeTables, "*_late")
	}
}

// IsExcluded returns true if the given table name (qualified as `db.table`
// or bare `table`) matches any pattern in ExcludeTables. Patterns are
// glob-style (`*` wildcard) and match against the unqualified table name.
func (c *Config) IsExcluded(table string) bool {
	bare := table
	if i := strings.LastIndex(table, "."); i >= 0 {
		bare = table[i+1:]
	}
	for _, pat := range c.ExcludeTables {
		if ok, _ := filepath.Match(pat, bare); ok {
			return true
		}
	}
	return false
}

// Validate returns an error if required fields are missing or inconsistent.
// Must be called before any builder or router uses the config.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.TZ == "" {
		return fmt.Errorf("rollup.tz is required when rollup.enabled = true")
	}
	if _, err := time.LoadLocation(c.TZ); err != nil {
		return fmt.Errorf("rollup.tz %q is not a valid timezone: %w", c.TZ, err)
	}
	if c.CoverageThreshold <= 0 || c.CoverageThreshold > 1 {
		return fmt.Errorf("rollup.coverage_threshold must be in (0, 1], got %v", c.CoverageThreshold)
	}
	if c.HLLLgK < 4 || c.HLLLgK > 21 {
		return fmt.Errorf("rollup.hll_lg_k must be in [4, 21], got %d", c.HLLLgK)
	}
	return nil
}
