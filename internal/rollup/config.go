package rollup

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// Config is the rollup configuration. The full surface is:
//
//	[rollup]
//	enabled                = true   # default false; on builder nodes also needs builder=true
//	dim_cardinality_max    = 1024   # ≤ this → RoleDim (per-dim variant). Raise
//	                                # it to promote mid-card columns. Cols
//	                                # > 1024 still get HighCard=true so they
//	                                # do NOT join the dim-rich cross-product.
//	sketch_cardinality_max = 100000 # > dim_cardinality_max but ≤ this → RoleSketch (HLL)
//	                                # > sketch_cardinality_max → RoleDrop
//	[rollup.tables."db.table"]      # optional per-table escape hatches
//	sketch_columns   = [...]
//	ignore_columns   = [...]
//	quantile_columns = [...]
//	keep_columns     = [...]
//	time_column      = "..."
//
// See docs/rollups.md for the operator-facing reference.
type Config struct {
	Enabled              bool                   `mapstructure:"enabled"`
	DimCardinalityMax    int64                  `mapstructure:"dim_cardinality_max"`
	SketchCardinalityMax int64                  `mapstructure:"sketch_cardinality_max"`
	Tables               map[string]TableConfig `mapstructure:"tables"`
}

// TableConfig holds optional per-table escape hatches that override schema
// inference. All fields are optional; an absent field means "infer from
// schema".
type TableConfig struct {
	SketchColumns   []string `mapstructure:"sketch_columns"`
	IgnoreColumns   []string `mapstructure:"ignore_columns"`
	QuantileColumns []string `mapstructure:"quantile_columns"`
	TimeColumn      string   `mapstructure:"time_column"`
	// KeepColumns forces named columns to classify as RoleDim regardless of
	// cardinality. Useful for high-card columns the operator filters on often.
	// They are NOT added to the dim-rich cross-product variant (which would
	// explode row count) — only a single-dim `by_<col>__1d` variant gets them.
	KeepColumns []string `mapstructure:"keep_columns"`
}

// Exported sketch precision defaults. Mirrors the package-private
// defaultHLLLgK / defaultTDigestK in specgen.go; exported so cmd/arc callers
// (e.g. `arc rollup propose`) can hand them to the proposer without
// re-importing the constants from elsewhere.
const (
	DefaultHLLLgK   = defaultHLLLgK
	DefaultTDigestK = defaultTDigestK
)

// Sampler returns column statistics for the source (db, table). Implementations
// run a DESCRIBE + COUNT(DISTINCT) sweep over the source's parquet files; see
// internal/rollup/sampler.go for the production DuckDB-backed implementation.
type Sampler interface {
	SampleSourceColumns(ctx context.Context, db, table string) ([]ColumnStats, error)
}

// DBTable identifies one (database, table) pair to consider for rollup
// generation. Used by Specs to enumerate the set of tables to infer schemas
// for.
type DBTable struct {
	Database string
	Table    string
}

// Specs runs schema inference per-table and returns the union of generated
// RollupSpec variants. The set of tables considered is the union of cfg.Tables
// (which carries optional escape-hatch hints) and knownTables (discovered from
// the storage backend's database directory). Tables in cfg.Tables but absent
// from the database are still attempted — sampling will fail and they'll be
// skipped with a warning.
//
// Errors from a single table's sampling or inference are logged but don't sink
// the whole call: a single unreachable or schema-less table doesn't block
// rollups for healthy tables.
func (c Config) Specs(ctx context.Context, sampler Sampler, knownTables []DBTable) ([]RollupSpec, error) {
	if !c.Enabled {
		return nil, nil
	}
	if sampler == nil {
		return nil, fmt.Errorf("rollup Specs: nil sampler")
	}

	// Build the deduped (db, table) work-list. cfg.Tables uses lowercase
	// "db.table" keys (see TableKey); knownTables come straight from the
	// backend (typically already lowercase but normalize defensively).
	type tableEntry struct {
		db, tbl string
		hints   TableConfig
	}
	seen := map[string]struct{}{}
	var work []tableEntry

	for key, tc := range c.Tables {
		db, tbl, ok := splitTableKey(key)
		if !ok {
			log.Warn().Str("key", key).Msg("rollup config: skipping table entry — key must be \"db.table\"")
			continue
		}
		k := strings.ToLower(db + "." + tbl)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		work = append(work, tableEntry{db: db, tbl: tbl, hints: tc})
	}
	for _, dt := range knownTables {
		k := strings.ToLower(dt.Database + "." + dt.Table)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		work = append(work, tableEntry{db: dt.Database, tbl: dt.Table})
	}

	var out []RollupSpec
	for _, w := range work {
		stats, err := sampler.SampleSourceColumns(ctx, w.db, w.tbl)
		if err != nil {
			log.Warn().Err(err).Str("db", w.db).Str("table", w.tbl).
				Msg("rollup inference: source sampling failed, skipping table")
			continue
		}
		if len(stats) == 0 {
			log.Debug().Str("db", w.db).Str("table", w.tbl).
				Msg("rollup inference: no columns sampled, skipping table")
			continue
		}
		schema, err := InferSchema(stats, w.hints, ThresholdConfig{
			DimCardinalityMax:    c.DimCardinalityMax,
			SketchCardinalityMax: c.SketchCardinalityMax,
		})
		if err != nil {
			log.Warn().Err(err).Str("db", w.db).Str("table", w.tbl).
				Msg("rollup inference: schema inference failed, skipping table")
			continue
		}
		specs := GenerateSpecs(w.db, w.tbl, schema)
		out = append(out, specs...)
	}
	return out, nil
}

// splitTableKey accepts a "db.table" key (as used in [rollup.tables] section
// names) and returns the two parts. Keys with no dot or multiple dots are
// rejected — the second case avoids ambiguity for tables whose names contain
// a dot (which Arc doesn't currently support).
func splitTableKey(key string) (db, table string, ok bool) {
	parts := strings.SplitN(key, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// ParseConfig reads the [rollup] section from a viper instance and applies
// defaults aligned with industry practice:
//   - 1024 dim cutoff keeps the dim-rich cross-product manageable
//     (ClickHouse LowCardinality is most useful < 10K, but Arc's dim-rich
//     cross-product cost is multiplicative across dims, so we go lower).
//   - 100K sketch cutoff matches ClickHouse's "degraded above" threshold
//     and Druid's general recommendation that beyond this, raw data
//     storage is preferable to HLL approximation.
func ParseConfig(v *viper.Viper) (Config, error) {
	var cfg Config
	if err := v.UnmarshalKey("rollup", &cfg); err != nil {
		return cfg, fmt.Errorf("rollup config: %w", err)
	}
	if cfg.DimCardinalityMax == 0 {
		cfg.DimCardinalityMax = 1024
	}
	if cfg.SketchCardinalityMax == 0 {
		cfg.SketchCardinalityMax = 100000
	}
	if cfg.SketchCardinalityMax < cfg.DimCardinalityMax {
		return cfg, fmt.Errorf("rollup: sketch_cardinality_max (%d) must be >= dim_cardinality_max (%d)",
			cfg.SketchCardinalityMax, cfg.DimCardinalityMax)
	}
	return cfg, nil
}

// TableKey returns "db.table" lookup keys for case-insensitive matching.
func (c Config) TableKey(db, table string) string {
	return strings.ToLower(db + "." + table)
}

// normalizeBucketString accepts forms like "1h", "5m", "1d", "1w" and
// translates "d"/"w" into Go-parseable forms (time.ParseDuration only knows
// up to "h").
func normalizeBucketString(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "w") {
		n := strings.TrimSuffix(s, "w")
		return fmt.Sprintf("%dh", parseLeadingInt(n)*7*24)
	}
	if strings.HasSuffix(s, "d") {
		n := strings.TrimSuffix(s, "d")
		return fmt.Sprintf("%dh", parseLeadingInt(n)*24)
	}
	return s
}

func parseLeadingInt(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int(c-'0')
	}
	if n == 0 {
		return 1
	}
	return n
}
