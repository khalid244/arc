package rollup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// Config is the rollup configuration. See docs/rollups.md for the
// operator-facing reference.
type Config struct {
	// --- tiered fields (PRIMARY) -------------------------------------
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

	// --- DEPRECATED: legacy v1 rollup knobs --------------------------
	// Retained so existing TOML parses without error; no longer drive any
	// builder or router. Will be removed in a future Arc release.
	DimCardinalityMax    int64         `mapstructure:"dim_cardinality_max"`
	SketchCardinalityMax int64         `mapstructure:"sketch_cardinality_max"`
	BuildGrace           time.Duration `mapstructure:"build_grace"`
}

// TableOverride is `[rollup.tables."db.table"]` in arc.toml.
type TableOverride struct {
	TimeColumn  string   `mapstructure:"time_column"`
	ForceKeep   []string `mapstructure:"force_keep"`
	ForceSketch []string `mapstructure:"force_sketch"`
	IgnoreCols  []string `mapstructure:"ignore_cols"`

	// Deprecated — legacy v1 knobs retained for parse compat.
	SketchColumns   []string `mapstructure:"sketch_columns"`
	KeepColumns     []string `mapstructure:"keep_columns"`
	IgnoreColumns   []string `mapstructure:"ignore_columns"`
	QuantileColumns []string `mapstructure:"quantile_columns"`
}

// TableConfig is a deprecated alias for TableOverride. Retained so
// legacy callers (inference.go, scheduler.go, etc.) keep compiling.
// Will be removed in a future cleanup.
type TableConfig = TableOverride

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
//
// DescribeSourceColumns returns the column SET (name + type) without the
// expensive COUNT(DISTINCT) per column. Used by SpecsCached to verify that
// a table's column shape hasn't changed since the persisted-specs cache was
// written, so we can skip the full inference run.
type Sampler interface {
	SampleSourceColumns(ctx context.Context, db, table string) ([]ColumnStats, error)
	DescribeSourceColumns(ctx context.Context, db, table string) ([]ColumnStats, error)
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

	// Build the deduped (db, table) work-list. Shared with SpecsCached so
	// the cache verifier and the inference path iterate the same set.
	work := mergeTableEntries(c, knownTables)

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

// SpecsCached is Specs() with a persistent cache. On startup it tries to
// load the previous run's inferred specs from S3 and verify that:
//   - the rollup config (cardinality knobs + per-table hints) hasn't changed
//   - every covered table's column SET hasn't changed (cheap DESCRIBE per
//     table — no COUNT(DISTINCT) sweep)
//
// If both checks pass, the persisted specs are returned verbatim. This is
// what removes the restart-time non-determinism: the same sample-derived
// classifications survive the restart instead of being recomputed from
// (slightly different) live data.
//
// On any miss — first run, schema change, or config change — falls back to
// running full Specs() and writing the result back to the cache.
//
// Errors loading or saving the cache are LOGGED but never block: a flaky
// cache write must not stop rollup builds. The result is "always at least
// as good as the uncached path".
func (c Config) SpecsCached(
	ctx context.Context,
	backend storage.Backend,
	sampler Sampler,
	knownTables []DBTable,
	log zerolog.Logger,
) ([]RollupSpec, error) {
	if !c.Enabled {
		return nil, nil
	}
	if sampler == nil {
		return nil, fmt.Errorf("rollup SpecsCached: nil sampler")
	}
	if backend == nil {
		// No backend means no cache; fall through to uncached behavior.
		return c.Specs(ctx, sampler, knownTables)
	}

	cfgFP := c.Fingerprint()
	work := mergeTableEntries(c, knownTables)

	persisted, err := LoadPersistedSpecs(ctx, backend)
	if err != nil {
		log.Warn().Err(err).Msg("rollup SpecsCached: failed to load persisted specs; will re-infer")
	}

	if persisted != nil && persisted.ConfigFingerprint == cfgFP {
		// Verify each table's column shape against the persisted fingerprint.
		// We use the cheap DESCRIBE-only path (no COUNT DISTINCT), so this
		// stays fast even on wide tables.
		allMatch := true
		for _, w := range work {
			key := strings.ToLower(w.db + "." + w.tbl)
			cols, derr := sampler.DescribeSourceColumns(ctx, w.db, w.tbl)
			if derr != nil || len(cols) == 0 {
				// Table not present yet (e.g. rolled out of retention) — skip;
				// don't invalidate cache for the others.
				continue
			}
			if SchemaFingerprint(cols) != persisted.SchemaFingerprints[key] {
				log.Info().
					Str("db", w.db).
					Str("table", w.tbl).
					Msg("rollup SpecsCached: table schema changed; re-running inference")
				allMatch = false
				break
			}
		}
		if allMatch {
			log.Info().
				Int("specs", len(persisted.Specs)).
				Time("inferred_at", persisted.InferredAt).
				Msg("rollup SpecsCached: cache hit; reusing persisted specs")
			return persisted.Specs, nil
		}
	} else if persisted != nil {
		log.Info().
			Str("stored_config_fp", persisted.ConfigFingerprint).
			Str("current_config_fp", cfgFP).
			Msg("rollup SpecsCached: rollup config changed; re-running inference")
	}

	// Cache miss: run full inference.
	specs, err := c.Specs(ctx, sampler, knownTables)
	if err != nil {
		return nil, err
	}

	// Capture per-table schema fingerprints so the next restart can verify
	// them. We do this by re-running DescribeSourceColumns — cheap (no
	// COUNT DISTINCT, no sample materialization).
	schemaFPs := make(map[string]string, len(work))
	for _, w := range work {
		key := strings.ToLower(w.db + "." + w.tbl)
		cols, derr := sampler.DescribeSourceColumns(ctx, w.db, w.tbl)
		if derr != nil || len(cols) == 0 {
			continue
		}
		schemaFPs[key] = SchemaFingerprint(cols)
	}

	ps := &PersistedSpecs{
		Specs:              specs,
		SchemaFingerprints: schemaFPs,
		ConfigFingerprint:  cfgFP,
		InferredAt:         time.Now().UTC(),
	}
	if err := SavePersistedSpecs(ctx, backend, ps); err != nil {
		log.Warn().Err(err).Msg("rollup SpecsCached: failed to persist specs (specs still loaded in memory)")
	} else {
		log.Info().
			Int("specs", len(specs)).
			Int("tables", len(schemaFPs)).
			Msg("rollup SpecsCached: persisted inference output for next restart")
	}
	return specs, nil
}

// mergeTableEntries dedupes the (db, table) work-list the same way Specs()
// does. Factored out so SpecsCached can iterate the same set without
// duplicating Specs's logic.
func mergeTableEntries(c Config, knownTables []DBTable) []tableEntry {
	seen := map[string]struct{}{}
	var work []tableEntry
	for key, tc := range c.Tables {
		db, tbl, ok := splitTableKey(key)
		if !ok {
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
	return work
}

// tableEntry is shared between Specs and SpecsCached so the work-list
// construction matches exactly. Not exported.
type tableEntry struct {
	db, tbl string
	hints   TableConfig
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
	if cfg.BuildGrace == 0 {
		cfg.BuildGrace = 1 * time.Hour
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
