package rollup

import "time"

// Config is Rollup's entire operator surface (the [rollup] section of
// arc.toml). Cube *definitions* are never configured here — they are derived from
// each table's schema. This only governs whether/where/how often to build.
type Config struct {
	Enabled bool // master switch

	TimeCol string // time column name (default "time")
	Grain   string // base cube grain (default "hour"); finer grains cost storage

	ForwardTick time.Duration // build cadence (default 5m)
	Grace       time.Duration // seal delay before a day is built (default 6h)
	RebuildDays int           // recent days re-built each pass for late data (default 2)

	Databases           []string // allow-list; empty = all discovered databases
	ExcludeMeasurements []string // measurements to skip

	MaxDimCard    int // shared-dim cardinality cap (default 1024)
	MaxPerDimCard int // per-dim-cube cardinality cap (default 50000)
	MaxDims       int // max per-dim cubes per table (default 16)

	MemLimit       string // DuckDB memory_limit per build (default "2GB"; spills beyond)
	BuildThreads   int    // DuckDB threads per build connection (default 4; <0 => DuckDB default = host cores)
	StoragePrefix  string // object prefix for cubes + manifests (default "_arc/rollup")
	MaxDaysPerTick int    // backfill pacing: max day-builds per cube per tick (default 120)

	// Compaction merges sealed daily cube files into one file per month, so a
	// long-range query reads a few files instead of one per day (the per-file S3
	// round-trip dominates long-range latency). A month is compacted once it has
	// CompactMinDays sealed daily files past the rebuild floor — those are never
	// rebuilt, so there is no double-count risk.
	CompactMinDays    int // sealed daily files in a month before compacting (default 7; 0 disables)
	CompactMaxPerTick int // max months compacted per cube per tick (default 6)

	// DimRich builds one exact cube over ALL eligible low/medium-card dimensions,
	// so multi-dimension queries (e.g. site × response) roll up. Costs more storage
	// and slower reads for single-dim queries (the 1-dim cubes still serve those).
	DimRich        bool // default false
	DimRichMaxDims int  // skip the dim-rich cube above this many dims (default 12)
}

func (c Config) withDefaults() Config {
	if c.TimeCol == "" {
		c.TimeCol = "time"
	}
	if c.Grain == "" {
		c.Grain = "hour"
	}
	if c.ForwardTick == 0 {
		c.ForwardTick = 5 * time.Minute
	}
	if c.Grace == 0 {
		c.Grace = 6 * time.Hour
	}
	if c.RebuildDays == 0 {
		c.RebuildDays = 2
	}
	if c.MaxDimCard == 0 {
		c.MaxDimCard = 1024
	}
	if c.MaxPerDimCard == 0 {
		c.MaxPerDimCard = 50000
	}
	if c.MaxDims == 0 {
		c.MaxDims = 16
	}
	if c.MemLimit == "" {
		c.MemLimit = "2GB"
	}
	if c.BuildThreads == 0 {
		// Build containers are often CPU-throttled (e.g. --cpus 0.5) while DuckDB
		// still sees host core count via nproc, so it would spawn N=cores threads —
		// each buffering parquet row groups, multiplying memory under a fixed
		// memory_limit until a sketch-heavy COPY OOMs. 4 bounds that working set
		// (DuckDB's #1 OOM remedy) while keeping enough parallelism for the COPY.
		//
		// 0 is the unset zero value (the common case) and maps to this safe default.
		// An operator who genuinely wants DuckDB's host-core threading sets a NEGATIVE
		// value (e.g. -1): withDefaults leaves it <0, and configureBuildConn skips the
		// `SET threads` statement entirely (its guard is `threads > 0`), so DuckDB picks
		// its own default. That path is now actually reachable — previously 0 meant
		// "host cores" in the doc but withDefaults rewrote it to 4, so it never could be.
		c.BuildThreads = 4
	}
	if c.StoragePrefix == "" {
		c.StoragePrefix = "_arc/rollup"
	}
	if c.MaxDaysPerTick == 0 {
		c.MaxDaysPerTick = 120
	}
	if c.CompactMinDays == 0 {
		c.CompactMinDays = 7
	}
	if c.CompactMaxPerTick == 0 {
		c.CompactMaxPerTick = 6
	}
	if c.DimRichMaxDims == 0 {
		c.DimRichMaxDims = 12
	}
	return c
}

func (c Config) classifyConfig() ClassifyConfig {
	return ClassifyConfig{
		MaxDimCard:    c.MaxDimCard,
		MaxPerDimCard: c.MaxPerDimCard,
		MaxDims:       c.MaxDims,
	}
}
