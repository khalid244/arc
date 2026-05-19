package tiered

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

const highCardThreshold = 10000

// ClassifierConfig holds the per-table inputs needed to auto-classify a table
// when its spec is missing on first run.
type ClassifierConfig struct {
	Source      string
	DimColumns  []string
	ForceKeep   []string
	ForceSketch []string
	IgnoreCols  []string
}

// Scheduler drives hierarchical precalc builds for a set of tables.
// On each tick it walks tiers fine→coarse (1h → 1d → 1w → 1mo) and
// publishes any newly-sealed buckets. Higher tiers are gated on the
// lower-tier watermark so 1d only builds from sealed 1h buckets, etc.
type Scheduler struct {
	Publisher  *Publisher
	SpecStore  *SpecStore
	// FilesFor returns the FileIndex for a given table name.
	// Production supplies an *S3FileIndex per table; tests use MemoryFileIndex.
	FilesFor   func(table string) FileIndex

	// Now returns the current time. Overridable for tests. Defaults to time.Now.
	Now func() time.Time

	// SourceWatermark returns the latest time for which source data is
	// available for the given table. In production this queries the ingest
	// WAL or source parquet metadata; in tests it is a stub.
	SourceWatermark func(ctx context.Context, table string) (time.Time, error)

	// EarliestSource returns the earliest bucket time available in source for
	// `table`. Production injects an S3 LIST-based implementation that walks
	// the `<db>/<table>/YYYY/` prefixes and returns the oldest YYYY/MM/DD that
	// contains at least one parquet file. Tests may inject a fixed time.
	// When nil or when it returns an error, nextBucketStart falls back to 2026-01-01.
	EarliestSource func(ctx context.Context, table string) (time.Time, error)

	// Tables to manage. Each one has its own spec + manifest.
	Tables []string

	// Tiers to build in fine→coarse order. Defaults to all 4.
	Tiers []Tier

	// Variants to build at each tier.
	// Deprecated: the scheduler now derives variants from the table Spec via
	// variantsForSpec. This field is kept for backward compatibility — when
	// non-empty it overrides the spec-driven list, treating each entry as a
	// sketch-only variantPlan (Dim="").
	Variants []string

	// DimRichCap is the maximum EffectiveCard a Dim may have for the dim-rich
	// ("all") variant to be published. Defaults to 100.
	DimRichCap int

	// GraceWindow: a bucket is "sealed" only when bucket_end + GraceWindow ≤ Now.
	GraceWindow time.Duration

	// RecentGrace is how far back from Now the scheduler builds. Buckets whose
	// end time is within the last RecentGrace of Now() are not built yet, to
	// avoid partial windows at the leading edge. Defaults to 48h if zero.
	RecentGrace time.Duration

	// Interval between ticks. Defaults to 5 minutes.
	Interval time.Duration

	// RebuildHorizon, when > 0, enables a periodic rebuild pass that
	// re-materializes all buckets in [now-RebuildHorizon, now-RecentGrace].
	// Default 0 disables the pass entirely.
	RebuildHorizon time.Duration

	// RebuildInterval controls how often the rebuild pass runs. Default 24h.
	// Only used when RebuildHorizon > 0.
	RebuildInterval time.Duration

	// BuildArgsFor supplies MetricCols, HLLCols, KLLCols per-table.
	BuildArgsFor map[string]BuildArgs

	// ClassifierConfigFor holds per-table auto-classify inputs. When a table's
	// spec is missing and an entry with Source set exists here, the scheduler
	// runs Classify and persists the spec before proceeding with the normal tick.
	ClassifierConfigFor map[string]ClassifierConfig

	// CoverageThreshold is forwarded to Classify when auto-classifying.
	// Defaults to 0.99 if zero.
	CoverageThreshold float64

	// TZ is forwarded to Classify and used as the spec timezone.
	TZ string

	// StorageBucket is the S3 bucket name. Used to auto-derive a classifier
	// source when the per-table ClassifierConfig.Source is empty. Empty means
	// "no auto-source" — operator must supply Source explicitly in that case.
	StorageBucket string

	// ClassifySampleDays restricts the classifier source to the last N days
	// (using the table's time column). 0 means "no time filter — classify
	// the full source". Default in cmd/arc/main.go: 3.
	ClassifySampleDays int

	// MemoryLimit is forwarded to ClassifyOpts.MemoryLimit. e.g., "8GB".
	// Default "" leaves the DuckDB session unchanged.
	MemoryLimit string

	// Metrics sink for build counters and watermark-lag gauge. Optional; nil = no metrics.
	Metrics MetricsSink

	Logger zerolog.Logger
}

// Run blocks until ctx is cancelled, calling runOnce on each tick.
func (s *Scheduler) Run(ctx context.Context) error {
	s.applyDefaults()

	forwardTick := time.NewTicker(s.Interval)
	defer forwardTick.Stop()

	var rebuildTick *time.Ticker
	if s.RebuildHorizon > 0 {
		if s.RebuildInterval == 0 {
			s.RebuildInterval = 24 * time.Hour
		}
		rebuildTick = time.NewTicker(s.RebuildInterval)
		defer rebuildTick.Stop()
	}

	for {
		var rebuildC <-chan time.Time
		if rebuildTick != nil {
			rebuildC = rebuildTick.C
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-forwardTick.C:
			s.runOnce(ctx)
		case <-rebuildC:
			s.runRebuild(ctx)
		}
	}
}

func (s *Scheduler) applyDefaults() {
	if s.Interval == 0 {
		s.Interval = 5 * time.Minute
	}
	if s.Now == nil {
		s.Now = time.Now
	}
	if len(s.Tiers) == 0 {
		s.Tiers = []Tier{Tier1h}
	}
	if s.DimRichCap == 0 {
		s.DimRichCap = 100
	}
	if s.CoverageThreshold == 0 {
		s.CoverageThreshold = 0.99
	}
	if s.RecentGrace == 0 {
		s.RecentGrace = 48 * time.Hour
	}
}

func (s *Scheduler) runOnce(ctx context.Context) {
	s.applyDefaults()
	for _, table := range s.Tables {
		s.tickTable(ctx, table)
	}
}

func (s *Scheduler) runRebuild(ctx context.Context) {
	s.applyDefaults()
	horizonStart := s.Now().Add(-s.RebuildHorizon)
	for _, table := range s.Tables {
		s.rebuildTable(ctx, table, horizonStart)
	}
}

func (s *Scheduler) rebuildTable(ctx context.Context, table string, horizonStart time.Time) {
	spec, err := s.SpecStore.Get(ctx, table)
	if err != nil {
		return
	}
	now := s.Now()
	effectiveMax := now.Add(-s.RecentGrace)
	for _, tier := range s.Tiers {
		plans := variantsForSpec(&spec, s.DimRichCap)
		if len(s.Variants) > 0 {
			plans = nil
			for _, v := range s.Variants {
				plans = append(plans, variantPlan{Variant: v})
			}
		}
		for _, plan := range plans {
			s.rebuildTierVariant(ctx, table, &spec, tier, plan, horizonStart, effectiveMax)
		}
	}
}

func (s *Scheduler) rebuildTierVariant(ctx context.Context, table string, spec *Spec, tier Tier, plan variantPlan, horizonStart, effectiveMax time.Time) {
	current := nextBucketStart(horizonStart, tier, spec.TZ)
	for i := 0; i < 366*24; i++ {
		nextEnd := bucketEnd(current, tier, spec.TZ)
		if nextEnd.After(effectiveMax) {
			return
		}
		partition := variantPartitionPath(table, tier, plan.Variant, current)
		if partition != "" {
			if existing, err := s.Publisher.Backend.List(ctx, partition); err == nil {
				for _, k := range existing {
					_ = s.Publisher.Backend.Delete(ctx, k)
				}
			}
		}
		if err := s.publishBucket(ctx, table, spec, plan, tier, current, nextEnd); err != nil {
			s.Logger.Warn().Err(err).Time("bucket", current).Str("table", table).Str("tier", string(tier)).Str("variant", plan.Variant).Msg("rebuild publish failed")
		}
		current = nextEnd
	}
}

// variantPartitionPath returns the storage prefix (directory) for all files
// belonging to a single bucket variant — used by the rebuild pass to LIST
// and DELETE existing files before publishing the replacement.
func variantPartitionPath(table string, tier Tier, variant string, bucket time.Time) string {
	sample := VariantPath(table, tier, variant, bucket, "_dummy")
	idx := strings.LastIndex(sample, "/")
	if idx < 0 {
		return ""
	}
	return sample[:idx+1]
}

func (s *Scheduler) tickTable(ctx context.Context, table string) {
	spec, err := s.SpecStore.Get(ctx, table)
	if err != nil {
		if s.canAutoClassify(table) {
			s.Logger.Info().Str("table", table).Msg("spec missing; auto-classifying")
			newSpec, cerr := s.autoClassify(ctx, table)
			if cerr != nil {
				s.Logger.Warn().Str("table", table).Err(cerr).Msg("auto-classify failed; will retry next tick")
				return
			}
			if perr := s.SpecStore.Put(ctx, table, newSpec); perr != nil {
				s.Logger.Warn().Str("table", table).Err(perr).Msg("spec persist failed; will retry next tick")
				return
			}
			spec = newSpec
			s.Logger.Info().Str("table", table).Int("dims", len(spec.Dims)).Msg("auto-classify complete; spec persisted")
		} else {
			s.Logger.Warn().Str("table", table).Err(err).Msg("spec not available and no classifier config; manual seeding required")
			return
		}
	}

	files := s.FilesFor(table)

	srcWM, err := s.SourceWatermark(ctx, table)
	if err != nil {
		s.Logger.Warn().Str("table", table).Err(err).Msg("source watermark unavailable; skipping")
		return
	}

	for _, tier := range s.Tiers {
		s.tickTableTier(ctx, table, &spec, files, tier, srcWM)
	}

	if s.Metrics != nil {
		now := s.Now()
		var maxLag int64
		plans := variantsForSpec(&spec, s.DimRichCap)
		if len(s.Variants) > 0 {
			plans = nil
			for _, v := range s.Variants {
				plans = append(plans, variantPlan{Variant: v})
			}
		}
		for _, tier := range s.Tiers {
			for _, plan := range plans {
				wm, wmOk, werr := files.Watermark(ctx, string(tier), plan.Variant)
				if werr != nil || !wmOk {
					continue
				}
				if lag := int64(now.Sub(wm).Seconds()); lag > maxLag {
					maxLag = lag
				}
			}
		}
		s.Metrics.SetMaxWatermarkLagSeconds(maxLag)
	}
}

// planCursor tracks a single variantPlan's progress through buckets.
type planCursor struct {
	plan         variantPlan
	current      time.Time // exclusive end of last published bucket (== next bucket's lo)
	effectiveMax time.Time // upper bound past which this plan must not build
	bucketsBuilt int       // number of buckets built this tick
	stopped      bool      // hit cap or error; no more work this tick
}

func (s *Scheduler) tickTableTier(ctx context.Context, table string, spec *Spec, files FileIndex, tier Tier, sourceWatermark time.Time) {
	var rawPlans []variantPlan
	if len(s.Variants) > 0 {
		for _, v := range s.Variants {
			rawPlans = append(rawPlans, variantPlan{Variant: v})
		}
	} else {
		rawPlans = variantsForSpec(spec, s.DimRichCap)
	}

	cutoff := s.Now().Add(-s.RecentGrace)

	// Build one cursor per plan, initialising each plan's starting position
	// and effectiveMax. Plans that have nothing to build this tick are excluded.
	// 100 keeps the pod near 100% CPU during backfill regardless of per-bucket
	// cost in [1s, 3s]; 24 left 60-86% of CPU idle between ticks (see
	// TestMaxBucketsPerTick_Simulation). When ticks overrun the 5-min interval
	// Go's Ticker buffers one event so the next cycle fires immediately — no
	// drift, no work lost.
	const maxBucketsPerTick = 100
	cursors := make([]*planCursor, 0, len(rawPlans))
	for _, plan := range rawPlans {
		variant := plan.Variant

		wm, wmOk, err := files.Watermark(ctx, string(tier), variant)
		var current time.Time
		if err == nil && wmOk {
			current = wm
		}
		if current.IsZero() && s.EarliestSource != nil {
			if earliest, eerr := s.EarliestSource(ctx, table); eerr == nil && !earliest.IsZero() {
				loc, lerr := time.LoadLocation(spec.TZ)
				if lerr != nil {
					loc = time.UTC
				}
				current = earliest.In(loc).Truncate(0)
			}
		}

		effectiveMax := sourceWatermark
		if cutoff.Before(effectiveMax) {
			effectiveMax = cutoff
		}

		next := nextBucketStart(current, tier, spec.TZ)
		nextEnd := bucketEnd(next, tier, spec.TZ)
		if nextEnd.Add(s.GraceWindow).After(effectiveMax) {
			continue
		}

		cursors = append(cursors, &planCursor{
			plan:         plan,
			current:      current,
			effectiveMax: effectiveMax,
		})
	}

	if len(cursors) == 0 {
		return
	}

	// Determine the earliest starting bucket across all active cursors.
	var minStart time.Time
	for _, c := range cursors {
		next := nextBucketStart(c.current, tier, spec.TZ)
		if minStart.IsZero() || next.Before(minStart) {
			minStart = next
		}
	}

	// Iterate buckets from minStart. At each bucket, materialise source ONCE
	// and build every cursor that is ready (i.e., whose next bucket == bucketLo
	// and hasn't yet hit its per-plan cap). The loop continues until all cursors
	// are stopped or all per-plan caps are reached.
	bucketLo := minStart
	for {
		bucketHi := bucketEnd(bucketLo, tier, spec.TZ)

		// Collect cursors ready for this bucket (next == bucketLo, within cap).
		var ready []*planCursor
		for _, c := range cursors {
			if c.stopped {
				continue
			}
			if c.bucketsBuilt >= maxBucketsPerTick {
				c.stopped = true
				continue
			}
			next := nextBucketStart(c.current, tier, spec.TZ)
			if !next.Equal(bucketLo) {
				continue
			}
			// Double-check effectiveMax (may have changed since cursor init).
			if bucketHi.Add(s.GraceWindow).After(c.effectiveMax) {
				c.stopped = true
				continue
			}
			ready = append(ready, c)
		}

		if len(ready) == 0 {
			// No cursor is ready for this bucket. If any cursor's next bucket
			// is still ahead of bucketLo, advance to the next bucket; otherwise
			// we're done.
			advanceTo := time.Time{}
			for _, c := range cursors {
				if c.stopped {
					continue
				}
				if c.bucketsBuilt >= maxBucketsPerTick {
					continue
				}
				next := nextBucketStart(c.current, tier, spec.TZ)
				if advanceTo.IsZero() || next.Before(advanceTo) {
					advanceTo = next
				}
			}
			if advanceTo.IsZero() || !advanceTo.After(bucketLo) {
				break
			}
			bucketLo = advanceTo
			continue
		}

		_, sourceSQL := s.resolveBucketSourceSQL(ctx, table, spec, bucketLo, bucketHi)
		if sourceSQL == "" {
			// No source files for any UTC day this bucket touches. Advance
			// each cursor past this bucket so the next tick doesn't re-iterate
			// it; no file is written.
			for _, c := range ready {
				c.current = bucketHi
				c.bucketsBuilt++
			}
		} else {
			args, ok := s.BuildArgsFor[table]
			if !ok {
				for _, c := range ready {
					c.stopped = true
				}
			} else {
				args.Tier = Tier1h
				args.Source = sourceSQL
				if err := s.Publisher.PublishAllVariants(ctx, table, spec, args, s.DimRichCap, Tier1h, bucketLo, bucketHi); err != nil {
					s.Logger.Warn().Err(err).
						Str("table", table).Str("tier", string(tier)).Time("bucket", bucketLo).
						Msg("publish all variants failed")
					for _, c := range ready {
						c.stopped = true
					}
				} else {
					for _, c := range ready {
						c.current = bucketHi
						c.bucketsBuilt++
					}
				}
			}
		}

		bucketLo = bucketHi

		// Check if all cursors are done.
		allDone := true
		for _, c := range cursors {
			if !c.stopped && c.bucketsBuilt < maxBucketsPerTick {
				next := nextBucketStart(c.current, tier, spec.TZ)
				nextEnd := bucketEnd(next, tier, spec.TZ)
				if !nextEnd.Add(s.GraceWindow).After(c.effectiveMax) {
					allDone = false
					break
				}
			}
		}
		if allDone {
			break
		}
	}
}

// resolveBucketSourceSQL returns the DuckDB source SQL expression for a Tier1h
// bucket and a temp table name to use. If StorageBucket is set, it filters to
// only days that have files (avoiding empty-glob errors). The tempName is a
// stable, deterministic string safe to use as a DuckDB identifier.
func (s *Scheduler) resolveBucketSourceSQL(ctx context.Context, table string, spec *Spec, lo, hi time.Time) (tempName, sourceSQL string) {
	args, ok := s.BuildArgsFor[table]
	if !ok {
		return "", ""
	}
	// Tests and operators without an S3-backed setup use a literal source
	// (e.g., a bare table name). Skip the partition-existence filter — let
	// the operator-provided Source drive the build.
	if s.StorageBucket == "" {
		return "__arc_bucket_src", args.Source
	}

	parts := strings.SplitN(table, ".", 2)
	db, tbl := parts[0], ""
	if len(parts) == 2 {
		tbl = parts[1]
	}
	days := utcDaysCoveringWindow(lo, hi)
	days = filterDaysWithFiles(ctx, s.Publisher.Backend, db, tbl, days)
	if len(days) == 0 {
		// No source data overlapping this bucket's UTC days. Returning empty
		// signals the caller to skip the build — falling back to the operator's
		// full-bucket glob would 1) scan the entire 80GB+ table per variant
		// (slow) and 2) race the compactor on today's actively-rewritten files
		// (404s). Sparse-source days produce no rollup file for that bucket;
		// queries for that range fall through to source-scan as designed.
		return "", ""
	}
	if expr := buildWindowSourceFromDays(s.StorageBucket, db, tbl, days); expr != "" {
		sourceSQL = expr
	} else {
		sourceSQL = args.Source
	}

	tempName = "__arc_bucket_src"
	return tempName, sourceSQL
}

// materializeBucketSource creates (or replaces) a DuckDB table named
// tempName populated with all rows from sourceSQL. The caller drops it when done.
//
// Note: this is a regular (non-TEMP) table on purpose. *sql.DB is a connection
// pool and DuckDB's TEMP TABLEs are session-local — a TEMP TABLE created on
// one pooled conn is invisible to subsequent COPY statements on a different
// pooled conn. The main DuckDB is in-memory, so a regular table is still
// fast and ephemeral; the caller's DROP cleans it up.
func materializeBucketSource(ctx context.Context, db *sql.DB, sourceSQL, tempName string) error {
	stmt := fmt.Sprintf("CREATE OR REPLACE TABLE %s AS SELECT * FROM %s", tempName, sourceSQL)
	_, err := db.ExecContext(ctx, stmt)
	return err
}

// publishBucketWith1hSource publishes a single Tier1h bucket for one plan.
// If tempName is non-empty the plan reads from the already-materialised temp
// table; otherwise it falls back to sourceSQL (direct read_parquet).
func (s *Scheduler) publishBucketWith1hSource(ctx context.Context, table string, spec *Spec, plan variantPlan, lo, hi time.Time, sourceSQL, tempName string) error {
	args, ok := s.BuildArgsFor[table]
	if !ok {
		return fmt.Errorf("no BuildArgs for table %s", table)
	}
	args.Tier = Tier1h
	if tempName != "" {
		args.Source = tempName
	} else if sourceSQL != "" {
		args.Source = sourceSQL
	}
	switch {
	case plan.Variant == "sketch":
		return s.Publisher.PublishSketchVariant(ctx, table, spec, args, Tier1h, plan.Variant, lo, hi)
	case plan.Dim != "":
		return s.Publisher.PublishPerDimVariant(ctx, table, spec, args, plan.Dim, Tier1h, lo, hi)
	case plan.Variant == "all":
		return s.Publisher.PublishDimRichVariant(ctx, table, spec, args, s.DimRichCap, Tier1h, lo, hi)
	}
	return fmt.Errorf("scheduler: unknown variant %q", plan.Variant)
}

// publishBucket builds individual buckets independently (rebuild path).
// Always Tier1h post single-tier migration.
func (s *Scheduler) publishBucket(ctx context.Context, table string, spec *Spec, plan variantPlan, tier Tier, lo, hi time.Time) error {
	args, ok := s.BuildArgsFor[table]
	if !ok {
		return fmt.Errorf("no BuildArgs for table %s", table)
	}
	args.Tier = tier
	parts := strings.SplitN(table, ".", 2)
	db, tbl := parts[0], ""
	if len(parts) == 2 {
		tbl = parts[1]
	}
	// Filter UTC days to those that actually have source files, then build
	// a path list only of populated days. If NO days have files, fall back
	// to the operator-derived source (whose WHERE filter yields 0 rows)
	// so DuckDB doesn't error on a path that matches zero files.
	days := utcDaysCoveringWindow(lo, hi)
	days = filterDaysWithFiles(ctx, s.Publisher.Backend, db, tbl, days)
	if len(days) > 0 {
		args.Source = buildWindowSourceFromDays(s.StorageBucket, db, tbl, days)
	}
	switch {
	case plan.Variant == "sketch":
		return s.Publisher.PublishSketchVariant(ctx, table, spec, args, tier, plan.Variant, lo, hi)
	case plan.Dim != "":
		return s.Publisher.PublishPerDimVariant(ctx, table, spec, args, plan.Dim, tier, lo, hi)
	case plan.Variant == "all":
		return s.Publisher.PublishDimRichVariant(ctx, table, spec, args, s.DimRichCap, tier, lo, hi)
	}
	return fmt.Errorf("scheduler: unknown variant %q", plan.Variant)
}

func (s *Scheduler) canAutoClassify(table string) bool {
	cfg, ok := s.ClassifierConfigFor[table]
	if !ok {
		return false
	}
	// Source can come from config or be derived from StorageBucket.
	hasSource := cfg.Source != "" || s.StorageBucket != ""
	// DimColumns can come from config or be auto-discovered via DESCRIBE.
	return hasSource
}

func (s *Scheduler) autoClassify(ctx context.Context, table string) (Spec, error) {
	cfg := s.ClassifierConfigFor[table]

	source := cfg.Source
	if source == "" {
		if s.StorageBucket == "" {
			return Spec{}, fmt.Errorf("auto-classify: no source configured and StorageBucket is empty")
		}
		tablePath := strings.ReplaceAll(table, ".", "/")
		source = fmt.Sprintf("SELECT * FROM read_parquet('s3://%s/%s/**/*.parquet')", s.StorageBucket, tablePath)
		s.Logger.Info().Str("table", table).Str("source", source).Msg("auto-derived classifier source from convention")
	}

	dimColumns := cfg.DimColumns
	if len(dimColumns) == 0 {
		skip := make([]string, 0, len(cfg.IgnoreCols)+len(cfg.ForceSketch))
		skip = append(skip, cfg.IgnoreCols...)
		skip = append(skip, cfg.ForceSketch...)
		discovered, err := s.discoverStringColumns(ctx, source, skip)
		if err != nil {
			return Spec{}, fmt.Errorf("auto-discover dim columns: %w", err)
		}
		dimColumns = discovered
		s.Logger.Info().Str("table", table).Strs("dim_columns", dimColumns).Msg("auto-discovered dim columns")
	}

	tc := ""
	if buildArgs, ok := s.BuildArgsFor[table]; ok {
		tc = buildArgs.TimeColumn
	}

	if s.ClassifySampleDays > 0 {
		parts := strings.SplitN(table, ".", 2)
		db, tbl := parts[0], ""
		if len(parts) == 2 {
			tbl = parts[1]
		}
		source = buildDateScopedSource(s.StorageBucket, db, tbl, s.ClassifySampleDays, time.Now(), source)
		s.Logger.Info().
			Str("table", table).
			Int("sample_days", s.ClassifySampleDays).
			Msg("classifier source scoped to last N day-partitions")
	}

	const classifySampleRows = 2_000_000
	const classifySampleSeed = 42
	const sampleTempTable = "__tiered_classify_sample"

	createSample := fmt.Sprintf(
		"CREATE OR REPLACE TEMP TABLE %s AS %s USING SAMPLE reservoir(%d ROWS) REPEATABLE (%d)",
		sampleTempTable, source, classifySampleRows, classifySampleSeed,
	)
	if _, err := s.Publisher.DB.ExecContext(ctx, createSample); err != nil {
		s.Logger.Warn().Err(err).Str("table", table).Msg("reservoir sample failed; classifier will scan source directly")
	} else {
		defer func() {
			_, _ = s.Publisher.DB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", sampleTempTable))
		}()
		source = fmt.Sprintf("SELECT * FROM %s", sampleTempTable)
		s.Logger.Info().Str("table", table).Int("sample_rows", classifySampleRows).Msg("materialized classifier reservoir sample")
	}

	if len(dimColumns) > 0 {
		pre := preClassifyCardinalities(ctx, s.Publisher.DB, source, dimColumns)
		var keep, autoSketch []string
		for _, col := range dimColumns {
			if c, ok := pre[col]; ok && c > highCardThreshold {
				autoSketch = append(autoSketch, col)
				s.Logger.Info().Str("table", table).Str("col", col).Int64("approx_distinct", c).Msg("excluding high-card col from classifier (auto force_sketch)")
			} else {
				keep = append(keep, col)
			}
		}
		dimColumns = keep
		cfg.ForceSketch = append(append([]string(nil), cfg.ForceSketch...), autoSketch...)
	}

	spec, err := Classify(ctx, s.Publisher.DB, ClassifyOpts{
		Source:            source,
		TimeColumn:        tc,
		DimColumns:        dimColumns,
		CoverageThreshold: s.CoverageThreshold,
		DimRichCap:        s.DimRichCap,
		Table:             table,
		TZ:                s.TZ,
		MemoryLimit:       s.MemoryLimit,
	})
	if err != nil {
		return Spec{}, fmt.Errorf("classify: %w", err)
	}
	for _, col := range cfg.IgnoreCols {
		delete(spec.Dims, col)
	}
	for _, col := range cfg.ForceSketch {
		spec.Dims[col] = DimSpec{Role: "Sketch"}
	}
	for _, col := range cfg.ForceKeep {
		if d, ok := spec.Dims[col]; ok {
			d.Role = "Dim"
			spec.Dims[col] = d
		}
	}
	return spec, nil
}

// buildWindowSource returns a read_parquet expression scoped to the day-level
// partitions the window [windowStart, windowEnd) falls in. A day-level
// recursive glob covers both the hour-level live files
// (YYYY/MM/DD/HH/*.parquet) and the day-level compacted files
// (YYYY/MM/DD/*_compacted.parquet). DuckDB still applies the time-range
// WHERE filter to drop rows outside the window.
//
// When the spec timezone is offset from UTC, a 24h window in spec TZ can
// straddle two UTC calendar days; this helper emits paths for every UTC
// day the window touches.
//
// Empty storageBucket returns "" so the caller falls back to the operator-
// provided source (test path).
func buildWindowSource(storageBucket, db, table string, windowStart, windowEnd time.Time) string {
	if storageBucket == "" || db == "" || table == "" {
		return ""
	}
	return buildWindowSourceFromDays(storageBucket, db, table, utcDaysCoveringWindow(windowStart, windowEnd))
}

// utcDaysCoveringWindow returns each UTC calendar day touched by [windowStart, windowEnd).
// For a 24h window starting at midnight in a non-UTC timezone the result will be two days.
func utcDaysCoveringWindow(windowStart, windowEnd time.Time) []time.Time {
	loUTC := windowStart.UTC()
	hiUTC := windowEnd.UTC()
	startDay := time.Date(loUTC.Year(), loUTC.Month(), loUTC.Day(), 0, 0, 0, 0, time.UTC)
	endDay := time.Date(hiUTC.Year(), hiUTC.Month(), hiUTC.Day(), 0, 0, 0, 0, time.UTC)
	if hiUTC.Equal(endDay) {
		endDay = endDay.AddDate(0, 0, -1)
	}
	var out []time.Time
	for d := startDay; !d.After(endDay); d = d.AddDate(0, 0, 1) {
		out = append(out, d)
	}
	return out
}

// filterDaysWithFiles returns the subset of `days` whose S3 partition prefix
// has at least one object. Days with no files are dropped so the resulting
// read_parquet path list is non-empty per pattern (DuckDB errors otherwise).
// On any List error a day is conservatively kept (better to attempt and fail
// loudly than silently drop a real partition).
func filterDaysWithFiles(ctx context.Context, backend storage.Backend, db, table string, days []time.Time) []time.Time {
	if backend == nil {
		return days
	}
	var out []time.Time
	for _, d := range days {
		prefix := fmt.Sprintf("%s/%s/%04d/%02d/%02d/", db, table, d.Year(), int(d.Month()), d.Day())
		keys, err := backend.List(ctx, prefix)
		if err != nil {
			out = append(out, d)
			continue
		}
		if len(keys) > 0 {
			out = append(out, d)
		}
	}
	return out
}

func buildWindowSourceFromDays(storageBucket, db, table string, days []time.Time) string {
	if len(days) == 0 {
		return ""
	}
	var paths []string
	for _, d := range days {
		paths = append(paths, fmt.Sprintf("'s3://%s/%s/%s/%04d/%02d/%02d/**/*.parquet'",
			storageBucket, db, table, d.Year(), int(d.Month()), d.Day()))
	}
	if len(paths) == 1 {
		return fmt.Sprintf("read_parquet(%s, union_by_name=true)", paths[0])
	}
	return fmt.Sprintf("read_parquet([%s], union_by_name=true)", strings.Join(paths, ", "))
}

// buildDateScopedSource returns a read_parquet expression that scopes the
// scan to only the last `days` calendar days (in UTC) of the source table's
// partition layout. Arc's S3 layout is <db>/<table>/YYYY/MM/DD/HH/*.parquet,
// so each day-prefix covers a 24h slice plus any daily compacted files.
// Falls back to fallbackSource if storageBucket is empty (test path).
func buildDateScopedSource(storageBucket, db, table string, days int, now time.Time, fallbackSource string) string {
	if storageBucket == "" || db == "" || table == "" || days <= 0 {
		return fallbackSource
	}
	var paths []string
	for i := 0; i < days; i++ {
		d := now.UTC().AddDate(0, 0, -i)
		paths = append(paths, fmt.Sprintf("'s3://%s/%s/%s/%04d/%02d/%02d/**/*.parquet'",
			storageBucket, db, table, d.Year(), int(d.Month()), d.Day()))
	}
	return fmt.Sprintf("SELECT * FROM read_parquet([%s], union_by_name=true)", strings.Join(paths, ", "))
}

// discoverStringColumns runs DESCRIBE on the source and returns VARCHAR columns
// minus anything in skip. Used by autoClassify when dim_columns is not specified.
func (s *Scheduler) discoverStringColumns(ctx context.Context, source string, skip []string) ([]string, error) {
	q := "DESCRIBE " + source
	rows, err := s.Publisher.DB.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("describe source: %w", err)
	}
	defer rows.Close()

	skipSet := make(map[string]bool, len(skip))
	for _, c := range skip {
		skipSet[c] = true
	}
	var out []string
	for rows.Next() {
		var name, typ, nullable, key, dflt, extra sql.NullString
		if err := rows.Scan(&name, &typ, &nullable, &key, &dflt, &extra); err != nil {
			return nil, fmt.Errorf("scan describe row: %w", err)
		}
		if !name.Valid || !typ.Valid {
			continue
		}
		if skipSet[name.String] {
			continue
		}
		if !strings.Contains(strings.ToUpper(typ.String), "VARCHAR") {
			continue
		}
		out = append(out, name.String)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// preClassifyCardinalities returns approx_count_distinct per column via a
// single scan. Returns an empty map on error so the caller falls back to
// classifying the full column set (best-effort pre-pass).
func preClassifyCardinalities(ctx context.Context, db *sql.DB, source string, cols []string) map[string]int64 {
	if len(cols) == 0 {
		return nil
	}
	var selects []string
	for _, c := range cols {
		selects = append(selects, fmt.Sprintf("approx_count_distinct(%s) AS c_%s", c, c))
	}
	q := fmt.Sprintf("WITH src AS (%s) SELECT %s FROM src", source, strings.Join(selects, ", "))
	row := db.QueryRowContext(ctx, q)
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := row.Scan(ptrs...); err != nil {
		return nil
	}
	out := make(map[string]int64, len(cols))
	for i, c := range cols {
		switch v := vals[i].(type) {
		case int64:
			out[c] = v
		case int32:
			out[c] = int64(v)
		case uint64:
			out[c] = int64(v)
		case float64:
			out[c] = int64(v)
		}
	}
	return out
}

// nextBucketStart returns the start of the bucket following `after` at the
// given tier's granularity in the given timezone. If after is zero it returns
// 2026-01-01 00:00 in the spec's timezone as the default starting point.
func nextBucketStart(after time.Time, tier Tier, tz string) time.Time {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	if after.IsZero() {
		return time.Date(2026, 1, 1, 0, 0, 0, 0, loc)
	}
	in := after.In(loc)
	if tier == Tier1h {
		return time.Date(in.Year(), in.Month(), in.Day(), 0, 0, 0, 0, loc)
	}
	return in
}

// bucketEnd returns the exclusive end time of the bucket starting at `start`
// for the given tier (always 24h post-migration since 1h is the only tier).
func bucketEnd(start time.Time, tier Tier, tz string) time.Time {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	start = start.In(loc)
	if tier == Tier1h {
		return start.AddDate(0, 0, 1)
	}
	return start
}
