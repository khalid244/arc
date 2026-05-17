package tiered

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

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
	Publisher     *Publisher
	SpecStore     *SpecStore
	ManifestStore *ManifestStore

	// Source of "now" — overridable for tests. Defaults to time.Now.
	Now func() time.Time

	// SourceWatermark returns the latest time for which source data is
	// available for the given table. In production this queries the ingest
	// WAL or source parquet metadata; in tests it is a stub.
	SourceWatermark func(ctx context.Context, table string) (time.Time, error)

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

	// Interval between ticks. Defaults to 5 minutes.
	Interval time.Duration

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

	// Metrics sink for build counters and watermark-lag gauge. Optional; nil = no metrics.
	Metrics MetricsSink

	Logger zerolog.Logger
}

// Run blocks until ctx is cancelled, calling runOnce on each tick.
func (s *Scheduler) Run(ctx context.Context) error {
	s.applyDefaults()

	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			s.runOnce(ctx)
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
		s.Tiers = []Tier{Tier1h, Tier1d, Tier1w, Tier1mo}
	}
	if s.DimRichCap == 0 {
		s.DimRichCap = 100
	}
	if s.CoverageThreshold == 0 {
		s.CoverageThreshold = 0.99
	}
}

func (s *Scheduler) runOnce(ctx context.Context) {
	s.applyDefaults()
	for _, table := range s.Tables {
		s.tickTable(ctx, table)
	}
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

	manifest, err := s.ManifestStore.Get(ctx, table)
	if err != nil {
		manifest = &Manifest{Table: table}
	}

	srcWM, err := s.SourceWatermark(ctx, table)
	if err != nil {
		s.Logger.Warn().Str("table", table).Err(err).Msg("source watermark unavailable; skipping")
		return
	}

	for _, tier := range s.Tiers {
		s.tickTableTier(ctx, table, &spec, manifest, tier, srcWM)
	}

	if s.Metrics != nil {
		now := s.Now()
		var maxLag int64
		for _, wm := range manifest.Watermarks {
			if lag := int64(now.Sub(wm).Seconds()); lag > maxLag {
				maxLag = lag
			}
		}
		s.Metrics.SetMaxWatermarkLagSeconds(maxLag)
	}
}

func (s *Scheduler) tickTableTier(ctx context.Context, table string, spec *Spec, manifest *Manifest, tier Tier, sourceWatermark time.Time) {
	var plans []variantPlan
	if len(s.Variants) > 0 {
		for _, v := range s.Variants {
			plans = append(plans, variantPlan{Variant: v})
		}
	} else {
		plans = variantsForSpec(spec, s.DimRichCap)
	}
	for _, plan := range plans {
		s.tickTableTierVariantPlan(ctx, table, spec, manifest, tier, plan, sourceWatermark)
	}
}

func (s *Scheduler) tickTableTierVariantPlan(ctx context.Context, table string, spec *Spec, manifest *Manifest, tier Tier, plan variantPlan, sourceWatermark time.Time) {
	variant := plan.Variant
	current := manifest.Watermark(string(tier), variant)

	effectiveMax := sourceWatermark
	if tier != Tier1h {
		finer := finerTierFor(tier)
		if w := manifest.Watermark(string(finer), variant); !w.IsZero() && w.Before(effectiveMax) {
			effectiveMax = w
		}
	}

	const maxBucketsPerTick = 24
	for i := 0; i < maxBucketsPerTick; i++ {
		next := nextBucketStart(current, tier, spec.TZ)
		nextEnd := bucketEnd(next, tier, spec.TZ)
		if nextEnd.Add(s.GraceWindow).After(effectiveMax) {
			break
		}
		if err := s.publishBucket(ctx, table, spec, plan, tier, next, nextEnd); err != nil {
			s.Logger.Warn().Err(err).
				Str("table", table).
				Str("tier", string(tier)).
				Str("variant", variant).
				Time("bucket", next).
				Msg("publish failed")
			return
		}
		if m, err := s.ManifestStore.Get(ctx, table); err == nil {
			*manifest = *m
		}
		current = nextEnd
	}

	if !current.Equal(manifest.Watermark(string(tier), variant)) {
		manifest.SetWatermark(string(tier), variant, current)
		_ = s.ManifestStore.Put(ctx, table, manifest)
	}
}

func (s *Scheduler) publishBucket(ctx context.Context, table string, spec *Spec, plan variantPlan, tier Tier, lo, hi time.Time) error {
	args, ok := s.BuildArgsFor[table]
	if !ok {
		return fmt.Errorf("no BuildArgs for table %s", table)
	}
	args.Tier = tier
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
	return ok && cfg.Source != "" && len(cfg.DimColumns) > 0
}

func (s *Scheduler) autoClassify(ctx context.Context, table string) (Spec, error) {
	cfg := s.ClassifierConfigFor[table]
	tc := ""
	if buildArgs, ok := s.BuildArgsFor[table]; ok {
		tc = buildArgs.TimeColumn
	}
	spec, err := Classify(ctx, s.Publisher.DB, ClassifyOpts{
		Source:            cfg.Source,
		TimeColumn:        tc,
		DimColumns:        cfg.DimColumns,
		CoverageThreshold: s.CoverageThreshold,
		DimRichCap:        s.DimRichCap,
		Table:             table,
		TZ:                s.TZ,
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
	switch tier {
	case Tier1h:
		return in
	case Tier1d:
		return time.Date(in.Year(), in.Month(), in.Day(), 0, 0, 0, 0, loc)
	case Tier1w:
		return mondayOf(in, loc)
	case Tier1mo:
		return time.Date(in.Year(), in.Month(), 1, 0, 0, 0, 0, loc)
	}
	return in
}

// bucketEnd returns the exclusive end time of the bucket starting at `start`
// for the given tier.
func bucketEnd(start time.Time, tier Tier, tz string) time.Time {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	start = start.In(loc)
	switch tier {
	case Tier1h:
		return start.Add(time.Hour)
	case Tier1d:
		return start.AddDate(0, 0, 1)
	case Tier1w:
		return start.AddDate(0, 0, 7)
	case Tier1mo:
		return start.AddDate(0, 1, 0)
	}
	return start
}

// mondayOf returns the Monday of the week containing t, truncated to midnight.
func mondayOf(t time.Time, loc *time.Location) time.Time {
	dow := int(t.Weekday()) // Sunday = 0
	if dow == 0 {
		dow = 7
	}
	delta := -(dow - 1)
	d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
	return d.AddDate(0, 0, delta)
}

// finerTierFor returns the next-finer tier: 1d→1h, 1w→1d, 1mo→1w.
func finerTierFor(t Tier) Tier {
	switch t {
	case Tier1d:
		return Tier1h
	case Tier1w:
		return Tier1d
	case Tier1mo:
		return Tier1w
	}
	return ""
}
