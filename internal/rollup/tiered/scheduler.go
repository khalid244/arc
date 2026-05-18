package tiered

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

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

func (s *Scheduler) tickTableTier(ctx context.Context, table string, spec *Spec, files FileIndex, tier Tier, sourceWatermark time.Time) {
	var plans []variantPlan
	if len(s.Variants) > 0 {
		for _, v := range s.Variants {
			plans = append(plans, variantPlan{Variant: v})
		}
	} else {
		plans = variantsForSpec(spec, s.DimRichCap)
	}
	for _, plan := range plans {
		s.tickTableTierVariantPlan(ctx, table, spec, files, tier, plan, sourceWatermark)
	}
}

func (s *Scheduler) tickTableTierVariantPlan(ctx context.Context, table string, spec *Spec, files FileIndex, tier Tier, plan variantPlan, sourceWatermark time.Time) {
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
	if tier != Tier1h {
		finer := finerTierFor(tier)
		if fw, fwOk, fwerr := files.Watermark(ctx, string(finer), variant); fwerr == nil && fwOk && fw.Before(effectiveMax) {
			effectiveMax = fw
		}
	}
	cutoff := s.Now().Add(-s.RecentGrace)
	if cutoff.Before(effectiveMax) {
		effectiveMax = cutoff
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
		current = nextEnd
	}
}

func (s *Scheduler) publishBucket(ctx context.Context, table string, spec *Spec, plan variantPlan, tier Tier, lo, hi time.Time) error {
	args, ok := s.BuildArgsFor[table]
	if !ok {
		return fmt.Errorf("no BuildArgs for table %s", table)
	}
	args.Tier = tier
	if tier == Tier1h {
		parts := strings.SplitN(table, ".", 2)
		db, tbl := parts[0], ""
		if len(parts) == 2 {
			tbl = parts[1]
		}
		if ws := buildWindowSource(s.StorageBucket, db, tbl, lo); ws != "" {
			args.Source = ws
		}
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
// partition that windowStart falls in. A day-level recursive glob covers both
// the hour-level live files (YYYY/MM/DD/HH/*.parquet) and the day-level
// compacted files (YYYY/MM/DD/*_compacted.parquet). DuckDB still applies the
// time-range WHERE filter to drop rows outside [windowStart, windowEnd).
//
// Empty storageBucket returns "" so the caller falls back to the operator-
// provided source (test path).
func buildWindowSource(storageBucket, db, table string, windowStart time.Time) string {
	if storageBucket == "" || db == "" || table == "" {
		return ""
	}
	t := windowStart.UTC()
	return fmt.Sprintf(
		"read_parquet('s3://%s/%s/%s/%04d/%02d/%02d/**/*.parquet', union_by_name=true)",
		storageBucket, db, table, t.Year(), int(t.Month()), t.Day(),
	)
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
	switch tier {
	case Tier1h:
		return time.Date(in.Year(), in.Month(), in.Day(), 0, 0, 0, 0, loc)
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
		return start.AddDate(0, 0, 1)
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
