package rollup

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

// BuildWindower is the subset of Builder the scheduler depends on.
type BuildWindower interface {
	BuildWindow(ctx context.Context, spec RollupSpec, fromTable string, windowStart, windowEnd time.Time) error
}

// BatchedBuildWindower is the optional batched extension. When the scheduler's
// Builder satisfies this interface AND multiple specs share a (fromTable,
// windowStart, windowEnd, bucketColumn) batch key, the scheduler calls
// BuildWindowBatch to read the source once and emit N parquets in a single
// subprocess. Builders that only implement BuildWindower take the per-spec
// path (legacy behavior).
type BatchedBuildWindower interface {
	BuildWindower
	BuildWindowBatch(ctx context.Context, specs []RollupSpec, fromTable string, windowStart, windowEnd time.Time) (BatchResult, error)
}

// WMReader/WMWriter let the scheduler use either WatermarkStore or an in-memory fake.
type WMReader interface {
	Get(ctx context.Context, storagePath string) (Watermark, error)
}
type WMWriter interface {
	Put(ctx context.Context, w Watermark) error
}
type WMReadWriter interface {
	WMReader
	WMWriter
}

// hourlyBuildGrace is the fixed grace period applied to all rollup builds.
// Buckets newer than now() - hourlyBuildGrace are deferred to absorb late
// arrivals on the source side. Not user-tunable.
const hourlyBuildGrace = 5 * time.Minute

// Scheduler walks the configured rollups on each tick and asks the Builder to
// process any window whose end-time has elapsed past hourlyBuildGrace.
type Scheduler struct {
	Specs     []RollupSpec
	Builder   BuildWindower
	WMStore   WMReadWriter
	Logger    zerolog.Logger
	Clock     func() time.Time
	TickEvery time.Duration
	Control   *Control // optional; nil = no pause/rebuild support

	// FromTableResolver returns the SQL expression to use as the FROM target
	// for the build of one window. Production injects
	// ReadParquetFromTableWindow(backend, spec, windowStart) so each window
	// reads only the partition path it covers (e.g. just that day's files)
	// rather than the full source-table prefix. Tests leave this nil and get
	// bare table names via the default resolver.
	FromTableResolver func(spec RollupSpec, windowStart time.Time) string

	// EarliestSourceFunc returns the earliest bucket time available in spec's
	// source data. When the spec has no watermark yet (fresh start, populated
	// DB), the scheduler backfills from this point forward instead of building
	// only the most recent bucket. Returns zero-time to fall back to the
	// "single-bucket lookback" behavior. Optional.
	EarliestSourceFunc func(ctx context.Context, spec RollupSpec) (time.Time, error)
}

// specPlan captures one spec's next eligible window for batch grouping.
type specPlan struct {
	spec        RollupSpec
	storagePath string
	fromTable   string
	windowStart time.Time
	windowEnd   time.Time
}

// batchKey identifies specs that can share a source scan within one window.
type batchKey struct {
	fromTable   string
	bucketCol   string
	windowStart time.Time
	windowEnd   time.Time
}

// Run blocks until ctx is cancelled, ticking at TickEvery.
func (s *Scheduler) Run(ctx context.Context) {
	if s.Clock == nil {
		s.Clock = func() time.Time { return time.Now().UTC() }
	}
	if s.TickEvery == 0 {
		s.TickEvery = 30 * time.Second
	}
	t := time.NewTicker(s.TickEvery)
	defer t.Stop()

	s.tick(ctx) // run once immediately

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tick(ctx)
		}
	}
}

// tick drains all eligible windows for every spec on this tick. On each
// iteration it plans every spec's next eligible window, groups plans by
// (fromTable, windowStart, windowEnd, bucketColumn), and dispatches each
// group via the batched builder (or per-spec for size-1 groups / builders
// that don't implement BatchedBuildWindower). The loop repeats until no
// spec has work left this tick. This lets specs that share a source day
// pay for one S3 scan instead of N.
func (s *Scheduler) tick(ctx context.Context) {
	now := s.Clock()
	for {
		if ctx.Err() != nil {
			return
		}

		plans := make([]*specPlan, 0, len(s.Specs))
		for _, spec := range s.Specs {
			p, err := s.planSpec(ctx, spec, now)
			if err != nil {
				s.Logger.Error().Err(err).Str("rollup", spec.Name).Msg("rollup plan failed")
				continue
			}
			if p != nil {
				plans = append(plans, p)
			}
		}
		if len(plans) == 0 {
			return
		}

		groups := make(map[batchKey][]*specPlan, len(plans))
		for _, p := range plans {
			k := batchKey{p.fromTable, p.spec.BucketColumn, p.windowStart, p.windowEnd}
			groups[k] = append(groups[k], p)
		}

		for k, group := range groups {
			s.runGroup(ctx, k, group)
			if ctx.Err() != nil {
				return
			}
		}
	}
}

// runGroup dispatches one batch key's specs. Size-1 groups always take the
// per-spec BuildWindow path (preserves test fakes that don't implement the
// batched interface); size>1 groups use BuildWindowBatch when available and
// fall back to per-spec calls otherwise.
func (s *Scheduler) runGroup(ctx context.Context, k batchKey, group []*specPlan) {
	specs := make([]RollupSpec, 0, len(group))
	for _, p := range group {
		specs = append(specs, p.spec)
	}

	if len(specs) == 1 {
		if err := s.Builder.BuildWindow(ctx, specs[0], k.fromTable, k.windowStart, k.windowEnd); err != nil {
			s.Logger.Error().Err(err).Str("rollup", specs[0].Name).Msg("rollup window build failed")
		}
		return
	}

	batchBuilder, ok := s.Builder.(BatchedBuildWindower)
	if !ok {
		// Builder doesn't support batching; run specs in this group one at a time.
		for _, p := range group {
			if err := s.Builder.BuildWindow(ctx, p.spec, k.fromTable, k.windowStart, k.windowEnd); err != nil {
				s.Logger.Error().Err(err).Str("rollup", p.spec.Name).Msg("rollup window build failed")
			}
		}
		return
	}

	result, err := batchBuilder.BuildWindowBatch(ctx, specs, k.fromTable, k.windowStart, k.windowEnd)
	if err != nil {
		s.Logger.Error().Err(err).
			Int("specs", len(specs)).
			Time("window_start", k.windowStart).
			Msg("rollup batch build failed")
		return
	}
	for name, outcome := range result.PerSpec {
		if !outcome.OK {
			s.Logger.Error().
				Str("rollup", name).
				Str("error", outcome.Err).
				Msg("spec failed in batch")
		}
	}
}

// planSpec computes the next eligible window for spec, applying pause,
// rebuild-on-demand, and drift-fingerprint resets in the process. Returns
// nil if there's nothing to do this tick (paused, watermark already past
// the cutoff, or just-reset and waiting for next iteration).
//
// Drift handling: when the stored watermark's SpecFingerprint differs from
// spec.Fingerprint(), the operator changed this variant's shape in arc.toml
// (different dims, different aggregates, different bucket interval). The
// watermark is reset to zero so the next loop iteration rebuilds from the
// earliest source bucket. Existing parquet under the old shape stays on
// disk — queries continue to see the old data until the new builds overwrite
// the same key prefix.
func (s *Scheduler) planSpec(ctx context.Context, spec RollupSpec, now time.Time) (*specPlan, error) {
	if s.Control != nil && s.Control.IsPaused(spec.Name) {
		return nil, nil
	}
	storagePath := spec.StoragePath()
	if s.Control != nil && s.Control.PopRebuildRequest(spec.Name) {
		zero := Watermark{Rollup: spec.Name, StoragePath: storagePath, BucketInterval: spec.BucketInterval, SpecFingerprint: spec.Fingerprint()}
		if err := s.WMStore.Put(ctx, zero); err != nil {
			return nil, fmt.Errorf("reset watermark for rebuild: %w", err)
		}
	}
	if cur := spec.Fingerprint(); cur != "" {
		wm, err := s.WMStore.Get(ctx, storagePath)
		if err != nil {
			return nil, fmt.Errorf("read watermark for drift check: %w", err)
		}
		if !wm.IsZero() && wm.SpecFingerprint != "" && wm.SpecFingerprint != cur {
			s.Logger.Warn().
				Str("rollup", spec.Name).
				Str("stored_fingerprint", wm.SpecFingerprint).
				Str("current_fingerprint", cur).
				Msg("rollup spec changed shape; resetting watermark to rebuild")
			zero := Watermark{Rollup: spec.Name, StoragePath: storagePath, BucketInterval: spec.BucketInterval, SpecFingerprint: cur}
			if err := s.WMStore.Put(ctx, zero); err != nil {
				return nil, fmt.Errorf("reset watermark for drift: %w", err)
			}
		}
	}

	cutoff := now.Add(-hourlyBuildGrace).Truncate(spec.BucketInterval)

	wm, err := s.WMStore.Get(ctx, storagePath)
	if err != nil {
		return nil, fmt.Errorf("read watermark: %w", err)
	}
	var windowStart time.Time
	if wm.Watermark.IsZero() {
		windowStart = cutoff.Add(-spec.BucketInterval)
		if s.EarliestSourceFunc != nil {
			// EarliestSourceFunc typically runs a MIN(time) over the full
			// source table — on a multi-month corpus this can take many
			// minutes the first time. Bound it so a single zero-watermark
			// spec can't starve every other spec's tick. On timeout we fall
			// back to the default single-window lookback; the spec will
			// build forward from there and the user can re-trigger a
			// rebuild via Control if a deeper backfill is needed.
			esCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			earliest, err := s.EarliestSourceFunc(esCtx, spec)
			cancel()
			if err != nil {
				s.Logger.Warn().Err(err).
					Str("rollup", spec.Name).
					Msg("scheduler: EarliestSourceFunc failed/timed out; using default lookback")
			} else if !earliest.IsZero() {
				earliest = earliest.Truncate(spec.BucketInterval)
				if earliest.Before(windowStart) {
					windowStart = earliest
					s.Logger.Info().
						Str("rollup", spec.Name).
						Time("backfill_start", windowStart).
						Time("cutoff", cutoff).
						Msg("scheduler: backfilling from earliest source bucket")
				}
			}
		}
	} else {
		windowStart = wm.Watermark
	}
	windowEnd := windowStart.Add(spec.BucketInterval)
	if windowEnd.After(cutoff) {
		return nil, nil
	}

	var fromTable string
	if s.FromTableResolver != nil {
		fromTable = s.FromTableResolver(spec, windowStart)
	} else {
		fromTable = chooseFromTable(spec)
	}
	return &specPlan{
		spec:        spec,
		storagePath: storagePath,
		fromTable:   fromTable,
		windowStart: windowStart,
		windowEnd:   windowEnd,
	}, nil
}

// chooseFromTable returns the table the builder reads from:
// "<database>.<source_table>".
func chooseFromTable(spec RollupSpec) string {
	return fmt.Sprintf("%s.%s", spec.Database, spec.SourceTable)
}
