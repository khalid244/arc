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

// WMReader/WMWriter let the scheduler use either WatermarkStore or an in-memory fake.
type WMReader interface {
	Get(ctx context.Context, rollupName string) (Watermark, error)
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
	// for spec. Production injects ReadParquetFromTable(backend, spec). Tests
	// leave this nil and get bare table names via the default resolver.
	FromTableResolver func(spec RollupSpec) string

	// EarliestSourceFunc returns the earliest bucket time available in spec's
	// source data. When the spec has no watermark yet (fresh start, populated
	// DB), the scheduler backfills from this point forward instead of building
	// only the most recent bucket. Returns zero-time to fall back to the
	// "single-bucket lookback" behavior. Optional.
	EarliestSourceFunc func(ctx context.Context, spec RollupSpec) (time.Time, error)
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

func (s *Scheduler) tick(ctx context.Context) {
	now := s.Clock()
	for _, spec := range s.Specs {
		if err := s.processSpec(ctx, spec, now); err != nil {
			s.Logger.Error().Err(err).Str("rollup", spec.Name).Msg("rollup tick failed")
		}
	}
}

// processSpec drains all eligible windows for spec on this tick, advancing
// the watermark window-by-window. After downtime or for backfill, this lets a
// spec catch up quickly instead of one window per tick.
//
// Drift handling: before each pass, the stored watermark's SpecFingerprint
// is compared with spec.Fingerprint(). A mismatch means the operator
// edited arc.toml in a way that changed this variant's shape (different
// dims, different aggregates, different bucket interval). The watermark
// is reset to zero so the next loop iteration rebuilds from the earliest
// source bucket. Existing parquet under the old shape stays on disk —
// queries continue to see the old data until the new builds overwrite
// (the scheduler writes one window per pass into the same key prefix).
func (s *Scheduler) processSpec(ctx context.Context, spec RollupSpec, now time.Time) error {
	if s.Control != nil && s.Control.IsPaused(spec.Name) {
		return nil
	}
	if s.Control != nil && s.Control.PopRebuildRequest(spec.Name) {
		zero := Watermark{Rollup: spec.Name, BucketInterval: spec.BucketInterval, SpecFingerprint: spec.Fingerprint()}
		if err := s.WMStore.Put(ctx, zero); err != nil {
			return fmt.Errorf("reset watermark for rebuild: %w", err)
		}
	}
	// Spec-drift check: stored fingerprint ≠ current spec's fingerprint means
	// the variant's shape changed since the last build. Reset to force
	// re-backfill so queries don't see a mix of old- and new-shape parquet.
	if cur := spec.Fingerprint(); cur != "" {
		wm, err := s.WMStore.Get(ctx, spec.Name)
		if err != nil {
			return fmt.Errorf("read watermark for drift check: %w", err)
		}
		if !wm.IsZero() && wm.SpecFingerprint != "" && wm.SpecFingerprint != cur {
			s.Logger.Warn().
				Str("rollup", spec.Name).
				Str("stored_fingerprint", wm.SpecFingerprint).
				Str("current_fingerprint", cur).
				Msg("rollup spec changed shape; resetting watermark to rebuild")
			zero := Watermark{Rollup: spec.Name, BucketInterval: spec.BucketInterval, SpecFingerprint: cur}
			if err := s.WMStore.Put(ctx, zero); err != nil {
				return fmt.Errorf("reset watermark for drift: %w", err)
			}
		}
	}

	cutoff := now.Add(-hourlyBuildGrace).Truncate(spec.BucketInterval)
	var fromTable string
	if s.FromTableResolver != nil {
		fromTable = s.FromTableResolver(spec)
	} else {
		fromTable = chooseFromTable(spec)
	}

	for {
		wm, err := s.WMStore.Get(ctx, spec.Name)
		if err != nil {
			return fmt.Errorf("read watermark: %w", err)
		}
		var windowStart time.Time
		if wm.Watermark.IsZero() {
			windowStart = cutoff.Add(-spec.BucketInterval)
			if s.EarliestSourceFunc != nil {
				if earliest, err := s.EarliestSourceFunc(ctx, spec); err == nil && !earliest.IsZero() {
					// Align to bucket boundary so windows tile cleanly.
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
			return nil
		}
		if err := s.Builder.BuildWindow(ctx, spec, fromTable, windowStart, windowEnd); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// chooseFromTable returns the table the builder reads from:
// "<database>.<source_table>".
func chooseFromTable(spec RollupSpec) string {
	return fmt.Sprintf("%s.%s", spec.Database, spec.SourceTable)
}
