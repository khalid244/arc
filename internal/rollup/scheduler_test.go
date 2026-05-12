package rollup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type fakeBuilder struct {
	calls atomic.Int64
}

func (f *fakeBuilder) BuildWindow(ctx context.Context, spec RollupSpec, fromTable string, ws, we time.Time) error {
	f.calls.Add(1)
	return nil
}

func TestScheduler_RunsOneTickPerInterval(t *testing.T) {
	specs := []RollupSpec{
		{
			Name: "s__1m", Database: "d", SourceTable: "s",
			BucketColumn: "ts", BucketInterval: time.Minute,
		},
	}

	fb := &fakeBuilder{}
	// "now" must be far enough past the pending bucket that the fixed 5m
	// grace doesn't suppress the build. Watermark at 11:50, now at 12:00:30 →
	// cutoff = 11:55, which is > 11:50, so [11:50, 11:51) is eligible.
	now := time.Date(2026, 5, 10, 12, 0, 30, 0, time.UTC)
	clock := func() time.Time { return now }

	wmStore := newInMemWMStore()
	_ = wmStore.Put(context.Background(), Watermark{
		Rollup:         "s__1m",
		BucketInterval: time.Minute,
		Watermark:      time.Date(2026, 5, 10, 11, 50, 0, 0, time.UTC),
	})

	sched := &Scheduler{
		Specs:     specs,
		Builder:   fb,
		WMStore:   wmStore,
		Logger:    zerolog.Nop(),
		Clock:     clock,
		TickEvery: 10 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	sched.Run(ctx)

	if fb.calls.Load() == 0 {
		t.Fatal("expected at least one BuildWindow call")
	}
}

func TestScheduler_NoBuildWhenWindowEmpty(t *testing.T) {
	specs := []RollupSpec{
		{
			Name: "s__1h", Database: "d", SourceTable: "s",
			BucketColumn: "ts", BucketInterval: time.Hour,
		},
	}
	fb := &fakeBuilder{}
	// At 12:00:30 with a 5m grace, cutoff = 11:55:30 truncated to 11:00, so
	// any watermark >= 12:00 still leaves nothing eligible.
	now := time.Date(2026, 5, 10, 12, 0, 30, 0, time.UTC)
	clock := func() time.Time { return now }
	wmStore := newInMemWMStore()
	_ = wmStore.Put(context.Background(), Watermark{
		Rollup:         "s__1h",
		BucketInterval: time.Hour,
		Watermark:      time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	})

	sched := &Scheduler{
		Specs: specs, Builder: fb, WMStore: wmStore,
		Logger: zerolog.Nop(), Clock: clock, TickEvery: 5 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	sched.Run(ctx)

	if fb.calls.Load() != 0 {
		t.Errorf("expected 0 builds, got %d", fb.calls.Load())
	}
}

type inMemWMStore struct {
	m map[string]Watermark
}

func newInMemWMStore() *inMemWMStore { return &inMemWMStore{m: map[string]Watermark{}} }

func (s *inMemWMStore) Get(_ context.Context, name string) (Watermark, error) {
	return s.m[name], nil
}
func (s *inMemWMStore) Put(_ context.Context, w Watermark) error {
	s.m[w.Rollup] = w
	return nil
}
