package compaction

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// compaction.requeue_failed_tiers: on 2026-11-17 the 03:00Z daily request
// coalesced behind a running hourly cycle; its drain (cycle 441) failed in the
// same second with "circuit breaker is open" while listing databases, and the
// request was gone -- nothing asked for daily again until the next 03:00Z.

// rootListBackend fails the root listing (the database list) while fail is
// set, the way an open storage circuit breaker did in cycle 441.
type rootListBackend struct {
	storage.Backend
	fail  atomic.Bool
	lists atomic.Int32
}

func (b *rootListBackend) List(ctx context.Context, prefix string) ([]string, error) {
	if prefix == "" {
		b.lists.Add(1)
		if b.fail.Load() {
			return nil, errors.New("circuit breaker is open")
		}
	}
	return b.Backend.List(ctx, prefix)
}

type rqRig struct {
	m       *Manager
	backend *rootListBackend

	hourlyFinds atomic.Int32
	dailyFinds  atomic.Int32
	// dailyFindErr fails daily discovery.
	dailyFindErr atomic.Bool
	// dailyCandidate makes daily discover one two-file partition whose batch
	// fails (batchErr), so the tier starts a batch.
	dailyCandidate atomic.Bool
	batches        atomic.Int32
	// hourlyEntered/hourlyBlock pin the hourly cycle in flight.
	hourlyEntered chan struct{}
	hourlyBlock   chan struct{}
}

func newRQRig(t *testing.T, requeue bool, delay time.Duration) *rqRig {
	t.Helper()
	m, backend, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	if err := backend.Write(context.Background(), "db/cpu/2026/11/08/03/seed.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	m.ManifestManager = nil
	rb := &rootListBackend{Backend: backend}
	m.StorageBackend = rb
	m.RequeueFailedTiers = requeue
	m.RequeueMinDelay = delay

	r := &rqRig{m: m, backend: rb, hourlyEntered: make(chan struct{}, 1)}
	m.Tiers = []Tier{
		interleaveTier{name: "hourly", find: func(ctx context.Context, measurement string) ([]Candidate, error) {
			r.hourlyFinds.Add(1)
			select {
			case r.hourlyEntered <- struct{}{}:
			default:
			}
			if r.hourlyBlock != nil {
				select {
				case <-r.hourlyBlock:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, nil
		}},
		interleaveTier{name: "daily", find: func(ctx context.Context, measurement string) ([]Candidate, error) {
			r.dailyFinds.Add(1)
			if r.dailyFindErr.Load() {
				return nil, errors.New("circuit breaker is open")
			}
			if r.dailyCandidate.Load() {
				return []Candidate{ilCandidate("daily", "db/cpu/2026/11/01", 2)}, nil
			}
			return nil, nil
		}},
	}
	m.compactBatchForTest = func(context.Context, Candidate) error {
		r.batches.Add(1)
		return errors.New("batch failed")
	}
	return r
}

// tick runs a cycle the way a scheduler cron tick does.
func (r *rqRig) tick(tiers ...string) error {
	_, err := r.m.runCycleInternal(context.Background(), cycleSourceScheduler, nil, tiers)
	return err
}

func (r *rqRig) requeued() map[string]time.Time {
	r.m.pendMu.Lock()
	defer r.m.pendMu.Unlock()
	out := map[string]time.Time{}
	for k, v := range r.m.requeued {
		out[k] = v
	}
	return out
}

func (r *rqRig) pending() []string {
	r.m.pendMu.Lock()
	defer r.m.pendMu.Unlock()
	return append([]string(nil), r.m.pending...)
}

// failDrainedDaily reproduces cycles 440/441: an hourly cycle is in flight,
// the daily tick coalesces behind it, storage breaks, the hourly cycle ends
// and the drained daily fails at its database listing.
func (r *rqRig) failDrainedDaily(t *testing.T) {
	t.Helper()
	r.hourlyBlock = make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- r.tick("hourly") }()
	select {
	case <-r.hourlyEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("hourly cycle never entered its tier")
	}
	if err := r.tick("daily"); err != nil {
		t.Fatalf("colliding daily tick: %v, want coalesced", err)
	}
	r.backend.fail.Store(true)
	close(r.hourlyBlock)
	if err := waitDone(t, done); err != nil {
		t.Fatalf("hourly owner: %v", err)
	}
	r.hourlyBlock = nil
	var drained []CycleRecord
	for _, rec := range r.m.CyclePage(20).Cycles {
		if rec.Source == cycleSourceCoalesced {
			drained = append(drained, rec)
		}
	}
	if len(drained) != 1 || !slices.Equal(drained[0].Tiers, []string{"daily"}) || drained[0].Status != "failed" {
		t.Fatalf("drained cycles = %+v, want one failed daily drain", drained)
	}
	if r.dailyFinds.Load() != 0 {
		t.Fatalf("daily discovery ran %d times before the retry", r.dailyFinds.Load())
	}
	if r.m.IsCycleRunning() {
		t.Fatal("claim not released after the failed drain")
	}
}

// The failed drained daily is put back and the next hourly tick after the
// delay runs it: nothing else asks for daily before the next 03:00Z.
func TestRequeue_DailyFailedAtStartRunsOnALaterHourlyTick(t *testing.T) {
	r := newRQRig(t, true, 100*time.Millisecond)
	r.failDrainedDaily(t)
	if _, ok := r.requeued()["daily"]; !ok {
		t.Fatalf("requeued = %v, want daily", r.requeued())
	}

	r.backend.fail.Store(false)
	time.Sleep(150 * time.Millisecond)
	if err := r.tick("hourly"); err != nil {
		t.Fatal(err)
	}
	if got := r.dailyFinds.Load(); got != 1 {
		t.Fatalf("daily discovery ran %d times after the next hourly tick, want 1", got)
	}
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued after the successful retry = %v, want empty", got)
	}
	newest := r.m.CyclePage(1).Cycles[0]
	if newest.Source != cycleSourceCoalesced || !slices.Equal(newest.Tiers, []string{"daily"}) || newest.Status != "completed" {
		t.Fatalf("retry record = %+v, want a completed coalesced daily", newest)
	}

	// Run once: a later tick does not run daily again.
	time.Sleep(150 * time.Millisecond)
	if err := r.tick("hourly"); err != nil {
		t.Fatal(err)
	}
	if got := r.dailyFinds.Load(); got != 1 {
		t.Fatalf("daily discovery ran %d times, want 1: a settled retry ran again", got)
	}
}

// Default off: today's behaviour, the request is dropped.
func TestRequeue_OffDropsTheFailedDaily(t *testing.T) {
	r := newRQRig(t, false, 100*time.Millisecond)
	r.failDrainedDaily(t)
	r.backend.fail.Store(false)
	time.Sleep(150 * time.Millisecond)
	if err := r.tick("hourly"); err != nil {
		t.Fatal(err)
	}
	if got := r.dailyFinds.Load(); got != 0 {
		t.Fatalf("daily ran %d times with requeue_failed_tiers off, want 0", got)
	}
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v with the flag off", got)
	}
}

// A long outage: one retry per delay window, never a loop inside one owner.
func TestRequeue_NoHotLoopDuringOutage(t *testing.T) {
	r := newRQRig(t, true, 200*time.Millisecond)
	r.backend.fail.Store(true)

	if err := r.tick("daily"); err == nil {
		t.Fatal("daily tick with storage down reported success")
	}
	if got := r.backend.lists.Load(); got != 1 {
		t.Fatalf("database listings = %d after one failed daily tick, want 1 (no immediate retry)", got)
	}
	if r.m.IsCycleRunning() {
		t.Fatal("claim held after the failed tick")
	}
	if got := r.pending(); len(got) != 0 {
		t.Fatalf("pending = %v, want empty: the retry waits out the delay", got)
	}

	// Ticks inside the window run only their own tier.
	for i := 0; i < 3; i++ {
		_ = r.tick("hourly")
	}
	if got := r.backend.lists.Load(); got != 4 {
		t.Fatalf("database listings = %d after 3 hourly ticks inside the delay, want 4", got)
	}

	// After the window one tick retries daily once, which fails and re-arms.
	time.Sleep(250 * time.Millisecond)
	_ = r.tick("hourly")
	if got := r.backend.lists.Load(); got != 6 {
		t.Fatalf("database listings = %d after the due tick, want 6 (hourly + one daily retry)", got)
	}
	_ = r.tick("hourly")
	if got := r.backend.lists.Load(); got != 7 {
		t.Fatalf("database listings = %d, want 7: the failed retry must wait another delay", got)
	}
	if at, ok := r.requeued()["daily"]; !ok || time.Until(at) <= 0 {
		t.Fatalf("daily retry = %v (present %v), want re-armed in the future", at, ok)
	}
}

// A daily tier whose discovery failed without starting a batch is retried.
func TestRequeue_FailedDailyDiscoveryIsRetried(t *testing.T) {
	r := newRQRig(t, true, 100*time.Millisecond)
	r.dailyFindErr.Store(true)
	if err := r.tick("daily"); err == nil {
		t.Fatal("daily tick with failing discovery reported success")
	}
	if _, ok := r.requeued()["daily"]; !ok {
		t.Fatalf("requeued = %v, want daily", r.requeued())
	}

	r.dailyFindErr.Store(false)
	time.Sleep(150 * time.Millisecond)
	if err := r.tick("hourly"); err != nil {
		t.Fatal(err)
	}
	if got := r.dailyFinds.Load(); got != 2 {
		t.Fatalf("daily discovery ran %d times, want 2 (failed + retry)", got)
	}
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v after the retry succeeded", got)
	}
}

// A tier that started a batch did its work; only the tier that failed without
// starting one is put back.
func TestRequeue_OnlyTiersWithoutAStartedBatch(t *testing.T) {
	r := newRQRig(t, true, time.Hour)
	r.dailyCandidate.Store(true)
	// Daily starts a batch that fails: the cycle fails, daily is not retried.
	if err := r.tick("daily"); err == nil {
		t.Fatal("cycle with a failed batch reported success")
	}
	if r.batches.Load() != 1 {
		t.Fatalf("batches = %d, want 1", r.batches.Load())
	}
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v, want empty: daily started a batch", got)
	}

	// Hourly completes cleanly, daily discovery fails: only daily is retried.
	r.dailyCandidate.Store(false)
	r.dailyFindErr.Store(true)
	if err := r.tick("hourly", "daily"); err == nil {
		t.Fatal("cycle with a failed daily discovery reported success")
	}
	got := r.requeued()
	if _, ok := got["daily"]; !ok || len(got) != 1 {
		t.Fatalf("requeued = %v, want only daily", got)
	}
}

// A successful cycle does not requeue, and a successful run of a tier clears
// its pending retry so it does not run twice.
func TestRequeue_SuccessfulCycleDoesNotRequeue(t *testing.T) {
	r := newRQRig(t, true, 100*time.Millisecond)
	if err := r.tick("hourly", "daily"); err != nil {
		t.Fatal(err)
	}
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v after a successful cycle", got)
	}

	r.backend.fail.Store(true)
	_ = r.tick("daily")
	if _, ok := r.requeued()["daily"]; !ok {
		t.Fatal("failed daily not requeued")
	}
	r.backend.fail.Store(false)
	if err := r.tick("daily"); err != nil { // the next daily cron, before the retry is due
		t.Fatal(err)
	}
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v after daily succeeded", got)
	}
	time.Sleep(150 * time.Millisecond)
	if err := r.tick("hourly"); err != nil {
		t.Fatal(err)
	}
	if got := r.dailyFinds.Load(); got != 2 {
		t.Fatalf("daily discovery ran %d times, want 2: a cleared retry ran", got)
	}
}

// Only scheduled and coalesced work is retried: an operator's own trigger
// reports its failure to the operator, and a cancelled cycle is not a failure.
func TestRequeue_NotForOperatorTriggersOrCancellation(t *testing.T) {
	r := newRQRig(t, true, time.Hour)
	r.backend.fail.Store(true)
	if _, err := r.m.RunCompactionCycleForTiers(context.Background(), []string{"daily"}); err == nil {
		t.Fatal("trigger with storage down reported success")
	}
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v after an operator trigger", got)
	}

	r.backend.fail.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = r.m.runCycleInternal(ctx, cycleSourceScheduler, nil, []string{"daily"})
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v after a cancelled cycle", got)
	}

	// An operator's full-scope run that settles the tier clears its retry.
	r.backend.fail.Store(true)
	_ = r.tick("daily")
	if _, ok := r.requeued()["daily"]; !ok {
		t.Fatal("failed scheduled daily not requeued")
	}
	r.backend.fail.Store(false)
	if _, err := r.m.RunCompactionCycleForTiers(context.Background(), []string{"daily"}); err != nil {
		t.Fatal(err)
	}
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v after an operator run settled daily", got)
	}
}
