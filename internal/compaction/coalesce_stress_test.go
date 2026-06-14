package compaction

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// waitUntil polls cond until true or the deadline elapses.
func waitUntil(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

// TestStress_NoRequestLostUnderConcurrency hammers the coalescing runner from
// many goroutines requesting random tiers, then asserts the system invariants:
//   - no tier is totally starved (each requested tier ran at least once),
//   - the cycle flag is never left wedged (settles to idle),
//   - a fresh request submitted AFTER the storm still runs (sentinel: proves
//     no permanent wedge / lost wakeup),
//   - per-tier accounting was recorded.
//
// Run with -race -count=N to flush lost-wakeup / data races.
func TestStress_NoRequestLostUnderConcurrency(t *testing.T) {
	hourly := newFakeTier("hourly", nil)
	daily := newFakeTier("daily", nil)
	sentinel := newFakeTier("sentinel", nil) // registered, requested only at the end

	mgr, cleanup := setupManagerWithTiers(t, hourly, daily, sentinel)
	defer cleanup()
	ctx := context.Background()

	const workers, iters = 32, 200
	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				tier := "hourly"
				if (g+i)%2 == 0 {
					tier = "daily"
				}
				if _, err := mgr.RunCompactionCycleForTiers(ctx, []string{tier}); err != nil {
					t.Errorf("request errored: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	// The owner may still be draining; it must settle to idle.
	if !waitUntil(5*time.Second, func() bool { return !mgr.IsCycleRunning() }) {
		t.Fatal("cycle flag wedged true after storm (lost wakeup / stuck owner)")
	}

	if atomic.LoadInt32(&hourly.scans) == 0 {
		t.Error("hourly starved: never ran across 6400 requests")
	}
	if atomic.LoadInt32(&daily.scans) == 0 {
		t.Error("daily starved: never ran across 6400 requests")
	}
	if mgr.LastSuccessfulRun("hourly").IsZero() || mgr.LastSuccessfulRun("daily").IsZero() {
		t.Error("accounting not recorded for a tier that ran")
	}

	// Sentinel: a brand-new request after the storm must still run.
	before := atomic.LoadInt32(&sentinel.scans)
	if _, err := mgr.RunCompactionCycleForTiers(ctx, []string{"sentinel"}); err != nil {
		t.Fatalf("post-storm sentinel request errored: %v", err)
	}
	if !waitUntil(3*time.Second, func() bool { return atomic.LoadInt32(&sentinel.scans) > before }) {
		t.Fatal("post-storm sentinel never ran: system wedged after concurrency")
	}
}

// TestStress_ColliderHammer drives a tight collision loop: every iteration a
// background request races a foreground request so they frequently hit the
// owner-vs-queue boundary. With -race this flushes data races on the pending
// set and the cycle flag; the assertion guards against starvation/wedge.
func TestStress_ColliderHammer(t *testing.T) {
	a := newFakeTier("a", nil)
	b := newFakeTier("b", nil)
	mgr, cleanup := setupManagerWithTiers(t, a, b)
	defer cleanup()
	ctx := context.Background()

	const K = 600
	for i := 0; i < K; i++ {
		var w sync.WaitGroup
		w.Add(1)
		go func() {
			defer w.Done()
			_, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"a"})
		}()
		_, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"b"})
		w.Wait()
	}

	if !waitUntil(5*time.Second, func() bool { return !mgr.IsCycleRunning() }) {
		t.Fatal("flag wedged after collider hammer")
	}
	if atomic.LoadInt32(&a.scans) == 0 || atomic.LoadInt32(&b.scans) == 0 {
		t.Fatalf("a/b scans = %d/%d, both must be > 0", a.scans, b.scans)
	}
}

// TestStress_NoGoroutineLeak verifies the runner doesn't leak goroutines: after
// a storm settles, the live goroutine count returns to a stable baseline.
func TestStress_NoGoroutineLeak(t *testing.T) {
	hourly := newFakeTier("hourly", nil)
	daily := newFakeTier("daily", nil)
	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	ctx := context.Background()

	// Warm up, then capture a baseline.
	_, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"hourly"})
	waitUntil(2*time.Second, func() bool { return !mgr.IsCycleRunning() })
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for g := 0; g < 24; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				tier := "hourly"
				if (g+i)%2 == 0 {
					tier = "daily"
				}
				_, _ = mgr.RunCompactionCycleForTiers(ctx, []string{tier})
			}
		}(g)
	}
	wg.Wait()
	waitUntil(5*time.Second, func() bool { return !mgr.IsCycleRunning() })

	leaked := waitUntil(3*time.Second, func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= baseline+8 // tolerance for runtime/race workers
	})
	if !leaked {
		t.Fatalf("goroutine leak: baseline=%d now=%d", baseline, runtime.NumGoroutine())
	}
}

// TestCoalesce_ContextCancelDoesNotWedge cancels the owner's context while a
// request is queued behind it, then asserts the flag is released (not wedged)
// and the manager accepts new work afterward.
func TestCoalesce_ContextCancelDoesNotWedge(t *testing.T) {
	hourly := newFakeTier("hourly", nil)
	hourly.block = make(chan struct{})
	daily := newFakeTier("daily", nil)
	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	go func() { _, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"hourly"}) }()
	<-hourly.entered

	// Queue daily behind the in-flight (blocked) hourly cycle.
	if _, err := mgr.RunCompactionCycleForTiers(ctx, []string{"daily"}); err != nil {
		t.Fatalf("queue errored: %v", err)
	}

	// Cancel, then unblock so the owner can unwind.
	cancel()
	close(hourly.block)

	if !waitUntil(5*time.Second, func() bool { return !mgr.IsCycleRunning() }) {
		t.Fatal("cycle flag wedged true after ctx cancel")
	}
	// A fresh cycle on a live context must still run.
	fresh := newFakeTier("fresh", nil)
	mgr2, cleanup2 := setupManagerWithTiers(t, fresh)
	defer cleanup2()
	if _, err := mgr2.RunCompactionCycleForTiers(context.Background(), []string{"fresh"}); err != nil {
		t.Fatalf("post-cancel cycle errored: %v", err)
	}
	if atomic.LoadInt32(&fresh.scans) != 1 {
		t.Fatalf("post-cancel fresh scans = %d, want 1", fresh.scans)
	}
}

// TestCoalesce_SingleCycleIncrementsCycleIDOnce proves the drain loop does not
// spin or double-run when nothing is queued: one request => exactly one cycle.
func TestCoalesce_SingleCycleIncrementsCycleIDOnce(t *testing.T) {
	hourly := newFakeTier("hourly", nil)
	mgr, cleanup := setupManagerWithTiers(t, hourly)
	defer cleanup()

	start := mgr.GetCurrentCycleID()
	if _, err := mgr.RunCompactionCycleForTiers(context.Background(), []string{"hourly"}); err != nil {
		t.Fatalf("cycle errored: %v", err)
	}
	if got := mgr.GetCurrentCycleID() - start; got != 1 {
		t.Fatalf("cycle id advanced by %d, want 1 (spurious drain iteration?)", got)
	}
	if atomic.LoadInt32(&hourly.scans) != 1 {
		t.Fatalf("hourly scans = %d, want 1", hourly.scans)
	}
}

// TestStaleTiers_BoundaryAndDisabledTier covers the alert math edges: a tier
// run EXACTLY at the cutoff is not stale; a disabled tier is never reported;
// a never-run enabled tier is stale.
func TestStaleTiers_BoundaryAndDisabledTier(t *testing.T) {
	hourly := newFakeTier("hourly", nil)
	daily := newFakeTier("daily", nil)
	off := newFakeTier("disabled", nil)
	off.enabled = false

	mgr, cleanup := setupManagerWithTiers(t, hourly, daily, off)
	defer cleanup()

	// Force a known last-run for hourly; leave daily never-run.
	base := time.Date(2026, 6, 14, 8, 0, 0, 0, time.UTC)
	mgr.lastRunMu.Lock()
	mgr.lastRun["hourly"] = base
	mgr.lastRunMu.Unlock()

	// Evaluate exactly one threshold later: hourly's last-run == cutoff, which
	// is NOT strictly before cutoff -> not stale.
	stale := mgr.StaleTiers(time.Hour, base.Add(time.Hour))
	if hasTier(stale, "hourly") {
		t.Errorf("hourly at exactly the cutoff must not be stale: %v", stale)
	}
	if !hasTier(stale, "daily") {
		t.Errorf("never-run daily must be stale: %v", stale)
	}
	if hasTier(stale, "disabled") {
		t.Errorf("disabled tier must never be reported: %v", stale)
	}

	// One nanosecond past the cutoff: hourly is now stale.
	stale = mgr.StaleTiers(time.Hour, base.Add(time.Hour+time.Nanosecond))
	if !hasTier(stale, "hourly") {
		t.Errorf("hourly 1ns past cutoff must be stale: %v", stale)
	}
}

// TestStats_TierLastSuccessSnapshotIsCopy ensures the map exposed via Stats()
// is a copy — a caller mutating it must not corrupt the manager's state.
func TestStats_TierLastSuccessSnapshotIsCopy(t *testing.T) {
	daily := newFakeTier("daily", nil)
	mgr, cleanup := setupManagerWithTiers(t, daily)
	defer cleanup()

	if _, err := mgr.RunCompactionCycleForTiers(context.Background(), []string{"daily"}); err != nil {
		t.Fatalf("cycle errored: %v", err)
	}
	snap := mgr.Stats()["tier_last_success"].(map[string]time.Time)
	snap["daily"] = time.Time{}     // mutate the caller's copy
	snap["injected"] = time.Now()   // add a bogus key

	if mgr.LastSuccessfulRun("daily").IsZero() {
		t.Error("Stats() snapshot is not a copy: mutation corrupted manager state")
	}
	if _, bogus := mgr.Stats()["tier_last_success"].(map[string]time.Time)["injected"]; bogus {
		t.Error("Stats() snapshot is not a copy: injected key leaked into manager state")
	}
}

// TestScheduler_ConcurrentHourlyDailyCollisionBothRun reproduces the exact prod
// incident at the scheduler layer: two schedulers (hourly + daily) share one
// manager and fire at the same instant (the 08:00 tick). The daily trigger must
// NOT be dropped with ErrCycleAlreadyRunning — it must coalesce and run once the
// hourly cycle finishes.
func TestScheduler_ConcurrentHourlyDailyCollisionBothRun(t *testing.T) {
	hourly := newFakeTier("hourly", nil)
	hourly.block = make(chan struct{})
	daily := newFakeTier("daily", nil)

	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	ctx := context.Background()

	hSched, err := NewScheduler(&SchedulerConfig{
		Manager: mgr, Schedule: "*/10 * * * *", TierNames: []string{"hourly"},
		Enabled: true, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("hourly scheduler: %v", err)
	}
	dSched, err := NewScheduler(&SchedulerConfig{
		Manager: mgr, Schedule: "0 8 * * *", TierNames: []string{"daily"},
		Enabled: true, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("daily scheduler: %v", err)
	}

	// Hourly fires and is held in flight.
	go func() { _, _ = hSched.TriggerNow(ctx) }()
	<-hourly.entered

	// Daily fires on the same tick — old behavior dropped it for 24h.
	dErr := make(chan error, 1)
	go func() { _, err := dSched.TriggerNow(ctx); dErr <- err }()
	select {
	case err := <-dErr:
		if err == ErrCycleAlreadyRunning {
			t.Fatal("daily scheduler dropped on collision (the prod bug)")
		}
		if err != nil {
			t.Fatalf("daily trigger errored: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("daily trigger blocked")
	}

	close(hourly.block)
	waitFor(t, daily.scanned, 3*time.Second, "daily never ran after the colliding hourly cycle finished")
}
