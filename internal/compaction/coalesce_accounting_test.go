package compaction

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// ---------------------------------------------------------------------------
// Test scaffolding: a controllable Tier stub.
//
// fakeTier implements the Tier interface and returns NO candidates, so cycles
// exercise the scheduler/manager control flow without spawning DuckDB
// subprocesses. It can optionally block inside FindCandidates so a test can
// pin a cycle "in flight" and then race a second request against it — exactly
// the 08:00 hourly/daily collision that starved daily compaction in prod.
// ---------------------------------------------------------------------------

type scanRecorder struct {
	mu    sync.Mutex
	order []string
}

func (r *scanRecorder) add(name string) {
	r.mu.Lock()
	r.order = append(r.order, name)
	r.mu.Unlock()
}

func (r *scanRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

type fakeTier struct {
	name    string
	enabled bool
	rec     *scanRecorder

	// entered is signaled (once, non-blocking) the first time FindCandidates
	// is invoked — lets a test wait until a cycle is genuinely inside the tier.
	entered chan struct{}
	// block, if non-nil, holds FindCandidates until the test closes it,
	// pinning the owning cycle in flight.
	block chan struct{}
	// scanned receives the tier name after each completed scan.
	scanned chan string

	scans int32
}

func (f *fakeTier) GetTierName() string       { return f.name }
func (f *fakeTier) GetPartitionLevel() string { return "day" }

func (f *fakeTier) FindCandidates(ctx context.Context, _ /*database*/, _ /*measurement*/ string) ([]Candidate, error) {
	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	atomic.AddInt32(&f.scans, 1)
	if f.rec != nil {
		f.rec.add(f.name)
	}
	if f.scanned != nil {
		select {
		case f.scanned <- f.name:
		default:
		}
	}
	return nil, nil // no candidates -> no compaction subprocess
}

func (f *fakeTier) ShouldCompact([]string, time.Time) bool { return false }
func (f *fakeTier) IsCompactedFile(string) bool            { return false }
func (f *fakeTier) IsEnabled() bool                        { return f.enabled }
func (f *fakeTier) GetStats() map[string]interface{}       { return map[string]interface{}{} }
func (f *fakeTier) GetMaxOutputBytes() int64               { return 0 }

func newFakeTier(name string, rec *scanRecorder) *fakeTier {
	return &fakeTier{
		name:    name,
		enabled: true,
		rec:     rec,
		entered: make(chan struct{}, 1),
		scanned: make(chan string, 8),
	}
}

func setupManagerWithTiers(t *testing.T, tiers ...Tier) (*Manager, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "arc-coalesce-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create storage backend: %v", err)
	}

	// Seed one db/measurement so the tier loop actually invokes FindCandidates.
	if err := backend.Write(context.Background(), "testdb/events/2025/01/01/00/seed.parquet", []byte("x")); err != nil {
		backend.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to seed storage: %v", err)
	}

	mgr := NewManager(&ManagerConfig{
		StorageBackend: backend,
		LockManager:    NewLockManager(),
		MaxConcurrent:  2,
		TempDirectory:  tmpDir + "/temp",
		Tiers:          tiers,
		Logger:         logger,
	})

	return mgr, func() {
		backend.Close()
		os.RemoveAll(tmpDir)
	}
}

// waitFor blocks until ch delivers a value or the deadline elapses.
func waitFor(t *testing.T, ch chan string, d time.Duration, msg string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatal(msg)
		return ""
	}
}

// ===========================================================================
// FEATURE #1 — Coalescing runner: a requested tier-set is never silently
// dropped when it collides with an in-flight cycle. It is queued and run when
// the in-flight cycle finishes, so a low-frequency tier (daily) can't be
// starved by a high-frequency one (hourly).
// ===========================================================================

// TestCoalesce_RequestDuringInFlightCycleStillRuns is the direct regression
// test for the prod bug: at 08:00 the hourly cycle held the manager and the
// daily request was dropped with ErrCycleAlreadyRunning, never to retry for
// 24h. After the fix, daily must still run once the hourly cycle completes.
func TestCoalesce_RequestDuringInFlightCycleStillRuns(t *testing.T) {
	rec := &scanRecorder{}
	hourly := newFakeTier("hourly", rec)
	hourly.block = make(chan struct{}) // pin the hourly cycle in flight
	daily := newFakeTier("daily", rec)

	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	ctx := context.Background()

	// Cycle A (hourly) starts and blocks inside the tier, holding the lock.
	go func() { _, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"hourly"}) }()
	select {
	case <-hourly.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("hourly cycle never entered the tier")
	}
	if !mgr.IsCycleRunning() {
		t.Fatal("expected a cycle to be in flight")
	}

	// Request B (daily) collides with the in-flight cycle. The call must
	// return promptly (queued, not blocking) — the drain happens on A's
	// goroutine after it releases.
	bErr := make(chan error, 1)
	go func() {
		_, err := mgr.RunCompactionCycleForTiers(ctx, []string{"daily"})
		bErr <- err
	}()
	select {
	case err := <-bErr:
		t.Logf("queued daily request returned err=%v (coalescing impl should be nil)", err)
	case <-time.After(2 * time.Second):
		t.Fatal("daily request blocked instead of queueing")
	}

	// Release A. Daily must NOT have been dropped.
	close(hourly.block)

	if name := waitFor(t, daily.scanned, 3*time.Second,
		"daily was DROPPED: it never ran after the in-flight cycle finished"); name != "daily" {
		t.Fatalf("unexpected scan %q", name)
	}
	if got := atomic.LoadInt32(&daily.scans); got != 1 {
		t.Fatalf("daily scans = %d, want 1", got)
	}
}

// TestCoalesce_QueuedRequestReturnsWithoutError pins the contract that a
// coalesced request is a success, not an error — so the scheduler stops
// logging "Scheduled compaction failed: compaction cycle already running".
func TestCoalesce_QueuedRequestReturnsWithoutError(t *testing.T) {
	rec := &scanRecorder{}
	hourly := newFakeTier("hourly", rec)
	hourly.block = make(chan struct{})
	daily := newFakeTier("daily", rec)

	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	ctx := context.Background()

	go func() { _, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"hourly"}) }()
	<-hourly.entered

	_, err := mgr.RunCompactionCycleForTiers(ctx, []string{"daily"})
	if err == ErrCycleAlreadyRunning {
		t.Fatal("queued request returned ErrCycleAlreadyRunning; collision must coalesce, not drop")
	}
	if err != nil {
		t.Fatalf("queued request returned unexpected error: %v", err)
	}

	close(hourly.block)
	waitFor(t, daily.scanned, 3*time.Second, "daily never ran after queueing")
}

// TestCoalesce_DuplicatePendingRunsOnce verifies the pending set is a UNION:
// two daily requests arriving during one in-flight cycle collapse to a single
// daily run, not two.
func TestCoalesce_DuplicatePendingRunsOnce(t *testing.T) {
	rec := &scanRecorder{}
	hourly := newFakeTier("hourly", rec)
	hourly.block = make(chan struct{})
	daily := newFakeTier("daily", rec)

	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	ctx := context.Background()

	go func() { _, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"hourly"}) }()
	<-hourly.entered

	_, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"daily"})
	_, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"daily"})

	close(hourly.block)

	waitFor(t, daily.scanned, 3*time.Second, "daily never ran after queueing")
	// Give any (erroneous) second drain a chance to happen, then assert once.
	time.Sleep(250 * time.Millisecond)
	if got := atomic.LoadInt32(&daily.scans); got != 1 {
		t.Fatalf("daily scans = %d, want 1 (duplicate pending requests must merge)", got)
	}
}

// TestCoalesce_PendingPreservesTierHierarchy verifies that when hourly+daily
// are both pending, the drain runs them in hierarchy order (hourly before
// daily) — daily consumes hourly's output, so order must be preserved.
func TestCoalesce_PendingPreservesTierHierarchy(t *testing.T) {
	rec := &scanRecorder{}
	hourly := newFakeTier("hourly", rec)
	first := newFakeTier("first-blocker", rec) // a distinct in-flight cycle to collide with
	first.block = make(chan struct{})
	daily := newFakeTier("daily", rec)

	mgr, cleanup := setupManagerWithTiers(t, first, hourly, daily)
	defer cleanup()
	ctx := context.Background()

	go func() { _, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"first-blocker"}) }()
	<-first.entered

	// Queue both real tiers while the blocker holds the cycle.
	_, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"hourly", "daily"})

	close(first.block)

	waitFor(t, daily.scanned, 3*time.Second, "daily never ran after queueing")
	order := rec.snapshot()
	// Drop the blocker; the remaining drained scans must be hourly then daily.
	var hi, di = -1, -1
	for i, n := range order {
		if n == "hourly" {
			hi = i
		}
		if n == "daily" {
			di = i
		}
	}
	if hi == -1 || di == -1 || hi > di {
		t.Fatalf("expected hourly to scan before daily in the drained cycle, order=%v", order)
	}
}

// TestCoalesce_NoContentionRunsEachTierExactlyOnce is a GUARD test (expected
// to pass before and after): when nothing is queued, the drain loop must not
// re-run tiers. Protects against the fix introducing duplicate/looping cycles.
func TestCoalesce_NoContentionRunsEachTierExactlyOnce(t *testing.T) {
	rec := &scanRecorder{}
	hourly := newFakeTier("hourly", rec)
	daily := newFakeTier("daily", rec)

	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()

	if _, err := mgr.RunCompactionCycleForTiers(context.Background(), []string{"hourly", "daily"}); err != nil {
		t.Fatalf("cycle failed: %v", err)
	}
	if h := atomic.LoadInt32(&hourly.scans); h != 1 {
		t.Fatalf("hourly scans = %d, want 1", h)
	}
	if d := atomic.LoadInt32(&daily.scans); d != 1 {
		t.Fatalf("daily scans = %d, want 1", d)
	}
}

// TestCoalesce_HourlyOverlapReRunsNotDropped covers the hourly self-overlap
// (the prod "00:10 already running, skipping" line): when an hourly tick fires
// while the previous hourly cycle is still running, the overlapping tick must
// be coalesced and re-run after — back-to-back catch-up, not silently dropped.
func TestCoalesce_HourlyOverlapReRunsNotDropped(t *testing.T) {
	rec := &scanRecorder{}
	hourly := newFakeTier("hourly", rec)
	hourly.block = make(chan struct{})

	mgr, cleanup := setupManagerWithTiers(t, hourly)
	defer cleanup()
	ctx := context.Background()

	// Cycle A: hourly, blocked in flight.
	go func() { _, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"hourly"}) }()
	<-hourly.entered

	// The next hourly tick fires before A finishes.
	_, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"hourly"})

	// Release A. We expect TWO hourly scans: the in-flight one, then the
	// coalesced re-run drained after it.
	close(hourly.block)
	waitFor(t, hourly.scanned, 3*time.Second, "first (in-flight) hourly scan never completed")
	waitFor(t, hourly.scanned, 3*time.Second, "overlapping hourly tick was DROPPED, not re-run")
	if got := atomic.LoadInt32(&hourly.scans); got != 2 {
		t.Fatalf("hourly scans = %d, want 2 (in-flight + coalesced re-run)", got)
	}
}

// TestCoalesce_DailyNotStarvedBySustainedHourlyOverlap is the key guarantee
// behind the fix: even when hourly is overrunning its window (back-to-back
// catch-up), a daily tick that lands during the in-flight cycle joins the
// pending UNION and runs in the very next drain — it is never starved, which
// is exactly what the old drop-the-loser design failed to ensure.
func TestCoalesce_DailyNotStarvedBySustainedHourlyOverlap(t *testing.T) {
	rec := &scanRecorder{}
	hourly := newFakeTier("hourly", rec)
	hourly.block = make(chan struct{})
	daily := newFakeTier("daily", rec)

	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	ctx := context.Background()

	// Cycle A: hourly, blocked in flight.
	go func() { _, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"hourly"}) }()
	<-hourly.entered

	// During A, BOTH the next hourly tick and the daily tick fire (the 08:00
	// collision, but now with hourly also overrunning).
	_, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"hourly"})
	_, _ = mgr.RunCompactionCycleForTiers(ctx, []string{"daily"})

	close(hourly.block)

	waitFor(t, daily.scanned, 3*time.Second,
		"daily was starved by overlapping hourly: it never ran")
	if got := atomic.LoadInt32(&daily.scans); got != 1 {
		t.Fatalf("daily scans = %d, want 1", got)
	}
}

// ===========================================================================
// FEATURE #2 — Per-tier run accounting + staleness detection. The bug hid for
// a month because nothing recorded "the daily tier last ran at X". These tests
// pin: (a) a successful tier scan stamps last-run, (b) finding zero candidates
// STILL counts as a run (we alert on "did the scan happen", not "was there
// work"), (c) a staleness query flags never-run and overdue tiers — the alert
// backstop — and (d) it's exported for a Prometheus gauge.
// ===========================================================================

// TestRunAccounting_ProcessedTierUpdatesLastRun: a cycle that processes daily
// stamps daily's last-run; a tier NOT in the cycle is left untouched.
func TestRunAccounting_ProcessedTierUpdatesLastRun(t *testing.T) {
	rec := &scanRecorder{}
	hourly := newFakeTier("hourly", rec)
	daily := newFakeTier("daily", rec)

	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()

	before := time.Now()
	if _, err := mgr.RunCompactionCycleForTiers(context.Background(), []string{"daily"}); err != nil {
		t.Fatalf("cycle failed: %v", err)
	}

	dailyRun := mgr.LastSuccessfulRun("daily")
	if dailyRun.Before(before) {
		t.Fatalf("daily last-run = %v, want >= %v", dailyRun, before)
	}
	if hr := mgr.LastSuccessfulRun("hourly"); !hr.IsZero() {
		t.Fatalf("hourly last-run = %v, want zero (it was not in the cycle)", hr)
	}
}

// TestRunAccounting_ZeroCandidatesStillCountsAsRun: the critical semantic —
// the fake tier returns no candidates, yet last-run must advance. Otherwise a
// healthy "nothing to do" daily looks identical to a dead daily.
func TestRunAccounting_ZeroCandidatesStillCountsAsRun(t *testing.T) {
	daily := newFakeTier("daily", nil) // returns zero candidates

	mgr, cleanup := setupManagerWithTiers(t, daily)
	defer cleanup()

	before := time.Now()
	if _, err := mgr.RunCompactionCycleForTiers(context.Background(), []string{"daily"}); err != nil {
		t.Fatalf("cycle failed: %v", err)
	}
	if got := mgr.LastSuccessfulRun("daily"); got.Before(before) {
		t.Fatalf("daily last-run = %v, want a fresh stamp even with 0 candidates", got)
	}
}

// TestRunAccounting_StaleTiersFlagsNeverRunAndOverdue is the alert backstop:
// StaleTiers(threshold, now) must report (a) an enabled tier that has NEVER
// run, and (b) one whose last run is older than the threshold — while NOT
// reporting one that ran within the threshold.
func TestRunAccounting_StaleTiersFlagsNeverRunAndOverdue(t *testing.T) {
	hourly := newFakeTier("hourly", nil)
	daily := newFakeTier("daily", nil)

	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()

	// (a) Nothing has run yet: every enabled tier is stale.
	stale := mgr.StaleTiers(time.Hour, time.Now())
	if !hasTier(stale, "hourly") || !hasTier(stale, "daily") {
		t.Fatalf("never-run tiers not flagged stale: got %v", stale)
	}

	// Run only daily.
	start := time.Now()
	if _, err := mgr.RunCompactionCycleForTiers(context.Background(), []string{"daily"}); err != nil {
		t.Fatalf("cycle failed: %v", err)
	}

	// (b) Evaluated 1 minute later: daily ran recently -> not stale; hourly
	// still never ran -> stale.
	stale = mgr.StaleTiers(time.Hour, start.Add(time.Minute))
	if hasTier(stale, "daily") {
		t.Fatalf("daily ran 1m ago but was flagged stale: %v", stale)
	}
	if !hasTier(stale, "hourly") {
		t.Fatalf("hourly never ran but was not flagged stale: %v", stale)
	}

	// (c) Evaluated 2 hours later with a 1h threshold: daily is now overdue.
	stale = mgr.StaleTiers(time.Hour, start.Add(2*time.Hour))
	if !hasTier(stale, "daily") {
		t.Fatalf("daily is 2h overdue (1h threshold) but not flagged stale: %v", stale)
	}
}

// TestRunAccounting_TierLastSuccessExposedInStats: the per-tier last-run map is
// surfaced via Stats() so a Prometheus gauge / healthcheck can read it.
func TestRunAccounting_TierLastSuccessExposedInStats(t *testing.T) {
	daily := newFakeTier("daily", nil)

	mgr, cleanup := setupManagerWithTiers(t, daily)
	defer cleanup()

	if _, err := mgr.RunCompactionCycleForTiers(context.Background(), []string{"daily"}); err != nil {
		t.Fatalf("cycle failed: %v", err)
	}

	raw, ok := mgr.Stats()["tier_last_success"]
	if !ok {
		t.Fatal("Stats() missing 'tier_last_success'")
	}
	m, ok := raw.(map[string]time.Time)
	if !ok {
		t.Fatalf("'tier_last_success' = %T, want map[string]time.Time", raw)
	}
	if _, ok := m["daily"]; !ok {
		t.Fatalf("'tier_last_success' missing daily entry: %v", m)
	}
}

func hasTier(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
