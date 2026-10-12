package compaction

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// Coalescing (fork b738de5f) merged with upstream's cycle claim (#1153) and
// per-cycle history (#1162). These pin the merge-specific contracts.

// A drained (coalesced) pass is a cycle of its own: it gets its own id and a
// finished history record, so #1162's one-record-per-cycle accounting holds.
func TestCoalesce_DrainedPassIsItsOwnCycle(t *testing.T) {
	rec := &scanRecorder{}
	hourly := newFakeTier("hourly", rec)
	hourly.block = make(chan struct{})
	daily := newFakeTier("daily", rec)
	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	ctx := context.Background()

	ownerDone := make(chan int64, 1)
	go func() {
		id, _ := mgr.RunCompactionCycleForTiers(ctx, []string{"hourly"})
		ownerDone <- id
	}()
	<-hourly.entered
	if id, err := mgr.RunCompactionCycleForTiers(ctx, []string{"daily"}); err != nil || id != 0 {
		t.Fatalf("coalesced request = (%d, %v), want (0, nil)", id, err)
	}
	close(hourly.block)
	ownerID := <-ownerDone
	waitFor(t, daily.scanned, 3*time.Second, "coalesced daily never ran")

	// The claim is released only after the drain, so a later cycle can run.
	deadline := time.Now().Add(3 * time.Second)
	for mgr.IsCycleRunning() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if mgr.IsCycleRunning() {
		t.Fatal("claim never released after the drain")
	}

	page := mgr.CyclePage(10)
	if len(page.Cycles) != 2 {
		t.Fatalf("cycle history has %d records, want 2 (owner + drained pass): %+v", len(page.Cycles), page.Cycles)
	}
	drained, owner := page.Cycles[0], page.Cycles[1] // newest first
	if owner.CycleID != ownerID || drained.CycleID != ownerID+1 {
		t.Fatalf("cycle ids owner=%d drained=%d, want %d and %d", owner.CycleID, drained.CycleID, ownerID, ownerID+1)
	}
	if drained.Source != cycleSourceCoalesced || drained.Status != "completed" || drained.FinishedAt.IsZero() {
		t.Fatalf("drained record = %+v, want a finished coalesced cycle", drained)
	}
	if len(drained.Tiers) != 1 || drained.Tiers[0] != "daily" {
		t.Fatalf("drained tiers = %v, want [daily]", drained.Tiers)
	}
}

// A cycle held through ClaimCycle (the API trigger) never drains: a colliding
// unscoped request is still coalesced, not refused, and the next cycle that
// owns a loop runs it after its own work.
func TestCoalesce_BehindAnAPIClaimRunsWithTheNextCycle(t *testing.T) {
	rec := &scanRecorder{}
	hourly := newFakeTier("hourly", rec)
	daily := newFakeTier("daily", rec)
	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	ctx := context.Background()

	claim, err := mgr.ClaimCycle()
	if err != nil {
		t.Fatal(err)
	}
	if id, err := mgr.RunCompactionCycleForTiers(ctx, []string{"daily"}); err != nil || id != 0 {
		t.Fatalf("request behind an API claim = (%d, %v), want coalesced (0, nil)", id, err)
	}
	// Scoped requests keep the refusal.
	if _, err := mgr.RunCompactionCycleForDatabase(ctx, "testdb", []string{"daily"}); err != ErrCycleAlreadyRunning {
		t.Fatalf("scoped request behind a claim: err = %v, want ErrCycleAlreadyRunning", err)
	}
	claim.Release()
	if got := atomic.LoadInt32(&daily.scans); got != 0 {
		t.Fatalf("daily ran under the API claim (%d scans)", got)
	}

	if _, err := mgr.RunCompactionCycleForTiers(ctx, []string{"hourly"}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&daily.scans); got != 1 {
		t.Fatalf("daily scans = %d, want 1 (drained after the next owner's own cycle)", got)
	}
	if got := atomic.LoadInt32(&hourly.scans); got != 1 {
		t.Fatalf("hourly scans = %d, want 1", got)
	}
}

// A cancelled owner keeps the pending work for the next owner instead of
// dropping it (dropping it delayed a colliding daily tier by a whole day).
func TestCoalesce_CancelledOwnerKeepsPendingWork(t *testing.T) {
	rec := &scanRecorder{}
	hourly := newFakeTier("hourly", rec)
	hourly.block = make(chan struct{})
	daily := newFakeTier("daily", rec)
	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()

	ownerCtx, cancel := context.WithCancel(context.Background())
	ownerDone := make(chan error, 1)
	go func() {
		_, err := mgr.RunCompactionCycleForTiers(ownerCtx, []string{"hourly"})
		ownerDone <- err
	}()
	<-hourly.entered
	if _, err := mgr.RunCompactionCycleForTiers(context.Background(), []string{"daily"}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-ownerDone; err == nil {
		t.Fatal("cancelled owner reported success")
	}
	if got := atomic.LoadInt32(&daily.scans); got != 0 {
		t.Fatalf("daily ran under a cancelled context (%d scans)", got)
	}

	hourly.block = nil
	if _, err := mgr.RunCompactionCycleForTiers(context.Background(), []string{"hourly"}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&daily.scans); got != 1 {
		t.Fatalf("daily scans = %d, want 1: pending work was dropped by the cancelled owner", got)
	}
}
