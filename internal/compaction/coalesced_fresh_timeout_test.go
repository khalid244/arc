package compaction

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// compaction.coalesced_fresh_timeout: a daily coalesced behind an hourly
// cycle that began at 02:40Z ran on that cycle's context and died at 02:40Z
// the next day, ~14.8 h into its own work (11-15, 11-16, 11-17).

// coalescedRun starts an owner hourly cycle on ownerCtx, coalesces a daily
// request behind it and lets the owner's own cycle finish after holdOwner.
// The daily drain then blocks in discovery until its context ends or the
// caller closes daily.block. Returns the owner's done channel.
func coalescedRun(t *testing.T, mgr *Manager, hourly, daily *fakeTier, ownerCtx context.Context, holdOwner time.Duration) <-chan error {
	t.Helper()
	hourly.block = make(chan struct{})
	daily.block = make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := mgr.runCycleInternal(ownerCtx, cycleSourceScheduler, nil, []string{"hourly"})
		done <- err
	}()
	select {
	case <-hourly.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("owner never entered the hourly tier")
	}
	if id, err := mgr.runCycleInternal(context.Background(), cycleSourceScheduler, nil, []string{"daily"}); err != nil || id != 0 {
		t.Fatalf("daily tick = (%d, %v), want coalesced", id, err)
	}
	time.Sleep(holdOwner)
	close(hourly.block)
	select {
	case <-daily.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("coalesced daily never started")
	}
	return done
}

func drainedRecord(t *testing.T, mgr *Manager) CycleRecord {
	t.Helper()
	for _, rec := range mgr.CyclePage(10).Cycles {
		if rec.Source == cycleSourceCoalesced {
			return rec
		}
	}
	t.Fatalf("no coalesced cycle in history: %+v", mgr.CyclePage(10).Cycles)
	return CycleRecord{}
}

// Default off: today's behaviour, the drain dies at the owner's deadline.
func TestCoalescedFreshTimeout_OffInheritsOwnerDeadline(t *testing.T) {
	hourly, daily := newFakeTier("hourly", nil), newFakeTier("daily", nil)
	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	mgr.CycleTimeout = 10 * time.Second

	ownerCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := coalescedRun(t, mgr, hourly, daily, ownerCtx, 0)
	if err := waitDone(t, done); err != nil {
		t.Fatalf("owner's own cycle: %v", err)
	}
	if rec := drainedRecord(t, mgr); rec.Status != "timed_out" {
		t.Fatalf("drained daily = %+v, want timed_out at the owner's deadline", rec)
	}
	if got := atomic.LoadInt32(&daily.scans); got != 0 {
		t.Fatalf("daily scans = %d, want 0", got)
	}
}

// On: the drain outlives the owner's deadline.
func TestCoalescedFreshTimeout_DrainOutlivesOwnerDeadline(t *testing.T) {
	hourly, daily := newFakeTier("hourly", nil), newFakeTier("daily", nil)
	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	mgr.CycleTimeout = 10 * time.Second
	mgr.CoalescedFreshTimeout = true

	ownerCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := coalescedRun(t, mgr, hourly, daily, ownerCtx, 0)
	<-ownerCtx.Done()
	time.Sleep(100 * time.Millisecond)
	close(daily.block)
	if err := waitDone(t, done); err != nil {
		t.Fatalf("owner's own cycle: %v", err)
	}
	if rec := drainedRecord(t, mgr); rec.Status != "completed" {
		t.Fatalf("drained daily = %+v, want completed past the owner's deadline", rec)
	}
	if got := atomic.LoadInt32(&daily.scans); got != 1 {
		t.Fatalf("daily scans = %d, want 1", got)
	}
}

// On: the drain's budget is cycle_timeout from the drain's own start. The owner
// ran longer than cycle_timeout, so a budget counted from the owner's start
// would already be spent.
func TestCoalescedFreshTimeout_BudgetCountsFromDrainStart(t *testing.T) {
	hourly, daily := newFakeTier("hourly", nil), newFakeTier("daily", nil)
	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	mgr.CycleTimeout = 300 * time.Millisecond
	mgr.CoalescedFreshTimeout = true

	ownerCtx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	done := coalescedRun(t, mgr, hourly, daily, ownerCtx, 400*time.Millisecond)
	if err := waitDone(t, done); err != nil {
		t.Fatalf("owner's own cycle: %v", err)
	}
	rec := drainedRecord(t, mgr)
	if rec.Status != "timed_out" {
		t.Fatalf("drained daily = %+v, want timed_out on its own budget", rec)
	}
	if ran := rec.FinishedAt.Sub(rec.StartedAt); ran < 250*time.Millisecond || ran > 5*time.Second {
		t.Fatalf("drained daily ran %v, want about cycle_timeout (300ms) from its own start", ran)
	}
}

// On: cancelling the owner (shutdown, a cancelled trigger) still stops the
// drain.
func TestCoalescedFreshTimeout_OwnerCancelStopsDrain(t *testing.T) {
	hourly, daily := newFakeTier("hourly", nil), newFakeTier("daily", nil)
	mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
	defer cleanup()
	mgr.CycleTimeout = 10 * time.Second
	mgr.CoalescedFreshTimeout = true

	ownerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := coalescedRun(t, mgr, hourly, daily, ownerCtx, 0)
	cancel()
	_ = waitDone(t, done)
	if rec := drainedRecord(t, mgr); rec.Status != "cancelled" {
		t.Fatalf("drained daily = %+v, want cancelled with the owner", rec)
	}
	if mgr.IsCycleRunning() {
		t.Fatal("claim not released")
	}
}

// On: an owner whose own cycle used up its budget still drains the queue,
// each drain on its own budget; off keeps the work pending for the next owner.
func TestCoalescedFreshTimeout_TimedOutOwnerStillDrains(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		hourly, daily := newFakeTier("hourly", nil), newFakeTier("daily", nil)
		mgr, cleanup := setupManagerWithTiers(t, hourly, daily)
		mgr.CycleTimeout = 10 * time.Second
		mgr.CoalescedFreshTimeout = fresh
		hourly.block = make(chan struct{}) // held until the owner times out

		ownerCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		done := make(chan error, 1)
		go func() {
			_, err := mgr.runCycleInternal(ownerCtx, cycleSourceScheduler, nil, []string{"hourly"})
			done <- err
		}()
		<-hourly.entered
		if _, err := mgr.runCycleInternal(context.Background(), cycleSourceScheduler, nil, []string{"daily"}); err != nil {
			t.Fatal(err)
		}
		if err := waitDone(t, done); err == nil {
			t.Fatalf("fresh=%v: timed-out owner reported success", fresh)
		}
		want := int32(0)
		if fresh {
			want = 1
		}
		if got := atomic.LoadInt32(&daily.scans); got != want {
			t.Fatalf("fresh=%v: daily scans = %d, want %d", fresh, got, want)
		}
		cancel()
		cleanup()
	}
}
