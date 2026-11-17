package compaction

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Review follow-ups for compaction.requeue_failed_tiers: what counts as
// settled, Stats visibility, the compactor gate in the drain loop, and the
// prod setting hourly_during_daily=true.

func seed(t *testing.T, r *rqRig, path string) {
	t.Helper()
	if err := r.backend.Backend.Write(context.Background(), path, []byte("x")); err != nil {
		t.Fatal(err)
	}
}

// failDailyOnce queues a daily retry: a daily tick fails at the database list.
func (r *rqRig) failDailyOnce(t *testing.T) {
	t.Helper()
	r.backend.fail.Store(true)
	_ = r.tick("daily")
	r.backend.fail.Store(false)
	if _, ok := r.requeued()["daily"]; !ok {
		t.Fatal("failed daily tick not requeued")
	}
}

// A tier that started a batch is settled even though discovery failed for
// another measurement (prod cycles 408 and 426): it is not retried, and its
// pending retry is cleared.
func TestRequeue_StartedBatchSettlesDespiteDiscoveryErrors(t *testing.T) {
	r := newRQRig(t, true, time.Hour)
	seed(t, r, "db/mem/2026/11/08/03/seed.parquet")
	r.failDailyOnce(t)

	r.dailyFindFn = func(_ context.Context, measurement string) ([]Candidate, error) {
		if measurement == "mem" {
			return nil, errors.New("circuit breaker is open")
		}
		return []Candidate{ilCandidate("daily", "db/cpu/2026/11/01", 2)}, nil
	}
	if err := r.tick("daily"); err == nil {
		t.Fatal("cycle with a discovery error and a failed batch reported success")
	}
	if r.batches.Load() != 1 {
		t.Fatalf("batches = %d, want 1", r.batches.Load())
	}
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v, want empty: daily started a batch", got)
	}
}

// A database whose measurements could not be listed leaves the tier
// unsettled, even when the tier's own discovery saw no error.
func TestRequeue_MeasurementListFailureIsNotSettled(t *testing.T) {
	r := newRQRig(t, true, time.Hour)
	seed(t, r, "db2/cpu/2026/11/08/03/seed.parquet")
	prefix := "db2/"
	r.backend.failPrefix.Store(&prefix)
	if err := r.tick("daily"); err == nil {
		t.Fatal("cycle with a failed measurement listing reported success")
	}
	if _, ok := r.requeued()["daily"]; !ok {
		t.Fatalf("requeued = %v, want daily: db2 was never scanned", r.requeued())
	}
}

// An incomplete spoke-namespace expansion is not a clean scan, so it does not
// clear a pending retry.
func TestRequeue_IncompleteNamespacesDoNotSettle(t *testing.T) {
	r := newRQRig(t, true, time.Hour)
	r.failDailyOnce(t)
	r.m.SetNamespaceExpander(func(context.Context) (map[string]struct{}, error) {
		return nil, errors.New("namespace lookup failed")
	})
	if err := r.tick("daily"); err != nil {
		t.Fatalf("daily tick: %v", err)
	}
	if _, ok := r.requeued()["daily"]; !ok {
		t.Fatal("a scan with incomplete namespaces cleared the daily retry")
	}
}

// Scoped cycles neither queue a retry nor clear one, whatever their source.
func TestRequeue_ScopedCyclesDoNotTakePart(t *testing.T) {
	r := newRQRig(t, true, time.Hour)
	r.failDailyOnce(t)
	if _, err := r.m.RunCompactionCycleForDatabase(context.Background(), "db", []string{"daily"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.requeued()["daily"]; !ok {
		t.Fatal("a database-scoped run cleared the unscoped daily retry")
	}

	r2 := newRQRig(t, true, time.Hour)
	r2.dailyFindErr.Store(true)
	if _, err := r2.m.runCycleInternal(context.Background(), cycleSourceScheduler, []string{"db"}, []string{"daily"}); err == nil {
		t.Fatal("scoped cycle with failing discovery reported success")
	}
	if got := r2.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v after a scoped cycle", got)
	}
}

// A paused cycle is not a failure to retry.
func TestRequeue_PausedCycleIsNotRequeued(t *testing.T) {
	r := newRQRig(t, true, time.Hour)
	r.m.SetPauseGate(func() bool { return true })
	if err := r.tick("daily"); !errors.Is(err, ErrCompactionPaused) {
		t.Fatalf("tick under pause: %v, want ErrCompactionPaused", err)
	}
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v after a paused cycle", got)
	}
}

// Disabled and unknown tiers are never queued.
func TestRequeue_OnlyEnabledTiers(t *testing.T) {
	r := newRQRig(t, true, time.Hour)
	weekly := newFakeTier("weekly", nil)
	weekly.enabled = false
	r.m.Tiers = append(r.m.Tiers, weekly)
	r.backend.fail.Store(true)
	_ = r.tick("daily", "weekly", "monthly")
	got := r.requeued()
	if _, ok := got["daily"]; !ok || len(got) != 1 {
		t.Fatalf("requeued = %v, want only daily", got)
	}
}

// A promoted retry that is cancelled is dropped, not left behind.
func TestRequeue_CancelledRetryIsDropped(t *testing.T) {
	r := newRQRig(t, true, 50*time.Millisecond)
	r.failDailyOnce(t)
	time.Sleep(80 * time.Millisecond)

	entered := make(chan struct{}, 1)
	r.dailyFindFn = func(ctx context.Context, _ string) ([]Candidate, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.m.runCycleInternal(ctx, cycleSourceScheduler, nil, []string{"hourly"})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("promoted daily retry never ran")
	}
	if st := r.m.requeueSnapshot()["daily"]; st.Waiting {
		t.Fatalf("running retry reported as waiting: %+v", st)
	}
	cancel()
	_ = waitDone(t, done)
	if got := r.requeued(); len(got) != 0 {
		t.Fatalf("requeued = %v after the retry was cancelled", got)
	}
}

// Stats reports each retry: when, how many consecutive failures, why.
func TestRequeue_StatsReportsRetries(t *testing.T) {
	r := newRQRig(t, true, 50*time.Millisecond)
	snapshot := func() map[string]RequeuedTier {
		t.Helper()
		got, ok := r.m.Stats()["requeued_tiers"].(map[string]RequeuedTier)
		if !ok {
			t.Fatalf("Stats requeued_tiers = %T", r.m.Stats()["requeued_tiers"])
		}
		return got
	}
	if raw, _ := json.Marshal(r.m.Stats()["requeued_tiers"]); string(raw) != "{}" {
		t.Fatalf("requeued_tiers JSON = %s, want {}", raw)
	}

	r.backend.fail.Store(true)
	_ = r.tick("daily")
	st := snapshot()["daily"]
	if st.Attempts != 1 || !st.Waiting || !strings.Contains(st.LastError, "circuit breaker is open") || time.Until(st.RetryAfter) <= 0 {
		t.Fatalf("daily retry = %+v, want attempt 1, waiting, the error, a future retry_after", st)
	}

	time.Sleep(80 * time.Millisecond)
	_ = r.tick("hourly") // storage still down: the promoted retry fails again
	if st := snapshot()["daily"]; st.Attempts != 2 || !st.Waiting {
		t.Fatalf("daily retry = %+v, want attempt 2, waiting", st)
	}

	r.backend.fail.Store(false)
	time.Sleep(80 * time.Millisecond)
	if err := r.tick("hourly"); err != nil {
		t.Fatal(err)
	}
	if got := snapshot(); len(got) != 0 {
		t.Fatalf("requeued_tiers = %v after the retry succeeded", got)
	}
}

// The drain loop re-checks the compactor gate: a node that lost the lease
// keeps coalesced work and retries pending instead of running them. With both
// flags off it drains as before.
func TestRequeue_DrainRechecksCompactorGate(t *testing.T) {
	t.Run("flags on", func(t *testing.T) {
		r := newRQRig(t, true, 50*time.Millisecond)
		var leased atomic.Bool
		leased.Store(true)
		r.m.SetCompactGate(leased.Load)

		r.hourlyBlock = make(chan struct{})
		done := make(chan error, 1)
		go func() { done <- r.tick("hourly") }()
		<-r.hourlyEntered
		_ = r.tick("daily")
		leased.Store(false)
		close(r.hourlyBlock)
		_ = waitDone(t, done)
		r.hourlyBlock = nil
		if got := r.dailyFinds.Load(); got != 0 {
			t.Fatalf("daily ran %d times after the lease was lost", got)
		}
		if got := r.pending(); !slices.Equal(got, []string{"daily"}) {
			t.Fatalf("pending = %v, want [daily] kept", got)
		}
		if r.m.IsCycleRunning() {
			t.Fatal("claim not released")
		}
		leased.Store(true)
		if err := r.tick("hourly"); err != nil {
			t.Fatal(err)
		}
		if got := r.dailyFinds.Load(); got != 1 {
			t.Fatalf("daily ran %d times after the lease came back, want 1", got)
		}

		// A due retry is not promoted without the lease either.
		r.failDailyOnce(t)
		time.Sleep(80 * time.Millisecond)
		leased.Store(false)
		_ = r.tick("hourly")
		if st := r.m.requeueSnapshot()["daily"]; !st.Waiting || st.Attempts != 1 {
			t.Fatalf("daily retry = %+v, want still waiting without the lease", st)
		}
		if got := r.dailyFinds.Load(); got != 1 {
			t.Fatalf("daily ran %d times without the lease", got)
		}
	})

	t.Run("flags off", func(t *testing.T) {
		r := newRQRig(t, false, time.Hour)
		r.m.SetCompactGate(func() bool { return false })
		r.hourlyBlock = make(chan struct{})
		done := make(chan error, 1)
		go func() { done <- r.tick("hourly") }()
		<-r.hourlyEntered
		_ = r.tick("daily")
		close(r.hourlyBlock)
		_ = waitDone(t, done)
		if got := r.dailyFinds.Load(); got != 1 {
			t.Fatalf("daily ran %d times with the flags off, want 1 (today's behaviour)", got)
		}
	})
}

// hourly_during_daily=true, the production setting.
func TestRequeue_WithHourlyDuringDaily(t *testing.T) {
	t.Run("failed drained daily is retried", func(t *testing.T) {
		r := newRQRig(t, true, 50*time.Millisecond)
		r.m.HourlyDuringDaily = true
		r.failDrainedDaily(t)
		r.backend.fail.Store(false)
		time.Sleep(80 * time.Millisecond)
		if err := r.tick("hourly"); err != nil {
			t.Fatal(err)
		}
		if got := r.dailyFinds.Load(); got != 1 {
			t.Fatalf("daily ran %d times, want 1", got)
		}
		if got := r.requeued(); len(got) != 0 {
			t.Fatalf("requeued = %v after the retry", got)
		}
	})

	t.Run("failed interleaved hourly pass is not requeued", func(t *testing.T) {
		r := newRQRig(t, true, time.Hour)
		r.m.HourlyDuringDaily = true
		r.dailyFindFn = func(_ context.Context, measurement string) ([]Candidate, error) {
			if measurement != "cpu" {
				return nil, nil
			}
			return []Candidate{ilCandidate("daily", "db/cpu/2026/11/01", 20)}, nil // two batches
		}
		first, release := make(chan struct{}), make(chan struct{})
		var n atomic.Int32
		r.m.compactBatchForTest = func(context.Context, Candidate) error {
			if n.Add(1) == 1 {
				close(first)
				<-release
			}
			return nil
		}
		done := make(chan error, 1)
		go func() { done <- r.tick("daily") }()
		waitClosed(t, first, "first daily batch")
		if id, err := r.m.runCycleInternal(context.Background(), cycleSourceScheduler, nil, []string{"hourly"}); err != nil || id != 0 {
			t.Fatalf("hourly tick = (%d, %v), want coalesced", id, err)
		}
		r.backend.fail.Store(true) // the interleaved pass fails at its database list
		close(release)
		if err := waitDone(t, done); err != nil {
			t.Fatalf("daily cycle: %v", err)
		}
		if n.Load() != 2 {
			t.Fatalf("daily batches = %d, want 2", n.Load())
		}
		var inter []CycleRecord
		for _, rec := range r.m.CyclePage(10).Cycles {
			if rec.Source == cycleSourceInterleaved {
				inter = append(inter, rec)
			}
		}
		if len(inter) != 1 || inter[0].Status != "failed" {
			t.Fatalf("interleaved records = %+v, want one failed pass", inter)
		}
		if got := r.requeued(); len(got) != 0 {
			t.Fatalf("requeued = %v, want empty: daily settled and interleaved passes are not requeued", got)
		}
	})
}

// The scheduler reports the owner's own outcome: a cycle that failed on its
// own is "failed" even when the deadline passes while it drains coalesced work.
func TestScheduler_OwnFailureNotReportedAsInterrupted(t *testing.T) {
	r := newRQRig(t, false, time.Hour)
	var buf bytes.Buffer
	s, err := NewScheduler(&SchedulerConfig{
		Manager: r.m, Schedule: "5 * * * *", TierNames: []string{"daily"},
		CycleTimeout: 300 * time.Millisecond, Logger: zerolog.New(&buf),
	})
	if err != nil {
		t.Fatal(err)
	}
	entered, proceed := make(chan struct{}), make(chan struct{})
	r.dailyFindFn = func(context.Context, string) ([]Candidate, error) {
		close(entered)
		<-proceed
		return nil, errors.New("discovery failed")
	}
	r.hourlyBlock = make(chan struct{}) // the drained hourly runs until the deadline

	done := make(chan struct{})
	go func() { s.runCompaction(); close(done) }()
	waitClosed(t, entered, "daily discovery")
	if id, err := r.m.runCycleInternal(context.Background(), cycleSourceScheduler, nil, []string{"hourly"}); err != nil || id != 0 {
		t.Fatalf("hourly tick = (%d, %v), want coalesced", id, err)
	}
	close(proceed)
	waitClosed(t, done, "scheduled run")
	out := buf.String()
	if !strings.Contains(out, "Scheduled compaction failed") || strings.Contains(out, "Scheduled compaction interrupted") {
		t.Fatalf("scheduler log = %s, want failed, not interrupted", out)
	}
}
