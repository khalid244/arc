package main

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/rs/zerolog"
)

type fakeStager struct {
	pending atomic.Int64
	err     error
}

func (f *fakeStager) Pending() (int, error) { return int(f.pending.Load()), f.err }

// TestStagerShutdownHungCloseDoesNotSkipWAL models an ingest node's shutdown
// with the same registration helpers and priorities main.go uses. A stager
// whose Close hangs (an in-flight upload against a dead object store) must not
// consume the coordinator's budget: closeWithin gives up after the stager's
// share, wal-purge sees the staged file and RETAINS the WAL, and the `wal`
// component still runs. Skipping `wal` loses its newest entries.
func TestStagerShutdownHungCloseDoesNotSkipWAL(t *testing.T) {
	const timeout = 2 * time.Second
	coordinator := shutdown.New(timeout, zerolog.Nop())
	var (
		mu    sync.Mutex
		order []string
	)
	record := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, name)
	}
	stager := &fakeStager{}
	purged := false

	coordinator.Register("arrow-buffer", shutdownFunc(func() error {
		record("arrow-buffer")
		stager.pending.Store(1) // the final flush staged one file
		return nil
	}), shutdown.PriorityBuffer)

	release := make(chan struct{})
	defer close(release)
	registerStagerShutdown(coordinator, "fresh-stager", stagerCloseBudget(timeout), func() error {
		record("fresh-stager")
		<-release // never uploads within the budget
		return nil
	})

	coordinator.Register("wal-purge", shutdownFunc(func() error {
		record("wal-purge")
		if n, err := stagedFilesPending([]pendingStager{stager}); err != nil || n > 0 {
			return nil // retain
		}
		purged = true
		return nil
	}), 35)

	walClosed := false
	coordinator.Register("wal", shutdownFunc(func() error {
		record("wal")
		walClosed = true
		return nil
	}), shutdown.PriorityWAL)

	start := time.Now()
	err := coordinator.Shutdown()
	if !errors.Is(err, ingest.ErrStagedNotUploaded) {
		t.Fatalf("Shutdown() error = %v, want the stager's ErrStagedNotUploaded", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !walClosed {
		t.Fatalf("wal component skipped (order %v)", order)
	}
	if purged {
		t.Fatal("WAL purged while a staged file was not uploaded")
	}
	if want := []string{"arrow-buffer", "fresh-stager", "wal-purge", "wal"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("shutdown order = %v, want %v", order, want)
	}
	if elapsed := time.Since(start); elapsed >= timeout {
		t.Fatalf("shutdown took %s, the whole budget", elapsed)
	}
}

// A stager that drains lets wal-purge proceed.
func TestStagerShutdownDrainedAllowsPurge(t *testing.T) {
	coordinator := shutdown.New(2*time.Second, zerolog.Nop())
	stager := &fakeStager{}
	stager.pending.Store(3)
	registerStagerShutdown(coordinator, "late-stager", time.Second, func() error {
		stager.pending.Store(0)
		return nil
	})
	purged := false
	coordinator.Register("wal-purge", shutdownFunc(func() error {
		if n, err := stagedFilesPending([]pendingStager{stager}); err == nil && n == 0 {
			purged = true
		}
		return nil
	}), 35)
	if err := coordinator.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if !purged {
		t.Fatal("WAL not purged after the stager drained")
	}
}

func TestStagedFilesPendingErrorIsNotDrained(t *testing.T) {
	ok := &fakeStager{}
	broken := &fakeStager{err: fmt.Errorf("readdir: permission denied")}
	if _, err := stagedFilesPending([]pendingStager{ok, broken}); err == nil {
		t.Fatal("an unreadable stage directory must not count as drained")
	}
	ok.pending.Store(2)
	two := &fakeStager{}
	two.pending.Store(1)
	if n, err := stagedFilesPending([]pendingStager{ok, two}); err != nil || n != 3 {
		t.Fatalf("stagedFilesPending = %d, %v; want 3, nil", n, err)
	}
	if n, err := stagedFilesPending(nil); err != nil || n != 0 {
		t.Fatalf("no stagers: %d, %v; want 0, nil", n, err)
	}
}

// The HTTP drain, the buffer close and both stagers together must leave part
// of server.shutdown_timeout for wal-purge and the WAL close, at every value.
func TestShutdownBudgetsLeaveRoomForWAL(t *testing.T) {
	for _, secs := range []int{1, 5, 30, 60, 120, 280, 600} {
		timeout := time.Duration(secs) * time.Second
		http := httpDrainBudget(timeout)
		if http > maxHTTPDrain {
			t.Errorf("timeout %s: HTTP drain %s exceeds the %s cap", timeout, http, maxHTTPDrain)
		}
		stager := stagerCloseBudget(timeout)
		if stager <= 0 {
			t.Errorf("timeout %s: stager budget %s", timeout, stager)
		}
		used := http + timeout/2 + 2*stager
		if spare := timeout - used; spare < timeout/20 {
			t.Errorf("timeout %s: phases use %s, leaving %s for the WAL close", timeout, used, spare)
		}
		if d := stagerDrainBudget(stager); d >= stager || d <= 0 {
			t.Errorf("timeout %s: drain budget %s not inside close budget %s", timeout, d, stager)
		}
	}
	// Prod ingest: shutdown_timeout = 280 keeps the fork's 30s HTTP drain and
	// gives each stager more than its old fixed 30s final drain.
	if got := httpDrainBudget(280 * time.Second); got != 30*time.Second {
		t.Errorf("HTTP drain at 280s = %s, want 30s", got)
	}
	if got := stagerCloseBudget(280 * time.Second); got != 44*time.Second {
		t.Errorf("stager budget at 280s = %s, want 44s", got)
	}
}

func TestCloseWithinReturnsCloseResult(t *testing.T) {
	want := errors.New("boom")
	if err := closeWithin("x", time.Second, func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("closeWithin = %v, want %v", err, want)
	}
	if err := closeWithin("x", 0, func() error { return nil }); err != nil {
		t.Fatalf("zero budget closes synchronously: %v", err)
	}
}
