package compaction

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// SEM-5: the shutdown hook used to wait for an in-flight reorg run with no
// bound and without cancelling it, so a long drain held the whole shutdown
// sequence until SIGKILL. Shutdown must cancel the run, honour its own ctx,
// and leave a manifest state the next process recovers without loss or
// duplicates.

func shutdownRig(t *testing.T, days int) (*hookBackend, *Reorganizer, string, string) {
	t.Helper()
	t.Setenv("TZ", "UTC")
	b, tmp := newHookBackend(t)
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	if err := os.MkdirAll(lateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	makeBucketManyDays(t, db, lateDir, "events_late_20260520_030000_0.parquet", days)
	db.Close()
	scratch := filepath.Join(tmp, "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	r := newReorg(b, scratch, zerolog.New(io.Discard))
	r.DownloadWorkers = 1
	return b, r, lateDir, filepath.Join(tmp, "posthog", "events")
}

// blockUploadsAfter lets the first n reorg output uploads through and blocks
// the next one until its context ends (a slow PUT that only a cancellation
// stops). started is closed when the blocking upload begins.
func blockUploadsAfter(b *hookBackend, n int) (started chan struct{}) {
	started = make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	seen := 0
	b.set(func(b *hookBackend) {
		b.writeRHook = func(ctx context.Context, path string) error {
			if !strings.Contains(path, reorgFileMarker) {
				return nil
			}
			mu.Lock()
			seen++
			k := seen
			mu.Unlock()
			if k <= n {
				return nil
			}
			once.Do(func() { close(started) })
			<-ctx.Done()
			return ctx.Err()
		}
	})
	return started
}

func TestReorg_ShutdownCancelsInFlightRunAndStateIsRecoverable(t *testing.T) {
	const days = 5
	b, r, lateDir, eventsDir := shutdownRig(t, days)
	started := blockUploadsAfter(b, 2)

	runCtx, cancelRun := context.WithTimeout(context.Background(), time.Hour)
	defer cancelRun()
	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(runCtx) }()

	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("the run never reached the blocking upload")
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	begin := time.Now()
	if err := r.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v (the in-flight run was not cancelled)", err)
	}
	if took := time.Since(begin); took > 5*time.Second {
		t.Fatalf("Shutdown took %s; it must cancel the run, not wait it out", took)
	}
	select {
	case <-runErr:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Shutdown")
	}
	if r.IsRunning() {
		t.Fatal("the run flag must be released after Shutdown")
	}

	// Recoverable state: sources untouched, one pending manifest that records
	// the writer's deadline.
	if n := countParquetFlat(t, lateDir); n != 1 {
		t.Fatalf("a cancelled chunk must keep its sources, got %d", n)
	}
	keys, err := r.ManifestManager.List(context.Background())
	if err != nil || len(keys) != 1 {
		t.Fatalf("want one pending manifest after the cancelled upload, got %v (%v)", keys, err)
	}
	man, err := r.ManifestManager.Read(context.Background(), keys[0])
	if err != nil {
		t.Fatal(err)
	}
	if man.Status != ReorgStatusPending || man.Deadline.IsZero() {
		t.Fatalf("manifest status=%q deadline=%v, want pending with the writer's deadline", man.Status, man.Deadline)
	}

	// A stopped Reorganizer does not start new runs.
	b.set(func(b *hookBackend) { b.writeRHook = nil })
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run after Shutdown: %v", err)
	}
	if left, _ := r.ManifestManager.List(context.Background()); len(left) != 1 || left[0] != keys[0] {
		t.Fatalf("Run after Shutdown must be a no-op, manifests now %v", left)
	}

	// The next process recovers once the dead writer's deadline has passed.
	rewriteManifestFields(t, b, keys[0], func(m map[string]any) {
		shiftTimeField(t, m, "deadline", -2*time.Hour)
		shiftTimeField(t, m, "updated_at", -2*time.Hour)
	})
	next := newReorg(b, r.TempDirectory, zerolog.New(io.Discard))
	if err := next.Run(context.Background()); err != nil {
		t.Fatalf("next process run: %v", err)
	}
	assertNoDuplicateDays(t, eventsDir, days)
	if n := countParquetFlat(t, lateDir); n != 0 {
		t.Errorf("the next process must drain the sources, %d remain", n)
	}
}

// Shutdown is bounded by its own context even when the run does not stop
// (here: a storage call that ignores cancellation).
func TestReorg_ShutdownHonoursItsContext(t *testing.T) {
	b, r, _, _ := shutdownRig(t, 3)
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	b.set(func(b *hookBackend) {
		b.writeRHook = func(_ context.Context, path string) error {
			if !strings.Contains(path, reorgFileMarker) {
				return nil
			}
			once.Do(func() { close(started) })
			<-release // ignores ctx on purpose
			return errors.New("released")
		}
	})
	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(context.Background()) }()
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("the run never reached the upload")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	begin := time.Now()
	err := r.Shutdown(ctx)
	took := time.Since(begin)
	close(release)
	<-runErr
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want the shutdown context's deadline error", err)
	}
	if took > 3*time.Second {
		t.Fatalf("Shutdown blocked %s past its 200ms context", took)
	}
}

func TestReorg_ShutdownWithoutRunIsImmediateAndStopsFutureRuns(t *testing.T) {
	_, r, lateDir, _ := shutdownRig(t, 2)
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown with nothing running: %v", err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run after Shutdown: %v", err)
	}
	if n := countParquetFlat(t, lateDir); n != 1 {
		t.Fatalf("a stopped reorganizer must not drain, %d sources left (want 1)", n)
	}
}
