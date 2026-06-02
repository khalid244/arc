package compaction

import (
	"context"
	"io"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestReorg_RunGuard_PreventsOverlap verifies the overlap guard that lets the
// cron and the manual API trigger coexist safely: while one drain holds the run
// flag, a second Run() must no-op (return nil) WITHOUT clearing the in-flight
// run's flag. Two concurrent drains would race on pre-manifest source files and
// emit duplicate target files.
func TestReorg_RunGuard_PreventsOverlap(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, err := storage.NewLocalBackend(tmp, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	r := newReorg(backend, tmp, logger)

	// Simulate an in-flight drain by taking the flag (as Run's CAS would).
	if !r.running.CompareAndSwap(false, true) {
		t.Fatal("run flag should start cleared")
	}
	if !r.IsRunning() {
		t.Fatal("IsRunning must report true while the flag is held")
	}

	// A concurrent Run() must lose the CAS and no-op — returning nil and, crucially,
	// NOT clearing the holder's flag via a stray defer.
	if err := r.Run(ctx); err != nil {
		t.Fatalf("guarded Run should no-op with nil, got %v", err)
	}
	if !r.IsRunning() {
		t.Error("guarded Run cleared another run's flag (defer ran on the losing path)")
	}

	// Release and confirm a normal Run() acquires + releases cleanly.
	r.running.Store(false)
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run after release: %v", err)
	}
	if r.IsRunning() {
		t.Error("flag must be cleared after a completed Run")
	}
}
