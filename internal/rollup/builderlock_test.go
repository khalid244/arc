package rollup

import (
	"context"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func newTestLock(t *testing.T, idOverride string) (*BuilderLock, storage.Backend) {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	return NewBuilderLock(backend, zerolog.Nop(), idOverride), backend
}

func TestBuilderLock_FreshAcquire(t *testing.T) {
	l, _ := newTestLock(t, "node-A")
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
}

func TestBuilderLock_SameInstanceReclaims(t *testing.T) {
	// Simulate restart of the same node: two BuilderLock instances using the
	// same backing storage and the same instance ID. Second Acquire must NOT
	// fail with "another builder holds the lock" — it must reclaim.
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	first := NewBuilderLock(backend, zerolog.Nop(), "node-A")
	if err := first.Acquire(context.Background()); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	// Don't release — simulating a crash.
	second := NewBuilderLock(backend, zerolog.Nop(), "node-A")
	if err := second.Acquire(context.Background()); err != nil {
		t.Fatalf("same-instance reclaim should succeed, got: %v", err)
	}
}

func TestBuilderLock_DifferentInstanceBlocked(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	first := NewBuilderLock(backend, zerolog.Nop(), "node-A")
	if err := first.Acquire(context.Background()); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	second := NewBuilderLock(backend, zerolog.Nop(), "node-B")
	if err := second.Acquire(context.Background()); err == nil {
		t.Fatal("different-instance acquire should fail while lock is fresh")
	}
}

func TestBuilderLock_StableInstanceIDDeterministic(t *testing.T) {
	a := stableInstanceID()
	b := stableInstanceID()
	if a != b {
		t.Errorf("stableInstanceID is not stable: %q vs %q", a, b)
	}
	if a == "" {
		t.Error("stableInstanceID returned empty")
	}
}
