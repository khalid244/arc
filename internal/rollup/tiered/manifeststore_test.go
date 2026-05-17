package tiered

import (
	"context"
	"errors"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestManifestStore_PutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	store := NewManifestStore(backend)
	m := &Manifest{Table: "t", Generation: 1}
	if err := store.Put(ctx, "t", m); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, "t")
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 1 {
		t.Errorf("Generation = %d, want 1", got.Generation)
	}
}

func TestManifestStore_StalePutReturnsConflict(t *testing.T) {
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	store := NewManifestStore(backend)
	if err := store.Put(ctx, "t", &Manifest{Table: "t", Generation: 5}); err != nil {
		t.Fatal(err)
	}
	stale := &Manifest{Table: "t", Generation: 5} // same generation = stale
	err := store.Put(ctx, "t", stale)
	if !errors.Is(err, ErrManifestStale) {
		t.Errorf("expected ErrManifestStale, got %v", err)
	}
	older := &Manifest{Table: "t", Generation: 4}
	err = store.Put(ctx, "t", older)
	if !errors.Is(err, ErrManifestStale) {
		t.Errorf("expected ErrManifestStale for older Gen, got %v", err)
	}
}

func TestManifestStore_FreshGenerationAdvances(t *testing.T) {
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	store := NewManifestStore(backend)
	for gen := int64(1); gen <= 5; gen++ {
		if err := store.Put(ctx, "t", &Manifest{Table: "t", Generation: gen}); err != nil {
			t.Fatalf("gen %d: %v", gen, err)
		}
	}
	got, _ := store.Get(ctx, "t")
	if got.Generation != 5 {
		t.Errorf("final Generation = %d, want 5", got.Generation)
	}
}

func TestManifestStore_FirstPutSucceedsWithoutExisting(t *testing.T) {
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	store := NewManifestStore(backend)
	// First put with any Generation must succeed (no existing manifest).
	if err := store.Put(ctx, "t", &Manifest{Table: "t", Generation: 1}); err != nil {
		t.Errorf("first put should succeed: %v", err)
	}
}
