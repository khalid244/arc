package api

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/rollup/tiered"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestTieredRefresher_RefreshInstallsDeps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	specStore := tiered.NewSpecStore(backend)
	manStore := tiered.NewManifestStore(backend)

	if err := specStore.Put(ctx, "default.events", tiered.Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]tiered.DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"x", "y"}, EffectiveCard: 2},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manStore.Put(ctx, "default.events", &tiered.Manifest{
		Table: "default.events", Generation: 1,
	}); err != nil {
		t.Fatal(err)
	}

	h := &QueryHandler{}
	r := &TieredRefresher{
		Handler: h, SpecStore: specStore, ManifestStore: manStore,
		Tables: []string{"default.events"},
		Logger: zerolog.Nop(),
	}
	r.refresh(ctx)
	if deps := h.tieredDepsFor("default.events"); deps == nil {
		t.Fatal("expected deps installed")
	} else {
		if deps.Spec == nil || deps.Spec.Table != "default.events" {
			t.Errorf("Spec wrong: %+v", deps.Spec)
		}
		if deps.Manifest == nil || deps.Manifest.Generation != 1 {
			t.Errorf("Manifest wrong: %+v", deps.Manifest)
		}
	}
}

func TestTieredRefresher_SkipsTableWithMissingSpec(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())
	r := &TieredRefresher{
		Handler:       &QueryHandler{},
		SpecStore:     tiered.NewSpecStore(backend),
		ManifestStore: tiered.NewManifestStore(backend),
		Tables:        []string{"never.exists"},
		Logger:        zerolog.Nop(),
	}
	r.refresh(ctx)
	if deps := r.Handler.tieredDepsFor("never.exists"); deps != nil {
		t.Error("expected no deps for missing spec table")
	}
}

func TestTieredRefresher_UpdatesAcrossRefreshes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())
	specStore := tiered.NewSpecStore(backend)
	manStore := tiered.NewManifestStore(backend)

	specStore.Put(ctx, "t", tiered.Spec{Table: "t", TZ: "UTC"})
	manStore.Put(ctx, "t", &tiered.Manifest{Table: "t", Generation: 1})

	h := &QueryHandler{}
	r := &TieredRefresher{
		Handler: h, SpecStore: specStore, ManifestStore: manStore,
		Tables: []string{"t"}, Logger: zerolog.Nop(),
	}
	r.refresh(ctx)
	if got := h.tieredDepsFor("t").Manifest.Generation; got != 1 {
		t.Errorf("first refresh Generation = %d, want 1", got)
	}

	manStore.Put(ctx, "t", &tiered.Manifest{Table: "t", Generation: 2})
	r.refresh(ctx)
	if got := h.tieredDepsFor("t").Manifest.Generation; got != 2 {
		t.Errorf("second refresh Generation = %d, want 2", got)
	}
}

func TestTieredRefresher_StartStopWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &TieredRefresher{
		Handler:  &QueryHandler{},
		Interval: 10 * time.Millisecond,
		Logger:   zerolog.Nop(),
		Tables:   []string{},
	}
	r.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)
}
