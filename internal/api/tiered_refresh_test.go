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

	if err := specStore.Put(ctx, "default.events", tiered.Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]tiered.DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"x", "y"}, EffectiveCard: 2},
		},
	}); err != nil {
		t.Fatal(err)
	}

	h := &QueryHandler{}
	r := &TieredRefresher{
		Handler:   h,
		SpecStore: specStore,
		FilesFor:  func(table string) tiered.FileIndex { return &tiered.MemoryFileIndex{} },
		Tables:    []string{"default.events"},
		Logger:    zerolog.Nop(),
	}
	r.refresh(ctx)
	deps := h.tieredDepsFor("default.events")
	if deps == nil {
		t.Fatal("expected deps installed")
	}
	if deps.Spec == nil || deps.Spec.Table != "default.events" {
		t.Errorf("Spec wrong: %+v", deps.Spec)
	}
	if deps.Files == nil {
		t.Error("Files should be non-nil")
	}
}

func TestTieredRefresher_SkipsTableWithMissingSpec(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())
	r := &TieredRefresher{
		Handler:   &QueryHandler{},
		SpecStore: tiered.NewSpecStore(backend),
		FilesFor:  func(table string) tiered.FileIndex { return &tiered.MemoryFileIndex{} },
		Tables:    []string{"never.exists"},
		Logger:    zerolog.Nop(),
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

	specStore.Put(ctx, "t", tiered.Spec{Table: "t", TZ: "UTC", TimeColumn: "v1"})

	h := &QueryHandler{}
	r := &TieredRefresher{
		Handler:   h,
		SpecStore: specStore,
		FilesFor:  func(table string) tiered.FileIndex { return &tiered.MemoryFileIndex{} },
		Tables:    []string{"t"},
		Logger:    zerolog.Nop(),
	}
	r.refresh(ctx)
	if got := h.tieredDepsFor("t").Spec.TimeColumn; got != "v1" {
		t.Errorf("first refresh TimeColumn = %q, want v1", got)
	}

	specStore.Put(ctx, "t", tiered.Spec{Table: "t", TZ: "UTC", TimeColumn: "v2"})
	r.refresh(ctx)
	if got := h.tieredDepsFor("t").Spec.TimeColumn; got != "v2" {
		t.Errorf("second refresh TimeColumn = %q, want v2", got)
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
