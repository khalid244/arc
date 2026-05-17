package tiered

import (
	"context"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestSpecStore_PutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	store := NewSpecStore(backend)
	s := Spec{
		Table:      "default.events",
		TZ:         "Asia/Riyadh",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"val_a", "val_b"}, EffectiveCard: 2},
		},
		BuilderVersion:    "v1",
		CoverageThreshold: 0.99,
	}
	if err := store.Put(ctx, "default.events", s); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := store.Get(ctx, "default.events")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Table != s.Table || got.TZ != s.TZ {
		t.Errorf("roundtrip mismatch: %+v vs %+v", got, s)
	}
	if len(got.Dims) != 1 {
		t.Errorf("Dims = %v", got.Dims)
	}
	if got.Dims["dim_a"].EffectiveCard != 2 {
		t.Errorf("EffectiveCard = %d, want 2", got.Dims["dim_a"].EffectiveCard)
	}
}

func TestSpecStore_GetMissingReturnsError(t *testing.T) {
	ctx := context.Background()
	backend, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	store := NewSpecStore(backend)
	_, err := store.Get(ctx, "no.such.table")
	if err == nil {
		t.Error("Get of nonexistent spec should error")
	}
}
