package rollup

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func newManifestTestBackend(t *testing.T) storage.Backend {
	t.Helper()
	dir := t.TempDir()
	b, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("local backend: %v", err)
	}
	return b
}

func TestManifestStore_RoundTrip(t *testing.T) {
	backend := newManifestTestBackend(t)
	store := NewManifestStore(backend, zerolog.Nop())
	ctx := context.Background()

	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	m := WindowManifest{
		RollupName:  "events__1h",
		WindowStart: start,
		WindowEnd:   end,
		OutputKey:   "main/events__1h/dt=2026-05-10/window_20260510-120000-130000.parquet",
		CreatedAt:   time.Now().UTC(),
	}

	if err := store.Write(ctx, m); err != nil {
		t.Fatalf("Write: %v", err)
	}

	keys, err := store.List(ctx, "events__1h")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("expected 1 manifest key, got %d", len(keys))
	}

	got, err := store.Read(ctx, keys[0])
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.RollupName != m.RollupName {
		t.Errorf("RollupName: got %q want %q", got.RollupName, m.RollupName)
	}
	if !got.WindowStart.Equal(m.WindowStart) {
		t.Errorf("WindowStart: got %v want %v", got.WindowStart, m.WindowStart)
	}
	if !got.WindowEnd.Equal(m.WindowEnd) {
		t.Errorf("WindowEnd: got %v want %v", got.WindowEnd, m.WindowEnd)
	}
	if got.OutputKey != m.OutputKey {
		t.Errorf("OutputKey: got %q want %q", got.OutputKey, m.OutputKey)
	}

	if err := store.Delete(ctx, "events__1h", start, end); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	keys, err = store.List(ctx, "events__1h")
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected 0 manifest keys after delete, got %d", len(keys))
	}
}

func TestManifestStore_DeleteNotFoundIsOK(t *testing.T) {
	backend := newManifestTestBackend(t)
	store := NewManifestStore(backend, zerolog.Nop())
	ctx := context.Background()

	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	// Deleting a non-existent manifest should not error.
	if err := store.Delete(ctx, "events__1h", start, end); err != nil {
		t.Errorf("Delete non-existent: %v", err)
	}
}

// fakeWMStore implements WMReadWriter for tests.
type fakeWMStore struct {
	watermarks map[string]Watermark
}

func newFakeWMStore() *fakeWMStore {
	return &fakeWMStore{watermarks: make(map[string]Watermark)}
}

func (f *fakeWMStore) Get(_ context.Context, rollupName string) (Watermark, error) {
	return f.watermarks[rollupName], nil
}

func (f *fakeWMStore) Put(_ context.Context, w Watermark) error {
	f.watermarks[w.Rollup] = w
	return nil
}

func TestRecover_OutputExists_AdvancesWatermark(t *testing.T) {
	backend := newManifestTestBackend(t)
	store := NewManifestStore(backend, zerolog.Nop())
	wmStore := newFakeWMStore()
	ctx := context.Background()
	logger := zerolog.Nop()

	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	outputKey := "main/events__1h/dt=2026-05-10/window_20260510-120000-130000.parquet"

	// Write the manifest (simulating a crash after manifest write but before watermark advance).
	m := WindowManifest{
		RollupName:  "events__1h",
		WindowStart: start,
		WindowEnd:   end,
		OutputKey:   outputKey,
		CreatedAt:   time.Now().UTC(),
	}
	if err := store.Write(ctx, m); err != nil {
		t.Fatalf("Write manifest: %v", err)
	}

	// Simulate the parquet being present (build completed before crash).
	if err := backend.Write(ctx, outputKey, []byte("fake parquet")); err != nil {
		t.Fatalf("Write parquet: %v", err)
	}

	if err := Recover(ctx, "events__1h", store, wmStore, logger); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// Watermark should have advanced to window end.
	wm := wmStore.watermarks["events__1h"]
	if !wm.Watermark.Equal(end) {
		t.Errorf("watermark: got %v want %v", wm.Watermark, end)
	}

	// Manifest should be gone.
	keys, err := store.List(ctx, "events__1h")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected 0 manifests after recovery, got %d", len(keys))
	}
}

func TestRecover_OutputMissing_DeletesManifest(t *testing.T) {
	backend := newManifestTestBackend(t)
	store := NewManifestStore(backend, zerolog.Nop())
	wmStore := newFakeWMStore()
	ctx := context.Background()
	logger := zerolog.Nop()

	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	m := WindowManifest{
		RollupName:  "events__1h",
		WindowStart: start,
		WindowEnd:   end,
		OutputKey:   "main/events__1h/dt=2026-05-10/window_20260510-120000-130000.parquet",
		CreatedAt:   time.Now().UTC(),
	}
	if err := store.Write(ctx, m); err != nil {
		t.Fatalf("Write manifest: %v", err)
	}

	// Output parquet does NOT exist (crash happened before upload completed).
	if err := Recover(ctx, "events__1h", store, wmStore, logger); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// Watermark should NOT have advanced.
	wm := wmStore.watermarks["events__1h"]
	if !wm.Watermark.IsZero() {
		t.Errorf("watermark should be zero, got %v", wm.Watermark)
	}

	// Manifest should be gone.
	keys, err := store.List(ctx, "events__1h")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected 0 manifests after recovery, got %d", len(keys))
	}
}

func TestRecover_Idempotent(t *testing.T) {
	backend := newManifestTestBackend(t)
	store := NewManifestStore(backend, zerolog.Nop())
	wmStore := newFakeWMStore()
	ctx := context.Background()
	logger := zerolog.Nop()

	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	outputKey := "main/events__1h/dt=2026-05-10/window_20260510-120000-130000.parquet"

	m := WindowManifest{
		RollupName:  "events__1h",
		WindowStart: start,
		WindowEnd:   end,
		OutputKey:   outputKey,
		CreatedAt:   time.Now().UTC(),
	}
	if err := store.Write(ctx, m); err != nil {
		t.Fatalf("Write manifest: %v", err)
	}
	if err := backend.Write(ctx, outputKey, []byte("fake parquet")); err != nil {
		t.Fatalf("Write parquet: %v", err)
	}

	// First recovery.
	if err := Recover(ctx, "events__1h", store, wmStore, logger); err != nil {
		t.Fatalf("first Recover: %v", err)
	}
	wm1 := wmStore.watermarks["events__1h"]

	// Second recovery (no manifests left, no-op).
	if err := Recover(ctx, "events__1h", store, wmStore, logger); err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	wm2 := wmStore.watermarks["events__1h"]

	if !wm1.Watermark.Equal(wm2.Watermark) {
		t.Errorf("idempotency: watermark changed on second recover: %v → %v", wm1.Watermark, wm2.Watermark)
	}
}

func TestRecover_WatermarkNotRegressedIfAlreadyAdvanced(t *testing.T) {
	backend := newManifestTestBackend(t)
	store := NewManifestStore(backend, zerolog.Nop())
	wmStore := newFakeWMStore()
	ctx := context.Background()
	logger := zerolog.Nop()

	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	outputKey := "main/events__1h/dt=2026-05-10/window_20260510-120000-130000.parquet"

	// Pre-populate watermark further ahead than the manifest's window_end.
	futureEnd := end.Add(2 * time.Hour)
	wmStore.watermarks["events__1h"] = Watermark{
		Rollup:    "events__1h",
		Watermark: futureEnd,
	}

	m := WindowManifest{
		RollupName:  "events__1h",
		WindowStart: start,
		WindowEnd:   end,
		OutputKey:   outputKey,
		CreatedAt:   time.Now().UTC(),
	}
	if err := store.Write(ctx, m); err != nil {
		t.Fatalf("Write manifest: %v", err)
	}
	if err := backend.Write(ctx, outputKey, []byte("fake parquet")); err != nil {
		t.Fatalf("Write parquet: %v", err)
	}

	if err := Recover(ctx, "events__1h", store, wmStore, logger); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// Watermark should remain at futureEnd, not regress to end.
	wm := wmStore.watermarks["events__1h"]
	if !wm.Watermark.Equal(futureEnd) {
		t.Errorf("watermark regressed: got %v want %v", wm.Watermark, futureEnd)
	}
}

func TestRecover_StaleManifestIsProcessed(t *testing.T) {
	backend := newManifestTestBackend(t)
	store := NewManifestStore(backend, zerolog.Nop())
	wmStore := newFakeWMStore()
	ctx := context.Background()

	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	outputKey := "main/events__1h/dt=2026-05-10/window_20260510-120000-130000.parquet"

	// Stale manifest: created 8 days ago.
	m := WindowManifest{
		RollupName:  "events__1h",
		WindowStart: start,
		WindowEnd:   end,
		OutputKey:   outputKey,
		CreatedAt:   time.Now().UTC().Add(-8 * 24 * time.Hour),
	}
	if err := store.Write(ctx, m); err != nil {
		t.Fatalf("Write manifest: %v", err)
	}
	// Output missing → should still clean up (after logging loudly).
	if err := Recover(ctx, "events__1h", store, wmStore, zerolog.Nop()); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	keys, _ := store.List(ctx, "events__1h")
	if len(keys) != 0 {
		t.Errorf("expected manifest deleted after stale recovery, got %d", len(keys))
	}
}
