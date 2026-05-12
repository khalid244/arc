package rollup

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestWatermark_RoundtripLocal(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}

	wm := Watermark{
		Rollup:               "analytics__events__1h",
		BucketInterval:       time.Hour,
		Watermark:            time.Date(2026, 5, 10, 13, 0, 0, 0, time.UTC),
		LastBuildCompletedAt: time.Date(2026, 5, 10, 13, 5, 12, 0, time.UTC),
		LastBuildWindowStart: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
		LastBuildWindowEnd:   time.Date(2026, 5, 10, 13, 0, 0, 0, time.UTC),
	}

	store := NewWatermarkStore(backend)
	if err := store.Put(context.Background(), wm); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := store.Get(context.Background(), wm.Rollup)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.Watermark.Equal(wm.Watermark) {
		t.Errorf("watermark: got %v want %v", got.Watermark, wm.Watermark)
	}
	if got.BucketInterval != wm.BucketInterval {
		t.Errorf("interval: got %v want %v", got.BucketInterval, wm.BucketInterval)
	}
}

func TestWatermark_GetMissingReturnsZero(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	store := NewWatermarkStore(backend)
	got, err := store.Get(context.Background(), "missing__1h")
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}
	if !got.Watermark.IsZero() {
		t.Errorf("expected zero watermark for missing, got %v", got.Watermark)
	}
}
