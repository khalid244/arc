package rollup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type countingStore struct {
	calls atomic.Int64
	val   Watermark
}

func (c *countingStore) Get(_ context.Context, name string) (Watermark, error) {
	c.calls.Add(1)
	return c.val, nil
}

func TestWatermarkCache_HitsBackingOnceWithinTTL(t *testing.T) {
	cs := &countingStore{val: Watermark{Rollup: "r", Watermark: time.Now().UTC()}}
	c := NewWatermarkCache(cs, 100*time.Millisecond)

	for i := 0; i < 5; i++ {
		_, _ = c.Get(context.Background(), "r")
	}
	if cs.calls.Load() != 1 {
		t.Errorf("expected 1 backing call, got %d", cs.calls.Load())
	}
}

func TestWatermarkCache_RefetchesAfterTTL(t *testing.T) {
	cs := &countingStore{val: Watermark{Rollup: "r"}}
	c := NewWatermarkCache(cs, 10*time.Millisecond)

	_, _ = c.Get(context.Background(), "r")
	time.Sleep(20 * time.Millisecond)
	_, _ = c.Get(context.Background(), "r")
	if cs.calls.Load() != 2 {
		t.Errorf("expected 2 backing calls (TTL expired), got %d", cs.calls.Load())
	}
}
