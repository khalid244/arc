package rollup

import (
	"context"
	"errors"
	"sync"
	"time"
)

var errReadOnlyCache = errors.New("watermark cache is read-only (use NewWatermarkCacheReadWrite)")

// WatermarkCache wraps a WMReadWriter (when used for both reads and writes)
// with a per-key TTL cache. Safe for concurrent use. Errors are not cached.
//
// Writes propagate to the backing store and replace the cached entry, so
// readers immediately see the new value (no TTL wait). Use this both for the
// query-side rewriter and the builder-side scheduler so they share a single
// view of watermark state — without sharing, the rewriter sees stale values
// for up to TTL after a build.
type WatermarkCache struct {
	src WMReader
	dst WMWriter // optional; nil → Put returns an error
	ttl time.Duration

	mu      sync.RWMutex
	entries map[string]wmEntry
}

type wmEntry struct {
	wm    Watermark
	until time.Time
}

// NewWatermarkCache wraps a read-only source. Use NewWatermarkCacheReadWrite
// to also propagate writes through the cache.
func NewWatermarkCache(src WMReader, ttl time.Duration) *WatermarkCache {
	return &WatermarkCache{
		src:     src,
		ttl:     ttl,
		entries: map[string]wmEntry{},
	}
}

// NewWatermarkCacheReadWrite wraps a backend that supports both reads and
// writes. Put updates the backend and refreshes the cache atomically.
func NewWatermarkCacheReadWrite(rw WMReadWriter, ttl time.Duration) *WatermarkCache {
	return &WatermarkCache{
		src:     rw,
		dst:     rw,
		ttl:     ttl,
		entries: map[string]wmEntry{},
	}
}

func (c *WatermarkCache) Get(ctx context.Context, rollupName string) (Watermark, error) {
	now := time.Now()
	c.mu.RLock()
	if e, ok := c.entries[rollupName]; ok && now.Before(e.until) {
		c.mu.RUnlock()
		return e.wm, nil
	}
	c.mu.RUnlock()

	wm, err := c.src.Get(ctx, rollupName)
	if err != nil {
		return Watermark{}, err
	}
	c.mu.Lock()
	c.entries[rollupName] = wmEntry{wm: wm, until: now.Add(c.ttl)}
	c.mu.Unlock()
	return wm, nil
}

// Put writes through to the backing store, then updates the cache. Returns an
// error if no writer is configured (use NewWatermarkCacheReadWrite).
func (c *WatermarkCache) Put(ctx context.Context, w Watermark) error {
	if c.dst == nil {
		return errReadOnlyCache
	}
	if err := c.dst.Put(ctx, w); err != nil {
		return err
	}
	c.mu.Lock()
	c.entries[w.Rollup] = wmEntry{wm: w, until: time.Now().Add(c.ttl)}
	c.mu.Unlock()
	return nil
}
