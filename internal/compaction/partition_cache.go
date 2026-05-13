package compaction

import (
	"sync"
	"time"
)

// partitionState captures the cache's knowledge of one storage partition
// (e.g. "default/downloads/2026/05/13/01"). The cache lives in memory on
// the compactor pod. It is populated incrementally by per-cycle scans and
// reconciled against S3 by a rolling per-measurement cursor that walks
// backward through history one chunk per cycle.
type partitionState struct {
	// FullyCompacted is true when the partition has a `_compacted.parquet`
	// (or `_daily.parquet`) marker and no other input files to compact.
	// Subsequent cycles can skip listing this partition's prefix.
	FullyCompacted bool

	// NewestFileTime is the file-mtime of the most recent file observed
	// when this entry was written. New writes after this point invalidate
	// FullyCompacted on the next inspection.
	NewestFileTime time.Time

	// FileCount snapshots how many parquet files lived in the partition
	// when last seen.
	FileCount int

	// UpdatedAt is when the cache entry was last written.
	UpdatedAt time.Time
}

// PartitionCache is an in-memory record of every partition the compactor
// has seen, keyed by full partition path. It eliminates the per-cycle
// flat List(prefix=db/measurement/) traversal: steady-state cycles do a
// short incremental scan of recent hours plus one rolling reconciliation
// chunk that walks backward through history.
type PartitionCache struct {
	mu               sync.RWMutex
	states           map[string]partitionState
	reconcileCursors map[string]time.Time // measurement-key -> NEXT day to reconcile
	chunkSize        time.Duration        // size of one reconciliation chunk
	windowDays       int                  // how many days the cursor walks before wrapping
}

// NewPartitionCache returns a cache configured with the given reconcile
// chunk size and total window. chunkSize is typically 24h; windowDays
// bounds how far back the rolling cursor walks before wrapping (e.g. 90
// = touch every partition at least once every 90 cycles).
func NewPartitionCache(chunkSize time.Duration, windowDays int) *PartitionCache {
	if chunkSize <= 0 {
		chunkSize = 24 * time.Hour
	}
	if windowDays <= 0 {
		windowDays = 90
	}
	return &PartitionCache{
		states:           make(map[string]partitionState),
		reconcileCursors: make(map[string]time.Time),
		chunkSize:        chunkSize,
		windowDays:       windowDays,
	}
}

// Get returns the cached state for a partition path, or zero+false.
func (c *PartitionCache) Get(partitionPath string) (partitionState, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.states[partitionPath]
	return s, ok
}

// Set replaces (or inserts) the entry for partitionPath. UpdatedAt is
// stamped automatically.
func (c *PartitionCache) Set(partitionPath string, s partitionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s.UpdatedAt = time.Now().UTC()
	c.states[partitionPath] = s
}

// MarkCompacted records that the partition is now fully compacted.
// Called from the worker after a successful compaction subprocess.
func (c *PartitionCache) MarkCompacted(partitionPath string, newestFileTime time.Time, fileCount int) {
	c.Set(partitionPath, partitionState{
		FullyCompacted: true,
		NewestFileTime: newestFileTime,
		FileCount:      fileCount,
	})
}

// Invalidate drops an entry so the next scan re-evaluates it from S3.
func (c *PartitionCache) Invalidate(partitionPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.states, partitionPath)
}

// Size returns the number of cached entries; useful for observability.
func (c *PartitionCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.states)
}

// NextReconcileChunk returns [start, end) for the next reconciliation
// chunk for this measurement. The returned range is one chunkSize wide
// and walks backward through history. When the cursor falls below
// now - windowDays, it wraps back to yesterday and starts a fresh
// rotation. measurementKey is typically "<db>/<measurement>".
func (c *PartitionCache) NextReconcileChunk(measurementKey string) (start, end time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UTC().Truncate(24 * time.Hour)
	cursor, ok := c.reconcileCursors[measurementKey]
	if !ok || cursor.IsZero() {
		// First call: start reconciling from yesterday and walk backward.
		cursor = now.Add(-24 * time.Hour)
	}

	end = cursor.Add(c.chunkSize)
	start = cursor

	// Advance for next call: move backward by chunkSize.
	next := cursor.Add(-c.chunkSize)
	earliest := now.Add(-time.Duration(c.windowDays) * 24 * time.Hour)
	if next.Before(earliest) {
		// Wrapped past the window — restart at yesterday.
		next = now.Add(-24 * time.Hour)
	}
	c.reconcileCursors[measurementKey] = next

	return start, end
}

// CursorState returns the current reconcile cursor for the given
// measurement. Used for observability/tests.
func (c *PartitionCache) CursorState(measurementKey string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.reconcileCursors[measurementKey]
	return t, ok
}

// MergeFresh updates cache entries from a fresh scan result (typically
// the union of incremental + reconciliation views). Entries that show
// up in fresh overwrite existing cached state. Entries previously cached
// but not present in fresh are LEFT ALONE — fresh may be a partial view
// (only the day we just reconciled), not the entire history. Use
// Invalidate if you need to drop a specific entry.
func (c *PartitionCache) MergeFresh(fresh map[string]partitionState) {
	if len(fresh) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UTC()
	for path, s := range fresh {
		s.UpdatedAt = now
		c.states[path] = s
	}
}
