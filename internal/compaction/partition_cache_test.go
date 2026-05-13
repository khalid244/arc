package compaction

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPartitionCache_GetSet(t *testing.T) {
	c := NewPartitionCache(24*time.Hour, 30)
	if s, ok := c.Get("missing"); ok {
		t.Errorf("Get on missing key should return false; got %+v", s)
	}

	want := partitionState{
		FullyCompacted: true,
		NewestFileTime: time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
		FileCount:      5,
	}
	c.Set("default/m/2026/05/13/12", want)

	got, ok := c.Get("default/m/2026/05/13/12")
	if !ok {
		t.Fatal("Get after Set returned false")
	}
	if got.FullyCompacted != want.FullyCompacted {
		t.Errorf("FullyCompacted: got %v want %v", got.FullyCompacted, want.FullyCompacted)
	}
	if !got.NewestFileTime.Equal(want.NewestFileTime) {
		t.Errorf("NewestFileTime: got %v want %v", got.NewestFileTime, want.NewestFileTime)
	}
	if got.FileCount != want.FileCount {
		t.Errorf("FileCount: got %d want %d", got.FileCount, want.FileCount)
	}
	if got.UpdatedAt.IsZero() {
		t.Error("Set should stamp UpdatedAt")
	}
}

func TestPartitionCache_MarkCompacted(t *testing.T) {
	c := NewPartitionCache(24*time.Hour, 30)
	now := time.Now().UTC()
	c.MarkCompacted("p1", now, 10)

	got, ok := c.Get("p1")
	if !ok {
		t.Fatal("MarkCompacted did not persist")
	}
	if !got.FullyCompacted {
		t.Error("FullyCompacted should be true after MarkCompacted")
	}
	if !got.NewestFileTime.Equal(now) {
		t.Errorf("NewestFileTime: got %v want %v", got.NewestFileTime, now)
	}
	if got.FileCount != 10 {
		t.Errorf("FileCount: got %d want 10", got.FileCount)
	}
}

func TestPartitionCache_Invalidate(t *testing.T) {
	c := NewPartitionCache(24*time.Hour, 30)
	c.Set("p1", partitionState{FullyCompacted: true})
	c.Invalidate("p1")
	if _, ok := c.Get("p1"); ok {
		t.Error("entry should be gone after Invalidate")
	}
	// Invalidating a missing key is a no-op
	c.Invalidate("never-existed")
}

func TestPartitionCache_RollingCursor_WalksBackward(t *testing.T) {
	c := NewPartitionCache(24*time.Hour, 7) // 1-day chunks, 7-day window
	now := time.Now().UTC().Truncate(24 * time.Hour)

	// Each subsequent call returns a chunk one day earlier.
	prevStart := time.Time{}
	for i := 0; i < 5; i++ {
		start, end := c.NextReconcileChunk("default/m")
		if end.Sub(start) != 24*time.Hour {
			t.Errorf("iter %d: chunk size = %v want 24h", i, end.Sub(start))
		}
		if !prevStart.IsZero() && !start.Before(prevStart) {
			t.Errorf("iter %d: cursor did not walk backward (prev=%v this=%v)", i, prevStart, start)
		}
		prevStart = start
		// First call should yield "yesterday"
		if i == 0 && !start.Equal(now.Add(-24*time.Hour)) {
			t.Errorf("first chunk should start at yesterday=%v, got %v", now.Add(-24*time.Hour), start)
		}
	}
}

func TestPartitionCache_RollingCursor_WrapsAfterWindow(t *testing.T) {
	c := NewPartitionCache(24*time.Hour, 3) // 3-day window
	now := time.Now().UTC().Truncate(24 * time.Hour)

	// Walk through the entire window plus one more chunk to force a wrap.
	starts := make([]time.Time, 0, 5)
	for i := 0; i < 5; i++ {
		start, _ := c.NextReconcileChunk("k")
		starts = append(starts, start)
	}

	// Confirm the cursor wrapped: at least one pair (i, j) with j > i+1
	// should have starts[j] >= starts[i].
	wrapped := false
	for i := 0; i < len(starts)-1; i++ {
		if !starts[i+1].Before(starts[i]) {
			wrapped = true
			break
		}
	}
	if !wrapped {
		t.Errorf("cursor never wrapped after exceeding window: %+v", starts)
	}

	// After wrap, the cursor should be back near "yesterday".
	last := starts[len(starts)-1]
	yesterday := now.Add(-24 * time.Hour)
	if last.Before(yesterday.Add(-2 * 24 * time.Hour)) {
		t.Errorf("after wrap cursor should be near yesterday=%v, got %v", yesterday, last)
	}
}

func TestPartitionCache_CursorPerMeasurement(t *testing.T) {
	c := NewPartitionCache(24*time.Hour, 30)
	// Different measurement keys advance independently.
	startA1, _ := c.NextReconcileChunk("a")
	startB1, _ := c.NextReconcileChunk("b")
	startA2, _ := c.NextReconcileChunk("a")

	if !startA1.Equal(startB1) {
		t.Errorf("first call for two measurements should yield same date: A=%v B=%v", startA1, startB1)
	}
	if !startA2.Before(startA1) {
		t.Errorf("measurement a's cursor should have advanced backward: 1st=%v 2nd=%v", startA1, startA2)
	}

	// Measurement b's cursor should still be at its initial position
	// because only a has been called twice.
	startB2, _ := c.NextReconcileChunk("b")
	if !startB2.Before(startB1) {
		t.Errorf("measurement b's cursor advanced incorrectly: 1st=%v 2nd=%v", startB1, startB2)
	}
}

func TestPartitionCache_ConcurrentAccess(t *testing.T) {
	c := NewPartitionCache(24*time.Hour, 30)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			path := fmt.Sprintf("p/%d", i%10)
			c.Set(path, partitionState{FileCount: i})
			_, _ = c.Get(path)
			c.MarkCompacted(path, time.Now().UTC(), i)
			c.NextReconcileChunk(fmt.Sprintf("m/%d", i%4))
		}(i)
	}
	wg.Wait()
	if c.Size() < 1 {
		t.Errorf("expected entries after concurrent writes, got size=%d", c.Size())
	}
}

func TestPartitionCache_MergeFresh_AddsAndUpdates(t *testing.T) {
	c := NewPartitionCache(24*time.Hour, 30)
	c.Set("existing", partitionState{FileCount: 1})
	c.Set("untouched", partitionState{FullyCompacted: true, FileCount: 99})

	fresh := map[string]partitionState{
		"existing":  {FileCount: 42, FullyCompacted: true},
		"brand-new": {FileCount: 7},
	}
	c.MergeFresh(fresh)

	got, _ := c.Get("existing")
	if got.FileCount != 42 || !got.FullyCompacted {
		t.Errorf("existing should have been overwritten: %+v", got)
	}
	got, _ = c.Get("brand-new")
	if got.FileCount != 7 {
		t.Errorf("brand-new should be present: %+v", got)
	}
	got, ok := c.Get("untouched")
	if !ok || got.FileCount != 99 {
		t.Errorf("untouched entry should remain: ok=%v entry=%+v", ok, got)
	}
}

func TestPartitionCache_MergeFresh_EmptyIsNoOp(t *testing.T) {
	c := NewPartitionCache(24*time.Hour, 30)
	c.Set("p1", partitionState{FileCount: 5})
	c.MergeFresh(nil)
	c.MergeFresh(map[string]partitionState{})
	got, ok := c.Get("p1")
	if !ok || got.FileCount != 5 {
		t.Errorf("empty MergeFresh should be a no-op; got=%+v", got)
	}
}

func TestPartitionCache_Defaults(t *testing.T) {
	// Zero chunkSize and zero windowDays should populate sane defaults.
	c := NewPartitionCache(0, 0)
	start, end := c.NextReconcileChunk("m")
	if end.Sub(start) != 24*time.Hour {
		t.Errorf("default chunk size should be 24h, got %v", end.Sub(start))
	}
}
