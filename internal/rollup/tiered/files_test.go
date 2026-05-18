package tiered

import (
	"context"
	"testing"
	"time"
)

func TestMemoryFileIndex_Watermark_MaxBucketHi(t *testing.T) {
	ctx := context.Background()
	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/db/events/1h/2026/05/01/sketch/a.parquet",
			"_arc/rollup/db/events/1h/2026/05/02/sketch/b.parquet",
			"_arc/rollup/db/events/1h/2026/05/03/sketch/c.parquet",
		},
	}

	wm, ok, err := idx.Watermark(ctx, "1h", "sketch")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	want := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	if !wm.Equal(want) {
		t.Errorf("Watermark = %v, want %v", wm, want)
	}
}

func TestMemoryFileIndex_EarliestBucketLo_MinBucketLo(t *testing.T) {
	ctx := context.Background()
	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/db/events/1h/2026/05/03/sketch/c.parquet",
			"_arc/rollup/db/events/1h/2026/05/01/sketch/a.parquet",
			"_arc/rollup/db/events/1h/2026/05/02/sketch/b.parquet",
		},
	}

	lo, ok, err := idx.EarliestBucketLo(ctx, "1h", "sketch")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	want := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	if !lo.Equal(want) {
		t.Errorf("EarliestBucketLo = %v, want %v", lo, want)
	}
}

func TestMemoryFileIndex_FilesForTierVariantWindow_Overlap(t *testing.T) {
	ctx := context.Background()
	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/db/events/1h/2026/05/01/sketch/d1.parquet",
			"_arc/rollup/db/events/1h/2026/05/02/sketch/d2.parquet",
			"_arc/rollup/db/events/1h/2026/05/03/sketch/d3.parquet",
			"_arc/rollup/db/events/1h/2026/05/04/sketch/d4.parquet",
		},
	}

	lo := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	hi := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	got, err := idx.FilesForTierVariantWindow(ctx, "1h", "sketch", lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("FilesForTierVariantWindow = %d files, want 2 (d2, d3); got: %v", len(got), got)
	}
}

func TestMemoryFileIndex_MalformedPathsIgnored(t *testing.T) {
	ctx := context.Background()
	idx := &MemoryFileIndex{
		Paths: []string{
			"not-a-rollup-path/something.parquet",
			"_arc/rollup/db/events/1h/2026/05/01/sketch/valid.parquet",
			"_arc/rollup/db/events/1h/bad/partition/sketch/bad.parquet",
		},
	}

	got, err := idx.FilesForTierVariant(ctx, "1h", "sketch")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Errorf("FilesForTierVariant = %d, want 1 (only valid path); got: %v", len(got), got)
	}
}

func TestMemoryFileIndex_WrongTierOrVariantNotReturned(t *testing.T) {
	ctx := context.Background()
	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/db/events/1h/2026/05/01/sketch/a.parquet",
			"_arc/rollup/db/events/1h/2026/05/01/by_site/b.parquet",
			"_arc/rollup/db/events/1d/2026/05/01/sketch/c.parquet",
		},
	}

	got, err := idx.FilesForTierVariant(ctx, "1h", "sketch")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "_arc/rollup/db/events/1h/2026/05/01/sketch/a.parquet" {
		t.Errorf("FilesForTierVariant = %v, want only 1h/sketch entry", got)
	}
}

func TestMemoryFileIndex_EmptyWhenNoPaths(t *testing.T) {
	ctx := context.Background()
	idx := &MemoryFileIndex{}

	wm, ok, err := idx.Watermark(ctx, "1h", "sketch")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Errorf("Watermark ok=true on empty index, want false; wm=%v", wm)
	}

	lo, ok2, err2 := idx.EarliestBucketLo(ctx, "1h", "sketch")
	if err2 != nil {
		t.Fatal(err2)
	}
	if ok2 {
		t.Errorf("EarliestBucketLo ok=true on empty index, want false; lo=%v", lo)
	}
}
