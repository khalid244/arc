package tiered

import (
	"context"
	"testing"
	"time"
)

func mustTime(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func mustTimeHM(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04", s)
	if err != nil {
		panic(err)
	}
	return t
}

// idxFor1hWatermark builds a MemoryFileIndex with two 1h files for the
// given variant:
//   - anchor file at 2026-01-01 so EarliestBucketLo precedes any test's TimeLo
//   - a file ending at the given watermark time
func idxFor1hWatermark(variant string, wm time.Time) *MemoryFileIndex {
	wm = wm.UTC()
	var paths []string
	anchor := mustTime("2026-01-01")
	if p := VariantPath("db.events", Tier1h, variant, anchor, "anchor"); p != "" {
		paths = append(paths, p)
	}
	lo := wm.AddDate(0, 0, -1)
	if p := VariantPath("db.events", Tier1h, variant, lo, "wm"); p != "" {
		paths = append(paths, p)
	}
	return &MemoryFileIndex{Paths: paths}
}

func TestPickTier_FullCoverage_NoOpenTail(t *testing.T) {
	ctx := context.Background()
	idx := idxFor1hWatermark("sketch", mustTime("2026-06-02"))
	shape := &QueryShape{
		BucketArg: "hour",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-06-01"),
	}
	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", 0)
	if !ok || tier != Tier1h {
		t.Fatalf("expected (Tier1h, ok=true), got (%q, %v)", tier, ok)
	}
	if !tailLo.Equal(shape.TimeHi) {
		t.Errorf("tailLo = %v, want TimeHi=%v (no open tail)", tailLo, shape.TimeHi)
	}
}

func TestPickTier_OpenTail(t *testing.T) {
	ctx := context.Background()
	// 1h tier files have day-aligned bucketHi (the day after the file's
	// stored date). Use a watermark on a day boundary so the
	// idx-builder's bucketHi-derivation matches.
	wm := mustTime("2026-05-31")
	idx := idxFor1hWatermark("sketch", wm)
	shape := &QueryShape{
		BucketArg: "hour",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-06-02"),
	}
	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", 0)
	if !ok || tier != Tier1h {
		t.Fatalf("expected (Tier1h, ok=true), got (%q, %v)", tier, ok)
	}
	if !tailLo.Equal(wm) {
		t.Errorf("tailLo = %v, want watermark %v", tailLo, wm)
	}
}

func TestPickTier_RefusesWhenNoWatermark(t *testing.T) {
	ctx := context.Background()
	idx := &MemoryFileIndex{}
	shape := &QueryShape{
		BucketArg: "hour",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-06-01"),
	}
	if _, _, ok := PickTier(ctx, shape, idx, "sketch", 0); ok {
		t.Fatal("expected ok=false when no watermark")
	}
}

func TestPickTier_RefusesWhenWatermarkBeforeTimeLo(t *testing.T) {
	ctx := context.Background()
	idx := idxFor1hWatermark("sketch", mustTime("2026-04-30"))
	shape := &QueryShape{
		BucketArg: "hour",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-06-01"),
	}
	if _, _, ok := PickTier(ctx, shape, idx, "sketch", 0); ok {
		t.Fatal("expected ok=false when watermark precedes TimeLo")
	}
}

func TestPickTier_RefusesWhenEarliestAfterTimeLo(t *testing.T) {
	ctx := context.Background()
	// Build an index with only files starting at 2026-05-15 — earliest >TimeLo.
	idx := &MemoryFileIndex{Paths: []string{
		VariantPath("db.events", Tier1h, "sketch", mustTime("2026-05-15"), "f"),
	}}
	shape := &QueryShape{
		BucketArg: "hour",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-05-20"),
	}
	if _, _, ok := PickTier(ctx, shape, idx, "sketch", 0); ok {
		t.Fatal("expected ok=false when rollup starts after TimeLo")
	}
}

func TestPickTier_GraceWindowAbsorbsTinyGap(t *testing.T) {
	ctx := context.Background()
	wm := mustTime("2026-06-01")
	idx := idxFor1hWatermark("sketch", wm)
	shape := &QueryShape{
		BucketArg: "hour",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    wm.Add(15 * time.Minute),
	}
	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", time.Hour)
	if !ok || tier != Tier1h {
		t.Fatalf("expected (Tier1h, ok=true), got (%q, %v)", tier, ok)
	}
	if !tailLo.Equal(shape.TimeHi) {
		t.Errorf("grace should have absorbed gap; tailLo=%v want %v", tailLo, shape.TimeHi)
	}
}

func TestPickTier_EmptyBucketArgPicksTierForScalar(t *testing.T) {
	ctx := context.Background()
	idx := idxFor1hWatermark("sketch", mustTime("2026-06-02"))
	shape := &QueryShape{
		BucketArg: "",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-06-01"),
	}
	tier, _, ok := PickTier(ctx, shape, idx, "sketch", 0)
	if !ok || tier != Tier1h {
		t.Fatalf("expected (Tier1h, ok=true) for scalar query, got (%q, %v)", tier, ok)
	}
}
