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

// idxWithWatermarks builds a MemoryFileIndex with files per (tier, variant):
//   - one early anchor file (bucketLo = 2026-01-01 in tier granularity) so that
//     EarliestBucketLo is well before any test's TimeLo
//   - one file whose bucketHi equals the given watermark time
//
// wms is a map of "tier.variant" → time (same key format as the old manifest).
func idxWithWatermarks(wms map[string]time.Time) *MemoryFileIndex {
	var paths []string
	anchor := mustTime("2026-01-01")
	for key, wm := range wms {
		var tier, variant string
		for i, c := range key {
			if c == '.' {
				tier = key[:i]
				variant = key[i+1:]
				break
			}
		}
		if tier == "" || variant == "" {
			continue
		}
		anchorLo := anchorBucketLo(tier, anchor)
		if !anchorLo.IsZero() {
			if p := VariantPath("db.events", Tier(tier), variant, anchorLo, "anchor"); p != "" {
				paths = append(paths, p)
			}
		}
		lo := bucketLoForWatermark(tier, wm)
		if lo.IsZero() {
			continue
		}
		path := VariantPath("db.events", Tier(tier), variant, lo, "testfile")
		if path != "" {
			paths = append(paths, path)
		}
	}
	return &MemoryFileIndex{Paths: paths}
}

// anchorBucketLo returns the bucketLo for the bucket containing t for the given tier.
func anchorBucketLo(tier string, t time.Time) time.Time {
	t = t.UTC()
	switch Tier(tier) {
	case Tier1h:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	case Tier1d:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case Tier1w:
		wd := int(t.Weekday())
		if wd == 0 {
			wd = 7
		}
		return t.AddDate(0, 0, 1-wd).Truncate(24 * time.Hour)
	case Tier1mo:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
	return time.Time{}
}

// bucketLoForWatermark returns the bucketLo such that bucketHi == wm for the given tier.
// wm must already be a valid bucket boundary (hour boundary for 1h, midnight for 1d,
// Monday midnight for 1w, first-of-month midnight for 1mo). For tier-appropriate
// boundaries the returned bucketLo produces a path whose ParseVariantPath gives bucketHi == wm.
func bucketLoForWatermark(tier string, wm time.Time) time.Time {
	wm = wm.UTC()
	switch Tier(tier) {
	case Tier1h:
		return wm.Add(-time.Hour)
	case Tier1d:
		return wm.AddDate(0, 0, -1)
	case Tier1w:
		return wm.AddDate(0, 0, -7)
	case Tier1mo:
		return wm.AddDate(0, -1, 0)
	}
	return time.Time{}
}

func TestPickTier_PicksMonthForMonthlyQuery_WhenAllTiersHaveWatermark(t *testing.T) {
	ctx := context.Background()
	// 2026-06-01 is a Monday AND first-of-month, so it is a valid bucket boundary
	// for all tiers: 1h (hourly), 1d (daily), 1w (ISO week start), 1mo (month start).
	wm := mustTime("2026-06-01")
	idx := idxWithWatermarks(map[string]time.Time{
		"1h.sketch":  wm,
		"1d.sketch":  wm,
		"1w.sketch":  wm,
		"1mo.sketch": wm,
	})
	shape := &QueryShape{
		BucketArg: "month",
		TimeLo:    mustTime("2026-03-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", 0)

	if !ok {
		t.Fatal("expected ok=true")
	}
	if tier != Tier1mo {
		t.Fatalf("expected Tier1mo, got %s", tier)
	}
	if !tailLo.Equal(shape.TimeHi) {
		t.Fatalf("expected tailLo == TimeHi (%v), got %v", shape.TimeHi, tailLo)
	}
}

func TestPickTier_PicksDayForDailyQuery(t *testing.T) {
	ctx := context.Background()
	wm := mustTime("2026-06-01")
	idx := idxWithWatermarks(map[string]time.Time{
		"1h.sketch":  wm,
		"1d.sketch":  wm,
		"1w.sketch":  wm,
		"1mo.sketch": wm,
	})
	shape := &QueryShape{
		BucketArg: "day",
		TimeLo:    mustTime("2026-03-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", 0)

	if !ok {
		t.Fatal("expected ok=true")
	}
	if tier != Tier1d {
		t.Fatalf("expected Tier1d, got %s", tier)
	}
	if !tailLo.Equal(shape.TimeHi) {
		t.Fatalf("expected tailLo == TimeHi (%v), got %v", shape.TimeHi, tailLo)
	}
}

func TestPickTier_PicksHourForHourlyQuery(t *testing.T) {
	ctx := context.Background()
	wm := mustTime("2026-06-01")
	idx := idxWithWatermarks(map[string]time.Time{
		"1h.sketch":  wm,
		"1d.sketch":  wm,
		"1w.sketch":  wm,
		"1mo.sketch": wm,
	})
	shape := &QueryShape{
		BucketArg: "hour",
		TimeLo:    mustTime("2026-03-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", 0)

	if !ok {
		t.Fatal("expected ok=true")
	}
	if tier != Tier1h {
		t.Fatalf("expected Tier1h, got %s", tier)
	}
	if !tailLo.Equal(shape.TimeHi) {
		t.Fatalf("expected tailLo == TimeHi (%v), got %v", shape.TimeHi, tailLo)
	}
}

func TestPickTier_PicksWeekForWeeklyQuery(t *testing.T) {
	ctx := context.Background()
	wm := mustTime("2026-06-01")
	idx := idxWithWatermarks(map[string]time.Time{
		"1h.sketch":  wm,
		"1d.sketch":  wm,
		"1w.sketch":  wm,
		"1mo.sketch": wm,
	})
	shape := &QueryShape{
		BucketArg: "week",
		TimeLo:    mustTime("2026-03-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", 0)

	if !ok {
		t.Fatal("expected ok=true")
	}
	if tier != Tier1w {
		t.Fatalf("expected Tier1w, got %s", tier)
	}
	if !tailLo.Equal(shape.TimeHi) {
		t.Fatalf("expected tailLo == TimeHi (%v), got %v", shape.TimeHi, tailLo)
	}
}

func TestPickTier_FallsBackToFinerWhenCoarserMissing(t *testing.T) {
	ctx := context.Background()
	wm := mustTime("2026-06-01")
	idx := idxWithWatermarks(map[string]time.Time{
		"1h.sketch": wm,
		"1d.sketch": wm,
	})
	shape := &QueryShape{
		BucketArg: "month",
		TimeLo:    mustTime("2026-03-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", 0)

	if !ok {
		t.Fatal("expected ok=true")
	}
	if tier != Tier1d {
		t.Fatalf("expected Tier1d (coarsest available), got %s", tier)
	}
	if !tailLo.Equal(shape.TimeHi) {
		t.Fatalf("expected tailLo == TimeHi (%v), got %v", shape.TimeHi, tailLo)
	}
}

func TestPickTier_FallsBackToRawWhenAllWatermarksZero(t *testing.T) {
	ctx := context.Background()
	idx := &MemoryFileIndex{}
	shape := &QueryShape{
		BucketArg: "day",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	_, _, ok := PickTier(ctx, shape, idx, "sketch", 0)

	if ok {
		t.Fatal("expected ok=false when no files set")
	}
}

func TestPickTier_OpenTailReturnsWatermark(t *testing.T) {
	ctx := context.Background()
	wm := mustTime("2026-05-10")
	idx := idxWithWatermarks(map[string]time.Time{
		"1d.sketch": wm,
	})
	shape := &QueryShape{
		BucketArg: "day",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", 0)

	if !ok {
		t.Fatal("expected ok=true")
	}
	if tier != Tier1d {
		t.Fatalf("expected Tier1d, got %s", tier)
	}
	if !tailLo.Equal(wm) {
		t.Fatalf("expected tailLo == watermark (%v), got %v", wm, tailLo)
	}
}

func TestPickTier_GraceWindowAbsorbsTinyGap(t *testing.T) {
	ctx := context.Background()
	// wm is exactly at a day boundary (midnight); TimeHi is 5 minutes later.
	// grace = 15m absorbs the 5-minute gap → no open tail.
	wm := mustTimeHM("2026-05-15 00:00")
	idx := idxWithWatermarks(map[string]time.Time{
		"1d.sketch": wm,
	})
	shape := &QueryShape{
		BucketArg: "day",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTimeHM("2026-05-15 00:05"),
	}

	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", 15*time.Minute)

	if !ok {
		t.Fatal("expected ok=true")
	}
	if tier != Tier1d {
		t.Fatalf("expected Tier1d, got %s", tier)
	}
	if !tailLo.Equal(shape.TimeHi) {
		t.Fatalf("expected tailLo == TimeHi (no open tail), got %v", tailLo)
	}
}

func TestPickTier_RefusesWhenWatermarkBeforeTimeLo(t *testing.T) {
	ctx := context.Background()
	wm := mustTime("2026-04-01")
	idx := idxWithWatermarks(map[string]time.Time{
		"1d.sketch":  wm,
		"1w.sketch":  wm,
		"1mo.sketch": wm,
		"1h.sketch":  wm,
	})
	shape := &QueryShape{
		BucketArg: "month",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	_, _, ok := PickTier(ctx, shape, idx, "sketch", 0)

	if ok {
		t.Fatal("expected ok=false when all watermarks are before TimeLo")
	}
}

func TestPickTier_PicksFinestWhenCoarserDontCover(t *testing.T) {
	ctx := context.Background()
	idx := idxWithWatermarks(map[string]time.Time{
		"1mo.sketch": mustTime("2026-04-01"),
		"1w.sketch":  mustTime("2026-04-01"),
		"1d.sketch":  mustTime("2026-05-15"),
	})
	shape := &QueryShape{
		BucketArg: "month",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", 0)

	if !ok {
		t.Fatal("expected ok=true")
	}
	if tier != Tier1d {
		t.Fatalf("expected Tier1d (1mo and 1w watermarks below TimeLo), got %s", tier)
	}
	if !tailLo.Equal(shape.TimeHi) {
		t.Fatalf("expected tailLo == TimeHi (%v), got %v", shape.TimeHi, tailLo)
	}
}

func TestPickTier_RefusesUnknownBucketArg(t *testing.T) {
	ctx := context.Background()
	wm := mustTime("2026-05-15")
	idx := idxWithWatermarks(map[string]time.Time{
		"1h.sketch": wm,
	})

	cases := []string{"minute", "year"}
	for _, bucketArg := range cases {
		shape := &QueryShape{
			BucketArg: bucketArg,
			TimeLo:    mustTime("2026-05-01"),
			TimeHi:    mustTime("2026-05-15"),
		}
		_, _, ok := PickTier(ctx, shape, idx, "sketch", 0)
		if ok {
			t.Fatalf("expected ok=false for BucketArg=%q", bucketArg)
		}
	}
}

func TestPickTier_EmptyBucketArgPicksTierForScalar(t *testing.T) {
	ctx := context.Background()
	wm := mustTime("2026-06-01")
	idx := idxWithWatermarks(map[string]time.Time{
		"1h.sketch":  wm,
		"1d.sketch":  wm,
		"1mo.sketch": wm,
	})
	shape := &QueryShape{
		BucketArg: "",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-05-15"),
	}
	tier, tailLo, ok := PickTier(ctx, shape, idx, "sketch", 0)
	if !ok {
		t.Fatal("expected ok=true for empty BucketArg (scalar aggregate)")
	}
	if tier != Tier1mo {
		t.Errorf("expected coarsest viable tier 1mo, got %s", tier)
	}
	if !tailLo.Equal(shape.TimeHi) {
		t.Errorf("expected no open tail (tailLo=timeHi), got tailLo=%v", tailLo)
	}
}
