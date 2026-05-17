package tiered

import (
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

func TestPickTier_PicksMonthForMonthlyQuery_WhenAllTiersHaveWatermark(t *testing.T) {
	wm := mustTime("2026-05-15")
	manifest := &Manifest{Watermarks: map[string]time.Time{
		"1h.sketch":  wm,
		"1d.sketch":  wm,
		"1w.sketch":  wm,
		"1mo.sketch": wm,
	}}
	shape := &QueryShape{
		BucketArg: "month",
		TimeLo:    mustTime("2026-03-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(shape, manifest, "sketch", 0)

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
	wm := mustTime("2026-05-15")
	manifest := &Manifest{Watermarks: map[string]time.Time{
		"1h.sketch":  wm,
		"1d.sketch":  wm,
		"1w.sketch":  wm,
		"1mo.sketch": wm,
	}}
	shape := &QueryShape{
		BucketArg: "day",
		TimeLo:    mustTime("2026-03-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(shape, manifest, "sketch", 0)

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
	wm := mustTime("2026-05-15")
	manifest := &Manifest{Watermarks: map[string]time.Time{
		"1h.sketch":  wm,
		"1d.sketch":  wm,
		"1w.sketch":  wm,
		"1mo.sketch": wm,
	}}
	shape := &QueryShape{
		BucketArg: "hour",
		TimeLo:    mustTime("2026-03-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(shape, manifest, "sketch", 0)

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
	wm := mustTime("2026-05-15")
	manifest := &Manifest{Watermarks: map[string]time.Time{
		"1h.sketch":  wm,
		"1d.sketch":  wm,
		"1w.sketch":  wm,
		"1mo.sketch": wm,
	}}
	shape := &QueryShape{
		BucketArg: "week",
		TimeLo:    mustTime("2026-03-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(shape, manifest, "sketch", 0)

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
	wm := mustTime("2026-05-15")
	manifest := &Manifest{Watermarks: map[string]time.Time{
		"1h.sketch": wm,
		"1d.sketch": wm,
	}}
	shape := &QueryShape{
		BucketArg: "month",
		TimeLo:    mustTime("2026-03-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(shape, manifest, "sketch", 0)

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
	manifest := &Manifest{}
	shape := &QueryShape{
		BucketArg: "day",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	_, _, ok := PickTier(shape, manifest, "sketch", 0)

	if ok {
		t.Fatal("expected ok=false when no watermarks set")
	}
}

func TestPickTier_OpenTailReturnsWatermark(t *testing.T) {
	wm := mustTime("2026-05-10")
	manifest := &Manifest{Watermarks: map[string]time.Time{
		"1d.sketch": wm,
	}}
	shape := &QueryShape{
		BucketArg: "day",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(shape, manifest, "sketch", 0)

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
	wm := mustTimeHM("2026-05-14 23:55")
	manifest := &Manifest{Watermarks: map[string]time.Time{
		"1d.sketch": wm,
	}}
	shape := &QueryShape{
		BucketArg: "day",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTimeHM("2026-05-15 00:00"),
	}

	tier, tailLo, ok := PickTier(shape, manifest, "sketch", 15*time.Minute)

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
	wm := mustTime("2026-04-01")
	manifest := &Manifest{Watermarks: map[string]time.Time{
		"1d.sketch":  wm,
		"1w.sketch":  wm,
		"1mo.sketch": wm,
		"1h.sketch":  wm,
	}}
	shape := &QueryShape{
		BucketArg: "month",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	_, _, ok := PickTier(shape, manifest, "sketch", 0)

	if ok {
		t.Fatal("expected ok=false when all watermarks are before TimeLo")
	}
}

func TestPickTier_PicksFinestWhenCoarserDontCover(t *testing.T) {
	manifest := &Manifest{Watermarks: map[string]time.Time{
		"1mo.sketch": mustTime("2026-04-01"),
		"1w.sketch":  mustTime("2026-04-01"),
		"1d.sketch":  mustTime("2026-05-15"),
	}}
	shape := &QueryShape{
		BucketArg: "month",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-05-15"),
	}

	tier, tailLo, ok := PickTier(shape, manifest, "sketch", 0)

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
	wm := mustTime("2026-05-15")
	manifest := &Manifest{Watermarks: map[string]time.Time{
		"1h.sketch": wm,
	}}

	cases := []string{"minute", "year"}
	for _, bucketArg := range cases {
		shape := &QueryShape{
			BucketArg: bucketArg,
			TimeLo:    mustTime("2026-05-01"),
			TimeHi:    mustTime("2026-05-15"),
		}
		_, _, ok := PickTier(shape, manifest, "sketch", 0)
		if ok {
			t.Fatalf("expected ok=false for BucketArg=%q", bucketArg)
		}
	}
}

func TestPickTier_EmptyBucketArgPicksTierForScalar(t *testing.T) {
	wm := mustTime("2026-05-15")
	manifest := &Manifest{Watermarks: map[string]time.Time{
		"1h.sketch":  wm,
		"1d.sketch":  wm,
		"1mo.sketch": wm,
	}}
	shape := &QueryShape{
		BucketArg: "",
		TimeLo:    mustTime("2026-05-01"),
		TimeHi:    mustTime("2026-05-15"),
	}
	tier, tailLo, ok := PickTier(shape, manifest, "sketch", 0)
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
