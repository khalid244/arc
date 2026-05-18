package tiered

import (
	"testing"
	"time"
)

func TestVariantPath_1h(t *testing.T) {
	got := VariantPath("default.events", Tier1h, "by_site",
		time.Date(2026, 5, 15, 14, 0, 0, 0, time.UTC), "abc")
	want := "_arc/rollup/default/events/1h/2026/05/15/by_site/abc.parquet"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestVariantPath_1d(t *testing.T) {
	got := VariantPath("default.events", Tier1d, "by_site",
		time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC), "abc")
	want := "_arc/rollup/default/events/1d/2026/05/15/by_site/abc.parquet"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestVariantPath_1w(t *testing.T) {
	// Week-aligned bucket: use ISO week. 2026-05-15 is Friday of ISO week 20.
	bucket := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC) // Monday of week 20
	got := VariantPath("default.events", Tier1w, "sketch", bucket, "xyz")
	want := "_arc/rollup/default/events/1w/2026/W20/sketch/xyz.parquet"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestVariantPath_1mo(t *testing.T) {
	got := VariantPath("default.events", Tier1mo, "all",
		time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), "m1")
	want := "_arc/rollup/default/events/1mo/2026/05/all/m1.parquet"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestParseVariantPath_1h(t *testing.T) {
	path := "_arc/rollup/default/events/1h/2026/05/15/by_site/abc.parquet"
	table, tier, variant, lo, hi, ok := ParseVariantPath(path)
	if !ok {
		t.Fatal("ParseVariantPath returned ok=false")
	}
	if table != "default.events" {
		t.Errorf("table = %q, want %q", table, "default.events")
	}
	if tier != "1h" {
		t.Errorf("tier = %q, want %q", tier, "1h")
	}
	if variant != "by_site" {
		t.Errorf("variant = %q, want %q", variant, "by_site")
	}
	wantLo := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	wantHi := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	if !lo.Equal(wantLo) {
		t.Errorf("bucketLo = %v, want %v", lo, wantLo)
	}
	if !hi.Equal(wantHi) {
		t.Errorf("bucketHi = %v, want %v", hi, wantHi)
	}
}

func TestParseVariantPath_1d(t *testing.T) {
	path := "_arc/rollup/default/events/1d/2026/05/15/by_site/abc.parquet"
	table, tier, variant, lo, hi, ok := ParseVariantPath(path)
	if !ok {
		t.Fatal("ParseVariantPath returned ok=false")
	}
	if table != "default.events" || tier != "1d" || variant != "by_site" {
		t.Errorf("got table=%q tier=%q variant=%q", table, tier, variant)
	}
	wantLo := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	wantHi := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	if !lo.Equal(wantLo) || !hi.Equal(wantHi) {
		t.Errorf("bucket = [%v, %v), want [%v, %v)", lo, hi, wantLo, wantHi)
	}
}

func TestParseVariantPath_1w(t *testing.T) {
	path := "_arc/rollup/default/events/1w/2026/W20/sketch/xyz.parquet"
	table, tier, variant, lo, hi, ok := ParseVariantPath(path)
	if !ok {
		t.Fatal("ParseVariantPath returned ok=false")
	}
	if table != "default.events" || tier != "1w" || variant != "sketch" {
		t.Errorf("got table=%q tier=%q variant=%q", table, tier, variant)
	}
	// Monday of ISO week 20 of 2026 is 2026-05-11.
	wantLo := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC)
	wantHi := time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC)
	if !lo.Equal(wantLo) || !hi.Equal(wantHi) {
		t.Errorf("bucket = [%v, %v), want [%v, %v)", lo, hi, wantLo, wantHi)
	}
}

func TestParseVariantPath_1mo(t *testing.T) {
	path := "_arc/rollup/default/events/1mo/2026/05/all/m1.parquet"
	table, tier, variant, lo, hi, ok := ParseVariantPath(path)
	if !ok {
		t.Fatal("ParseVariantPath returned ok=false")
	}
	if table != "default.events" || tier != "1mo" || variant != "all" {
		t.Errorf("got table=%q tier=%q variant=%q", table, tier, variant)
	}
	wantLo := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	wantHi := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	if !lo.Equal(wantLo) || !hi.Equal(wantHi) {
		t.Errorf("bucket = [%v, %v), want [%v, %v)", lo, hi, wantLo, wantHi)
	}
}

func TestParseVariantPath_Malformed(t *testing.T) {
	cases := []string{
		"",
		"_arc/rollup/default/events/1h/2025/02/sketch/8f1a5c9a.parquet", // missing day segment (only 4 after tier)
		"_arc/rollup/default/events/badtier/2026/05/15/sketch/f.parquet",
		"not/a/rollup/path",
		"_arc/rollup/x",
	}
	for _, path := range cases {
		_, _, _, _, _, ok := ParseVariantPath(path)
		if ok {
			t.Errorf("ParseVariantPath(%q) returned ok=true, want false", path)
		}
	}
}
