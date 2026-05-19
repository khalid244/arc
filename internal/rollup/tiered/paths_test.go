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

// Legacy 1d/1w/1mo paths must be ignored after the single-tier migration.
// (The files remain on storage for manual cleanup but should never be
// surfaced by the reader.)
func TestParseVariantPath_LegacyTiersIgnored(t *testing.T) {
	for _, path := range []string{
		"_arc/rollup/default/events/1d/2026/05/15/by_site/abc.parquet",
		"_arc/rollup/default/events/1w/2026/W20/sketch/xyz.parquet",
		"_arc/rollup/default/events/1mo/2026/05/all/m1.parquet",
	} {
		_, _, _, _, _, ok := ParseVariantPath(path)
		if ok {
			t.Errorf("ParseVariantPath(%q) returned ok=true; legacy tier paths must be ignored", path)
		}
	}
}

func TestParseVariantPath_Malformed(t *testing.T) {
	cases := []string{
		"",
		"_arc/rollup/default/events/1h/2025/02/sketch/8f1a5c9a.parquet",
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
