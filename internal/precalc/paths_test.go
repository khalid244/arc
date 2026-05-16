package precalc

import (
	"testing"
	"time"
)

func TestVariantPath_1d(t *testing.T) {
	got := VariantPath("default.downloads", Tier1d, "by_site",
		time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC), "abc")
	want := "precalc/table=default.downloads/tier=1d/year=2026/month=05/day=15/by_site/abc.parquet"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestVariantPath_1w(t *testing.T) {
	// Week-aligned bucket: use ISO week. 2026-05-15 is Friday of ISO week 20.
	bucket := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC) // Monday of week 20
	got := VariantPath("default.downloads", Tier1w, "sketch", bucket, "xyz")
	want := "precalc/table=default.downloads/tier=1w/year=2026/week=20/sketch/xyz.parquet"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestVariantPath_1mo(t *testing.T) {
	got := VariantPath("default.downloads", Tier1mo, "all",
		time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), "m1")
	want := "precalc/table=default.downloads/tier=1mo/year=2026/month=05/all/m1.parquet"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}
