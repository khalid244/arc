package tiered

import (
	"testing"
	"time"
)

// rollupCoversWindow asks: do the given parquet paths (each stamped
// with [bucketLo, bucketHi) by ParseVariantPath) fully tile the
// half-open window [lo, hi)? If false, the rollup has a gap inside
// the user's query window and the emitter must refuse — silently
// returning rollup results from a partial range would undercount.
//
// This is the safety net against bug B in the architecture review:
// "Coverage gaps from skipped builder windows (e.g. SIGSEGV on a
// date) — watermark advances past a hole; reader thinks rollup
// covers it."

func TestRollupCoversWindow_Empty(t *testing.T) {
	got := rollupCoversWindow([]string{}, mustTime("2026-05-14"), mustTime("2026-05-15"))
	if got {
		t.Error("empty file list cannot cover any window")
	}
}

func TestRollupCoversWindow_FullySingleFile(t *testing.T) {
	files := []string{
		"_arc/rollup/db/events/1h/2026/05/14/by_site/f.parquet",
	}
	if !rollupCoversWindow(files, mustTime("2026-05-14"), mustTime("2026-05-15")) {
		t.Error("a single 24h file should cover its own day")
	}
}

func TestRollupCoversWindow_ContiguousMultipleDays(t *testing.T) {
	files := []string{
		"_arc/rollup/db/events/1h/2026/05/14/by_site/a.parquet",
		"_arc/rollup/db/events/1h/2026/05/15/by_site/b.parquet",
		"_arc/rollup/db/events/1h/2026/05/16/by_site/c.parquet",
	}
	if !rollupCoversWindow(files, mustTime("2026-05-14"), mustTime("2026-05-17")) {
		t.Error("three contiguous days should fully tile a 3-day window")
	}
}

func TestRollupCoversWindow_GapBetweenDays_Refuses(t *testing.T) {
	// 5/15 missing — the SIGSEGV-skip scenario.
	files := []string{
		"_arc/rollup/db/events/1h/2026/05/14/by_site/a.parquet",
		"_arc/rollup/db/events/1h/2026/05/16/by_site/b.parquet",
	}
	if rollupCoversWindow(files, mustTime("2026-05-14"), mustTime("2026-05-17")) {
		t.Error("missing 5/15 must produce a gap detection")
	}
}

func TestRollupCoversWindow_WindowStartsBeforeFirstFile(t *testing.T) {
	files := []string{
		"_arc/rollup/db/events/1h/2026/05/15/by_site/a.parquet",
	}
	// Query: 5/14 → 5/16. File covers only 5/15.
	if rollupCoversWindow(files, mustTime("2026-05-14"), mustTime("2026-05-16")) {
		t.Error("must detect gap before first file")
	}
}

func TestRollupCoversWindow_WindowEndsAfterLastFile(t *testing.T) {
	files := []string{
		"_arc/rollup/db/events/1h/2026/05/14/by_site/a.parquet",
	}
	if rollupCoversWindow(files, mustTime("2026-05-14"), mustTime("2026-05-16")) {
		t.Error("must detect gap after last file")
	}
}

func TestRollupCoversWindow_OverlappingFiles_Covered(t *testing.T) {
	// Builder may emit overlapping ranges (e.g. backfill); coverage
	// must still detect full tiling.
	files := []string{
		"_arc/rollup/db/events/1h/2026/05/14/by_site/a.parquet",
		"_arc/rollup/db/events/1h/2026/05/14/by_site/b.parquet", // same window
		"_arc/rollup/db/events/1h/2026/05/15/by_site/c.parquet",
	}
	if !rollupCoversWindow(files, mustTime("2026-05-14"), mustTime("2026-05-16")) {
		t.Error("overlapping files should not break gap detection")
	}
}

func TestRollupCoversWindow_UnparseablePath_Refuses(t *testing.T) {
	// Files we can't introspect can't be vouched for. Conservative
	// behaviour: refuse to vouch for coverage.
	files := []string{"some/random/path.parquet"}
	if rollupCoversWindow(files, mustTime("2026-05-14"), mustTime("2026-05-15")) {
		t.Error("unparseable paths must not assert coverage")
	}
}

func TestRollupCoversWindow_DegenerateWindow_Covered(t *testing.T) {
	// lo == hi is a zero-width window — vacuously covered.
	files := []string{"_arc/rollup/db/events/1h/2026/05/14/by_site/a.parquet"}
	if !rollupCoversWindow(files, mustTime("2026-05-14"), mustTime("2026-05-14")) {
		t.Error("zero-width window is vacuously covered")
	}
}

// mustTime is shared with other tests; redefined-safe.
func mustTimeForCoverage(s string) time.Time {
	return mustTime(s)
}
