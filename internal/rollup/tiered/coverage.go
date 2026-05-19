package tiered

import (
	"sort"
	"time"
)

// rollupCoversWindow returns true when the given parquet paths fully
// tile [lo, hi). It parses each path's stamped [bucketLo, bucketHi)
// via ParseVariantPath and walks the intervals in order, refusing
// when any gap exists in the requested window.
//
// Why: the existing watermark-only logic in PickTier ("does the
// max bucketHi reach beyond TimeHi?") cannot detect HOLES in the
// rollup history — a SIGSEGV that skipped 3/11 leaves 3/10 and 3/12
// files; the watermark says 3/12 so PickTier returns "fully
// covered"; the read silently undercounts 3/11. This helper makes
// holes detectable, so the emitter can refuse with a loud fallback
// to source.
//
// Behaviour:
//   - lo >= hi (degenerate window) → true (vacuously covered)
//   - empty file list → false
//   - any path that ParseVariantPath can't read → false
//     (conservative: don't vouch for files we can't introspect)
//   - intervals may overlap; only gaps inside [lo, hi) cause a false
func rollupCoversWindow(paths []string, lo, hi time.Time) bool {
	if !lo.Before(hi) {
		return true
	}
	if len(paths) == 0 {
		return false
	}

	type interval struct{ lo, hi time.Time }
	intervals := make([]interval, 0, len(paths))
	for _, p := range paths {
		_, _, _, bLo, bHi, ok := ParseVariantPath(p)
		if !ok {
			return false
		}
		intervals = append(intervals, interval{bLo, bHi})
	}
	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].lo.Before(intervals[j].lo)
	})

	// Walk a moving "covered up to" cursor starting at lo. Each
	// interval extends the cursor if it overlaps the current
	// position; a gap (first interval starting after cursor) → false.
	cursor := lo
	for _, iv := range intervals {
		if iv.lo.After(cursor) {
			return false
		}
		if iv.hi.After(cursor) {
			cursor = iv.hi
		}
		if !cursor.Before(hi) {
			return true
		}
	}
	return !cursor.Before(hi)
}
