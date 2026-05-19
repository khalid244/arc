package tiered

import (
	"context"
	"time"
)

// PickTier returns the 1h tier and the open-tail boundary for the user query.
// Since the rollup system stores only 1h files, this reduces to a watermark
// check.
//
// Returns:
//   - Tier1h on success, "" on failure
//   - tailLo: shape.TimeHi when watermark + grace covers the full window
//     (no open tail), watermark itself otherwise (router uses source for
//     [tailLo, TimeHi)).
//   - ok=false when there is no usable watermark or rollup coverage starts
//     after shape.TimeLo (the rewrite would silently under-count earlier rows).
func PickTier(ctx context.Context, shape *QueryShape, files FileIndex, variant string, graceWindow time.Duration) (Tier, time.Time, bool) {
	wm, wmOk, err := files.Watermark(ctx, string(Tier1h), variant)
	if err != nil || !wmOk || wm.Before(shape.TimeLo) {
		return Tier(""), time.Time{}, false
	}
	if !shape.TimeLo.IsZero() {
		if earliest, eoOk, eoErr := files.EarliestBucketLo(ctx, string(Tier1h), variant); eoErr == nil && eoOk && earliest.After(shape.TimeLo) {
			return Tier(""), time.Time{}, false
		}
	}
	// Scalar aggregates (BucketArg=="") cannot tolerate an open tail in the
	// emitter — require full coverage and refuse otherwise.
	if shape.BucketArg == "" && !shape.TimeHi.IsZero() {
		if wm.Add(graceWindow).Before(shape.TimeHi) {
			return Tier(""), time.Time{}, false
		}
		return Tier1h, shape.TimeHi, true
	}
	if !wm.Add(graceWindow).Before(shape.TimeHi) {
		return Tier1h, shape.TimeHi, true
	}
	return Tier1h, wm, true
}
