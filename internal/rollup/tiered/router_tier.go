package tiered

import "time"

var tiersCoarseToFine = []Tier{Tier1mo, Tier1w, Tier1d, Tier1h}

var bucketArgRank = map[string]int{
	"":      99, // scalar aggregate (no date_trunc) — all tiers viable
	"hour":  0,
	"day":   1,
	"week":  2,
	"month": 3,
}

func (t Tier) rank() int {
	switch t {
	case Tier1h:
		return 0
	case Tier1d:
		return 1
	case Tier1w:
		return 2
	case Tier1mo:
		return 3
	}
	return -1
}

// PickTier returns the coarsest viable tier for the user query plus the
// boundary at which the open tail kicks in.
//
// Inputs:
//   - shape: the parsed QueryShape (has BucketArg, TimeLo, TimeHi)
//   - manifest: source of truth for watermarks per (tier, variant)
//   - variant: name of the variant we're picking the tier of (e.g., "sketch", "by_country", "all")
//   - graceWindow: how stale a watermark may be before it disqualifies a tier
//
// "Viable" means:
//  1. Tier bucket size DIVIDES the user's date_trunc argument (you can roll up
//     finer buckets to coarser, never the reverse). Concretely:
//     BucketArg=""      → all tiers (scalar aggregate, no date_trunc)
//     BucketArg="hour"  → 1h only
//     BucketArg="day"   → 1h, 1d
//     BucketArg="week"  → 1h, 1d, 1w
//     BucketArg="month" → 1h, 1d, 1w, 1mo
//  2. Tier watermark exists AND is >= shape.TimeLo (so the tier covers at least
//     the start of the user's range). If the watermark is below TimeLo, the
//     tier is useless for this query.
//
// Selection: pick the COARSEST viable tier (smallest row count).
//
// Returns:
//   - chosen tier
//   - tailLo: the open-tail boundary. The router reads precalc for
//     [shape.TimeLo, tailLo) and the next finer tier (or raw) for
//     [tailLo, shape.TimeHi).
//   - If watermark + graceWindow >= shape.TimeHi: tailLo == shape.TimeHi (no open tail)
//   - Else: tailLo == watermark (use finer tier from there)
//   - ok: false when no tier qualifies (caller falls back to source)
func PickTier(shape *QueryShape, manifest *Manifest, variant string, graceWindow time.Duration) (Tier, time.Time, bool) {
	userRank, ok := bucketArgRank[shape.BucketArg]
	if !ok {
		return Tier(""), time.Time{}, false
	}

	// For scalar aggregates (BucketArg==""), the emitter cannot handle an open
	// tail. Do a dedicated pass looking for the coarsest tier that has FULL
	// coverage (watermark + grace >= timeHi). If none exists, the finest tier is
	// returned with open tail (emitter will refuse it, falling back to source).
	if shape.BucketArg == "" && !shape.TimeHi.IsZero() {
		for _, tier := range tiersCoarseToFine {
			wm := manifest.Watermark(string(tier), variant)
			if wm.IsZero() || wm.Before(shape.TimeLo) {
				continue
			}
			if !wm.Add(graceWindow).Before(shape.TimeHi) {
				return tier, shape.TimeHi, true
			}
		}
		// No tier has full coverage. Try finest tier — emitter will refuse open tail.
		for i := len(tiersCoarseToFine) - 1; i >= 0; i-- {
			tier := tiersCoarseToFine[i]
			wm := manifest.Watermark(string(tier), variant)
			if !wm.IsZero() && !wm.Before(shape.TimeLo) {
				return tier, wm, true
			}
		}
		return Tier(""), time.Time{}, false
	}

	for _, tier := range tiersCoarseToFine {
		if tier.rank() > userRank {
			continue
		}
		wm := manifest.Watermark(string(tier), variant)
		if wm.IsZero() || wm.Before(shape.TimeLo) {
			continue
		}
		if !wm.Add(graceWindow).Before(shape.TimeHi) {
			return tier, shape.TimeHi, true
		}
		return tier, wm, true
	}
	return Tier(""), time.Time{}, false
}
