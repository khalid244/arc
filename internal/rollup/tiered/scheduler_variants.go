package tiered

import "sort"

// variantPlan describes one variant the scheduler should publish per
// (table, tier, bucket). Dim is the column name for per-dim variants;
// empty for sketch and all.
type variantPlan struct {
	Variant string
	Dim     string
}

// variantsForSpec returns the variants the scheduler should publish at a
// given tier for a table, in publish order:
//  1. sketch (cheapest; gates downstream variants if it fails)
//  2. by_<dim> for every dim with role Dim or PerDim AND non-empty kept-set
//  3. all (dim-rich) when at least one Dim has EffectiveCard <= dimRichCap
//
// Per-dim order is alphabetical for deterministic test snapshots.
func variantsForSpec(spec *Spec, dimRichCap int) []variantPlan {
	out := []variantPlan{{Variant: "sketch"}}

	var dimNames []string
	for name, d := range spec.Dims {
		if (d.Role == "Dim" || d.Role == "PerDim") && len(d.KeptValues) > 0 {
			dimNames = append(dimNames, name)
		}
	}
	sort.Strings(dimNames)
	for _, name := range dimNames {
		out = append(out, variantPlan{Variant: "by_" + name, Dim: name})
	}

	hasDimRich := false
	for _, d := range spec.Dims {
		if d.Role == "Dim" && d.EffectiveCard <= dimRichCap {
			hasDimRich = true
			break
		}
	}
	if hasDimRich {
		out = append(out, variantPlan{Variant: "all"})
	}
	return out
}
