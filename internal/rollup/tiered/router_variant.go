package tiered

import "sort"

// PickVariant returns the name of the smallest precalc variant that can serve
// the query. Returns empty string if no variant qualifies — caller falls back
// to source.
//
// Selection rules, in priority order:
//  1. If shape has no dim filters AND no GROUP BY dims → "sketch"
//  2. If exactly one dim involved (in GROUP BY or filters) → "by_<dim>"
//     (provided spec has a Dim/PerDim role for that column with non-empty
//     kept_values)
//  3. If multiple dims, ALL in dim_rich_dims (role=Dim, effective_card <=
//     dimRichCap) → "all"
//  4. Otherwise → "" (fallback)
//
// Kept-set check: for any filter `col = X`, `col IN (X, Y, ...)`, `col NOT IN`,
// the values must be in dim.KeptValues. If a filter value is in the implicit
// _OTHER_ bucket (i.e., not in kept_values), the rollup can't distinguish it
// from other tail values — return "" so caller falls back to source.
//
// IS NOT NULL doesn't need value-membership check (always covered).
func PickVariant(shape *QueryShape, spec *Spec, dimRichCap int) string {
	involved := involvedDims(shape)

	if len(involved) == 0 {
		return "sketch"
	}

	for _, dim := range involved {
		ds, ok := spec.Dims[dim]
		if !ok {
			return ""
		}
		if fp, hasFilter := shape.Filters[dim]; hasFilter {
			if !keptSetOK(fp, ds) {
				return ""
			}
		}
	}

	if len(involved) == 1 {
		dim := involved[0]
		ds := spec.Dims[dim]
		if ds.Role == "Dim" || ds.Role == "PerDim" {
			return "by_" + dim
		}
		return ""
	}

	for _, dim := range involved {
		ds := spec.Dims[dim]
		if ds.Role != "Dim" || ds.EffectiveCard > dimRichCap {
			return ""
		}
	}
	return "all"
}

// involvedDims returns the union of shape.GroupDims and keys of shape.Filters,
// in a stable order (GroupDims first in declaration order, then any
// filter-only dims sorted alphabetically). Sorting the filter-only tail
// keeps emitted SQL deterministic across runs — without it, Go's map
// iteration randomisation makes the same query produce different SQL
// strings (and therefore different DuckDB plan-cache keys).
func involvedDims(shape *QueryShape) []string {
	seen := make(map[string]bool, len(shape.GroupDims)+len(shape.Filters))
	dims := make([]string, 0, len(shape.GroupDims)+len(shape.Filters))
	for _, d := range shape.GroupDims {
		if !seen[d] {
			seen[d] = true
			dims = append(dims, d)
		}
	}
	filterOnly := make([]string, 0, len(shape.Filters))
	for d := range shape.Filters {
		if !seen[d] {
			seen[d] = true
			filterOnly = append(filterOnly, d)
		}
	}
	sort.Strings(filterOnly)
	dims = append(dims, filterOnly...)
	return dims
}

// keptSetOK returns true when the filter predicate's values are all present in
// the dim's KeptValues. IS NOT NULL always passes.
func keptSetOK(fp FilterPredicate, ds DimSpec) bool {
	if fp.Op == "IS NOT NULL" {
		return true
	}
	kept := make(map[string]bool, len(ds.KeptValues))
	for _, v := range ds.KeptValues {
		kept[v] = true
	}
	for _, v := range fp.Values {
		if !kept[v] {
			return false
		}
	}
	return true
}
