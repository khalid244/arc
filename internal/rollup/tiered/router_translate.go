package tiered

import (
	"fmt"
	"strings"
)

// TranslateAggregate maps a user-facing aggregate to the SQL fragments needed
// to compute it from the precalc storage columns. Returns:
//   - innerSelect: the comma-separated SELECT fragment(s) inside the rollup CTE
//     that select from the precalc parquet. May produce multiple columns
//     (e.g., for AVG we need both SUM(sum_x) AND SUM(cnt_x) in the inner).
//   - outerExpr: the expression to use in the outer SELECT that aggregates
//     across the rollup+fresh UNION. Must compose mergeable inner cols correctly.
//   - ok: false if the aggregate can't be translated for this variant
//     (e.g., COUNT(DISTINCT) when variant has no HLL column for that col).
//
// Variant arg is one of "sketch", "by_<col>", "all" — used to decide whether
// sketch columns are available (the "all" variant has none).
//
// The user's OutputAlias (if any) is preserved in the outerExpr via AS clause.
// The OuterExpr field on Aggregate (e.g., "_agg * 100" for AVG*100) wraps
// the inner expression to support nested aggregates.
func TranslateAggregate(a Aggregate, idx int, variant string) (innerSelect, outerExpr string, ok bool) {
	hasSketch := variant != "all"

	inner, outer, translated := translateCore(a, idx, hasSketch)
	if !translated {
		return "", "", false
	}

	if a.OuterExpr != "" {
		outer = "(" + outer + ")"
		outer = strings.ReplaceAll(a.OuterExpr, "_agg", outer)
	}

	if a.OutputAlias != "" {
		outer = outer + " AS " + a.OutputAlias
	}

	return inner, outer, true
}

// TranslateAggregateScalar is like TranslateAggregate but for the scalar
// aggregate path where the inner CTE has no GROUP BY and already reduces all
// rows to one. The outer expression must be a projection, not an aggregate.
func TranslateAggregateScalar(a Aggregate, idx int, variant string) (innerSelect, outerExpr string, ok bool) {
	hasSketch := variant != "all"

	inner, outer, translated := translateCoreScalar(a, idx, hasSketch)
	if !translated {
		return "", "", false
	}

	if a.OuterExpr != "" {
		outer = "(" + outer + ")"
		outer = strings.ReplaceAll(a.OuterExpr, "_agg", outer)
	}

	if a.OutputAlias != "" {
		outer = outer + " AS " + a.OutputAlias
	}

	return inner, outer, true
}

// translateCoreScalar returns inner/outer SQL for the scalar (no-bucket) path.
// The inner CTE aggregates all rows to ONE; the outer simply projects from it.
func translateCoreScalar(a Aggregate, idx int, hasSketch bool) (inner, outer string, ok bool) {
	col := a.Column
	id := fmt.Sprintf("_agg_%d", idx)

	switch a.Kind {
	case AggCountStar:
		inner = fmt.Sprintf("SUM(cnt) AS %s", id)
		outer = fmt.Sprintf("CAST(%s AS BIGINT)", id)
		return inner, outer, true

	case AggCount:
		if col == "" {
			return "", "", false
		}
		inner = fmt.Sprintf("SUM(cnt_%s) AS %s", col, id)
		outer = fmt.Sprintf("CAST(%s AS BIGINT)", id)
		return inner, outer, true

	case AggSum:
		if col == "" {
			return "", "", false
		}
		inner = fmt.Sprintf("SUM(sum_%s) AS %s", col, id)
		outer = id
		return inner, outer, true

	case AggAvg:
		if col == "" {
			return "", "", false
		}
		sumID := id + "_sum"
		cntID := id + "_cnt"
		inner = fmt.Sprintf("SUM(sum_%s) AS %s, SUM(cnt_%s) AS %s", col, sumID, col, cntID)
		outer = fmt.Sprintf("%s / NULLIF(%s, 0)", sumID, cntID)
		return inner, outer, true

	case AggMin:
		if col == "" {
			return "", "", false
		}
		inner = fmt.Sprintf("MIN(min_%s) AS %s", col, id)
		outer = id
		return inner, outer, true

	case AggMax:
		if col == "" {
			return "", "", false
		}
		inner = fmt.Sprintf("MAX(max_%s) AS %s", col, id)
		outer = id
		return inner, outer, true

	case AggCountDistinct:
		if !hasSketch {
			return "", "", false
		}
		inner = fmt.Sprintf("datasketch_hll_union(14, CAST(hll_%s AS sketch_hll)) AS %s", col, id)
		outer = fmt.Sprintf("CAST(datasketch_hll_estimate(%s) AS BIGINT)", id)
		return inner, outer, true

	case AggQuantile:
		if !hasSketch {
			return "", "", false
		}
		inner = fmt.Sprintf("datasketch_kll(200, CAST(kll_%s AS sketch_kll_double)) AS %s", col, id)
		outer = fmt.Sprintf("datasketch_kll_quantile(%s, %v::DOUBLE, false)", id, a.Quantile)
		return inner, outer, true
	}

	return "", "", false
}

// translateCore returns the raw inner and outer SQL without alias or wrapper.
func translateCore(a Aggregate, idx int, hasSketch bool) (inner, outer string, ok bool) {
	col := a.Column
	id := fmt.Sprintf("_agg_%d", idx)

	switch a.Kind {
	case AggCountStar:
		inner = fmt.Sprintf("SUM(cnt) AS %s", id)
		outer = fmt.Sprintf("CAST(SUM(%s) AS BIGINT)", id)
		return inner, outer, true

	case AggCount:
		if col == "" {
			return "", "", false
		}
		inner = fmt.Sprintf("SUM(cnt_%s) AS %s", col, id)
		outer = fmt.Sprintf("CAST(SUM(%s) AS BIGINT)", id)
		return inner, outer, true

	case AggSum:
		if col == "" {
			return "", "", false
		}
		inner = fmt.Sprintf("SUM(sum_%s) AS %s", col, id)
		outer = fmt.Sprintf("SUM(%s)", id)
		return inner, outer, true

	case AggAvg:
		if col == "" {
			return "", "", false
		}
		sumID := id + "_sum"
		cntID := id + "_cnt"
		inner = fmt.Sprintf("SUM(sum_%s) AS %s, SUM(cnt_%s) AS %s", col, sumID, col, cntID)
		outer = fmt.Sprintf("SUM(%s) / NULLIF(SUM(%s), 0)", sumID, cntID)
		return inner, outer, true

	case AggMin:
		if col == "" {
			return "", "", false
		}
		inner = fmt.Sprintf("MIN(min_%s) AS %s", col, id)
		outer = fmt.Sprintf("MIN(%s)", id)
		return inner, outer, true

	case AggMax:
		if col == "" {
			return "", "", false
		}
		inner = fmt.Sprintf("MAX(max_%s) AS %s", col, id)
		outer = fmt.Sprintf("MAX(%s)", id)
		return inner, outer, true

	case AggCountDistinct:
		if !hasSketch {
			return "", "", false
		}
		inner = fmt.Sprintf("datasketch_hll_union(14, CAST(hll_%s AS sketch_hll)) AS %s", col, id)
		outer = fmt.Sprintf("CAST(datasketch_hll_estimate(datasketch_hll_union(14, CAST(%s AS sketch_hll))) AS BIGINT)", id)
		return inner, outer, true

	case AggQuantile:
		if !hasSketch {
			return "", "", false
		}
		inner = fmt.Sprintf("datasketch_kll(200, CAST(kll_%s AS sketch_kll_double)) AS %s", col, id)
		outer = fmt.Sprintf("datasketch_kll_quantile(datasketch_kll(200, CAST(%s AS sketch_kll_double)), %v::DOUBLE, false)", id, a.Quantile)
		return inner, outer, true
	}

	return "", "", false
}
