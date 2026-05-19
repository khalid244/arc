package tiered

import (
	"fmt"
	"strings"
)

// ColMode picks which schema world a rewrite fragment targets. Rollup
// files have pre-aggregated columns (`<dim>_class`, `cnt`, `sum_x`,
// `min_x`, `max_x`, sketch blobs). Source files have raw rows.
//
// Every helper that references a dim column or builds an aggregate
// fragment routes through one of these so the open-tail fresh CTE and
// the rollup CTE can't accidentally desync — that desync caused two
// distinct production outages (`site_class` referenced over source,
// `SUM(cnt)` over source).
type ColMode int

const (
	// RollupMode: the SQL fragment reads pre-aggregated rollup parquet.
	RollupMode ColMode = iota
	// SourceMode: the SQL fragment reads raw source rows (the open-tail
	// "fresh" CTE, or any source-scan fallback).
	SourceMode
)

// dimClassExpr returns the SQL expression that yields a dim's
// classified value (matching the rollup builder's classification:
// kept_values stay, NULL → "_null_", everything else → "_other_").
// In RollupMode the column is already classified — just the name.
// In SourceMode the expression synthesises the classification.
func dimClassExpr(mode ColMode, dim string, kept []string) string {
	if mode == RollupMode {
		return dim + "_class"
	}
	if len(kept) == 0 {
		// No kept_values configured — only NULL handling is needed; the
		// rollup builder wouldn't have classified to "_other_" either.
		return fmt.Sprintf("COALESCE(%s, '_null_')", dim)
	}
	quoted := make([]string, len(kept))
	for i, v := range kept {
		quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	return fmt.Sprintf(
		"CASE WHEN %s IS NULL THEN '_null_' WHEN %s IN (%s) THEN %s ELSE '_other_' END",
		dim, dim, strings.Join(quoted, ","), dim)
}

// dimFilterCol returns the column name to use in a WHERE clause for
// `dim`. In RollupMode filters apply to the classified column; in
// SourceMode they apply to the raw column.
func dimFilterCol(mode ColMode, dim string) string {
	if mode == RollupMode {
		return dim + "_class"
	}
	return dim
}

// aggInnerFragment returns the inner-CTE aggregate fragment for the
// given mode. Returns (frag, true) on success, ("", false) when the
// aggregate cannot be expressed in the requested mode (currently:
// sketch-based aggregates in SourceMode have no exact equivalent
// without re-building the sketch, and column-based aggregates need a
// non-empty Column).
//
// Output column names follow the convention `_agg_<idx>` (and
// `_agg_<idx>_sum` / `_agg_<idx>_cnt` for AVG) so the outer SELECT
// stays mode-agnostic.
func aggInnerFragment(mode ColMode, agg Aggregate, idx int) (string, bool) {
	col := agg.Column
	id := fmt.Sprintf("_agg_%d", idx)
	switch agg.Kind {
	case AggCountStar:
		if mode == RollupMode {
			return fmt.Sprintf("SUM(cnt) AS %s", id), true
		}
		return fmt.Sprintf("COUNT(*) AS %s", id), true

	case AggCount:
		if col == "" {
			return "", false
		}
		if mode == RollupMode {
			return fmt.Sprintf("SUM(cnt_%s) AS %s", col, id), true
		}
		return fmt.Sprintf("COUNT(%s) AS %s", col, id), true

	case AggSum:
		if col == "" {
			return "", false
		}
		if mode == RollupMode {
			return fmt.Sprintf("SUM(sum_%s) AS %s", col, id), true
		}
		return fmt.Sprintf("SUM(%s) AS %s", col, id), true

	case AggAvg:
		if col == "" {
			return "", false
		}
		sumID := id + "_sum"
		cntID := id + "_cnt"
		if mode == RollupMode {
			return fmt.Sprintf("SUM(sum_%s) AS %s, SUM(cnt_%s) AS %s",
				col, sumID, col, cntID), true
		}
		return fmt.Sprintf("SUM(%s) AS %s, COUNT(%s) AS %s",
			col, sumID, col, cntID), true

	case AggMin:
		if col == "" {
			return "", false
		}
		if mode == RollupMode {
			return fmt.Sprintf("MIN(min_%s) AS %s", col, id), true
		}
		return fmt.Sprintf("MIN(%s) AS %s", col, id), true

	case AggMax:
		if col == "" {
			return "", false
		}
		if mode == RollupMode {
			return fmt.Sprintf("MAX(max_%s) AS %s", col, id), true
		}
		return fmt.Sprintf("MAX(%s) AS %s", col, id), true

	case AggCountDistinct:
		if mode != RollupMode {
			return "", false
		}
		return fmt.Sprintf(
			"datasketch_hll_union(14, CAST(hll_%s AS sketch_hll)) AS %s", col, id), true

	case AggQuantile:
		if mode != RollupMode {
			return "", false
		}
		return fmt.Sprintf(
			"datasketch_kll(200, CAST(kll_%s AS sketch_kll_double)) AS %s", col, id), true
	}
	return "", false
}
