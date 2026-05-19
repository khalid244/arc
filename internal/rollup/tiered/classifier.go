package tiered

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// ClassifyOpts is the input to Classify.
type ClassifyOpts struct {
	Source            string   // SQL fragment for the source data (e.g., "read_parquet(...)")
	TimeColumn        string   // name of TIMESTAMPTZ column
	DimColumns        []string // candidate dim columns (everything classifier should examine)
	CoverageThreshold float64
	DimRichCap        int
	Table             string
	TZ                string
	MemoryLimit       string // e.g., "8GB"; default "" leaves session unchanged
}

// Classify scans the source and emits a Spec.
//
// For each candidate dim column, computes the top-K values whose cumulative
// row count first reaches CoverageThreshold of the total. Those become the
// kept_values. Effective cardinality determines Role: ≤ DimRichCap → Dim;
// otherwise PerDim. (Sketch and Drop roles set later by force_sketch /
// ignore_cols overrides; not handled by classifier.)
func Classify(ctx context.Context, db *sql.DB, opts ClassifyOpts) (Spec, error) {
	if opts.MemoryLimit != "" {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET memory_limit = '%s'", opts.MemoryLimit)); err != nil {
			return Spec{}, fmt.Errorf("set memory_limit: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, "PRAGMA enable_object_cache"); err != nil {
		// not fatal
	}
	if _, err := db.ExecContext(ctx, "SET preserve_insertion_order = false"); err != nil {
		// not fatal
	}

	spec := Spec{
		Table:             opts.Table,
		TZ:                opts.TZ,
		TimeColumn:        opts.TimeColumn,
		Dims:              make(map[string]DimSpec, len(opts.DimColumns)),
		CoverageThreshold: opts.CoverageThreshold,
	}
	if len(opts.DimColumns) == 0 {
		return spec, nil
	}

	rows, err := scanAllDimFrequencies(ctx, db, opts.Source, opts.DimColumns, 0)
	if err != nil {
		return Spec{}, fmt.Errorf("scan dim frequencies: %w", err)
	}

	for _, dim := range opts.DimColumns {
		kept := computeKeptValues(rows[dim], opts.CoverageThreshold)
		role := "Dim"
		if len(kept) > opts.DimRichCap {
			role = "PerDim"
		}
		spec.Dims[dim] = DimSpec{
			Role:          role,
			KeptValues:    kept,
			EffectiveCard: len(kept),
		}
	}
	return spec, nil
}

// dimFreq is the per-value frequency for one dim.
type dimFreq struct {
	Val string
	N   int64
}

// scanAllDimFrequencies uses datasketch_frequent_items to get top values with
// their estimated counts per dim in a single table scan. The topK parameter is
// unused (kept for API symmetry; the sketch naturally returns all frequent items).
// Returns a map[dimName] -> sorted-desc-by-count list of (val, n).
func scanAllDimFrequencies(ctx context.Context, db *sql.DB, source string, dims []string, topK int) (map[string][]dimFreq, error) {
	// Source columns may contain characters that aren't valid in bare SQL
	// identifiers (e.g. PostHog has `feature/promoteSubscribeNow`). Quote
	// the reference with double-quotes and use a sanitised position-based
	// AS-name so the projection always parses. We map back to dim names by
	// column index at scan time.
	var selects []string
	for i, dim := range dims {
		quoted := `"` + strings.ReplaceAll(dim, `"`, `""`) + `"`
		selects = append(selects, fmt.Sprintf(
			"datasketch_frequent_items_get_frequent(datasketch_frequent_items(COALESCE(%s, '_null_')), 'NO_FALSE_NEGATIVES') AS topk_%d",
			quoted, i,
		))
	}
	q := fmt.Sprintf("WITH src AS (%s) SELECT %s FROM src", source, strings.Join(selects, ", "))

	row := db.QueryRowContext(ctx, q)
	raw := make([]interface{}, len(dims))
	ptrs := make([]interface{}, len(dims))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	if err := row.Scan(ptrs...); err != nil {
		return nil, fmt.Errorf("approx_top_k query: %w", err)
	}

	out := make(map[string][]dimFreq, len(dims))
	for i, dim := range dims {
		lst, _ := raw[i].([]interface{})
		freqs := make([]dimFreq, 0, len(lst))
		for _, elem := range lst {
			m, ok := elem.(map[string]interface{})
			if !ok {
				continue
			}
			item, _ := m["item"].(string)
			est, _ := m["estimate"].(int64)
			freqs = append(freqs, dimFreq{Val: item, N: est})
		}
		sort.Slice(freqs, func(a, b int) bool { return freqs[a].N > freqs[b].N })
		out[dim] = freqs
	}
	return out, nil
}

// keepAllUnderDistinct: columns with at most this many distinct values keep
// ALL of them in the spec, regardless of coverage_threshold. This protects
// low-cardinality enumerations (e.g., a `vpn` column with values true/false/null)
// from having minority values dropped just because one value covers > threshold.
const keepAllUnderDistinct = 20

// computeKeptValues applies the coverage-threshold rule to a sorted-desc
// freq list and returns the kept values (top-K covering >= threshold).
// Columns with ≤ keepAllUnderDistinct distinct values bypass the threshold
// and keep every non-empty value — small enumerations are a complete catalog.
func computeKeptValues(freqs []dimFreq, threshold float64) []string {
	var total int64
	for _, f := range freqs {
		total += f.N
	}
	if total == 0 {
		return nil
	}
	if len(freqs) <= keepAllUnderDistinct {
		var out []string
		for _, f := range freqs {
			if strings.TrimSpace(f.Val) != "" {
				out = append(out, f.Val)
			}
		}
		return out
	}
	cutoff := int64(float64(total) * threshold)
	var cum int64
	var out []string
	for _, f := range freqs {
		if cum < cutoff {
			if strings.TrimSpace(f.Val) != "" {
				out = append(out, f.Val)
			}
		}
		cum += f.N
	}
	return out
}
