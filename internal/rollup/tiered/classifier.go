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
}

// Classify scans the source and emits a Spec.
//
// For each candidate dim column, computes the top-K values whose cumulative
// row count first reaches CoverageThreshold of the total. Those become the
// kept_values. Effective cardinality determines Role: ≤ DimRichCap → Dim;
// otherwise PerDim. (Sketch and Drop roles set later by force_sketch /
// ignore_cols overrides; not handled by classifier.)
func Classify(ctx context.Context, db *sql.DB, opts ClassifyOpts) (Spec, error) {
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

	rows, err := scanAllDimFrequencies(ctx, db, opts.Source, opts.DimColumns)
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

// scanAllDimFrequencies runs ONE SQL query that computes COUNT(*) per
// (dim, value) tuple across all requested dims via GROUPING SETS.
// Returns a map[dimName] -> sorted-desc-by-count list of (val, n).
func scanAllDimFrequencies(ctx context.Context, db *sql.DB, source string, dims []string) (map[string][]dimFreq, error) {
	var selectParts []string
	var groupingSets []string
	for _, dim := range dims {
		selectParts = append(selectParts, fmt.Sprintf("GROUPING(%s) AS %s_g", dim, dim))
		selectParts = append(selectParts, fmt.Sprintf("COALESCE(%s, '_null_') AS %s_v", dim, dim))
		groupingSets = append(groupingSets, "("+dim+")")
	}
	selectParts = append(selectParts, "COUNT(*) AS n")
	sqlText := fmt.Sprintf(`
WITH src AS (%s)
SELECT %s
FROM src
GROUP BY GROUPING SETS (%s)
`,
		source,
		strings.Join(selectParts, ", "),
		strings.Join(groupingSets, ", "),
	)

	qrows, err := db.QueryContext(ctx, sqlText)
	if err != nil {
		return nil, fmt.Errorf("group-sets query: %w", err)
	}
	defer qrows.Close()

	groupings := make([]int64, len(dims))
	nullStrings := make([]sql.NullString, len(dims))
	scanArgs := make([]interface{}, 2*len(dims)+1)
	for i := range dims {
		scanArgs[2*i] = &groupings[i]
		scanArgs[2*i+1] = &nullStrings[i]
	}
	var n int64
	scanArgs[2*len(dims)] = &n

	out := make(map[string][]dimFreq, len(dims))
	for qrows.Next() {
		if err := qrows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		for i, dim := range dims {
			if groupings[i] == 0 {
				out[dim] = append(out[dim], dimFreq{Val: nullStrings[i].String, N: n})
				break
			}
		}
	}
	if err := qrows.Err(); err != nil {
		return nil, err
	}
	for _, dim := range dims {
		sort.Slice(out[dim], func(i, j int) bool { return out[dim][i].N > out[dim][j].N })
	}
	return out, nil
}

// computeKeptValues applies the coverage-threshold rule to a sorted-desc
// freq list and returns the kept values (top-K covering >= threshold).
func computeKeptValues(freqs []dimFreq, threshold float64) []string {
	var total int64
	for _, f := range freqs {
		total += f.N
	}
	if total == 0 {
		return nil
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
