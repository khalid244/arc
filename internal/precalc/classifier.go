package precalc

import (
	"context"
	"database/sql"
	"fmt"
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
	for _, dim := range opts.DimColumns {
		kept, err := keptValuesForDim(ctx, db, opts.Source, dim, opts.CoverageThreshold)
		if err != nil {
			return Spec{}, fmt.Errorf("classify dim %q: %w", dim, err)
		}
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

func keptValuesForDim(ctx context.Context, db *sql.DB, source, dim string, threshold float64) ([]string, error) {
	q := fmt.Sprintf(`
WITH src AS (%s),
freq AS (
  SELECT COALESCE(%s, '_null_') AS val, COUNT(*) AS n
  FROM src GROUP BY 1
),
total AS (SELECT SUM(n) AS T FROM freq),
ranked AS (
  SELECT val, n,
    SUM(n) OVER (ORDER BY n DESC ROWS UNBOUNDED PRECEDING) AS cum
  FROM freq
)
SELECT val
FROM ranked
WHERE cum - n < %f * (SELECT T FROM total)
ORDER BY n DESC`, source, dim, threshold)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var vals []string
	for rows.Next() {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		if v.Valid {
			vals = append(vals, v.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Filter empty just in case
	out := vals[:0]
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			out = append(out, v)
		}
	}
	return out, nil
}
