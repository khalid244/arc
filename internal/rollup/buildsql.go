package rollup

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// validIdentifier matches DuckDB-safe column/table identifiers.
var validIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// BuildWindowSQL generates the SELECT that populates the rollup table for the
// half-open window [windowStart, windowEnd).
//
// fromTable is the read source: the original table or its read_parquet glob.
// The caller is responsible for resolving the table-name SQL expression.
func BuildWindowSQL(spec RollupSpec, fromTable string, windowStart, windowEnd time.Time) (string, error) {
	if !validIdentifier.MatchString(spec.BucketColumn) {
		return "", fmt.Errorf("invalid bucket_column %q", spec.BucketColumn)
	}
	for _, d := range spec.KeepDimensions {
		if !validIdentifier.MatchString(d) {
			return "", fmt.Errorf("invalid keep dimension %q", d)
		}
	}
	for _, a := range spec.Aggregations {
		if !validIdentifier.MatchString(a.SourceColumn) {
			return "", fmt.Errorf("invalid aggregation column %q", a.SourceColumn)
		}
	}
	if windowEnd.Before(windowStart) || windowEnd.Equal(windowStart) {
		return "", errors.New("windowEnd must be > windowStart")
	}
	bucketSecs := int64(spec.BucketInterval / time.Second)
	if bucketSecs == 0 {
		return "", errors.New("bucket_interval must be at least one second")
	}

	var sb strings.Builder
	sb.WriteString("SELECT\n  time_bucket(INTERVAL '")
	fmt.Fprintf(&sb, "%d seconds', %s) AS bucket", bucketSecs, spec.BucketColumn)
	for _, d := range spec.KeepDimensions {
		fmt.Fprintf(&sb, ",\n  %s", d)
	}
	fmt.Fprintf(&sb, ",\n  COUNT(*) AS __row_count")

	for _, agg := range spec.Aggregations {
		for _, fn := range agg.Functions {
			expr, alias, err := aggExpression(agg.SourceColumn, fn, agg.SketchConfig)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(&sb, ",\n  %s AS %s", expr, alias)
		}
	}

	fmt.Fprintf(&sb, "\nFROM %s\n", fromTable)
	fmt.Fprintf(&sb, "WHERE %s >= TIMESTAMP '%s'\n",
		spec.BucketColumn, windowStart.UTC().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(&sb, "  AND %s < TIMESTAMP '%s'\n",
		spec.BucketColumn, windowEnd.UTC().Format("2006-01-02 15:04:05"))

	sb.WriteString("GROUP BY 1")
	for i := range spec.KeepDimensions {
		fmt.Fprintf(&sb, ", %s", spec.KeepDimensions[i])
	}

	return sb.String(), nil
}

// BuildSourceEdgeSelect returns a SELECT that produces ROLLUP-SHAPED rows
// (same projection as a freshly-built rollup window) from a SOURCE table for
// the given edge slices. It mirrors BuildWindowSQL's projection so the
// result can be UNION ALL'd with a rollup-branch read of the same shape.
//
// Difference from BuildWindowSQL:
//   - The bucket column is projected as `MIN(<bucket_col>) AS <bucket_col>`
//     (NOT bucket-start). This guarantees the projected `time` value is
//     within the user's filter range — the user's outer WHERE is preserved
//     verbatim and still excludes nothing it shouldn't.
//   - The WHERE is the operator-supplied edge clause (covers one or more
//     [Lo, Hi) slices), not a single half-open window.
//   - GROUP BY uses time_bucket on the source column so per-bucket
//     subtotals are produced even though the projection emits MIN(time).
//
// The caller (the rewriter's hybrid path) is responsible for ensuring the
// source has TIMESTAMPTZ time semantics matching the rollup (so that the
// projected MIN(time) and the rollup-branch's `bucket AS time` resolve to
// the same TZ space — see SET TimeZone='UTC' on every Arc connection).
func BuildSourceEdgeSelect(spec RollupSpec, fromTable string, edgeWhere string, dims []string, aggs []Aggregation) (string, error) {
	if !validIdentifier.MatchString(spec.BucketColumn) {
		return "", fmt.Errorf("invalid bucket_column %q", spec.BucketColumn)
	}
	for _, d := range dims {
		if !validIdentifier.MatchString(d) {
			return "", fmt.Errorf("invalid keep dimension %q", d)
		}
	}
	bucketSecs := int64(spec.BucketInterval / time.Second)
	if bucketSecs == 0 {
		return "", errors.New("bucket_interval must be at least one second")
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "SELECT MIN(%s) AS %s", spec.BucketColumn, spec.BucketColumn)
	for _, d := range dims {
		fmt.Fprintf(&sb, ", %s", d)
	}
	sb.WriteString(", COUNT(*) AS __row_count")
	for _, agg := range aggs {
		for _, fn := range agg.Functions {
			expr, alias, err := aggExpression(agg.SourceColumn, fn, agg.SketchConfig)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(&sb, ", %s AS %s", expr, alias)
		}
	}
	fmt.Fprintf(&sb, " FROM %s WHERE %s", fromTable, edgeWhere)
	fmt.Fprintf(&sb, " GROUP BY time_bucket(INTERVAL '%d seconds', %s)", bucketSecs, spec.BucketColumn)
	for _, d := range dims {
		fmt.Fprintf(&sb, ", %s", d)
	}
	return sb.String(), nil
}

// aggExpression returns (sql_expression, alias) for one (column, function) pair
// when reading from a SOURCE table. Sketch functions emit datasketch_* SQL that
// requires the datasketches DuckDB extension to be loaded.
func aggExpression(col string, fn AggFunction, sk *SketchConfig) (string, string, error) {
	switch fn {
	case AggSum:
		return fmt.Sprintf("SUM(%s)", col), col + "__sum", nil
	case AggCount:
		return fmt.Sprintf("COUNT(%s)", col), col + "__count", nil
	case AggMin:
		return fmt.Sprintf("MIN(%s)", col), col + "__min", nil
	case AggMax:
		return fmt.Sprintf("MAX(%s)", col), col + "__max", nil
	case AggHLL:
		if sk == nil {
			return "", "", fmt.Errorf("HLL aggregation on %q requires SketchConfig", col)
		}
		return fmt.Sprintf("datasketch_hll(%d, %s)", sk.HLLLgK, col), col + "__hll", nil
	case AggTDigest:
		if sk == nil {
			return "", "", fmt.Errorf("tdigest aggregation on %q requires SketchConfig", col)
		}
		return fmt.Sprintf("datasketch_tdigest(%d, %s)", sk.TDigestK, col), col + "__tdigest", nil
	}
	return "", "", fmt.Errorf("unknown aggregation function %q", fn)
}
