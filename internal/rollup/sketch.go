package rollup

import "fmt"

// MergeSketchExpr returns the SQL fragment that merges sketch values from a
// column (typically a stored sketch column like "user_id__hll") into a single
// sketch. Result is a sketch value that can be passed to EstimateSketchExpr.
//
// The cast (`::sketch_hll`, `::sketch_tdigest_double`) is required because
// sketches come back from Parquet as BLOB and the function resolver won't
// pick the sketch overload from BLOB.
func MergeSketchExpr(column string, fn AggFunction, sk *SketchConfig) string {
	switch fn {
	case AggHLL:
		return fmt.Sprintf("datasketch_hll_union(%d, %s::sketch_hll)", sk.HLLLgK, column)
	case AggTDigest:
		return fmt.Sprintf("datasketch_tdigest(%d, %s::sketch_tdigest_double)", sk.TDigestK, column)
	}
	return ""
}

// EstimateSketchExpr wraps a merged sketch expression in the appropriate
// scalar accessor. For HLL the quantile param is ignored; for t-digest it
// becomes the quantile (0.99 for P99, 0.5 for median, etc.).
func EstimateSketchExpr(mergedExpr string, fn AggFunction, quantile float64) string {
	switch fn {
	case AggHLL:
		return fmt.Sprintf("datasketch_hll_estimate(%s)", mergedExpr)
	case AggTDigest:
		return fmt.Sprintf("datasketch_tdigest_quantile(%s, %g)", mergedExpr, quantile)
	}
	return ""
}
