package tiered

import (
	"fmt"
	"sort"
	"strings"
)

// Tier identifies a precalc bucket size.
type Tier string

const (
	Tier1h  Tier = "1h"
	Tier1d  Tier = "1d"
	Tier1w  Tier = "1w"
	Tier1mo Tier = "1mo"
)

// DateTruncArg returns the `date_trunc` argument for the tier ("hour", "day",
// "week", "month").
func (t Tier) DateTruncArg() string {
	switch t {
	case Tier1h:
		return "hour"
	case Tier1d:
		return "day"
	case Tier1w:
		return "week"
	case Tier1mo:
		return "month"
	}
	return ""
}

// MetricCol describes a numeric column the builder should aggregate.
type MetricCol struct {
	Name    string
	Numeric bool
}

// BuildArgs is the input to SQL generators.
type BuildArgs struct {
	Tier       Tier
	Source     string // "read_parquet('...')" — fully qualified
	TimeColumn string // default "time" if empty
	MetricCols []MetricCol
	HLLCols    []string
	KLLCols    []string
	HLLLgK     int
	KLLk       int
}

func (a BuildArgs) timeCol() string {
	if a.TimeColumn == "" {
		return "time"
	}
	return a.TimeColumn
}

// BuildSketchVariantSQL emits the SQL for the no-dim sketch variant of one
// tier, built from a raw source. Returns a SELECT statement (no COPY wrapper);
// the caller wraps with COPY when ready to write.
func BuildSketchVariantSQL(a BuildArgs) string {
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT\n  date_trunc('%s', %s) AS bucket,\n  COUNT(*) AS cnt", a.Tier.DateTruncArg(), a.timeCol())
	for _, m := range a.MetricCols {
		if !m.Numeric {
			continue
		}
		fmt.Fprintf(&b, ",\n  COUNT(%s) AS cnt_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(%s) AS sum_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(%s * %s) AS sum_sq_%s", m.Name, m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MIN(%s) AS min_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MAX(%s) AS max_%s", m.Name, m.Name)
	}
	for _, c := range a.HLLCols {
		fmt.Fprintf(&b, ",\n  datasketch_hll(%d, %s) AS hll_%s", a.HLLLgK, c, c)
	}
	for _, c := range a.KLLCols {
		fmt.Fprintf(&b, ",\n  datasketch_kll(%d, %s) AS kll_%s", a.KLLk, c, c)
	}
	fmt.Fprintf(&b, "\nFROM %s\nGROUP BY 1", a.Source)
	return b.String()
}

// BuildPerDimVariantSQL emits the SQL for the per-dim variant of a single
// column. Values not in the dim's kept-set become "_OTHER_".
func BuildPerDimVariantSQL(a BuildArgs, spec *Spec, dim string) string {
	dimSpec := spec.Dims[dim]
	keptList := quoteKeptValues(dimSpec.KeptValues)
	classCol := fmt.Sprintf("CASE WHEN COALESCE(%s, '_null_') IN (%s) THEN COALESCE(%s, '_null_') ELSE '_OTHER_' END AS %s_class",
		dim, keptList, dim, dim)

	var b strings.Builder
	fmt.Fprintf(&b, "SELECT\n  date_trunc('%s', %s) AS bucket,\n  %s,\n  COUNT(*) AS cnt",
		a.Tier.DateTruncArg(), a.timeCol(), classCol)
	for _, m := range a.MetricCols {
		if !m.Numeric {
			continue
		}
		fmt.Fprintf(&b, ",\n  COUNT(%s) AS cnt_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(%s) AS sum_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MIN(%s) AS min_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MAX(%s) AS max_%s", m.Name, m.Name)
	}
	for _, c := range a.HLLCols {
		fmt.Fprintf(&b, ",\n  datasketch_hll(%d, %s) AS hll_%s", a.HLLLgK, c, c)
	}
	fmt.Fprintf(&b, "\nFROM %s\nGROUP BY 1, 2", a.Source)
	return b.String()
}

// BuildDimRichVariantSQL emits SQL for the dim-rich variant. Includes only
// dims with Role=Dim AND EffectiveCard <= dimRichCap. No sketches: storage
// bloat is unacceptable across the cross-product. Sketches live in the
// per-dim and sketch variants only.
func BuildDimRichVariantSQL(a BuildArgs, spec *Spec, dimRichCap int) string {
	var dims []string
	for name, dim := range spec.Dims {
		if dim.Role == "Dim" && dim.EffectiveCard <= dimRichCap {
			dims = append(dims, name)
		}
	}
	sort.Strings(dims) // deterministic column order

	var b strings.Builder
	fmt.Fprintf(&b, "SELECT\n  date_trunc('%s', %s) AS bucket", a.Tier.DateTruncArg(), a.timeCol())
	for _, dim := range dims {
		keptList := quoteKeptValues(spec.Dims[dim].KeptValues)
		fmt.Fprintf(&b, ",\n  CASE WHEN COALESCE(%s, '_null_') IN (%s) THEN COALESCE(%s, '_null_') ELSE '_OTHER_' END AS %s_class",
			dim, keptList, dim, dim)
	}
	fmt.Fprintf(&b, ",\n  COUNT(*) AS cnt")
	for _, m := range a.MetricCols {
		if !m.Numeric {
			continue
		}
		fmt.Fprintf(&b, ",\n  COUNT(%s) AS cnt_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(%s) AS sum_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(%s * %s) AS sum_sq_%s", m.Name, m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MIN(%s) AS min_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MAX(%s) AS max_%s", m.Name, m.Name)
	}
	fmt.Fprintf(&b, "\nFROM %s\nGROUP BY ALL", a.Source)
	return b.String()
}

// RollupArgs is the input to roll-up SQL generators (tier → tier+1).
type RollupArgs struct {
	TargetTier  Tier
	SourcePath  string   // path to a single lower-tier parquet (mutually exclusive with SourcePaths)
	SourcePaths []string // paths to multiple lower-tier parquets; when set, SourcePath is ignored
	MetricCols  []MetricCol
	HLLCols     []string
	KLLCols     []string
	HLLLgK      int
	KLLk        int
}

// sourceExpr returns the FROM expression for this RollupArgs.
// When SourcePaths has more than one entry it emits read_parquet([p1, p2, ...], union_by_name=true);
// when SourcePaths has exactly one entry it uses that path; otherwise falls back to SourcePath.
func (a RollupArgs) sourceExpr() string {
	paths := a.SourcePaths
	if len(paths) == 0 && a.SourcePath != "" {
		paths = []string{a.SourcePath}
	}
	if len(paths) == 1 {
		return "read_parquet('" + escapePath(paths[0]) + "')"
	}
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = "'" + escapePath(p) + "'"
	}
	return "read_parquet([" + strings.Join(quoted, ", ") + "], union_by_name=true)"
}

// BuildRollupSketchSQL emits SQL to roll up the sketch variant from one tier
// to the next-coarser tier. Mergeable aggregates compose: SUM(cnt), SUM(sum_x),
// MIN(min_x), MAX(max_x), datasketch_hll merging, datasketch_kll merging.
//
// Critical: sketch BLOBs must be explicitly CAST back to their sketch types
// after parquet round-trip — DuckDB loses the typed wrapper on disk.
func BuildRollupSketchSQL(a RollupArgs) string {
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT\n  date_trunc('%s', bucket) AS bucket,\n  SUM(cnt) AS cnt",
		a.TargetTier.DateTruncArg())
	for _, m := range a.MetricCols {
		if !m.Numeric {
			continue
		}
		fmt.Fprintf(&b, ",\n  SUM(cnt_%s) AS cnt_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(sum_%s) AS sum_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(sum_sq_%s) AS sum_sq_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MIN(min_%s) AS min_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MAX(max_%s) AS max_%s", m.Name, m.Name)
	}
	for _, c := range a.HLLCols {
		fmt.Fprintf(&b, ",\n  datasketch_hll_union(%d, CAST(hll_%s AS sketch_hll)) AS hll_%s",
			a.HLLLgK, c, c)
	}
	for _, c := range a.KLLCols {
		fmt.Fprintf(&b, ",\n  datasketch_kll(%d, CAST(kll_%s AS sketch_kll_double)) AS kll_%s",
			a.KLLk, c, c)
	}
	fmt.Fprintf(&b, "\nFROM %s\nGROUP BY 1", a.sourceExpr())
	return b.String()
}

// BuildRollupPerDimSQL emits SQL to roll up a per-dim variant from one
// tier to the next-coarser tier. The shape mirrors BuildRollupSketchSQL
// but groups by both the bucket and the dim_class column. The source
// path is the parquet at the finer tier; column names mirror the
// per-dim layout: bucket, <dim>_class, cnt, cnt_<m>, sum_<m>, sum_sq_<m>,
// min_<m>, max_<m>, hll_<col>, kll_<col>.
//
// Important: same as BuildRollupSketchSQL, sketch BLOBs must be CAST
// back to their typed forms (sketch_hll, sketch_kll_double) after
// parquet round-trip.
func BuildRollupPerDimSQL(a RollupArgs, dim string) string {
	var b strings.Builder
	classCol := dim + "_class"
	fmt.Fprintf(&b, "SELECT\n  date_trunc('%s', bucket) AS bucket,\n  %s,\n  SUM(cnt) AS cnt",
		a.TargetTier.DateTruncArg(), classCol)
	for _, m := range a.MetricCols {
		if !m.Numeric {
			continue
		}
		fmt.Fprintf(&b, ",\n  SUM(cnt_%s) AS cnt_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(sum_%s) AS sum_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MIN(min_%s) AS min_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MAX(max_%s) AS max_%s", m.Name, m.Name)
	}
	for _, c := range a.HLLCols {
		fmt.Fprintf(&b, ",\n  datasketch_hll_union(%d, CAST(hll_%s AS sketch_hll)) AS hll_%s",
			a.HLLLgK, c, c)
	}
	fmt.Fprintf(&b, "\nFROM %s\nGROUP BY 1, 2", a.sourceExpr())
	return b.String()
}

// BuildRollupDimRichSQL emits SQL to roll up the dim-rich variant. Groups
// by every dim_class column present in the source. Determines the dim
// columns from the spec — Dim role only, EffectiveCard <= dimRichCap.
//
// No sketches in dim-rich (storage cost too high per cross-product row),
// so only mergeable counts/sums/min/max are rolled up.
func BuildRollupDimRichSQL(a RollupArgs, spec *Spec, dimRichCap int) string {
	var dims []string
	for name, d := range spec.Dims {
		if d.Role == "Dim" && d.EffectiveCard <= dimRichCap {
			dims = append(dims, name)
		}
	}
	sort.Strings(dims)
	dimCols := make([]string, len(dims))
	for i, d := range dims {
		dimCols[i] = d + "_class"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "SELECT\n  date_trunc('%s', bucket) AS bucket", a.TargetTier.DateTruncArg())
	for _, c := range dimCols {
		fmt.Fprintf(&b, ",\n  %s", c)
	}
	fmt.Fprintf(&b, ",\n  SUM(cnt) AS cnt")
	for _, m := range a.MetricCols {
		if !m.Numeric {
			continue
		}
		fmt.Fprintf(&b, ",\n  SUM(cnt_%s) AS cnt_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(sum_%s) AS sum_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(sum_sq_%s) AS sum_sq_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MIN(min_%s) AS min_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MAX(max_%s) AS max_%s", m.Name, m.Name)
	}
	fmt.Fprintf(&b, "\nFROM %s\nGROUP BY ALL", a.sourceExpr())
	return b.String()
}

// allVariantDims returns the sorted list of dim column names that appear in any
// GROUPING SETS variant: per-dim dims (Role=Dim|PerDim, non-empty KeptValues)
// union dim-rich dims (Role=Dim, EffectiveCard<=dimRichCap). Sorted for
// determinism; this order matches the GROUPING_ID argument order.
func allVariantDims(spec *Spec, dimRichCap int) []string {
	seen := make(map[string]struct{})
	for name, d := range spec.Dims {
		if (d.Role == "Dim" || d.Role == "PerDim") && len(d.KeptValues) > 0 {
			seen[name] = struct{}{}
		}
		if d.Role == "Dim" && d.EffectiveCard <= dimRichCap {
			seen[name] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// VariantGroupingID returns the GROUPING_ID bitmask value for the given
// variant, given the ordered list of all dims in the GROUPING SETS query
// (as returned by allVariantDims). The bitmask follows DuckDB semantics:
// GROUPING_ID(d0, d1, ..., dn-1) = sum of GROUPING(di) * 2^(n-1-i).
//
// GROUPING(di)=1 when di is NOT in the current grouping set (i.e., NULL),
// GROUPING(di)=0 when di IS in the grouping set.
//
// So:
//   - sketch: no dims in grouping → all bits 1 → bitmask = 2^n - 1
//   - by_<dim>: only that dim is in grouping → all bits 1 except that dim's bit
//   - all: all dim-rich dims in grouping → bits 0 for those dims, 1 for others
//
// Returns -1 when the variant is not valid for the given dims list.
func VariantGroupingID(variant string, allDims []string, spec *Spec, dimRichCap int) int {
	n := len(allDims)
	dimIndex := make(map[string]int, n)
	for i, d := range allDims {
		dimIndex[d] = i
	}

	full := (1 << n) - 1 // all bits = 1 (sketch)

	switch {
	case variant == "sketch":
		return full

	case strings.HasPrefix(variant, "by_"):
		dim := variant[3:]
		idx, ok := dimIndex[dim]
		if !ok {
			return -1
		}
		// dim is grouped (GROUPING=0), rest are not (GROUPING=1)
		// bit for dim[idx] = 2^(n-1-idx)
		return full ^ (1 << (n - 1 - idx))

	case variant == "all":
		// dim-rich dims: Role=Dim, EffectiveCard<=dimRichCap
		bitmask := full
		for name, d := range spec.Dims {
			if d.Role == "Dim" && d.EffectiveCard <= dimRichCap {
				idx, ok := dimIndex[name]
				if !ok {
					continue
				}
				bitmask &^= 1 << (n - 1 - idx)
			}
		}
		return bitmask
	}
	return -1
}

// BuildAllVariantsSQL returns a single SELECT statement that uses GROUPING SETS
// to produce all variants for a bucket in one source scan. The result set has:
//
//   - bucket: time bucket (date_trunc)
//   - variant_id: BIGINT discriminator (GROUPING_ID of all dim class columns)
//   - <dim>_class: VARCHAR (NULL when this dim is not in the current grouping set)
//   - cnt: BIGINT
//   - metric aggregates (same as per per-variant SQL)
//   - hll_<col>: HLL sketch blob
//   - kll_<col>: KLL sketch blob (sketch variant only; NULLed for per-dim/all)
//
// Variant discriminator mapping is given by VariantGroupingID. The caller
// materialises this result and then COPYs filtered subsets to the 12 output files.
//
// Note: per-dim variants do NOT include KLL sketches (same as BuildPerDimVariantSQL).
// dim-rich variant has NO sketches at all (same as BuildDimRichVariantSQL).
// Sketch variant has both HLL and KLL sketches.
// To avoid bloat, sketch columns in per-dim and all rows are set to NULL via a
// CASE WHEN variant_id = <sketch_id> THEN ... ELSE NULL END wrapper — but since
// GROUPING SETS produces separate aggregate rows, the sketch aggregates are
// simply included unconditionally (DuckDB aggregates over whatever rows land
// in that grouping set). This is correct: sketch rows have all dims NULL, so
// the sketch aggregates are computed over all source rows for that bucket.
// Per-dim and dim-rich rows will also have HLL/KLL values — that's fine for
// the materialized table; the COPY filter for each variant selects only the
// columns appropriate for that variant.
func BuildAllVariantsSQL(a BuildArgs, spec *Spec, dimRichCap int) string {
	if a.HLLLgK == 0 {
		a.HLLLgK = 14
	}
	if a.KLLk == 0 {
		a.KLLk = 200
	}

	allDims := allVariantDims(spec, dimRichCap)

	// Collect per-dim dims (non-empty KeptValues) for grouping set construction.
	var perDimDims []string
	for _, name := range allDims {
		d := spec.Dims[name]
		if (d.Role == "Dim" || d.Role == "PerDim") && len(d.KeptValues) > 0 {
			perDimDims = append(perDimDims, name)
		}
	}

	// Collect dim-rich dims.
	var dimRichDims []string
	for _, name := range allDims {
		d := spec.Dims[name]
		if d.Role == "Dim" && d.EffectiveCard <= dimRichCap {
			dimRichDims = append(dimRichDims, name)
		}
	}
	hasDimRich := len(dimRichDims) > 0

	bucketExpr := fmt.Sprintf("date_trunc('%s', %s)", a.Tier.DateTruncArg(), a.timeCol())

	var b strings.Builder

	// SELECT clause
	fmt.Fprintf(&b, "SELECT\n  %s AS bucket", bucketExpr)

	// dim class columns (CASE expressions for classification)
	for _, name := range allDims {
		d := spec.Dims[name]
		keptList := quoteKeptValues(d.KeptValues)
		if keptList == "" {
			// No kept values — just emit the column name for dims that only appear
			// in dim-rich (EffectiveCard<=cap but no KeptValues). Use same CASE
			// pattern but with an empty IN list effectively (unreachable branch).
			// In practice allVariantDims only includes dims with kept values OR
			// dim-rich dims; dim-rich dims must have KeptValues from the classifier.
			// Fallback: treat everything as _OTHER_ when no kept values.
			fmt.Fprintf(&b, ",\n  COALESCE(%s, '_null_') AS %s_class", name, name)
		} else {
			fmt.Fprintf(&b, ",\n  CASE WHEN COALESCE(%s, '_null_') IN (%s) THEN COALESCE(%s, '_null_') ELSE '_OTHER_' END AS %s_class",
				name, keptList, name, name)
		}
	}

	// variant_id discriminator
	if len(allDims) > 0 {
		classColList := make([]string, len(allDims))
		for i, name := range allDims {
			classColList[i] = name + "_class"
		}
		fmt.Fprintf(&b, ",\n  GROUPING_ID(%s) AS variant_id", strings.Join(classColList, ", "))
	} else {
		// No dims at all — only sketch variant. Use 0 as constant discriminator.
		fmt.Fprintf(&b, ",\n  0 AS variant_id")
	}

	// COUNT
	fmt.Fprintf(&b, ",\n  COUNT(*) AS cnt")

	// Metric aggregates (same as sketch variant — most complete set)
	for _, m := range a.MetricCols {
		if !m.Numeric {
			continue
		}
		fmt.Fprintf(&b, ",\n  COUNT(%s) AS cnt_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(%s) AS sum_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(%s * %s) AS sum_sq_%s", m.Name, m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MIN(%s) AS min_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MAX(%s) AS max_%s", m.Name, m.Name)
	}

	// HLL sketches (present in sketch and per-dim variants)
	for _, c := range a.HLLCols {
		fmt.Fprintf(&b, ",\n  datasketch_hll(%d, %s) AS hll_%s", a.HLLLgK, c, c)
	}

	// KLL sketches (present in sketch variant only, but we include them for all
	// rows; the COPY filter for per-dim/all variants simply omits these columns)
	for _, c := range a.KLLCols {
		fmt.Fprintf(&b, ",\n  datasketch_kll(%d, %s) AS kll_%s", a.KLLk, c, c)
	}

	fmt.Fprintf(&b, "\nFROM %s", a.Source)

	// GROUPING SETS clause
	fmt.Fprintf(&b, "\nGROUP BY GROUPING SETS (")

	// sketch grouping set: (bucket)
	fmt.Fprintf(&b, "\n  (%s)", bucketExpr)

	// per-dim grouping sets: (bucket, <dim>_class)
	for _, name := range perDimDims {
		fmt.Fprintf(&b, ",\n  (%s, %s_class)", bucketExpr, name)
	}

	// dim-rich grouping set: (bucket, <all low-card dims>_class)
	// Skip when the dim-rich grouping set is identical to an already-emitted
	// per-dim grouping set. This happens only when there is exactly one
	// dim-rich dim AND that same dim also has a per-dim grouping set.
	// In that case both sets produce the same GROUPING_ID; emitting the
	// duplicate would double every (bucket, class) row in the materialised
	// table. The all-variant COPY reuses the per-dim rows via the shared id.
	if hasDimRich && !dimRichCollidesWithPerDim(perDimDims, dimRichDims) {
		dimRichCols := make([]string, len(dimRichDims))
		for i, name := range dimRichDims {
			dimRichCols[i] = name + "_class"
		}
		fmt.Fprintf(&b, ",\n  (%s, %s)", bucketExpr, strings.Join(dimRichCols, ", "))
	}

	fmt.Fprintf(&b, "\n)")

	return b.String()
}

// dimRichCollidesWithPerDim returns true when the dim-rich grouping set
// (bucket, dimRich[0]_class, ...) is identical to an existing per-dim
// grouping set (bucket, perDim[i]_class). That can only happen when
// dimRich has exactly one entry and that entry also appears in perDims.
// In the common case of multiple dims these grouping sets have different
// cardinalities and different GROUPING_IDs, so the function returns false.
func dimRichCollidesWithPerDim(perDims, dimRich []string) bool {
	if len(dimRich) != 1 {
		return false
	}
	single := dimRich[0]
	for _, p := range perDims {
		if p == single {
			return true
		}
	}
	return false
}

// quoteKeptValues returns a SQL-safe comma-separated list of quoted strings.
func quoteKeptValues(vals []string) string {
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	return strings.Join(out, ", ")
}
