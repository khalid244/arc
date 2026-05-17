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
	TargetTier Tier
	SourcePath string // path to lower-tier parquet
	MetricCols []MetricCol
	HLLCols    []string
	KLLCols    []string
	HLLLgK     int
	KLLk       int
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
	fmt.Fprintf(&b, "\nFROM read_parquet('%s')\nGROUP BY 1", escapePath(a.SourcePath))
	return b.String()
}

// quoteKeptValues returns a SQL-safe comma-separated list of quoted strings.
func quoteKeptValues(vals []string) string {
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	return strings.Join(out, ", ")
}
