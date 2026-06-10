package rollup

import (
	"fmt"
	"strconv"
	"strings"
)

// bucketExpr renders the time-bucket expression for a given grain over a column.
// An empty grain means "no bucketing" (a grand total), signalled by "".
// A "secs:N" grain (from Grafana's epoch-arithmetic $__timeGroup) re-buckets via
// epoch flooring; named calendar grains use date_trunc. Both are exact over an
// hourly cube because hour buckets are 3600s-aligned and N is a multiple of 3600.
func bucketExpr(grain, col string) string {
	if grain == "" {
		return ""
	}
	if strings.HasPrefix(grain, "secs:") {
		n, err := strconv.Atoi(grain[len("secs:"):])
		if err == nil && n > 0 {
			return fmt.Sprintf("to_timestamp((epoch(%s)::BIGINT // %d) * %d)", col, n, n)
		}
	}
	return fmt.Sprintf("date_trunc('%s', %s)", grain, col)
}

// readParquetFrom renders the FROM target for a read_parquet glob/list arg.
func readParquetFrom(sourceExpr string) string {
	return fmt.Sprintf("read_parquet(%s, union_by_name=true)", sourceExpr)
}

// buildSelectFrom is the aggregation SELECT over an arbitrary FROM target — a
// read_parquet(...) for the normal path, or a temp table holding one already-read
// day so the source is scanned once for every cube (build-speed opt #1). The
// emitted aggregation is identical regardless of the source, so results match.
//
// srcCols is the source's PHYSICAL column set (lower-cased name -> type, from
// describeColumnSet); nil means "assume everything present". Source schemas
// drift over time — sparse event properties come and go — so a spec column from
// the recent-sample profile can be entirely absent from an older period's
// Parquet. Any such column is rendered as a TYPED NULL cast (type from
// spec.ColTypes): a missing dimension groups as NULL (exactly what a raw
// union_by_name source scan returns for those rows) and a missing metric yields
// NULL aggregates — so the output parquet ALWAYS carries the full store schema
// and the build succeeds instead of failing with a Binder Error.
func (s CubeSpec) buildSelectFrom(from, timeCol, lo, hi string, srcCols map[string]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s AS bucket", bucketExpr(s.Grain, fmt.Sprintf("%q", timeCol)))
	dimRef := s.srcColRef(srcCols, "VARCHAR")
	for _, d := range s.Dims {
		if r := dimRef(d); r == quotedColRef(d) {
			fmt.Fprintf(&b, ", %s", r)
		} else {
			fmt.Fprintf(&b, ", %s AS %q", r, d) // drifted dim: typed NULL, aliased to the dim name
		}
	}
	for _, sc := range s.orderedStoreColsRef(s.srcColRef(srcCols, "DOUBLE")) {
		fmt.Fprintf(&b, ", %s AS %s", sc[1], sc[0])
	}
	fmt.Fprintf(&b, " FROM %s", from)
	fmt.Fprintf(&b, " WHERE %q >= TIMESTAMPTZ '%s' AND %q < TIMESTAMPTZ '%s'", timeCol, lo, timeCol, hi)
	// GROUP BY bucket + every dim by position (1..n+1).
	b.WriteString(" GROUP BY 1")
	for i := range s.Dims {
		fmt.Fprintf(&b, ", %d", i+2)
	}
	return b.String()
}

// BuildSelect returns the SELECT that aggregates raw source rows into cube rows
// (bucket, dims..., store-columns...) for [lo,hi). The caller wraps it in a COPY
// to publish a cube Parquet file. lo/hi are TIMESTAMPTZ literal bodies (UTC).
// Renders every spec column as-is (no drift probe — used where no DB is at hand,
// e.g. the merge-on-read source branches emitted at query time).
func (s CubeSpec) BuildSelect(sourceExpr, timeCol, lo, hi string) string {
	return s.buildSelectFrom(readParquetFrom(sourceExpr), timeCol, lo, hi, nil)
}

func copyWrap(selectSQL, dest string) string {
	return fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", selectSQL, dest)
}

// BuildCopyFrom wraps buildSelectFrom in a COPY — used by the single-scan day
// build where `from` is a temp table already populated from the day's source.
// srcCols is the table's probed physical column set (drift -> typed NULLs).
func (s CubeSpec) BuildCopyFrom(from, timeCol, lo, hi, dest string, srcCols map[string]string) string {
	return copyWrap(s.buildSelectFrom(from, timeCol, lo, hi, srcCols), dest)
}

// BuildCopySQL wraps BuildSelect in a COPY to a Parquet file (local path or s3://).
func (s CubeSpec) BuildCopySQL(sourceExpr, timeCol, lo, hi, dest string) string {
	return copyWrap(s.BuildSelect(sourceExpr, timeCol, lo, hi), dest)
}

// buildCopySQLCols is BuildCopySQL with the source's probed physical columns,
// so drifted columns render as typed NULLs (see buildSelectFrom).
func (s CubeSpec) buildCopySQLCols(sourceExpr, timeCol, lo, hi, dest string, srcCols map[string]string) string {
	return copyWrap(s.buildSelectFrom(readParquetFrom(sourceExpr), timeCol, lo, hi, srcCols), dest)
}
