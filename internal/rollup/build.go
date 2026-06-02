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

// quoteDims renders dimension columns as a comma-prefixed SQL fragment, each
// double-quoted. Returns "" for no dims.
func quoteDims(dims []string) string {
	if len(dims) == 0 {
		return ""
	}
	q := make([]string, len(dims))
	for i, d := range dims {
		q[i] = fmt.Sprintf("%q", d)
	}
	return ", " + strings.Join(q, ", ")
}

// readParquetFrom renders the FROM target for a read_parquet glob/list arg.
func readParquetFrom(sourceExpr string) string {
	return fmt.Sprintf("read_parquet(%s, union_by_name=true)", sourceExpr)
}

// buildSelectFrom is the aggregation SELECT over an arbitrary FROM target — a
// read_parquet(...) for the normal path, or a temp table holding one already-read
// day so the source is scanned once for every cube (build-speed opt #1). The
// emitted aggregation is identical regardless of the source, so results match.
func (s CubeSpec) buildSelectFrom(from, timeCol, lo, hi string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s AS bucket", bucketExpr(s.Grain, fmt.Sprintf("%q", timeCol)))
	b.WriteString(quoteDims(s.Dims))
	for _, sc := range s.orderedStoreCols() {
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
func (s CubeSpec) BuildSelect(sourceExpr, timeCol, lo, hi string) string {
	return s.buildSelectFrom(readParquetFrom(sourceExpr), timeCol, lo, hi)
}

func copyWrap(selectSQL, dest string) string {
	return fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", selectSQL, dest)
}

// BuildCopyFrom wraps buildSelectFrom in a COPY — used by the single-scan day
// build where `from` is a temp table already populated from the day's source.
func (s CubeSpec) BuildCopyFrom(from, timeCol, lo, hi, dest string) string {
	return copyWrap(s.buildSelectFrom(from, timeCol, lo, hi), dest)
}

// BuildCopySQL wraps BuildSelect in a COPY to a Parquet file (local path or s3://).
func (s CubeSpec) BuildCopySQL(sourceExpr, timeCol, lo, hi, dest string) string {
	return copyWrap(s.BuildSelect(sourceExpr, timeCol, lo, hi), dest)
}
