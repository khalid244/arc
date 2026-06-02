// Package rollup implements workload-driven automatic rollups for Arc.
//
// The design (docs/superpowers/specs/2026-05-30-rollup-rollup-design.md) keeps
// the proven mergeable-aggregate + merge-on-read core of prior rollup generations
// but replaces the two layers that kept failing: it chooses what to materialize
// from the observed query workload (not data sampling, not per-panel registration)
// and matches incoming queries by dimensional coverage (not exact SQL hash).
package rollup

import (
	"fmt"
	"sort"
	"strings"
)

// AggKind enumerates the aggregate functions Rollup can pre-compute and merge.
type AggKind int

const (
	AggCount         AggKind = iota // COUNT(*)
	AggCountCol                     // COUNT(col)
	AggSum                          // SUM(col)
	AggMin                          // MIN(col)
	AggMax                          // MAX(col)
	AggAvg                          // AVG(col)        -> SUM(col)/COUNT(col)
	AggCountDistinct                // COUNT(DISTINCT col) -> Theta sketch (approximate)
	AggPercentile                   // quantile(p, col)    -> KLL sketch (approximate)
	AggCondSum                      // SUM/COUNT(CASE WHEN <pred over dims> THEN x ELSE y END)
)

// Sketch precision constants. Industry defaults; fixed (not per-table tunable)
// because per-table sketch tuning was a source of complexity in prior designs.
// thetaLgK=14 gives ~0.8% std error — matching the prior HLL choice — while Theta
// additionally supports set algebra (union/intersect/difference) across cubes,
// which HLL cannot. Distinct counts stay mergeable exactly as before.
const (
	thetaLgK = 14
	kllK     = 600 // tighter rank error; keeps value error low after sketch merges
)

// Sketches have no native Parquet type, so they round-trip as BLOB. On read they
// must be cast back to the concrete sketch type before re-aggregation.
const (
	thetaType = "sketch_theta"
	kllType   = "sketch_kll_double" // percentiles apply to numeric (double) columns
)

// Aggregate is one output column of an aggregate query.
type Aggregate struct {
	Kind  AggKind
	Col   string  // source column; "" for COUNT(*)
	P     float64 // percentile rank in [0,1]; only for AggPercentile
	Alias string  // output alias

	// Conditional aggregate (AggCondSum) fields — SUM/COUNT over a CASE expression
	// whose WHEN predicate references only stored dimensions, so each cube row
	// (which aggregates _cnt source rows sharing identical dim values) takes a
	// single CASE branch and the sum re-aggregates exactly. See finalExpr.
	Cond      string   // rendered predicate over dims, e.g. `"response" = 200`
	CondCols  []string // dimension columns referenced by Cond (drives coverage)
	ThenCol   string   // metric column when THEN is a column ("" => THEN is a constant)
	ThenK     string   // numeric THEN constant (rendered) when ThenCol==""; e.g. "1"
	ElseK     string   // numeric ELSE constant (rendered); e.g. "0" (NULL else => "0")
	FromCount bool     // COUNT(CASE…) — yields BIGINT; SUM(CASE…) yields HUGEINT
}

// Approximate reports whether this aggregate is served by a sketch (Theta/KLL),
// so the compare harness can apply a >=99% tolerance instead of exact equality.
func (a Aggregate) Approximate() bool {
	return a.Kind == AggCountDistinct || a.Kind == AggPercentile
}

// QueryShape is the structural identity of an incoming aggregate query.
// Filters are captured but are deliberately NOT part of cube identity — coverage
// matching applies them post-aggregation, which is what dissolves the orphan /
// filter-mismatch bug class that killed the exact-hash generation.
type QueryShape struct {
	Source      string      // logical "db.measurement", e.g. "default.downloads"
	TimeCol     string      // time column, e.g. "time"
	Grain       string      // date_trunc unit / "secs:N" / "" = no bucket (total)
	BucketAlias string      // user's alias for the time bucket column ("" = "bucket")
	Dims        []string    // GROUP BY dimensions (excluding the time bucket)
	Aggs        []Aggregate // aggregate output columns
	Filters     []Filter    // WHERE predicates beyond the time range (applied post-agg)
	TimeLo      string      // inclusive lower bound, SQL timestamp literal body (no quotes)
	TimeHi      string      // exclusive upper bound
	Limit       int         // LIMIT n; 0 = no limit
	OrderBy     []OrderKey  // user ORDER BY, resolved to select-list positions; nil = none
}

// OrderKey is one ORDER BY term resolved to a 1-based select-list position, so the
// cube read can reproduce the user's ordering verbatim — essential for TopN
// (ORDER BY <agg> LIMIT n), where ordering by the group columns would return the
// wrong rows.
type OrderKey struct {
	Pos  int  // 1-based position in the emitted select list
	Desc bool // true => DESC
}

// CubeSpec is a materialized cube: a set of stored dims + mergeable counters at a
// fixed time granularity for one source.
type CubeSpec struct {
	Source string      // logical "db.measurement"
	Grain  string      // cube bucket granularity (e.g. "hour")
	Dims   []string    // dimension columns physically stored
	Aggs   []Aggregate // aggregates whose store columns are materialized
}

// FilterOp enumerates the WHERE predicate operators Rollup can re-apply post-agg.
type FilterOp int

const (
	OpEq FilterOp = iota
	OpNe
	OpIn
	OpNotIn
	OpIsNull
	OpIsNotNull
)

// Filter is a single post-aggregation predicate on a dimension column.
type Filter struct {
	Col    string
	Op     FilterOp
	Values []string // literal bodies (unquoted) for Eq/Ne/In/NotIn
}

// --- store-column naming (deterministic) -----------------------------------

func sanitize(col string) string {
	return strings.NewReplacer(".", "_", "\"", "", " ", "_").Replace(col)
}

// storeCols returns the physical cube columns this aggregate needs, each as a
// (name, buildExpr) pair where buildExpr aggregates raw source rows.
func (a Aggregate) storeCols() [][2]string {
	c := sanitize(a.Col)
	switch a.Kind {
	case AggCount:
		return [][2]string{{"_cnt", "count(*)"}}
	case AggCountCol:
		return [][2]string{{"_cnt_" + c, fmt.Sprintf("count(%q)", a.Col)}}
	case AggSum:
		return [][2]string{{"_sum_" + c, fmt.Sprintf("sum(%q)", a.Col)}}
	case AggMin:
		return [][2]string{{"_min_" + c, fmt.Sprintf("min(%q)", a.Col)}}
	case AggMax:
		return [][2]string{{"_max_" + c, fmt.Sprintf("max(%q)", a.Col)}}
	case AggAvg:
		return [][2]string{
			{"_sum_" + c, fmt.Sprintf("sum(%q)", a.Col)},
			{"_cnt_" + c, fmt.Sprintf("count(%q)", a.Col)},
		}
	case AggCountDistinct:
		return [][2]string{{"_theta_" + c, fmt.Sprintf("datasketch_theta(%d, %q)", thetaLgK, a.Col)}}
	case AggPercentile:
		return [][2]string{{"_kll_" + c, fmt.Sprintf("datasketch_kll(%d, %q)", kllK, a.Col)}}
	case AggCondSum:
		// Needs the row count (constant branches scale by _cnt) and, when the THEN
		// is a metric column, that column's sum. Both are already materialized by
		// the cube's plain COUNT/SUM aggs; this adds no new physical columns.
		cols := [][2]string{{"_cnt", "count(*)"}}
		if a.ThenCol != "" {
			tc := sanitize(a.ThenCol)
			cols = append(cols, [2]string{"_sum_" + tc, fmt.Sprintf("sum(%q)", a.ThenCol)})
		}
		return cols
	}
	return nil
}

// mergeExpr returns the expression that merges a store column across cube rows
// (re-aggregation), given the store column name.
func mergeExpr(name string) string {
	switch {
	case name == "_cnt" || strings.HasPrefix(name, "_cnt_"):
		return fmt.Sprintf("sum(%s)", name)
	case strings.HasPrefix(name, "_sum_"):
		return fmt.Sprintf("sum(%s)", name)
	case strings.HasPrefix(name, "_min_"):
		return fmt.Sprintf("min(%s)", name)
	case strings.HasPrefix(name, "_max_"):
		return fmt.Sprintf("max(%s)", name)
	case strings.HasPrefix(name, "_theta_"):
		// Like KLL, the build aggregate also merges a column of sketches.
		return fmt.Sprintf("datasketch_theta(%d, %s::%s)", thetaLgK, name, thetaType)
	case strings.HasPrefix(name, "_kll_"):
		return fmt.Sprintf("datasketch_kll(%d, %s::%s)", kllK, name, kllType)
	}
	return fmt.Sprintf("any_value(%s)", name)
}

// finalExpr returns the read-side expression that turns merged store columns into
// the user-visible aggregate value. Operates on the post-merge alias names.
func (a Aggregate) finalExpr() string {
	c := sanitize(a.Col)
	switch a.Kind {
	case AggCount:
		// Cast to BIGINT so the JSON type matches source COUNT(*) (a BIGINT number);
		// the HUGEINT that sum() yields would serialize as a string and break Grafana.
		return "sum(_cnt)::BIGINT"
	case AggCountCol:
		return fmt.Sprintf("sum(_cnt_%s)::BIGINT", c)
	case AggSum:
		return fmt.Sprintf("sum(_sum_%s)", c)
	case AggMin:
		return fmt.Sprintf("min(_min_%s)", c)
	case AggMax:
		return fmt.Sprintf("max(_max_%s)", c)
	case AggAvg:
		return fmt.Sprintf("sum(_sum_%s)/nullif(sum(_cnt_%s),0)", c, c)
	case AggCountDistinct:
		return fmt.Sprintf("datasketch_theta_estimate(datasketch_theta(%d, _theta_%s::%s))", thetaLgK, c, thetaType)
	case AggPercentile:
		return fmt.Sprintf("datasketch_kll_quantile(datasketch_kll(%d, _kll_%s::%s), %g, true)", kllK, c, kllType, a.P)
	case AggCondSum:
		expr := fmt.Sprintf("sum(CASE WHEN %s THEN %s ELSE %s END)", a.Cond, a.condThenFinal(), a.condElseFinal())
		if a.FromCount {
			// COUNT(CASE…) returns BIGINT at source; the sum() yields HUGEINT (a JSON
			// string). Cast back so the type — and Grafana rendering — matches source.
			return expr + "::BIGINT"
		}
		return expr
	}
	return "NULL"
}

// condThenFinal renders the THEN value against cube store columns: a metric column
// becomes its summed store column; a constant scales by the row count (_cnt rows
// all share the predicate's dim values, so they all take this branch).
func (a Aggregate) condThenFinal() string {
	if a.ThenCol != "" {
		return "_sum_" + sanitize(a.ThenCol)
	}
	if a.ThenK == "1" {
		return "_cnt"
	}
	return "(" + a.ThenK + ")*_cnt"
}

// condElseFinal renders the ELSE constant against cube rows. Zero contributes
// nothing, so it stays a bare literal; any other constant scales by _cnt.
func (a Aggregate) condElseFinal() string {
	if a.ElseK == "0" || a.ElseK == "" {
		return "0"
	}
	return "(" + a.ElseK + ")*_cnt"
}

// orderedStoreCols returns the deduped, deterministically-ordered set of store
// columns for a cube (the physical schema beyond bucket + dims).
func (s CubeSpec) orderedStoreCols() [][2]string {
	seen := map[string]string{}
	for _, a := range s.Aggs {
		for _, sc := range a.storeCols() {
			seen[sc[0]] = sc[1]
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([][2]string, len(names))
	for i, n := range names {
		out[i] = [2]string{n, seen[n]}
	}
	return out
}

// splitSource splits a logical "db.measurement" into (db, measurement),
// defaulting the database to "default" when unqualified.
func splitSource(source string) (string, string) {
	if parts := strings.SplitN(source, ".", 2); len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "default", source
}

// sourceDayGlobs returns the read_parquet path(s) for [lo,hi) day-pruned to the
// YYYY/MM/DD partition layout, so the builder only LISTs relevant date prefixes.
func sourceDayGlobs(s3Bucket, source string, days []string) string {
	db, m := splitSource(source)
	parts := make([]string, len(days))
	for i, d := range days { // d = "YYYY/MM/DD"
		parts[i] = fmt.Sprintf("'s3://%s/%s/%s/%s/**/*.parquet'", s3Bucket, db, m, d)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
