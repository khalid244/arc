// Package guards holds the three rewrite-safety gates the Rewrite entry
// point in rewriter.go consults before emitting merge-on-read SQL.
package rollup

import (
	"strings"
	"time"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// TimeRange is the half-open [Lo, Hi) interval recovered from a WHERE clause's
// top-level time predicates. The picker uses Lo/Hi to decide bucket coverage;
// the emitter uses them to build the rollup prefilter.
type TimeRange struct {
	Lo time.Time
	Hi time.Time
}

// translatableAggFuncs is the set of aggregate names we can rewrite onto a
// rollup variant. AllAggregatesTranslatable accepts only names from this set
// (plus approx_count_distinct, which is translatable via HLL).
var translatableAggFuncs = map[string]bool{
	"count":                 true,
	"sum":                   true,
	"avg":                   true,
	"min":                   true,
	"max":                   true,
	"approx_count_distinct": true,
	"percentile_cont":       true,
	"quantile_cont":         true,
}

// knownDuckDBAggregates is a deny-list of DuckDB aggregate function names we
// recognize but do NOT translate. A query that calls one of these silently
// passes per-bucket subtotal rows through the un-rewritten aggregate and
// returns wrong values, so we refuse the rewrite when one is found.
//
// Sourced from DuckDB's aggregate function reference. Keep in sync when
// DuckDB adds new aggregates.
var knownDuckDBAggregates = map[string]bool{
	"countif":            true,
	"fsum":               true,
	"sumkahan":           true,
	"kahan_sum":          true,
	"favg":               true,
	"first":              true,
	"last":               true,
	"any_value":          true,
	"arbitrary":          true,
	"arg_min":            true,
	"argmin":             true,
	"min_by":             true,
	"arg_min_null":       true,
	"arg_max":            true,
	"argmax":             true,
	"max_by":             true,
	"arg_max_null":       true,
	"list":               true,
	"array_agg":          true,
	"string_agg":         true,
	"group_concat":       true,
	"listagg":            true,
	"bit_and":            true,
	"bit_or":             true,
	"bit_xor":            true,
	"bitstring_agg":      true,
	"bool_and":           true,
	"bool_or":            true,
	"product":            true,
	"geometric_mean":     true,
	"geomean":            true,
	"weighted_avg":       true,
	"wavg":               true,
	"histogram":          true,
	"histogram_exact":    true,
	"approx_quantile":    true,
	"approx_top_k":       true,
	"reservoir_quantile": true,
	"corr":               true,
	"covar_pop":          true,
	"covar_samp":         true,
	"entropy":            true,
	"kurtosis":           true,
	"kurtosis_pop":       true,
	"mad":                true,
	"median":             true,
	"mode":               true,
	"quantile":           true,
	"quantile_disc":      true,
	"percentile_disc":    true,
	"regr_avgx":          true,
	"regr_avgy":          true,
	"regr_count":         true,
	"regr_intercept":     true,
	"regr_r2":            true,
	"regr_slope":         true,
	"regr_sxx":           true,
	"regr_sxy":           true,
	"regr_syy":           true,
	"stddev":             true,
	"stddev_pop":         true,
	"stddev_samp":        true,
	"var_pop":            true,
	"var_samp":           true,
	"variance":           true,
}

// HasTimeFilter reports whether WHERE contains a top-level predicate of the
// form `timeCol OP literal_or_now`. Predicates nested inside OR/NOT are NOT
// considered (their bounds would silently leak past the rewrite). Returns the
// recovered [Lo, Hi) range; ok requires BOTH endpoints to be present so the
// emitter has a closed range to project against the rollup parquet glob.
// Open-ended queries (`time >= X` with no upper bound) fall back to source.
//
// Inclusivity note: `>` is folded into `Lo` as if it were `>=`, and `<=` into
// `Hi` as if it were `<`. This introduces a boundary-row ambiguity at exact
// timestamps that bracket bucket edges (rare in dashboards which use Lo =
// bucket start and Hi = bucket end). If a use case ever cares about the
// boundary instant, store the operator alongside the bound.
func HasTimeFilter(sel *pg.SelectStmt, timeCol string) (TimeRange, bool) {
	if sel == nil {
		return TimeRange{}, false
	}
	where := sel.GetWhereClause()
	if where == nil {
		return TimeRange{}, false
	}
	var tr TimeRange
	loOK, hiOK := false, false
	unsafe := false

	var walk func(n *pg.Node, insideOr bool)
	walk = func(n *pg.Node, insideOr bool) {
		if n == nil || unsafe {
			return
		}
		if be := n.GetBoolExpr(); be != nil {
			switch be.GetBoolop() {
			case pg.BoolExprType_AND_EXPR:
				for _, a := range be.GetArgs() {
					walk(a, insideOr)
				}
			case pg.BoolExprType_OR_EXPR, pg.BoolExprType_NOT_EXPR:
				for _, a := range be.GetArgs() {
					walk(a, true)
				}
			}
			return
		}
		ax := n.GetAExpr()
		if ax == nil {
			return
		}
		left := ax.GetLexpr().GetColumnRef()
		if left == nil || columnName(left) != timeCol {
			return
		}
		ts, ok := extractTimestamp(ax.GetRexpr())
		if !ok {
			return
		}
		if insideOr {
			unsafe = true
			return
		}
		op := joinFuncName(ax.GetName())
		switch op {
		case ">=", ">":
			tr.Lo = ts
			loOK = true
		case "<=", "<":
			tr.Hi = ts
			hiOK = true
		}
	}
	walk(where, false)
	if unsafe {
		return TimeRange{}, false
	}
	return tr, loOK && hiOK
}

// AllAggregatesTranslatable reports whether every aggregate function call in
// sel maps to a known translation. Returns false when the SELECT list or
// HAVING clause references an aggregate from knownDuckDBAggregates that we
// don't translate (e.g. stddev, array_agg, countif) -- letting those pass
// through to the merge-on-read SELECT would aggregate per-bucket subtotals
// instead of source rows.
//
// Non-aggregate function calls (date_trunc, coalesce, concat, etc.) are
// always safe to leave alone and don't trip this guard.
//
// Note: "rank-flip protection" — refusing ORDER BY-sketch + LIMIT — was
// considered and rejected. In practice top-N-by-uniques queries (most
// common HLL use case in dashboards) have order-of-magnitude differences
// between adjacent ranks; HLL's ~1.6% error effectively never changes the
// top-5/10/20 ordering. The 40× speedup is worth the theoretical risk.
// Users who need exact ranks can raise HLL precision per-table.
func AllAggregatesTranslatable(sel *pg.SelectStmt) bool {
	if sel == nil {
		return false
	}
	ok := true
	visit := func(fc *pg.FuncCall) {
		if !ok {
			return
		}
		name := strings.ToLower(joinFuncName(fc.GetFuncname()))
		if name == "" {
			return
		}
		if translatableAggFuncs[name] {
			return
		}
		if knownDuckDBAggregates[name] {
			ok = false
		}
		// Otherwise the FuncCall is a non-aggregate scalar (date_trunc,
		// coalesce, etc.) and we leave it alone.
	}
	for _, t := range sel.GetTargetList() {
		walkFuncCalls(t, visit)
	}
	walkFuncCalls(sel.GetHavingClause(), visit)
	if !ok {
		return false
	}
	return true
}

// extractTimestamp pulls a time.Time out of a literal node, accepting both
// `TIMESTAMP '...'` typecast form and bare string literal. Returns ok=false
// when the node isn't a parseable timestamp literal.
//
// Validates the typecast target type — a predicate like `time >= '2026-01-01'::interval`
// must not be parsed as a timestamp just because the inner string matches a
// timestamp layout. Only timestamp/timestamptz/date casts are accepted.
func extractTimestamp(n *pg.Node) (time.Time, bool) {
	if n == nil {
		return time.Time{}, false
	}
	if tc := n.GetTypeCast(); tc != nil {
		if !isTimestampTypeName(tc.GetTypeName()) {
			return time.Time{}, false
		}
		if a := tc.GetArg().GetAConst(); a != nil && a.GetSval() != nil {
			return parseTSLit(a.GetSval().GetSval())
		}
	}
	if a := n.GetAConst(); a != nil && a.GetSval() != nil {
		return parseTSLit(a.GetSval().GetSval())
	}
	return time.Time{}, false
}

// isTimestampTypeName reports whether the parsed type name refers to a
// timestamp/timestamptz/date. The type name is a list of identifiers
// (pg_query represents `pg_catalog.timestamp` as ["pg_catalog","timestamp"]);
// we check the trailing identifier case-insensitively.
func isTimestampTypeName(tn *pg.TypeName) bool {
	if tn == nil {
		return false
	}
	names := tn.GetNames()
	if len(names) == 0 {
		return false
	}
	last := names[len(names)-1].GetString_()
	if last == nil {
		return false
	}
	switch strings.ToLower(last.GetSval()) {
	case "timestamp", "timestamptz", "date":
		return true
	}
	return false
}

// parseTSLit accepts the common timestamp literal forms users write in
// Grafana/dashboard SQL and returns the parsed UTC instant. Returns ok=false
// for unknown layouts so the rewriter falls back to source.
func parseTSLit(s string) (time.Time, bool) {
	for _, layout := range []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.000",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}
