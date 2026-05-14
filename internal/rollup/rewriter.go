package rollup

import (
	"context"
	"regexp"
	"strings"
	"time"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// Rewrite is the rollup rewrite entry point. Returns the rewritten SQL and
// ok=true if all three guards pass; the original SQL and ok=false otherwise.
//
//  1. Time-filter guard: WHERE must have a top-level `time_col >= ...` and
//     `time_col < ...` pair (open-ended ranges refuse).
//  2. Variant pick: at least one registered variant for the FROM table must
//     cover the query's bucket grain, kept-dim list, and aggregate set, AND
//     the variant must have at least one built bucket (non-zero watermark).
//  3. Aggregate translatability: every aggregate must map to a known
//     translation, and the query must not have an ORDER-BY-sketch-with-LIMIT
//     rank-flip risk.
//
// On any guard failure, returns (sql, false) so the caller can route to source.
// Single-statement SELECTs only; CTEs are handled by the caller via
// RewriteCTEs which recurses into each CTE body.
//
// wmRead is the rollup-watermark reader. When non-nil, Rewrite refuses any
// variant whose watermark is zero — without this gate, the emitted SQL
// references a `read_parquet('<glob>')` whose glob matches zero files,
// which DuckDB raises as a runtime "No files found" error. Tests that
// don't care about cold-start behavior may pass nil.
func Rewrite(ctx context.Context, sql string, reg *Registry, wmRead WMReader, sourceGlob SourceGlobFunc, rollupGlob RollupGlobFunc) (string, bool) {
	if reg == nil {
		return sql, false
	}
	// Pre-quote PostgreSQL-reserved keywords used as schema qualifiers.
	// `default.<table>` is a common shape in Arc (`default` is Arc's implicit
	// database) but `default` is a Postgres reserved word that pg_query_go
	// won't accept unquoted in a schema position. DuckDB happily parses both.
	// Substitute `default.` → `"default".` so the parser sees a quoted ident;
	// the rest of the rewrite emits SQL DuckDB consumes unchanged.
	parseSQL := quoteReservedSchemaKeywords(sql)
	tree, err := pg.Parse(parseSQL)
	if err != nil || len(tree.GetStmts()) != 1 {
		return sql, false
	}
	sel := tree.GetStmts()[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return sql, false
	}

	rv := findEligibleRangeVar(sel)
	if rv == nil {
		return sql, false
	}
	// Limitation: an unqualified FROM (`FROM t`, not `FROM db.t`) falls back
	// to source. Supporting it cleanly requires threading a default-database
	// through the api layer or having the caller textually qualify the table
	// before calling Rewrite; not worth the risk surface for the rewrite path.
	specs := reg.ForTable(rv.GetSchemaname(), rv.GetRelname())
	if len(specs) == 0 {
		return sql, false
	}
	timeCol := specs[0].BucketColumn

	// Guard 1: time filter (both bounds required so the emitter has a closed
	// range to project against the rollup parquet glob).
	tr, ok := HasTimeFilter(sel, timeCol)
	if !ok {
		return sql, false
	}

	// Guard 3: variant pick - smallest covering variant for the query's
	// bucket grain, kept-dim list, and aggregate set.
	qShape, ok := buildQueryShape(sel, timeCol)
	if !ok {
		return sql, false
	}
	// Sketch-based aggregates with no GROUP BY at all (single-value
	// percentile / global COUNT DISTINCT) require merging every rollup
	// row's sketch BLOB into one final aggregate. DuckDB's datasketches
	// t-digest aggregator triggers a cgo crash on that shape when the
	// merge input is large (dim-rich variant). Source handles these
	// queries in ~700ms; refusing here keeps the system stable.
	if qShape.usesSketch() && len(sel.GetGroupClause()) == 0 {
		return sql, false
	}
	variant := PickBestVariant(specs, qShape)
	if variant == nil {
		return sql, false
	}

	// Guard 2: aggregate translatability.
	if !AllAggregatesTranslatable(sel) {
		return sql, false
	}

	// Compute the rollup/fresh split point. Default: now truncated to the
	// variant's bucket grain. Clip to the variant's watermark when known
	// (wmRead non-nil) so we don't assume buckets exist past what the
	// builder has actually written. The watermark sits at the FIRST un-
	// built bucket, so it doubles as the safe upper edge of "what's in
	// the rollup parquet". Without this clip there's a daily window
	// between bucket-end and bucket-end+hourlyBuildGrace where the merge
	// silently drops the just-elapsed bucket (it's not in the rollup
	// CTE because nothing's been built, and it's not in the fresh CTE
	// because boundary > tr.Hi at that instant).
	now := time.Now().UTC()
	boundary := now.Truncate(variant.BucketInterval)
	if wmRead != nil {
		wm, err := wmRead.Get(ctx, variant.StoragePath())
		if err != nil {
			return sql, false
		}
		// No buckets built yet → the rollup parquet glob matches no files.
		// DuckDB errors at execution time on read_parquet of an empty glob,
		// and the rewriter has no fallback once it returns ok=true. Refuse.
		if wm.IsZero() {
			return sql, false
		}
		if wm.Watermark.Before(boundary) {
			boundary = wm.Watermark
		}
	}

	out, err := EmitMergeOnRead(sel, variant, tr, boundary, sourceGlob, rollupGlob)
	if err != nil {
		return sql, false
	}
	return out, true
}

// reservedSchemaKeywords lists Postgres-reserved tokens that Arc users
// commonly put in schema position. pg_query_go rejects them unquoted; we
// quote them in the SQL before parsing so the rewriter can do its job.
// DuckDB does not reserve these so the rewritten SQL stays valid downstream.
var reservedSchemaKeywords = []string{"default", "user", "order", "table", "limit"}

var reservedSchemaRe = func() []*regexp.Regexp {
	out := make([]*regexp.Regexp, 0, len(reservedSchemaKeywords))
	for _, kw := range reservedSchemaKeywords {
		out = append(out, regexp.MustCompile(`(?i)(\W|^)`+kw+`\.`))
	}
	return out
}()

// quoteReservedSchemaKeywords rewrites `<kw>.` to `"<kw>".` (case-preserving)
// for known reserved schema names so the SQL becomes pg-parseable.
func quoteReservedSchemaKeywords(sql string) string {
	for i, re := range reservedSchemaRe {
		kw := reservedSchemaKeywords[i]
		sql = re.ReplaceAllStringFunc(sql, func(match string) string {
			// Preserve the leading boundary char; quote the keyword as
			// matched (case-preserving) and re-append the trailing dot.
			lead := match[:len(match)-len(kw)-1]
			matched := match[len(lead) : len(match)-1]
			return lead + `"` + matched + `".`
		})
	}
	return sql
}

// findEligibleRangeVar returns the single base-table RangeVar in sel's FROM
// clause, or nil for joins, subqueries, set-ops, and other unsupported
// shapes. The variant picker needs a single (database, table) key to look up
// rollups; multi-table queries fall back to source.
func findEligibleRangeVar(sel *pg.SelectStmt) *pg.RangeVar {
	from := sel.GetFromClause()
	if len(from) != 1 {
		return nil
	}
	rv := from[0].GetRangeVar()
	if rv == nil {
		return nil
	}
	return rv
}

// buildQueryShape derives the picker's QueryShape from sel:
//
//   - BucketGrain: from date_trunc('<unit>', <time-col>) in GROUP BY or
//     SELECT list, defaulting to the variant's bucket interval (rollup
//     row granularity) when the query carries no explicit time-bucketing.
//   - NeededDims: every non-aggregate, non-time-bucket column referenced in
//     SELECT, GROUP BY, HAVING, or ORDER BY.
//   - NeededAggregates: one entry per aggregate function call in the SELECT
//     list / HAVING clause.
//
// Returns ok=false only when an unhandled SELECT-list construct prevents
// reliable shape extraction (e.g., a bare `*` reference, which the picker
// can't reason about column-wise).
func buildQueryShape(sel *pg.SelectStmt, timeCol string) (QueryShape, bool) {
	var q QueryShape

	q.BucketGrain = inferBucketGrain(sel, timeCol)

	dimSet := map[string]bool{}
	skip := map[string]bool{}

	// Time bucketings in GROUP BY are dim-like in the SQL sense, but the
	// picker treats them as the BucketGrain field. Track them so we can
	// exclude their bare-column form from NeededDims.
	skip[timeCol] = true

	// SELECT-list aliases are valid references in HAVING / ORDER BY but they
	// resolve to expressions on the merged view (often aggregate aliases like
	// `COUNT(*) AS n`). They are NOT source-column dims — excluding them here
	// stops the picker from looking for a variant that keeps `n` as a dim.
	for _, t := range sel.GetTargetList() {
		rt := t.GetResTarget()
		if rt == nil {
			continue
		}
		if rt.GetName() != "" {
			skip[rt.GetName()] = true
		}
	}

	addCol := func(c *pg.ColumnRef) {
		if c == nil {
			return
		}
		name := columnName(c)
		if name == "" || skip[name] {
			return
		}
		dimSet[name] = true
	}

	walkBareCols(sel.GetGroupClause(), addCol)
	for _, t := range sel.GetTargetList() {
		rt := t.GetResTarget()
		if rt == nil {
			continue
		}
		val := rt.GetVal()
		if isStarColumnRef(val) {
			return QueryShape{}, false
		}
		// Bare column refs in the SELECT list count as needed dims unless
		// they're aggregate args (handled separately) or the time column.
		walkBareColsExcludingAggs(val, addCol)
	}
	walkBareColsExcludingAggs(sel.GetHavingClause(), addCol)
	for _, s := range sel.GetSortClause() {
		walkBareColsExcludingAggs(s, addCol)
	}
	// WHERE-clause columns also count as needed: the picker must refuse a
	// variant that lacks them, otherwise the emit pass would silently drop
	// the filter and return wrong results. The time column is already in skip.
	walkBareColsExcludingAggs(sel.GetWhereClause(), addCol)

	for d := range dimSet {
		q.NeededDims = append(q.NeededDims, d)
	}

	// Aggregate enumeration.
	visit := func(fc *pg.FuncCall) {
		na, ok := neededAggFromFuncCall(fc)
		if !ok {
			return
		}
		q.NeededAggregates = append(q.NeededAggregates, na)
	}
	for _, t := range sel.GetTargetList() {
		walkFuncCalls(t, func(fc *pg.FuncCall) {
			if isAggregateName(fc) {
				visit(fc)
			}
		})
	}
	walkFuncCalls(sel.GetHavingClause(), func(fc *pg.FuncCall) {
		if isAggregateName(fc) {
			visit(fc)
		}
	})

	return q, true
}

// inferBucketGrain walks GROUP BY (and falls back to the SELECT list) looking
// for date_trunc('<unit>', ...) calls. Returns the unit's duration, or 24h
// when no truncation is present.
//
// The "no bucketing" default is daily, not hourly: queries like
// `SELECT country, COUNT(*) FROM t WHERE time >= ... GROUP BY country` ask
// for a single aggregate over the entire time range, with no preference for
// finer granularity. The picker's priority 1 ("bucket >= 1d, no sketches →
// __1d") then routes them to the dim-rich daily variant. Defaulting to 1h
// would force the picker to look for __1h, which doesn't exist in compact
// or balanced mode → source fallback for every non-time-bucketed query.
func inferBucketGrain(sel *pg.SelectStmt, timeCol string) time.Duration {
	var found time.Duration
	scan := func(n *pg.Node) {
		walkFuncCalls(n, func(fc *pg.FuncCall) {
			if strings.ToLower(joinFuncName(fc.GetFuncname())) != "date_trunc" {
				return
			}
			args := fc.GetArgs()
			if len(args) < 1 {
				return
			}
			unit := stringLit(args[0])
			if unit == "" {
				return
			}
			d := unitToDuration(unit)
			if d > 0 && (found == 0 || d < found) {
				found = d
			}
		})
	}
	for _, g := range sel.GetGroupClause() {
		scan(g)
	}
	if found != 0 {
		return found
	}
	for _, t := range sel.GetTargetList() {
		rt := t.GetResTarget()
		if rt == nil {
			continue
		}
		scan(rt.GetVal())
	}
	if found != 0 {
		return found
	}
	_ = timeCol
	// No explicit bucketing - default to daily so the picker routes to the
	// dim-rich __1d variant (which exists in every mode).
	return 24 * time.Hour
}

func stringLit(n *pg.Node) string {
	c := n.GetAConst()
	if c == nil {
		return ""
	}
	if s := c.GetSval(); s != nil {
		return s.GetSval()
	}
	return ""
}

func unitToDuration(unit string) time.Duration {
	switch strings.ToLower(unit) {
	case "minute":
		return time.Minute
	case "hour":
		return time.Hour
	case "day":
		return 24 * time.Hour
	case "week":
		return 7 * 24 * time.Hour
	}
	return 0
}

// neededAggFromFuncCall maps a SQL aggregate FuncCall to a NeededAgg{Op, Column}.
// Returns ok=false for aggregates we don't translate (the translatability guard
// will refuse those separately).
func neededAggFromFuncCall(fc *pg.FuncCall) (NeededAgg, bool) {
	name := strings.ToLower(joinFuncName(fc.GetFuncname()))
	col := firstColArgFromFuncCall(fc)
	if col == "" && fc.GetAggWithinGroup() {
		for _, ord := range fc.GetAggOrder() {
			if sb := ord.GetSortBy(); sb != nil {
				if c := sb.GetNode().GetColumnRef(); c != nil {
					col = columnName(c)
					break
				}
			}
		}
	}
	switch name {
	case "count":
		if fc.GetAggDistinct() {
			return NeededAgg{Op: "COUNT_DISTINCT", Column: col}, true
		}
		return NeededAgg{Op: "COUNT", Column: col}, true
	case "sum":
		return NeededAgg{Op: "SUM", Column: col}, true
	case "avg":
		return NeededAgg{Op: "AVG", Column: col}, true
	case "min":
		return NeededAgg{Op: "MIN", Column: col}, true
	case "max":
		return NeededAgg{Op: "MAX", Column: col}, true
	case "approx_count_distinct":
		return NeededAgg{Op: "COUNT_DISTINCT", Column: col}, true
	case "percentile_cont", "quantile_cont":
		return NeededAgg{Op: "PERCENTILE_CONT", Column: col}, true
	}
	return NeededAgg{}, false
}

// walkBareCols walks every node in items and calls visit on every ColumnRef
// found, descending through the expression node types the picker cares about.
// Used for GROUP BY items where every reference is dim-like.
func walkBareCols(items []*pg.Node, visit func(*pg.ColumnRef)) {
	for _, it := range items {
		walkBareColsExcludingAggs(it, visit)
	}
}

// walkBareColsExcludingAggs walks n and calls visit on every ColumnRef NOT
// nested inside an aggregate FuncCall. Bare column references inside the
// SELECT list (e.g. `SELECT service, COUNT(*)`) are dim-like; column
// references inside `SUM(x)` are aggregate args, not dims.
func walkBareColsExcludingAggs(n *pg.Node, visit func(*pg.ColumnRef)) {
	if n == nil {
		return
	}
	if rt := n.GetResTarget(); rt != nil {
		walkBareColsExcludingAggs(rt.GetVal(), visit)
		return
	}
	if c := n.GetColumnRef(); c != nil {
		visit(c)
		return
	}
	if fc := n.GetFuncCall(); fc != nil {
		if isAggregateName(fc) {
			return
		}
		for _, a := range fc.GetArgs() {
			walkBareColsExcludingAggs(a, visit)
		}
		return
	}
	if ax := n.GetAExpr(); ax != nil {
		walkBareColsExcludingAggs(ax.GetLexpr(), visit)
		walkBareColsExcludingAggs(ax.GetRexpr(), visit)
		return
	}
	if be := n.GetBoolExpr(); be != nil {
		for _, a := range be.GetArgs() {
			walkBareColsExcludingAggs(a, visit)
		}
		return
	}
	if ce := n.GetCaseExpr(); ce != nil {
		walkBareColsExcludingAggs(ce.GetArg(), visit)
		walkBareColsExcludingAggs(ce.GetDefresult(), visit)
		for _, a := range ce.GetArgs() {
			walkBareColsExcludingAggs(a, visit)
		}
		return
	}
	if cw := n.GetCaseWhen(); cw != nil {
		walkBareColsExcludingAggs(cw.GetExpr(), visit)
		walkBareColsExcludingAggs(cw.GetResult(), visit)
		return
	}
	if cl := n.GetCoalesceExpr(); cl != nil {
		for _, a := range cl.GetArgs() {
			walkBareColsExcludingAggs(a, visit)
		}
		return
	}
	if tc := n.GetTypeCast(); tc != nil {
		walkBareColsExcludingAggs(tc.GetArg(), visit)
		return
	}
	if sb := n.GetSortBy(); sb != nil {
		walkBareColsExcludingAggs(sb.GetNode(), visit)
		return
	}
}
