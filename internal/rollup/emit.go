package rollup

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// SourceGlobFunc returns the read_parquet glob pattern for the source table.
// (database, table) → glob string with no surrounding quotes. The emitter
// quotes/escapes the result itself.
type SourceGlobFunc func(database, table string) string

// RollupGlobFunc returns the read_parquet glob pattern for a rollup variant's
// parquet directory. Production wires this to storage.GetStoragePath so the
// emitter doesn't need to know about S3 vs local paths. When nil, the emitter
// falls back to the local-storage default layout.
type RollupGlobFunc func(variant *RollupSpec) string

// EmitMergeOnRead produces a single SQL statement that answers sel from a
// merged view of (rollup completed buckets) UNION (re-aggregated source rows
// for the current partial bucket). The result has the form:
//
//	WITH rollup AS (
//	  SELECT bucket AS <bucket-col>, <dims>, <pre-agg-cols-with-sketch-casts>
//	  FROM read_parquet('<rollup-glob>')
//	  WHERE bucket >= TIMESTAMP 'Lo' AND bucket < <boundary>
//	),
//	fresh AS (
//	  SELECT date_trunc(<grain>, <bucket-col>) AS <bucket-col>, <dims>, <fresh-pre-aggs>
//	  FROM <db>.<source>
//	  WHERE <time> >= <boundary> AND <time> < TIMESTAMP 'Hi'
//	  GROUP BY <bucket-col>, <dims>
//	)
//	SELECT <outer-projection>
//	FROM (SELECT * FROM rollup UNION ALL SELECT * FROM fresh) merged
//	GROUP BY <user's actual GROUP BY clause>
//
// The fresh branch reproduces the rollup-branch column shape (same names AND
// SQL types — sketch columns cast to sketch_hll/sketch_tdigest_double in the
// rollup branch to match the fresh-branch outputs of datasketch_hll/tdigest)
// so the UNION is type-safe. The outer SELECT applies the user-visible
// aggregates via the translation table mapped against the unified pre-agg
// columns, and groups by exactly the user's GROUP BY list — not the variant's
// KeepDimensions (which may carry more dims than the user asked for).
//
// boundary is the rollup/fresh split timestamp (Rewrite computes it as the
// MIN of `now.Truncate(bucket)` and the variant's watermark, so the rollup
// CTE never claims buckets the builder hasn't actually written).
func EmitMergeOnRead(sel *pg.SelectStmt, variant *RollupSpec, tr TimeRange, boundary time.Time, sourceGlob SourceGlobFunc, rollupGlob RollupGlobFunc) (string, error) {
	if variant == nil {
		return "", fmt.Errorf("emit merge-on-read: nil variant")
	}
	if sel == nil {
		return "", fmt.Errorf("emit merge-on-read: nil SelectStmt")
	}

	bucketCol := variant.BucketColumn
	bucketGrain, err := bucketGrainName(variant.BucketInterval)
	if err != nil {
		return "", fmt.Errorf("emit merge-on-read: %w", err)
	}
	// Emit `boundary` as a TIMESTAMP literal rather than `date_trunc('day',
	// NOW())` because Arc's downstream Phase-0b SQL rewriter mangles the
	// DuckDB form into a broken `to_timestamp(epoch(NOW()::BIGINT // ...))`.
	// A literal also makes the emitted SQL easier to debug and avoids any
	// downstream-rewrite surprises.
	boundaryLit := fmt.Sprintf("TIMESTAMP '%s'", tsLitMOR(boundary))
	dims := append([]string(nil), variant.KeepDimensions...)

	outerProjection, err := projectionForQuery(sel, variant)
	if err != nil {
		return "", fmt.Errorf("emit merge-on-read: %w", err)
	}

	// Outer GROUP BY mirrors the user's actual GROUP BY clause, including any
	// non-dim expressions like date_trunc(...) bucketings or alias references.
	// Falling back to variant.KeepDimensions would over-group when the user
	// projected fewer dims than the variant carries, producing inflated
	// cardinality (one row per kept-dim × user-dim).
	outerGroupBy, err := userGroupByExprs(sel)
	if err != nil {
		return "", fmt.Errorf("emit merge-on-read: %w", err)
	}

	rollupGlobStr := ""
	if rollupGlob != nil {
		rollupGlobStr = rollupGlob(variant)
	}
	if rollupGlobStr == "" {
		rollupGlobStr = rollupParquetGlob(variant)
	}
	// sourceGlob is intentionally NOT consulted here in Task 6: the fresh CTE
	// reads from the bare source-table reference, which DuckDB resolves to the
	// attached database's parquet view. Task 7 wires up a production-side
	// resolver if the FROM needs to be a read_parquet() glob instead.
	_ = sourceGlob

	rollupSelects, err := rollupBranchSelectCols(variant)
	if err != nil {
		return "", fmt.Errorf("emit merge-on-read: %w", err)
	}

	freshSelects := []string{
		fmt.Sprintf("date_trunc('%s', %s) AS %s", bucketGrain, bucketCol, bucketCol),
	}
	freshSelects = append(freshSelects, dims...)
	freshSketch, err := freshSketchExprs(variant)
	if err != nil {
		return "", fmt.Errorf("emit merge-on-read: %w", err)
	}
	freshSelects = append(freshSelects, freshSketch...)

	freshGroupBy := append([]string{bucketCol}, dims...)

	freshSource := variant.SourceTable

	// Clip the rollup branch's upper bound to MIN(user_Hi, boundary) so we
	// don't over-include buckets past what the user asked for. Likewise clip
	// the fresh branch's lower bound to MAX(user_Lo, boundary). When user_Hi
	// is in the past the fresh CTE collapses to an empty range and we elide
	// it entirely (DuckDB doesn't prune the read_parquet LIST before
	// evaluating the empty WHERE, so leaving the dead branch costs a full
	// source-prefix scan); when user_Lo is in the future the rollup CTE
	// collapses similarly and is elided.
	rollupUpper := boundaryLit
	freshLower := boundaryLit
	if tr.Hi.Before(boundary) {
		rollupUpper = fmt.Sprintf("TIMESTAMP '%s'", tsLitMOR(tr.Hi))
	}
	if tr.Lo.After(boundary) {
		freshLower = fmt.Sprintf("TIMESTAMP '%s'", tsLitMOR(tr.Lo))
	}
	// freshIsEmpty: user's upper bound is at or before the boundary, so the
	// fresh CTE's WHERE (`time >= boundary AND time < user_Hi`) is empty.
	// rollupIsEmpty: user's lower bound is at or after the boundary, so the
	// rollup CTE's WHERE (`bucket >= user_Lo AND bucket < boundary`) is empty.
	freshIsEmpty := !tr.Hi.After(boundary)
	rollupIsEmpty := !tr.Lo.Before(boundary)

	// Propagate the user's WHERE clause (sans the bucket-column comparisons
	// the time-filter guard already captured) into BOTH the rollup and fresh
	// CTEs. Without this, filters like `status='ns'` get silently dropped and
	// the rollup returns inflated counts.
	extraWhere, extraErr := residualWhere(sel.GetWhereClause(), bucketCol)
	if extraErr != nil {
		return "", fmt.Errorf("emit merge-on-read: %w", extraErr)
	}
	extraWhereSuffix := ""
	if extraWhere != "" {
		extraWhereSuffix = " AND (" + extraWhere + ")"
	}

	var sb strings.Builder
	// union_by_name=true tolerates schema drift inside the variant's parquet
	// directory: files written before a spec re-inference (e.g. `city` was a
	// kept dim, then re-classified as a sketch `city__hll`) keep their old
	// column set on disk and live alongside newer files. Without this flag,
	// DuckDB's binder fails on `<col>::sketch_hll AS <col>` for files
	// missing <col>, surfacing as the misleading "referenced before
	// defined" error. With it, missing columns become NULL and the
	// per-spec aggregates downstream treat them as empty sketches.
	switch {
	case freshIsEmpty && rollupIsEmpty:
		// Defensive: both branches empty. Emit a query that returns zero rows
		// with the right shape. Build a single-CTE form using the rollup
		// shape (rollupSelects) and a WHERE that's trivially false. Cheaper
		// than UNION ALL of two LIST-scanning empty CTEs.
		fmt.Fprintf(&sb, "WITH rollup AS (\n  SELECT %s\n  FROM read_parquet('%s', union_by_name=true)\n  WHERE 1=0\n)\n",
			strings.Join(rollupSelects, ", "),
			strings.ReplaceAll(rollupGlobStr, "'", "''"),
		)
	case freshIsEmpty:
		// User range is entirely covered by the rollup. Elide the fresh CTE
		// so we don't pay the source-prefix LIST cost on a dead branch.
		fmt.Fprintf(&sb, "WITH rollup AS (\n  SELECT %s\n  FROM read_parquet('%s', union_by_name=true)\n  WHERE bucket >= TIMESTAMP '%s' AND bucket < %s%s\n)\n",
			strings.Join(rollupSelects, ", "),
			strings.ReplaceAll(rollupGlobStr, "'", "''"),
			tsLitMOR(tr.Lo),
			rollupUpper,
			extraWhereSuffix,
		)
	case rollupIsEmpty:
		// User range is entirely in the in-flight bucket(s) past the
		// watermark. Elide the rollup CTE — DuckDB still has to scan
		// source, but at least we skip the read_parquet of an empty
		// rollup glob.
		fmt.Fprintf(&sb, "WITH fresh AS (\n  SELECT %s\n  FROM %s\n  WHERE %s >= %s AND %s < TIMESTAMP '%s'%s\n  GROUP BY %s\n)\n",
			strings.Join(freshSelects, ", "),
			freshSource,
			bucketCol, freshLower,
			bucketCol, tsLitMOR(tr.Hi),
			extraWhereSuffix,
			strings.Join(freshGroupBy, ", "),
		)
	default:
		fmt.Fprintf(&sb, "WITH rollup AS (\n  SELECT %s\n  FROM read_parquet('%s', union_by_name=true)\n  WHERE bucket >= TIMESTAMP '%s' AND bucket < %s%s\n),\n",
			strings.Join(rollupSelects, ", "),
			strings.ReplaceAll(rollupGlobStr, "'", "''"),
			tsLitMOR(tr.Lo),
			rollupUpper,
			extraWhereSuffix,
		)
		fmt.Fprintf(&sb, "fresh AS (\n  SELECT %s\n  FROM %s\n  WHERE %s >= %s AND %s < TIMESTAMP '%s'%s\n  GROUP BY %s\n)\n",
			strings.Join(freshSelects, ", "),
			freshSource,
			bucketCol, freshLower,
			bucketCol, tsLitMOR(tr.Hi),
			extraWhereSuffix,
			strings.Join(freshGroupBy, ", "),
		)
	}

	sb.WriteString("SELECT ")
	sb.WriteString(outerProjection)
	// Choose the inner FROM: a single CTE when one branch was elided, the
	// merged UNION when both are populated.
	switch {
	case freshIsEmpty && rollupIsEmpty:
		sb.WriteString("\nFROM rollup merged")
	case freshIsEmpty:
		sb.WriteString("\nFROM rollup merged")
	case rollupIsEmpty:
		sb.WriteString("\nFROM fresh merged")
	default:
		sb.WriteString("\nFROM (SELECT * FROM rollup UNION ALL SELECT * FROM fresh) merged")
	}
	if len(outerGroupBy) > 0 {
		fmt.Fprintf(&sb, "\nGROUP BY %s", strings.Join(outerGroupBy, ", "))
	}
	// HAVING: aggregates inside the predicate must be translated against
	// the variant the same way SELECT-list aggregates are; otherwise
	// `HAVING COUNT(*) > N` would compare against the count of merged rows
	// (≈7 per group), not the rollup'd __row_count sum.
	if h := sel.GetHavingClause(); h != nil {
		havingSQL, err := translateAggsInExpr(h, variant)
		if err != nil {
			return "", fmt.Errorf("emit merge-on-read: %w", err)
		}
		if havingSQL != "" {
			fmt.Fprintf(&sb, "\nHAVING %s", havingSQL)
		}
	}

	// Propagate user's ORDER BY / LIMIT / OFFSET so the outer SELECT returns
	// the same shape the user asked for. ORDER BY items may reference
	// SELECT-list aliases (e.g. `n` for `COUNT(*) AS n`); those resolve in
	// the outer SELECT because we preserved the aliases in outerProjection.
	if order, err := deparseOrderByList(sel.GetSortClause(), variant); err == nil && order != "" {
		fmt.Fprintf(&sb, "\nORDER BY %s", order)
	}
	if lim := sel.GetLimitCount(); lim != nil {
		if limStr, err := deparseExpr(lim); err == nil && limStr != "" {
			fmt.Fprintf(&sb, "\nLIMIT %s", limStr)
		}
	}
	if off := sel.GetLimitOffset(); off != nil {
		if offStr, err := deparseExpr(off); err == nil && offStr != "" {
			fmt.Fprintf(&sb, "\nOFFSET %s", offStr)
		}
	}

	return sb.String(), nil
}

// translateAggsInExpr deparses expr to SQL but rewrites every top-level
// aggregate FuncCall against the rollup translation table first. Used for
// HAVING and ORDER BY where the user's aggregates need to operate on the
// rollup'd pre-agg columns, not the merged view's row count.
//
// Implementation: deparse the expression to SQL, then run a textual replace
// over each translated aggregate's source span. Simpler than rebuilding the
// AST. Works because we anchor on aggregate calls (rare in HAVING) and
// because the AST already gives us each FuncCall's source-quoted form.
func translateAggsInExpr(expr *pg.Node, variant *RollupSpec) (string, error) {
	if expr == nil {
		return "", nil
	}
	out, err := deparseExpr(expr)
	if err != nil {
		return "", err
	}
	replacements := map[string]string{}
	walkFuncCalls(expr, func(fc *pg.FuncCall) {
		if !isAggregateName(fc) {
			return
		}
		t, ok := translateAggregate(fc, variant)
		if !ok {
			return
		}
		// Render the FuncCall by itself for text-level substitution.
		key, err := deparseExpr(&pg.Node{Node: &pg.Node_FuncCall{FuncCall: fc}})
		if err != nil || key == "" {
			return
		}
		replacements[key] = t
	})
	for from, to := range replacements {
		out = strings.ReplaceAll(out, from, to)
	}
	return out, nil
}

// deparseOrderByList renders each SortBy node back to SQL so the outer
// SELECT mirrors the user's ORDER BY. Aggregate function calls inside the
// sort key are translated against the variant the same way SELECT-list
// aggregates are — without this, `ORDER BY COUNT(*) DESC` would count rows
// in the merged rollup+fresh view (≈7 per group), not the actual rollup'd
// __row_count sum, giving the wrong top-N ordering. Returns "" when the
// SortClause is empty.
func deparseOrderByList(sortClause []*pg.Node, variant *RollupSpec) (string, error) {
	if len(sortClause) == 0 {
		return "", nil
	}
	parts := make([]string, 0, len(sortClause))
	for _, n := range sortClause {
		sb := n.GetSortBy()
		if sb == nil {
			continue
		}
		key := sb.GetNode()
		// If the sort key is a top-level aggregate call, translate it.
		// Compound expressions (e.g. `100 * COUNT(*)`) are left to
		// deparseExpr — those don't typically appear in ORDER BY.
		expr := ""
		if fc := key.GetFuncCall(); fc != nil && isAggregateName(fc) {
			t, ok := translateAggregate(fc, variant)
			if ok {
				expr = t
			}
		}
		if expr == "" {
			d, err := deparseExpr(key)
			if err != nil {
				return "", err
			}
			expr = d
		}
		dir := ""
		switch sb.GetSortbyDir() {
		case pg.SortByDir_SORTBY_ASC:
			dir = " ASC"
		case pg.SortByDir_SORTBY_DESC:
			dir = " DESC"
		}
		parts = append(parts, expr+dir)
	}
	return strings.Join(parts, ", "), nil
}

// bucketGrainName returns the date_trunc unit name for the variant's bucket
// interval. Currently supports hour and day; anything else is refused.
func bucketGrainName(d time.Duration) (string, error) {
	switch {
	case d == time.Hour:
		return "hour", nil
	case d == 24*time.Hour:
		return "day", nil
	}
	return "", fmt.Errorf("unsupported bucket interval %s (only 1h / 1d)", d)
}

// rollupParquetGlob returns the read_parquet glob for the variant's parquet
// directory under the default local layout. Mirrors builder.go:
// windowParquetPath, which writes to `<db>/<rollup_table>/dt=YYYY-MM-DD/window_*.parquet`.
// Production wires a RollupGlobFunc via EmitMergeOnRead that anchors this
// under the configured storage backend root (./data/arc, s3://..., etc.).
// This fallback is used only by tests and unconfigured callers.
func rollupParquetGlob(variant *RollupSpec) string {
	return fmt.Sprintf("./data/%s/%s/**/*.parquet", variant.Database, variant.RollupTableName())
}

// rollupBranchSelectCols returns the rollup CTE's projection list. The bucket
// column is projected as `bucket AS <variant.BucketColumn>` to match the
// builder's parquet schema (which writes `time_bucket(...) AS bucket`). Sketch
// columns are cast to their sketch types so the UNION ALL with the fresh
// branch (which builds sketches via datasketch_hll/tdigest) is type-safe.
func rollupBranchSelectCols(variant *RollupSpec) ([]string, error) {
	cols := []string{
		fmt.Sprintf("bucket AS %s", variant.BucketColumn),
	}
	cols = append(cols, variant.KeepDimensions...)
	cols = append(cols, "__row_count")
	for _, agg := range variant.Aggregations {
		for _, fn := range agg.Functions {
			col := preAggColName(agg.SourceColumn, fn)
			switch fn {
			case AggHLL:
				if agg.SketchConfig == nil {
					return nil, fmt.Errorf("HLL aggregation on %q requires SketchConfig", agg.SourceColumn)
				}
				cols = append(cols, fmt.Sprintf("%s::sketch_hll AS %s", col, col))
			case AggTDigest:
				if agg.SketchConfig == nil {
					return nil, fmt.Errorf("tdigest aggregation on %q requires SketchConfig", agg.SourceColumn)
				}
				cols = append(cols, fmt.Sprintf("%s::sketch_tdigest_double AS %s", col, col))
			default:
				cols = append(cols, col)
			}
		}
	}
	return cols, nil
}

// preAggColName returns the alias used by buildsql.go's aggExpression for a
// (source_column, function) pair. Keeping the names in sync is required so
// the rollup-branch parquet schema and the fresh-branch projection match
// under UNION ALL.
func preAggColName(col string, fn AggFunction) string {
	switch fn {
	case AggSum:
		return col + "__sum"
	case AggCount:
		return col + "__count"
	case AggMin:
		return col + "__min"
	case AggMax:
		return col + "__max"
	case AggHLL:
		return col + "__hll"
	case AggTDigest:
		return col + "__tdigest"
	}
	return col + "__" + string(fn)
}

// freshSketchExprs returns the projection list (excluding bucket/dims) that
// re-aggregates source rows into the rollup row shape: COUNT(*) AS __row_count
// plus one expression per (source_column, function) pair using the same
// datasketch_* / SUM / MIN / MAX / COUNT spelling as buildsql.go's
// aggExpression. Order must match rollupBranchSelectCols for the UNION
// columns to line up positionally.
func freshSketchExprs(variant *RollupSpec) ([]string, error) {
	exprs := []string{"COUNT(*) AS __row_count"}
	for _, agg := range variant.Aggregations {
		for _, fn := range agg.Functions {
			expr, err := freshSketchExpr(agg, fn)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, expr)
		}
	}
	return exprs, nil
}

func freshSketchExpr(agg Aggregation, fn AggFunction) (string, error) {
	col := agg.SourceColumn
	switch fn {
	case AggSum:
		return fmt.Sprintf("SUM(%s) AS %s__sum", col, col), nil
	case AggCount:
		return fmt.Sprintf("COUNT(%s) AS %s__count", col, col), nil
	case AggMin:
		return fmt.Sprintf("MIN(%s) AS %s__min", col, col), nil
	case AggMax:
		return fmt.Sprintf("MAX(%s) AS %s__max", col, col), nil
	case AggHLL:
		if agg.SketchConfig == nil {
			return "", fmt.Errorf("HLL aggregation on %q requires SketchConfig", col)
		}
		return fmt.Sprintf("datasketch_hll(%d, %s) AS %s__hll", agg.SketchConfig.HLLLgK, col, col), nil
	case AggTDigest:
		if agg.SketchConfig == nil {
			return "", fmt.Errorf("tdigest aggregation on %q requires SketchConfig", col)
		}
		return fmt.Sprintf("datasketch_tdigest(%d, %s) AS %s__tdigest", agg.SketchConfig.TDigestK, col, col), nil
	}
	return "", fmt.Errorf("unknown aggregation function %q", fn)
}

// userGroupByExprs returns the deparsed list of GROUP BY items from the user's
// SELECT. Each item is rendered as a single SQL fragment via the deparse
// round-trip so date_trunc(...) bucketings, alias references, and positional
// integers all survive intact. The result is suitable for direct use as the
// outer SELECT's GROUP BY list.
func userGroupByExprs(sel *pg.SelectStmt) ([]string, error) {
	var out []string
	for _, g := range sel.GetGroupClause() {
		text, err := deparseExpr(g)
		if err != nil {
			return nil, fmt.Errorf("deparse GROUP BY item: %w", err)
		}
		out = append(out, text)
	}
	return out, nil
}

// projectionForQuery walks sel.TargetList and rewrites each target into the
// outer-SELECT projection list for the merged CTE. Aggregate function calls
// are mapped via the translation table; non-aggregate expressions (dim
// references, literals, time bucketing) are passed through verbatim via a
// synthetic-SELECT deparse round-trip. SELECT * is refused: the merged
// relation has the rollup/fresh pre-agg shape, not the source schema.
func projectionForQuery(sel *pg.SelectStmt, variant *RollupSpec) (string, error) {
	var parts []string
	for _, target := range sel.GetTargetList() {
		rt := target.GetResTarget()
		if rt == nil {
			continue
		}
		expr := rt.GetVal()
		if isStarColumnRef(expr) {
			return "", fmt.Errorf("SELECT * is not translatable against rollup view")
		}
		text, err := translateTargetExpr(expr, variant)
		if err != nil {
			return "", err
		}
		if alias := rt.GetName(); alias != "" {
			text += " AS " + alias
		}
		parts = append(parts, text)
	}
	return strings.Join(parts, ", "), nil
}

// isStarColumnRef reports whether expr is a `*` reference (bare or
// table-qualified like `t.*`). pg_query encodes `*` as a ColumnRef whose
// trailing field is an A_Star node.
func isStarColumnRef(expr *pg.Node) bool {
	if expr == nil {
		return false
	}
	c := expr.GetColumnRef()
	if c == nil {
		return false
	}
	fields := c.GetFields()
	if len(fields) == 0 {
		return false
	}
	return fields[len(fields)-1].GetAStar() != nil
}

// translateTargetExpr returns the rewritten SQL text for one SELECT-list
// expression. If expr is an aggregate FuncCall we recognize, applies the
// translation table; otherwise deparses the expression verbatim. Refuses
// (returns error) when expr contains a NESTED aggregate (e.g.
// `100 * AVG(x)`, `SUM(failures) / COUNT(*)`, `CASE WHEN SUM(x) > 0 ...`) —
// verbatim pass-through would emit the original `AVG(x)` against the merged
// pre-agg subquery (which has x__sum/x__count, not x) and silently return
// wrong values. The caller is expected to detect refusal and fall back to
// source.
//
// Limitation: nested aggregates (`100 * AVG(x)`, `SUM(x) / COUNT(*)`,
// `CASE WHEN SUM(x) > 0 ...`) refuse the rewrite. Walking the expression
// tree and substituting translated aggregates in place would let common
// arithmetic-around-aggregate shapes survive — revisit if the workload
// shows enough demand. Refusal is the safe-but-conservative baseline.
func translateTargetExpr(expr *pg.Node, variant *RollupSpec) (string, error) {
	if expr == nil {
		return "", fmt.Errorf("nil target expression")
	}
	if fc := expr.GetFuncCall(); fc != nil && isAggregateName(fc) {
		if text, ok := translateAggregate(fc, variant); ok {
			return text, nil
		}
		// Aggregate we don't know how to translate against this variant.
		// Caller (guard 2) should have refused before we got here, but the
		// emitter must not silently produce a wrong answer.
		return "", fmt.Errorf("aggregate not translatable for variant %q", variant.Name)
	}
	if containsAggregate(expr) {
		return "", fmt.Errorf("nested aggregate in SELECT expression not translatable")
	}
	return deparseExpr(expr)
}

// containsAggregate reports whether expr (transitively) contains an aggregate
// FuncCall.
func containsAggregate(n *pg.Node) bool {
	found := false
	walkFuncCalls(n, func(fc *pg.FuncCall) {
		if !found && isAggregateName(fc) {
			found = true
		}
	})
	return found
}

// translateAggregate maps a user aggregate call onto the pre-aggregated
// columns exposed by the merged rollup/fresh view. Returns (sql, true) on
// success and ("", false) when the variant does not carry the required
// pre-aggregation (refuse so the caller can fall back to source).
//
// Translation table (sketch helpers from sketch.go ensure correct casts/K):
//
//	COUNT(*)                            → SUM(__row_count)
//	COUNT(col)                          → SUM(col__count) | SUM(__row_count) (col NotNull)
//	COUNT(DISTINCT col)                 → datasketch_hll_estimate(datasketch_hll_union(K, col__hll::sketch_hll))
//	SUM(col)                            → SUM(col__sum)
//	MIN(col)                            → MIN(col__min)
//	MAX(col)                            → MAX(col__max)
//	AVG(col)                            → SUM(col__sum) / NULLIF(SUM(col__count), 0)
//	percentile_cont(q) WITHIN GROUP ... → datasketch_tdigest_quantile(datasketch_tdigest(K, col__tdigest::sketch_tdigest_double), q)
//	SUM(CASE WHEN <pred> THEN N ELSE 0) → N * SUM(__row_count) FILTER (WHERE <pred>)
//	                                      when <pred> only references kept dims
func translateAggregate(fc *pg.FuncCall, variant *RollupSpec) (string, bool) {
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
	case "sum":
		// SUM(CASE WHEN <pred> THEN N ELSE 0) → N * SUM(__row_count) FILTER (WHERE <pred>)
		// when the predicate only references kept dim columns. This covers the
		// "success rate" / "error rate" dashboard pattern. Falls through to the
		// regular SUM(col) path when the arg isn't a CASE.
		if expr, ok := translateSumCaseFilter(fc, variant); ok {
			return expr, true
		}
		if !specHas(*variant, col, AggSum) {
			return "", false
		}
		return fmt.Sprintf("SUM(%s__sum)", col), true
	case "count":
		if fc.GetAggStar() {
			return "SUM(__row_count)", true
		}
		if fc.GetAggDistinct() {
			sk := findSketchConfig(*variant, col, AggHLL)
			if sk == nil {
				return "", false
			}
			return fmt.Sprintf("datasketch_hll_estimate(%s)",
				MergeSketchExpr(col+"__hll", AggHLL, sk)), true
		}
		if specHas(*variant, col, AggCount) {
			return fmt.Sprintf("SUM(%s__count)", col), true
		}
		if isNotNull(*variant, col) {
			return "SUM(__row_count)", true
		}
		return "", false
	case "avg":
		// Requires both __sum and __count for an exact merged average; the
		// __row_count fallback would over-count when col has NULLs. Rollup specs
		// generate both for any numeric agg, so this is satisfied in practice.
		if !specHas(*variant, col, AggSum) || !specHas(*variant, col, AggCount) {
			return "", false
		}
		return fmt.Sprintf("(SUM(%s__sum) / NULLIF(SUM(%s__count), 0))", col, col), true
	case "min":
		if !specHas(*variant, col, AggMin) {
			return "", false
		}
		return fmt.Sprintf("MIN(%s__min)", col), true
	case "max":
		if !specHas(*variant, col, AggMax) {
			return "", false
		}
		return fmt.Sprintf("MAX(%s__max)", col), true
	case "approx_count_distinct":
		sk := findSketchConfig(*variant, col, AggHLL)
		if sk == nil {
			return "", false
		}
		return fmt.Sprintf("datasketch_hll_estimate(%s)",
			MergeSketchExpr(col+"__hll", AggHLL, sk)), true
	case "quantile_cont", "percentile_cont":
		sk := findSketchConfig(*variant, col, AggTDigest)
		if sk == nil {
			return "", false
		}
		qIdx := 1
		if fc.GetAggWithinGroup() {
			qIdx = 0
		}
		if !isScalarNumericLit(fc, qIdx) {
			return "", false
		}
		q := numericLitArg(fc, qIdx)
		return EstimateSketchExpr(MergeSketchExpr(col+"__tdigest", AggTDigest, sk), AggTDigest, q), true
	}
	return "", false
}

// translateSumCaseFilter detects the SUM(CASE WHEN <pred> THEN N ELSE 0) /
// SUM(CASE WHEN <pred> THEN N END) shape and rewrites it onto the rollup's
// __row_count column via a FILTER (WHERE <pred>) clause. Returns ok=false
// for any shape we don't recognize (multiple WHEN branches, non-numeric
// THEN, non-zero/non-null ELSE) so the caller can fall back to source.
//
// The predicate's columns must all be kept dims in the variant; otherwise
// we can't evaluate the predicate at rollup-row granularity. The picker's
// dim-coverage check already enforces this when the SUM(CASE) refs dim
// columns from WHERE/GROUP BY, but a CASE that references a NEW column
// (not in any other clause) would slip through — we double-check here.
func translateSumCaseFilter(fc *pg.FuncCall, variant *RollupSpec) (string, bool) {
	args := fc.GetArgs()
	if len(args) != 1 {
		return "", false
	}
	ce := args[0].GetCaseExpr()
	if ce == nil {
		return "", false
	}
	whens := ce.GetArgs()
	if len(whens) != 1 {
		return "", false // multi-branch CASE not supported
	}
	cw := whens[0].GetCaseWhen()
	if cw == nil {
		return "", false
	}
	// THEN value: must be a numeric literal.
	thenN, ok := constNumeric(cw.GetResult())
	if !ok {
		return "", false
	}
	// ELSE value: nil (no ELSE) or 0.
	if def := ce.GetDefresult(); def != nil {
		elseN, ok := constNumeric(def)
		if !ok || elseN != 0 {
			return "", false
		}
	}
	// Predicate columns must all be kept dims.
	predCols := bareColsIn(cw.GetExpr())
	dims := map[string]bool{}
	for _, d := range variant.KeepDimensions {
		dims[d] = true
	}
	for c := range predCols {
		if !dims[c] {
			return "", false
		}
	}
	predSQL, err := deparseExpr(cw.GetExpr())
	if err != nil {
		return "", false
	}
	if thenN == 1 {
		return fmt.Sprintf("SUM(__row_count) FILTER (WHERE %s)", predSQL), true
	}
	return fmt.Sprintf("(%g * SUM(__row_count) FILTER (WHERE %s))", thenN, predSQL), true
}

// constNumeric returns the numeric value of a constant integer / float node.
func constNumeric(n *pg.Node) (float64, bool) {
	c := n.GetAConst()
	if c == nil {
		return 0, false
	}
	if v := c.GetIval(); v != nil {
		return float64(v.GetIval()), true
	}
	if v := c.GetFval(); v != nil {
		f, err := strconv.ParseFloat(v.GetFval(), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

// bareColsIn walks n and returns the set of bare ColumnRef names referenced.
func bareColsIn(n *pg.Node) map[string]bool {
	out := map[string]bool{}
	walkBareColsExcludingAggs(n, func(c *pg.ColumnRef) {
		name := columnName(c)
		if name != "" {
			out[name] = true
		}
	})
	return out
}

// deparseExpr renders one expression node back to SQL text by wrapping it in
// a synthetic SELECT, parsing the resulting tree, and deparsing. pg_query_go
// only exposes Deparse(*ParseResult) over a full statement, so this is the
// cleanest way to round-trip a single expression while preserving formatting
// quirks (operator spacing, string literal quoting, etc.). Strips the
// leading `SELECT ` prefix from the deparsed result.
func deparseExpr(expr *pg.Node) (string, error) {
	synth := &pg.ParseResult{
		Version: 0,
		Stmts: []*pg.RawStmt{
			{
				Stmt: &pg.Node{Node: &pg.Node_SelectStmt{SelectStmt: &pg.SelectStmt{
					TargetList: []*pg.Node{
						{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Val: expr}}},
					},
				}}},
			},
		},
	}
	out, err := pg.Deparse(synth)
	if err != nil {
		return "", fmt.Errorf("deparse expr: %w", err)
	}
	const prefix = "SELECT "
	if !strings.HasPrefix(out, prefix) {
		return "", fmt.Errorf("deparse expr: unexpected output %q", out)
	}
	return strings.TrimPrefix(out, prefix), nil
}

// tsLitMOR renders t as a TIMESTAMP literal body (no surrounding TIMESTAMP
// keyword). Truncates fractional zeros so whole-second values stay clean.
func tsLitMOR(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.999999999")
}

// residualWhere deparses where back to SQL after stripping the top-level
// time-column comparisons the guard already captured. Returns "" when no
// residual predicates remain. Top-level only: predicates inside OR/NOT
// are kept intact (those would have been refused by HasTimeFilter anyway).
//
// Errors when the residue still references the bucket column (e.g. an
// equality `time = X` slipped past stripBucketColPredicates, which only
// handles range operators). The rollup CTE renames bucket→time at SELECT
// time, so a WHERE clause that mentions `time` runs BEFORE the rename and
// errors with "column time not found" at execution. Forcing refusal here
// keeps the rewriter from emitting SQL that DuckDB rejects.
func residualWhere(where *pg.Node, bucketCol string) (string, error) {
	residue := stripBucketColPredicates(where, bucketCol)
	if residue == nil {
		return "", nil
	}
	if exprReferencesColumn(residue, bucketCol) {
		return "", fmt.Errorf("residual WHERE references bucket column %q (non-range predicate)", bucketCol)
	}
	return deparseExpr(residue)
}

// exprReferencesColumn reports whether n contains any ColumnRef to the
// named column (case-sensitive comparison — matches columnName() output).
func exprReferencesColumn(n *pg.Node, name string) bool {
	if n == nil {
		return false
	}
	found := false
	walkBareColsExcludingAggs(n, func(c *pg.ColumnRef) {
		if columnName(c) == name {
			found = true
		}
	})
	return found
}

// stripBucketColPredicates returns n with every top-level AExpr that compares
// bucketCol to a literal removed. Returns nil when nothing remains (e.g., the
// WHERE was just `time >= ... AND time < ...`).
func stripBucketColPredicates(n *pg.Node, bucketCol string) *pg.Node {
	if n == nil {
		return nil
	}
	if be := n.GetBoolExpr(); be != nil && be.GetBoolop() == pg.BoolExprType_AND_EXPR {
		kept := make([]*pg.Node, 0, len(be.GetArgs()))
		for _, a := range be.GetArgs() {
			if isBucketColComparison(a, bucketCol) {
				continue
			}
			kept = append(kept, a)
		}
		if len(kept) == 0 {
			return nil
		}
		if len(kept) == 1 {
			return kept[0]
		}
		// Reconstruct an AND node with the kept args.
		return &pg.Node{Node: &pg.Node_BoolExpr{BoolExpr: &pg.BoolExpr{
			Boolop: pg.BoolExprType_AND_EXPR,
			Args:   kept,
		}}}
	}
	// Single non-AND predicate: drop it if it's the bucket comparison, else keep.
	if isBucketColComparison(n, bucketCol) {
		return nil
	}
	return n
}

// isBucketColComparison reports whether n is a bare `bucketCol OP literal`
// comparison (the shape HasTimeFilter consumes).
func isBucketColComparison(n *pg.Node, bucketCol string) bool {
	ax := n.GetAExpr()
	if ax == nil {
		return false
	}
	op := joinFuncName(ax.GetName())
	switch op {
	case ">=", ">", "<=", "<":
	default:
		return false
	}
	left := ax.GetLexpr().GetColumnRef()
	if left == nil || columnName(left) != bucketCol {
		return false
	}
	return true
}

// isAggregateName reports whether fc is one of the SQL aggregates the rewriter
// translation table knows about. Used by translateTargetExpr to decide
// whether to map vs deparse a target expression.
func isAggregateName(fc *pg.FuncCall) bool {
	switch strings.ToLower(joinFuncName(fc.GetFuncname())) {
	case "sum", "count", "avg", "min", "max",
		"approx_count_distinct", "quantile_cont", "percentile_cont":
		return true
	}
	return false
}

// firstColArgFromFuncCall walks the FuncCall's args and returns the first
// ColumnRef name found, or "" if none.
func firstColArgFromFuncCall(fc *pg.FuncCall) string {
	for _, a := range fc.GetArgs() {
		if c := a.GetColumnRef(); c != nil {
			return columnName(c)
		}
	}
	return ""
}

// isScalarNumericLit reports whether fc.args[idx] is a plain numeric A_Const
// (int or float). Used to gate the percentile rewrite - list-form quantiles
// like `percentile_cont([0.25,0.5,0.75])` parse as a list_value FuncCall, not
// an A_Const, and would silently fall through to the 0.5 default.
func isScalarNumericLit(fc *pg.FuncCall, idx int) bool {
	args := fc.GetArgs()
	if idx >= len(args) {
		return false
	}
	c := args[idx].GetAConst()
	if c == nil {
		return false
	}
	return c.GetIval() != nil || c.GetFval() != nil
}

// numericLitArg returns the numeric literal at args[idx], or 0.5 if absent.
// Callers should pre-check isScalarNumericLit to avoid the 0.5 fallback.
func numericLitArg(fc *pg.FuncCall, idx int) float64 {
	args := fc.GetArgs()
	if idx >= len(args) {
		return 0.5
	}
	a := args[idx]
	if c := a.GetAConst(); c != nil {
		if fv := c.GetFval(); fv != nil {
			var f float64
			fmt.Sscanf(fv.GetFval(), "%g", &f)
			return f
		}
		if iv := c.GetIval(); iv != nil {
			return float64(iv.GetIval())
		}
	}
	return 0.5
}

// findSketchConfig returns the SketchConfig for the (col, fn) pair in spec,
// or nil if no aggregation entry matches. Used by the percentile/HLL
// translations to thread the variant's K (precision) into the merge SQL.
func findSketchConfig(spec RollupSpec, col string, fn AggFunction) *SketchConfig {
	for _, agg := range spec.Aggregations {
		if agg.SourceColumn != col {
			continue
		}
		for _, f := range agg.Functions {
			if f == fn {
				return agg.SketchConfig
			}
		}
	}
	return nil
}

// specHas reports whether spec carries the (col, fn) pre-aggregation.
func specHas(spec RollupSpec, col string, fn AggFunction) bool {
	for _, agg := range spec.Aggregations {
		if agg.SourceColumn != col {
			continue
		}
		for _, f := range agg.Functions {
			if f == fn {
				return true
			}
		}
	}
	return false
}

// isNotNull reports whether col is in spec.NotNull (i.e., the operator has
// declared the column to be non-null in the source). Enables the
// COUNT(col) -> SUM(__row_count) shortcut.
func isNotNull(spec RollupSpec, col string) bool {
	for _, d := range spec.NotNull {
		if d == col {
			return true
		}
	}
	return false
}
