package rollup

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// Parse turns a raw aggregate SQL string into a QueryShape by walking DuckDB's
// own parse tree (json_serialize_sql), so we match the engine's grammar exactly
// instead of re-implementing a SQL parser. ok=false means "unsupported — fall
// through to source"; the reason is for diagnostics only. The cardinal rule is
// fidelity: we only return ok=true when the shape provably computes the same
// result as sql, so callers can serve it from a cube. When in doubt, reject.
func Parse(sql string, timeCol string) (QueryShape, bool, string) {
	// The Grafana plugin expands $__timeGroup(time, interval) to epoch-arithmetic
	// bucketing, not date_trunc. Rewrite it to a date_trunc placeholder so the AST
	// walk recognises a time bucket, and recover the real (second-based) grain.
	norm, grainSecs := normalizeEpochBucket(sql, timeCol)
	node, reason := serializeSelect(norm)
	if node == nil {
		return QueryShape{}, false, reason
	}
	q, ok, reason := parseSelectNode(node, timeCol)
	if ok && grainSecs > 0 {
		q.Grain = secGrain(grainSecs)
	}
	return q, ok, reason
}

// epochBucketRe matches the plugin's $__timeGroup expansion:
//
//	to_timestamp((epoch_ns(<col>) // 1000000000 // <N>) * <N>)
//
// capturing the time column and the bucket width N in seconds.
var epochBucketRe = regexp.MustCompile(
	`(?i)to_timestamp\(\(\s*epoch_ns\(\s*(\w+)\s*\)\s*//\s*1000000000\s*//\s*(\d+)\s*\)\s*\*\s*\d+\s*\)`)

// normalizeEpochBucket replaces the epoch-arithmetic bucket over timeCol with a
// date_trunc('hour', col) placeholder (so the rest of the AST parses as a normal
// time-bucketed aggregate) and returns the real bucket width in seconds. Returns
// 0 when no such bucket is present.
func normalizeEpochBucket(sql, timeCol string) (string, int) {
	m := epochBucketRe.FindStringSubmatch(sql)
	if m == nil || !strings.EqualFold(m[1], timeCol) {
		return sql, 0
	}
	secs, err := strconv.Atoi(m[2])
	if err != nil || secs <= 0 {
		return sql, 0
	}
	placeholder := fmt.Sprintf("date_trunc('hour', %s)", m[1])
	return epochBucketRe.ReplaceAllString(sql, placeholder), secs
}

// secGrain encodes a second-based grain (e.g. a 6h Grafana bucket -> "secs:21600").
func secGrain(n int) string { return "secs:" + strconv.Itoa(n) }

// limitOf extracts a LIMIT n from the SELECT modifiers. Returns 0 (no limit) when
// absent or not a simple positive integer — the safe default, since omitting the
// LIMIT only returns more rows, never wrong values. Preserving it lets a Grafana
// `ORDER BY time LIMIT n` match source (the cube is already bucket-ordered).
func limitOf(node map[string]any) int {
	mods, _ := node["modifiers"].([]any)
	for _, raw := range mods {
		m, _ := raw.(map[string]any)
		if str(m["type"]) != "LIMIT_MODIFIER" {
			continue
		}
		lim, _ := m["limit"].(map[string]any)
		if lim == nil {
			continue
		}
		if s := constString(lim); s != "" {
			if n, err := strconv.Atoi(s); err == nil && n > 0 {
				return n
			}
		}
	}
	return 0
}

// parseOrderBy resolves the query's ORDER BY terms to 1-based positions in the
// emitted select list (bucket?, dims…, aggs…). Returns false if any term can't be
// mapped — the caller rejects only when a LIMIT makes ordering result-affecting.
func parseOrderBy(node map[string]any, q *QueryShape, timeCol string) bool {
	mods, _ := node["modifiers"].([]any)
	var orders []any
	for _, raw := range mods {
		m, _ := raw.(map[string]any)
		if str(m["type"]) == "ORDER_MODIFIER" {
			orders, _ = m["orders"].([]any)
			break
		}
	}
	if len(orders) == 0 {
		return true
	}
	pos, total := q.outputPositions()
	var keys []OrderKey
	for _, raw := range orders {
		o, _ := raw.(map[string]any)
		expr, _ := o["expression"].(map[string]any)
		p, ok := q.resolveOrderPos(expr, pos, total, timeCol)
		if !ok {
			return false
		}
		keys = append(keys, OrderKey{Pos: p, Desc: str(o["type"]) == "DESCENDING"})
	}
	q.OrderBy = keys
	return true
}

// outputPositions maps each select output's reference name (bucket alias, dim, agg
// alias) to its 1-based position, and returns the total column count.
func (q QueryShape) outputPositions() (map[string]int, int) {
	pos := map[string]int{}
	n := 0
	if q.Grain != "" {
		n++
		pos[q.bucketAlias()] = n
	}
	for _, d := range q.Dims {
		n++
		pos[d] = n
	}
	for _, a := range q.Aggs {
		n++
		if a.Alias != "" {
			pos[a.Alias] = n
		}
	}
	return pos, n
}

// resolveOrderPos maps one ORDER BY expression to a select-list position: a
// positional constant, a name reference (bucket/dim/agg alias), the time-bucket
// expression, or an aggregate expression matching one of the query's aggregates.
func (q QueryShape) resolveOrderPos(expr map[string]any, pos map[string]int, total int, timeCol string) (int, bool) {
	if expr == nil {
		return 0, false
	}
	if i, ok := constInt(expr); ok && expr["class"] == "CONSTANT" && i >= 1 && i <= total {
		return i, true
	}
	if name, ok := bareColumn(expr); ok {
		if p, found := pos[name]; found {
			return p, true
		}
		return 0, false
	}
	if _, ok := timeBucketGrain(expr, timeCol); ok && q.Grain != "" {
		return q.bucketPos(), true
	}
	if a, isAgg, reason := parseAggregate(expr, ""); isAgg && reason == "" {
		for i, qa := range q.Aggs {
			if qa.Kind == a.Kind && qa.Col == a.Col && qa.P == a.P {
				return q.aggBasePos() + i + 1, true
			}
		}
	}
	return 0, false
}

func (q QueryShape) bucketPos() int {
	if q.Grain != "" {
		return 1
	}
	return 0
}

// aggBasePos is the position just before the first aggregate (bucket? + dims).
func (q QueryShape) aggBasePos() int {
	base := len(q.Dims)
	if q.Grain != "" {
		base++
	}
	return base
}

// parseDB is a process-wide in-memory connection used purely to parse SQL. No
// data, no S3, no extensions — json_serialize_sql is a pure parser front-end. It
// is lazily opened ONCE under parseOnce: the read path calls parseConn from many
// query goroutines concurrently, so the unsynchronized lazy-init it used to do
// (read-then-assign the global) was itself a data race the race detector flags.
// sql.Open returns a *sql.DB connection pool that is safe for concurrent use, so
// once initialized every goroutine shares the one pool with no further locking.
var (
	parseOnce sync.Once
	parseDB   *sql.DB
	parseErr  error
)

func parseConn() (*sql.DB, error) {
	parseOnce.Do(func() {
		parseDB, parseErr = sql.Open("duckdb", "")
	})
	return parseDB, parseErr
}

// serializeSelect returns the SELECT_NODE of the (single) statement in sql, or a
// reason it can't be used. Anything that isn't a lone plain SELECT is rejected.
func serializeSelect(sql string) (map[string]any, string) {
	db, err := parseConn()
	if err != nil {
		return nil, "parser unavailable: " + err.Error()
	}
	var js string
	// json_serialize_sql requires a constant VARCHAR (it plans the inner query at
	// bind time), so the user SQL must be inlined as an escaped literal rather
	// than bound as a parameter.
	lit := "'" + strings.ReplaceAll(sql, "'", "''") + "'"
	// Cast the JSON result to VARCHAR so the driver returns a plain string rather
	// than a decoded JSON map.
	if err := db.QueryRow("SELECT json_serialize_sql(" + lit + ")::VARCHAR").Scan(&js); err != nil {
		return nil, "serialize failed: " + err.Error()
	}
	var root map[string]any
	if err := json.Unmarshal([]byte(js), &root); err != nil {
		return nil, "bad serialize json: " + err.Error()
	}
	if e, _ := root["error"].(bool); e {
		return nil, "parse error: " + str(root["error_message"])
	}
	stmts, _ := root["statements"].([]any)
	if len(stmts) != 1 {
		return nil, "expected exactly one statement"
	}
	stmt, _ := stmts[0].(map[string]any)
	node, _ := stmt["node"].(map[string]any)
	if node == nil || node["type"] != "SELECT_NODE" {
		return nil, "not a plain SELECT"
	}
	// CTEs change FROM-resolution semantics we don't model — reject if present.
	if cte, _ := node["cte_map"].(map[string]any); cte != nil {
		if m, _ := cte["map"].([]any); len(m) > 0 {
			return nil, "CTE not supported"
		}
	}
	return node, ""
}

func parseSelectNode(node map[string]any, timeCol string) (QueryShape, bool, string) {
	q := QueryShape{TimeCol: timeCol}

	// FROM must be a single base table; JOIN/subquery/VALUES change semantics.
	src, reason := parseSource(node)
	if reason != "" {
		return QueryShape{}, false, reason
	}
	q.Source = src

	// SELECT list -> grain (time bucket), dims, aggregates.
	grain, bucketAlias, dims, aggs, reason := parseSelectList(node, timeCol)
	if reason != "" {
		return QueryShape{}, false, reason
	}
	q.Grain = grain
	q.BucketAlias = bucketAlias
	q.Dims = dims
	q.Aggs = aggs
	q.Limit = limitOf(node)

	// ORDER BY must be reproduced exactly when a LIMIT is present, or TopN returns
	// the wrong rows. When it can't be mapped to the cube's output, only a LIMIT
	// makes it unsafe — without one, row order doesn't change the result set.
	if !parseOrderBy(node, &q, timeCol) && q.Limit > 0 {
		return QueryShape{}, false, "ORDER BY target not reproducible under LIMIT"
	}

	// WHERE -> time range (peeled off) + post-agg filters.
	lo, hi, filters, reason := parseWhere(node, timeCol)
	if reason != "" {
		return QueryShape{}, false, reason
	}
	q.TimeLo = lo
	q.TimeHi = hi
	q.Filters = filters

	// HAVING/QUALIFY/DISTINCT/sampling alter results in ways the cube can't model.
	if node["having"] != nil {
		return QueryShape{}, false, "HAVING not supported"
	}
	if node["qualify"] != nil {
		return QueryShape{}, false, "QUALIFY not supported"
	}
	if node["sample"] != nil {
		return QueryShape{}, false, "TABLESAMPLE not supported"
	}

	// A SELECT-list time bucket must be backed by a matching GROUP BY entry, and
	// every selected dim must be grouped, or the result isn't a clean aggregate.
	if reason := checkGroupBy(node, timeCol, grain != "", dims); reason != "" {
		return QueryShape{}, false, reason
	}
	return q, true, ""
}

// --- FROM --------------------------------------------------------------------

func parseSource(node map[string]any) (string, string) {
	ft, _ := node["from_table"].(map[string]any)
	if ft == nil {
		return "", "no FROM clause"
	}
	if ft["type"] != "BASE_TABLE" {
		return "", "FROM is not a single base table (" + str(ft["type"]) + ")"
	}
	table := str(ft["table_name"])
	if table == "" {
		return "", "empty table name"
	}
	// Qualify with the explicit schema if the user wrote db.table; else default.
	if schema := str(ft["schema_name"]); schema != "" {
		return schema + "." + table, ""
	}
	return "default." + table, ""
}

// --- SELECT list -------------------------------------------------------------

func parseSelectList(node map[string]any, timeCol string) (grain, bucketAlias string, dims []string, aggs []Aggregate, reason string) {
	list, _ := node["select_list"].([]any)
	if len(list) == 0 {
		return "", "", nil, nil, "empty select list"
	}
	for _, raw := range list {
		item, _ := raw.(map[string]any)
		alias := str(item["alias"])

		if g, ok := timeBucketGrain(item, timeCol); ok {
			if grain != "" {
				return "", "", nil, nil, "multiple time buckets"
			}
			grain = g
			bucketAlias = alias
			continue
		}
		if col, ok := bareColumn(item); ok {
			if col == timeCol {
				// Selecting the raw time column (no bucket) would make every row a
				// distinct group — not an aggregate shape we materialize.
				return "", "", nil, nil, "raw time column in select"
			}
			dims = append(dims, col)
			continue
		}
		if a, ok, r := parseAggregate(item, alias); r != "" {
			return "", "", nil, nil, r
		} else if ok {
			// SourceRefSQL emits `AS "<alias>"`, so an empty alias would render an
			// invalid zero-length identifier. Synthesize a stable one by position.
			if a.Alias == "" {
				a.Alias = fmt.Sprintf("agg%d", len(aggs))
			}
			aggs = append(aggs, a)
			continue
		}
		return "", "", nil, nil, "unsupported select expression: " + describeExpr(item)
	}
	if len(aggs) == 0 {
		return "", "", nil, nil, "no aggregate in select list"
	}
	return grain, bucketAlias, dims, aggs, ""
}

// timeBucketGrain recognises date_trunc('<grain>', timeCol) and
// time_bucket(INTERVAL '<n unit>', timeCol) over the time column.
func timeBucketGrain(item map[string]any, timeCol string) (string, bool) {
	if item["class"] != "FUNCTION" {
		return "", false
	}
	fn := str(item["function_name"])
	args, _ := item["children"].([]any)
	switch fn {
	case "date_trunc", "datetrunc":
		if len(args) != 2 {
			return "", false
		}
		unit := strings.ToLower(constString(arg(args, 0)))
		if !isGrain(unit) {
			return "", false
		}
		if col, ok := bareColumnExpr(arg(args, 1)); ok && col == timeCol {
			return unit, true
		}
	case "time_bucket":
		if len(args) != 2 {
			return "", false
		}
		if col, ok := bareColumnExpr(arg(args, 1)); !ok || col != timeCol {
			return "", false
		}
		if g, ok := intervalGrain(arg(args, 0)); ok {
			return g, true
		}
	}
	return "", false
}

func isGrain(u string) bool {
	switch u {
	case "hour", "day", "week", "month":
		return true
	}
	return false
}

// intervalGrain maps an INTERVAL literal to a grain, but only the unit-magnitude
// intervals our cubes can represent (1 hour/day/week/month); anything else (e.g.
// "15 minutes", "2 hours") has no matching cube grain and is rejected.
func intervalGrain(node map[string]any) (string, bool) {
	body := strings.ToLower(strings.TrimSpace(constString(unwrapCast(node))))
	switch body {
	case "1 hour", "hour":
		return "hour", true
	case "1 day", "day":
		return "day", true
	case "1 week", "week":
		return "week", true
	case "1 month", "month":
		return "month", true
	}
	return "", false
}

// parseAggregate recognises the supported aggregate functions. The bool reports
// "this was an aggregate FUNCTION node"; reason!="" means it was an aggregate we
// can't represent (so the whole query must fall through, not silently drop it).
func parseAggregate(item map[string]any, alias string) (Aggregate, bool, string) {
	if item["class"] != "FUNCTION" {
		return Aggregate{}, false, ""
	}
	fn := str(item["function_name"])
	args, _ := item["children"].([]any)
	distinct, _ := item["distinct"].(bool)
	// FILTER (WHERE ...) and ORDER BY inside an aggregate change its value.
	if item["filter"] != nil {
		return Aggregate{}, true, "aggregate with FILTER not supported"
	}
	if ob, _ := item["order_bys"].(map[string]any); ob != nil {
		if o, _ := ob["orders"].([]any); len(o) > 0 {
			return Aggregate{}, true, "ordered aggregate not supported"
		}
	}

	switch fn {
	case "count_star":
		return Aggregate{Kind: AggCount, Alias: alias}, true, ""
	case "count":
		if !distinct {
			if a, ok, reason := parseCaseAgg("count", arg(args, 0), alias); ok {
				return a, true, ""
			} else if reason != "" {
				return Aggregate{}, true, reason
			}
		}
		col, ok := bareColumnExpr(arg(args, 0))
		if !ok {
			return Aggregate{}, true, "count() over non-column"
		}
		if distinct {
			return Aggregate{Kind: AggCountDistinct, Col: col, Alias: alias}, true, ""
		}
		return Aggregate{Kind: AggCountCol, Col: col, Alias: alias}, true, ""
	case "sum", "min", "max", "avg":
		if fn == "sum" {
			if a, ok, reason := parseCaseAgg("sum", arg(args, 0), alias); ok {
				return a, true, ""
			} else if reason != "" {
				return Aggregate{}, true, reason
			}
		}
		col, ok := bareColumnExpr(arg(args, 0))
		if !ok {
			return Aggregate{}, true, fn + "() over non-column"
		}
		kind := map[string]AggKind{"sum": AggSum, "min": AggMin, "max": AggMax, "avg": AggAvg}[fn]
		return Aggregate{Kind: kind, Col: col, Alias: alias}, true, ""
	case "quantile_cont", "percentile_cont", "approx_quantile":
		if len(args) != 2 {
			return Aggregate{}, true, fn + "() needs (col, p)"
		}
		col, ok := bareColumnExpr(arg(args, 0))
		if !ok {
			return Aggregate{}, true, fn + "() over non-column"
		}
		p, ok := constFloat(arg(args, 1))
		if !ok || p < 0 || p > 1 {
			return Aggregate{}, true, fn + "() with non-constant or out-of-range p"
		}
		return Aggregate{Kind: AggPercentile, Col: col, P: p, Alias: alias}, true, ""
	}
	return Aggregate{}, true, "unsupported aggregate: " + fn
}

// parseCaseAgg recognises a conditional aggregate — SUM/COUNT(CASE WHEN <pred>
// THEN <x> [ELSE <y>] END) — whose predicate references only columns that will be
// cube dimensions. Return values:
//
//	(agg, true, "")     — a faithfully representable conditional aggregate
//	(_,   false, reason)— it IS a CASE but we can't model it (reject the query)
//	(_,   false, "")    — child is not a CASE at all (caller continues)
//
// Correctness: each cube row aggregates _cnt source rows that share identical
// dimension values, so the predicate (built only from dimensions) is constant
// across those rows — they all take one CASE branch. SUM therefore re-aggregates
// exactly: constant THEN/ELSE scale by _cnt, a metric THEN uses its summed column.
func parseCaseAgg(fn string, child map[string]any, alias string) (Aggregate, bool, string) {
	if child == nil || str(child["class"]) != "CASE" {
		return Aggregate{}, false, ""
	}
	checks, _ := child["case_checks"].([]any)
	if len(checks) != 1 {
		return Aggregate{}, false, "conditional aggregate needs exactly one WHEN"
	}
	chk, _ := checks[0].(map[string]any)
	whenExpr, _ := chk["when_expr"].(map[string]any)
	thenExpr, _ := chk["then_expr"].(map[string]any)
	elseExpr, _ := child["else_expr"].(map[string]any)

	cond, cols, ok := renderCondPredicate(whenExpr)
	if !ok {
		return Aggregate{}, false, "conditional aggregate predicate not representable"
	}
	a := Aggregate{Kind: AggCondSum, Alias: alias, Cond: cond, CondCols: cols, ThenK: "1", ElseK: "0"}

	if fn == "count" {
		a.FromCount = true
		// COUNT(CASE WHEN p THEN <non-null const> END) counts the predicate rows.
		// Only the THEN's non-nullness matters; a non-NULL ELSE would also count the
		// else rows, changing the meaning — reject it.
		if !isConstNonNull(thenExpr) {
			return Aggregate{}, false, "count(CASE) THEN must be a non-null constant"
		}
		if !isNullConst(elseExpr) {
			return Aggregate{}, false, "count(CASE) with a non-NULL ELSE not supported"
		}
		return a, true, ""
	}

	// fn == "sum": THEN may be a numeric constant or a single metric column; ELSE
	// must be a numeric constant (absent/NULL ELSE behaves as 0 — SUM skips NULL).
	if col, isCol := bareColumnExpr(thenExpr); isCol {
		a.ThenCol = col
		a.ThenK = ""
	} else if isNumericConst(thenExpr) {
		a.ThenK = constString(thenExpr)
	} else {
		return Aggregate{}, false, "sum(CASE) THEN must be a numeric constant or a column"
	}
	if isNullConst(elseExpr) {
		a.ElseK = "0"
	} else if isNumericConst(elseExpr) {
		a.ElseK = constString(elseExpr)
	} else {
		return Aggregate{}, false, "sum(CASE) ELSE must be a numeric constant"
	}
	return a, true, ""
}

// renderCondPredicate renders a CASE WHEN predicate to SQL over (eventual)
// dimension columns and returns the columns it references. Only constructs we can
// reproduce verbatim — comparisons (incl. ranges), AND/OR/NOT, IN/NOT IN, IS
// [NOT] NULL — are accepted; anything else returns ok=false so the whole
// conditional aggregate is rejected and the query falls through to source.
func renderCondPredicate(node map[string]any) (string, []string, bool) {
	if node == nil {
		return "", nil, false
	}
	class := str(node["class"])
	typ := str(node["type"])
	switch {
	case class == "CONJUNCTION":
		var op string
		switch typ {
		case "CONJUNCTION_AND":
			op = " AND "
		case "CONJUNCTION_OR":
			op = " OR "
		default:
			return "", nil, false
		}
		kids := childList(node)
		if len(kids) < 2 {
			return "", nil, false
		}
		var parts, cols []string
		for _, c := range kids {
			s, cc, ok := renderCondPredicate(c)
			if !ok {
				return "", nil, false
			}
			parts = append(parts, s)
			cols = append(cols, cc...)
		}
		return "(" + strings.Join(parts, op) + ")", cols, true

	case typ == "OPERATOR_NOT":
		kids := childList(node)
		if len(kids) != 1 {
			return "", nil, false
		}
		s, cc, ok := renderCondPredicate(kids[0])
		if !ok {
			return "", nil, false
		}
		return "NOT (" + s + ")", cc, true

	case class == "COMPARISON":
		op, ok := condCompareOp(typ)
		if !ok {
			return "", nil, false
		}
		left, _ := node["left"].(map[string]any)
		right, _ := node["right"].(map[string]any)
		if col, isCol := bareColumnExpr(left); isCol && isConst(right) {
			return fmt.Sprintf("%q %s %s", col, op, litValue(constString(right))), []string{col}, true
		}
		if col, isCol := bareColumnExpr(right); isCol && isConst(left) {
			return fmt.Sprintf("%q %s %s", col, flipCompare(op), litValue(constString(left))), []string{col}, true
		}
		return "", nil, false

	case typ == "COMPARE_IN" || typ == "COMPARE_NOT_IN":
		kids := childList(node)
		if len(kids) < 2 {
			return "", nil, false
		}
		col, isCol := bareColumn(kids[0])
		if !isCol {
			return "", nil, false
		}
		vals := make([]string, 0, len(kids)-1)
		for _, c := range kids[1:] {
			if !isConst(c) {
				return "", nil, false
			}
			vals = append(vals, litValue(constString(c)))
		}
		kw := "IN"
		if typ == "COMPARE_NOT_IN" {
			kw = "NOT IN"
		}
		return fmt.Sprintf("%q %s (%s)", col, kw, strings.Join(vals, ", ")), []string{col}, true

	case typ == "OPERATOR_IS_NULL" || typ == "OPERATOR_IS_NOT_NULL":
		kids := childList(node)
		if len(kids) != 1 {
			return "", nil, false
		}
		col, isCol := bareColumn(kids[0])
		if !isCol {
			return "", nil, false
		}
		kw := "IS NULL"
		if typ == "OPERATOR_IS_NOT_NULL" {
			kw = "IS NOT NULL"
		}
		return fmt.Sprintf("%q %s", col, kw), []string{col}, true
	}
	return "", nil, false
}

func condCompareOp(t string) (string, bool) {
	op, ok := map[string]string{
		"COMPARE_EQUAL":                "=",
		"COMPARE_NOTEQUAL":             "<>",
		"COMPARE_LESSTHAN":             "<",
		"COMPARE_GREATERTHAN":          ">",
		"COMPARE_LESSTHANOREQUALTO":    "<=",
		"COMPARE_GREATERTHANOREQUALTO": ">=",
	}[t]
	return op, ok
}

// flipCompare flips a comparison operator for the const-on-left rendering order.
func flipCompare(op string) string {
	return map[string]string{"=": "=", "<>": "<>", "<": ">", ">": "<", "<=": ">=", ">=": "<="}[op]
}

func isConst(n map[string]any) bool { return n != nil && str(n["class"]) == "CONSTANT" }

// isNullConst reports a NULL constant. An absent else_expr (nil) is also NULL.
func isNullConst(n map[string]any) bool {
	if n == nil {
		return true
	}
	val, _ := n["value"].(map[string]any)
	if val == nil {
		return false
	}
	b, _ := val["is_null"].(bool)
	return b
}

func isConstNonNull(n map[string]any) bool { return isConst(n) && !isNullConst(n) }

func isNumericConst(n map[string]any) bool {
	if !isConstNonNull(n) {
		return false
	}
	_, ok := constFloat(n)
	return ok
}

// --- WHERE -------------------------------------------------------------------

// parseWhere peels the two-sided time range (timeCol >= lo AND timeCol < hi) off
// the top-level AND conjunction and converts every remaining predicate into a
// Filter. The time range must be a clean >=/< pair so it maps onto the cube's
// half-open bucket model; other shapes (>, <=, BETWEEN, one-sided) are rejected.
func parseWhere(node map[string]any, timeCol string) (lo, hi string, filters []Filter, reason string) {
	where, _ := node["where_clause"].(map[string]any)
	if where == nil {
		return "", "", nil, "missing time range (no WHERE)"
	}
	for _, pred := range conjuncts(where) {
		if bound, val, ok := timeBound(pred, timeCol); ok {
			switch bound {
			case ">=":
				if lo != "" {
					return "", "", nil, "duplicate lower time bound"
				}
				lo = val
			case "<":
				if hi != "" {
					return "", "", nil, "duplicate upper time bound"
				}
				hi = val
			default:
				return "", "", nil, "time bound must be >= and < (got " + bound + ")"
			}
			continue
		}
		// A non-time predicate that still references the time column can't be a
		// dimension filter we re-apply post-agg.
		if mentionsColumn(pred, timeCol) {
			return "", "", nil, "unsupported predicate on time column"
		}
		f, ok := parseFilter(pred)
		if !ok {
			return "", "", nil, "unsupported WHERE predicate: " + describeExpr(pred)
		}
		filters = append(filters, f...)
	}
	if lo == "" || hi == "" {
		return "", "", nil, "WHERE lacks a two-sided time range"
	}
	return lo, hi, filters, ""
}

// conjuncts flattens a top-level AND tree into its leaf predicates. A non-AND
// node is its own single conjunct.
func conjuncts(node map[string]any) []map[string]any {
	if node["type"] == "CONJUNCTION_AND" {
		var out []map[string]any
		for _, c := range childList(node) {
			out = append(out, conjuncts(c)...)
		}
		return out
	}
	return []map[string]any{node}
}

// timeBound recognises a comparison between the time column and a timestamp/date
// literal, returning the operator (normalised so the column is on the left) and
// the literal body.
func timeBound(pred map[string]any, timeCol string) (op, val string, ok bool) {
	if pred["class"] != "COMPARISON" {
		return "", "", false
	}
	left, _ := pred["left"].(map[string]any)
	right, _ := pred["right"].(map[string]any)
	lc, lok := bareColumnExpr(left)
	rc, rok := bareColumnExpr(right)

	var lit map[string]any
	switch {
	case lok && lc == timeCol && !rok:
		lit, op = right, compareOp(str(pred["type"]), false)
	case rok && rc == timeCol && !lok:
		lit, op = left, compareOp(str(pred["type"]), true)
	default:
		return "", "", false
	}
	body, ok := timestampLiteral(lit)
	if !ok {
		return "", "", false
	}
	return op, body, true
}

// compareOp maps a DuckDB comparison type to its operator string, flipping the
// direction when the column was on the right-hand side.
func compareOp(t string, flipped bool) string {
	op := map[string]string{
		"COMPARE_GREATERTHANOREQUALTO": ">=",
		"COMPARE_LESSTHAN":             "<",
		"COMPARE_GREATERTHAN":          ">",
		"COMPARE_LESSTHANOREQUALTO":    "<=",
	}[t]
	if !flipped {
		return op
	}
	return map[string]string{">=": "<=", "<": ">", ">": "<", "<=": ">="}[op]
}

// timestampLiteral extracts the literal body from a TIMESTAMP/TIMESTAMPTZ/DATE
// cast over a constant (the form DuckDB produces for typed literals).
func timestampLiteral(node map[string]any) (string, bool) {
	if node["class"] == "CAST" {
		ct, _ := node["cast_type"].(map[string]any)
		switch str(ct["id"]) {
		case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "DATE":
		default:
			return "", false
		}
		child, _ := node["child"].(map[string]any)
		if child["class"] != "CONSTANT" {
			return "", false
		}
		return constString(child), true
	}
	// Bare string constant: the Grafana plugin's $__timeFilter emits
	// `time >= 'RFC3339'` (no cast). Accept it only when it parses as a timestamp.
	if node["class"] == "CONSTANT" {
		s := constString(node)
		if _, ok := parseTS(s); ok {
			return s, true
		}
	}
	return "", false
}

// parseFilter converts one WHERE predicate into Filter(s). It returns multiple
// filters only for the same-column OR case (os=a OR os=b -> IN). Anything not
// faithfully representable in the flat Filter model returns ok=false.
func parseFilter(pred map[string]any) ([]Filter, bool) {
	switch pred["type"] {
	case "COMPARE_EQUAL", "COMPARE_NOTEQUAL", "COMPARE_DISTINCT_FROM":
		col, lit, ok := colLit(pred)
		if !ok {
			return nil, false
		}
		op := OpEq
		if pred["type"] != "COMPARE_EQUAL" {
			op = OpNe
		}
		return []Filter{{Col: col, Op: op, Values: []string{lit}}}, true

	case "COMPARE_IN", "COMPARE_NOT_IN":
		kids := childList(pred)
		col, ok := bareColumnExpr(arg(toAny(kids), 0))
		if !ok || len(kids) < 2 {
			return nil, false
		}
		vals := make([]string, 0, len(kids)-1)
		for _, c := range kids[1:] {
			if c["class"] != "CONSTANT" {
				return nil, false
			}
			vals = append(vals, constString(c))
		}
		op := OpIn
		if pred["type"] == "COMPARE_NOT_IN" {
			op = OpNotIn
		}
		return []Filter{{Col: col, Op: op, Values: vals}}, true

	case "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL":
		kids := childList(pred)
		col, ok := bareColumnExpr(arg(toAny(kids), 0))
		if !ok || len(kids) != 1 {
			return nil, false
		}
		op := OpIsNull
		if pred["type"] == "OPERATOR_IS_NOT_NULL" {
			op = OpIsNotNull
		}
		return []Filter{{Col: col, Op: op}}, true

	case "CONJUNCTION_OR":
		// Only a disjunction of equality tests on a single column is faithfully
		// representable (col=a OR col=b == col IN (a,b)). Mixed columns or any
		// non-equality branch can't collapse to the flat Filter model -> reject.
		var col string
		var vals []string
		for _, c := range childList(pred) {
			if c["type"] != "COMPARE_EQUAL" {
				return nil, false
			}
			cc, lit, ok := colLit(c)
			if !ok || (col != "" && cc != col) {
				return nil, false
			}
			col = cc
			vals = append(vals, lit)
		}
		if col == "" {
			return nil, false
		}
		return []Filter{{Col: col, Op: OpIn, Values: vals}}, true
	}
	return nil, false
}

// colLit extracts (column, literal) from a binary comparison in either operand
// order.
func colLit(pred map[string]any) (col, lit string, ok bool) {
	left, _ := pred["left"].(map[string]any)
	right, _ := pred["right"].(map[string]any)
	if c, isCol := bareColumnExpr(left); isCol && right["class"] == "CONSTANT" {
		return c, constString(right), true
	}
	if c, isCol := bareColumnExpr(right); isCol && left["class"] == "CONSTANT" {
		return c, constString(left), true
	}
	return "", "", false
}

// --- GROUP BY validation -----------------------------------------------------

// checkGroupBy verifies the GROUP BY keys are exactly {time bucket?} ∪ dims,
// resolving positional (1,2,...) and named/expression group entries against the
// select list. A mismatch means the SQL groups differently than our shape would.
func checkGroupBy(node map[string]any, timeCol string, hasBucket bool, dims []string) string {
	groups, _ := node["group_expressions"].([]any)
	want := len(dims)
	if hasBucket {
		want++
	}
	if want == 0 {
		if len(groups) != 0 {
			return "GROUP BY without grouping columns in shape"
		}
		return ""
	}
	if len(groups) != want {
		return "GROUP BY arity does not match selected dims"
	}
	list, _ := node["select_list"].([]any)
	bucketSeen := false
	dimSeen := map[string]bool{}
	for _, raw := range groups {
		g, _ := raw.(map[string]any)
		// Positional reference resolves to the matching select item.
		if g["class"] == "CONSTANT" {
			i, ok := constInt(g)
			if !ok || i < 1 || i > len(list) {
				return "unresolvable positional GROUP BY"
			}
			g, _ = list[i-1].(map[string]any)
		} else if col, ok := bareColumn(g); ok {
			// A bare ref may be an output-alias reference (GROUP BY b) rather than a
			// real column; resolve it to the aliased select item when one matches.
			if item := selectItemByAlias(list, col); item != nil {
				g = item
			}
		}
		if _, ok := timeBucketGrain(g, timeCol); ok {
			bucketSeen = true
			continue
		}
		if col, ok := bareColumn(g); ok {
			dimSeen[col] = true
			continue
		}
		return "GROUP BY has an expression not in the shape"
	}
	if hasBucket != bucketSeen {
		return "time bucket present in SELECT but not GROUP BY (or vice versa)"
	}
	for _, d := range dims {
		if !dimSeen[d] {
			return "selected dim " + d + " is not grouped"
		}
	}
	return ""
}

// selectItemByAlias finds the select-list item carrying the given output alias.
func selectItemByAlias(list []any, alias string) map[string]any {
	for _, raw := range list {
		if item, _ := raw.(map[string]any); item != nil && str(item["alias"]) == alias {
			return item
		}
	}
	return nil
}

// --- small AST helpers -------------------------------------------------------

// bareColumn reports a single unqualified column reference (a dimension), so a
// qualified ref (t.col) or any expression is not mistaken for a dim.
func bareColumn(item map[string]any) (string, bool) {
	if item["class"] != "COLUMN_REF" {
		return "", false
	}
	names, _ := item["column_names"].([]any)
	if len(names) != 1 {
		return "", false
	}
	return str(names[0]), true
}

// bareColumnExpr is bareColumn for a possibly-nil node.
func bareColumnExpr(item map[string]any) (string, bool) {
	if item == nil {
		return "", false
	}
	return bareColumn(item)
}

func mentionsColumn(node map[string]any, col string) bool {
	if node == nil {
		return false
	}
	if c, ok := bareColumn(node); ok && c == col {
		return true
	}
	for _, v := range node {
		switch x := v.(type) {
		case map[string]any:
			if mentionsColumn(x, col) {
				return true
			}
		case []any:
			for _, e := range x {
				if m, ok := e.(map[string]any); ok && mentionsColumn(m, col) {
					return true
				}
			}
		}
	}
	return false
}

func unwrapCast(node map[string]any) map[string]any {
	if node != nil && node["class"] == "CAST" {
		if c, ok := node["child"].(map[string]any); ok {
			return c
		}
	}
	return node
}

// constString returns a constant's value rendered as a string (literal body),
// covering both VARCHAR values and numeric literals (which arrive as JSON
// numbers). DECIMAL literals are scaled back from their integer storage.
func constString(node map[string]any) string {
	node = unwrapCast(node)
	val, _ := node["value"].(map[string]any)
	if val == nil {
		return ""
	}
	switch v := val["value"].(type) {
	case string:
		return v
	case float64:
		if ti, _ := val["type"].(map[string]any); ti != nil && str(ti["id"]) == "DECIMAL" {
			if f, ok := constFloat(node); ok {
				return strconv.FormatFloat(f, 'f', -1, 64)
			}
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	}
	return ""
}

func constInt(node map[string]any) (int, bool) {
	val, _ := node["value"].(map[string]any)
	if val == nil {
		return 0, false
	}
	switch v := val["value"].(type) {
	case float64:
		return int(v), true
	case string:
		i, err := strconv.Atoi(v)
		return i, err == nil
	}
	return 0, false
}

// constFloat returns a numeric constant as a float, decoding DECIMAL literals
// (stored as a scaled integer + scale, e.g. 95 @ scale 2 -> 0.95).
func constFloat(node map[string]any) (float64, bool) {
	val, _ := node["value"].(map[string]any)
	if val == nil {
		return 0, false
	}
	var raw float64
	switch v := val["value"].(type) {
	case float64:
		raw = v
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		raw = f
	default:
		return 0, false
	}
	if ti, _ := val["type"].(map[string]any); ti != nil {
		if str(ti["id"]) == "DECIMAL" {
			if info, _ := ti["type_info"].(map[string]any); info != nil {
				if sc, ok := info["scale"].(float64); ok {
					for i := 0; i < int(sc); i++ {
						raw /= 10
					}
				}
			}
		}
	}
	return raw, true
}

func childList(node map[string]any) []map[string]any {
	kids, _ := node["children"].([]any)
	out := make([]map[string]any, 0, len(kids))
	for _, k := range kids {
		if m, ok := k.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func toAny(ms []map[string]any) []any {
	out := make([]any, len(ms))
	for i, m := range ms {
		out[i] = m
	}
	return out
}

func arg(args []any, i int) map[string]any {
	if i < 0 || i >= len(args) {
		return nil
	}
	m, _ := args[i].(map[string]any)
	return m
}

func describeExpr(node map[string]any) string {
	if fn := str(node["function_name"]); fn != "" {
		return fn + "()"
	}
	if t := str(node["type"]); t != "" {
		return t
	}
	return str(node["class"])
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
