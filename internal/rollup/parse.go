package rollup

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Parse turns a raw aggregate SQL string into a QueryShape by walking DuckDB's
// own parse tree (json_serialize_sql), so we match the engine's grammar exactly
// instead of re-implementing a SQL parser. ok=false means "unsupported — fall
// through to source"; the reason is for diagnostics only. The cardinal rule is
// fidelity: we only return ok=true when the shape provably computes the same
// result as sql, so callers can serve it from a cube. When in doubt, reject.
func Parse(sql string, timeCol string) (QueryShape, bool, string) {
	return ParseWithDB(sql, timeCol, "")
}

// ParseWithDB is Parse with the request's database context (the x-arc-database
// HTTP header) threaded through: an unqualified FROM table resolves to
// defaultDB when set, matching how the query handler executes the same SQL.
// Without it, a posthog dashboard's `FROM events` was parsed — and RECORDED —
// as default.events: it could never match posthog.events cubes and it polluted
// the default workload. An explicit db.table in the SQL always wins, mirroring
// the executor's precedence; defaultDB "" falls back to "default".
func ParseWithDB(sql, timeCol, defaultDB string) (QueryShape, bool, string) {
	return ParseWithOpts(sql, timeCol, defaultDB, ParseOpts{})
}

// ParseWithOpts is ParseWithDB with optional parser capabilities (ParseOpts);
// the zero ParseOpts is ParseWithDB.
func ParseWithOpts(sql, timeCol, defaultDB string, opts ParseOpts) (QueryShape, bool, string) {
	// The Grafana plugin expands $__timeGroup(time, interval) to epoch-arithmetic
	// bucketing, not date_trunc. Rewrite it to a date_trunc placeholder so the AST
	// walk recognises a time bucket, and recover the real (second-based) grain.
	norm, ok := normalizeEpochBucket(sql, timeCol)
	if !ok {
		return QueryShape{}, false, "reserved name " + epochPlaceholder
	}
	node, reason := serializeSelect(norm)
	if node == nil {
		return QueryShape{}, false, reason
	}
	return parseSelectNode(node, timeCol, defaultDB, opts)
}

// epochBucketRe matches the plugin's $__timeGroup expansion:
//
//	to_timestamp((epoch_ns(<col>) // 1000000000 // <N>) * <N>)
//
// capturing the time column, the divisor N and the multiplier (which must be N).
var epochBucketRe = regexp.MustCompile(
	`(?i)to_timestamp\(\(\s*epoch_ns\(\s*(\w+)\s*\)\s*//\s*1000000000\s*//\s*(\d+)\s*\)\s*\*\s*(\d+)\s*\)`)

// epochPlaceholder is the function each recognised epoch bucket is rewritten
// to before parsing: __arc_epoch_bucket(<col>, N). It keeps every bucket's own
// width N, so two buckets of different width never compare equal.
const epochPlaceholder = "__arc_epoch_bucket"

// normalizeEpochBucket rewrites each epoch-arithmetic bucket over timeCol whose
// multiplier equals its divisor, outside string literals, to the placeholder.
// ok=false when the SQL already names the placeholder (it cannot be spoofed).
func normalizeEpochBucket(sql, timeCol string) (string, bool) {
	if strings.Contains(strings.ToLower(sql), epochPlaceholder) {
		return "", false
	}
	var b strings.Builder
	last := 0
	for _, m := range epochBucketRe.FindAllStringSubmatchIndex(sql, -1) {
		col, div, mul := sql[m[2]:m[3]], sql[m[4]:m[5]], sql[m[6]:m[7]]
		n, err := strconv.Atoi(div)
		if !strings.EqualFold(col, timeCol) || div != mul || err != nil || n <= 0 || inStringLiteral(sql, m[0]) {
			continue // left as written: an unrecognised expression declines later
		}
		b.WriteString(sql[last:m[0]])
		fmt.Fprintf(&b, "%s(%s, %d)", epochPlaceholder, col, n)
		last = m[1]
	}
	b.WriteString(sql[last:])
	return b.String(), true
}

// inStringLiteral reports whether byte offset i of sql lies inside a
// single-quoted string literal (” escapes included).
func inStringLiteral(sql string, i int) bool {
	in := false
	for j := 0; j < i && j < len(sql); j++ {
		if sql[j] == '\'' {
			in = !in
		}
	}
	return in
}

// secGrain encodes a second-based grain (e.g. a 6h Grafana bucket -> "secs:21600").
func secGrain(n int) string { return "secs:" + strconv.Itoa(n) }

// limitOf extracts a LIMIT n from the SELECT modifiers. Returns 0 (no limit) when
// absent or not a simple positive integer — the safe default, since omitting the
// LIMIT only returns more rows, never wrong values. Preserving it lets a Grafana
// `ORDER BY time LIMIT n` match source (the cube is already bucket-ordered).
func limitOf(node map[string]any) int {
	for _, raw := range listOf(node["modifiers"]) {
		m, _ := raw.(map[string]any)
		if str(m["type"]) != "LIMIT_MODIFIER" {
			continue
		}
		lim, _ := m["limit"].(map[string]any)
		if n, ok := intConst(lim); ok && n > 0 {
			return n
		}
	}
	return 0
}

// parseOrderBy resolves the query's ORDER BY terms to 1-based positions in the
// emitted select list, which keeps the user's select-list order. Returns false
// if any term can't be mapped; the query is then declined, since row order is
// part of the result (and decides which rows a LIMIT keeps).
func parseOrderBy(node map[string]any, q *QueryShape, timeCol string, opts ParseOpts) bool {
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
		p, ok := q.resolveOrderPos(expr, pos, total, timeCol, opts)
		if !ok {
			return false
		}
		k := OrderKey{Pos: p, Desc: str(o["type"]) == "DESCENDING"}
		switch str(o["null_order"]) {
		case "NULLS FIRST", "NULLS_FIRST":
			k.Nulls = "NULLS FIRST"
		case "NULLS LAST", "NULLS_LAST":
			k.Nulls = "NULLS LAST"
		case "", "ORDER_DEFAULT":
		default:
			return false
		}
		keys = append(keys, k)
	}
	q.OrderBy = keys
	return true
}

// outputPositions maps each name an ORDER BY can use to its 1-based output
// position, and returns the column count. Output names (aliases, a bare dim's
// column name, the bucket alias) are registered first, as DuckDB binds an ORDER BY
// name to a select alias first; the underlying column of a renamed dim only when
// no output claims that name. A name claimed by two outputs maps to -1
// (ambiguous: the ORDER BY declines).
func (q QueryShape) outputPositions() (map[string]int, int) {
	items := q.outItems()
	pos := map[string]int{}
	for p, it := range items {
		name := strings.ToLower(q.outName(it)) // DuckDB names are case-insensitive
		if name == "" {
			continue
		}
		if old, ok := pos[name]; ok && old != p+1 {
			pos[name] = -1
			continue
		}
		pos[name] = p + 1
	}
	for p, it := range items {
		if it.Kind != OutDim {
			continue
		}
		if c := strings.ToLower(q.Dims[it.I]); c != strings.ToLower(q.outName(it)) {
			if _, claimed := pos[c]; !claimed {
				pos[c] = p + 1
			}
		}
	}
	return pos, len(items)
}

// outName is the output column name of a select item.
func (q QueryShape) outName(it OutItem) string {
	switch it.Kind {
	case OutBucket:
		return q.bucketAlias()
	case OutDim:
		if it.I < len(q.DimAliases) && q.DimAliases[it.I] != "" {
			return q.DimAliases[it.I]
		}
		return q.Dims[it.I]
	case OutExpr:
		return q.Exprs[it.I].Alias
	case OutAgg:
		return q.Aggs[it.I].Alias
	}
	return ""
}

// resolveOrderPos maps one ORDER BY expression to a select-list position: a
// positional constant, an output name, the time-bucket expression, an aggregate
// identical to one of the query's aggregates, or a repeated computed key.
func (q QueryShape) resolveOrderPos(expr map[string]any, pos map[string]int, total int, timeCol string, opts ParseOpts) (int, bool) {
	if expr == nil {
		return 0, false
	}
	if i, ok := intConst(expr); ok && i >= 1 && i <= total {
		return i, true
	}
	if name, ok := bareColumn(expr); ok {
		if p, found := pos[strings.ToLower(name)]; found && p > 0 {
			return p, true
		}
		return 0, false
	}
	if _, ok := timeBucketGrain(expr, timeCol); ok {
		if text, ok := renderExpr(expr); ok && q.bucketSQL != "" && text == q.bucketSQL {
			return q.posOf(OutBucket, 0), true
		}
		return 0, false // a bucket of another grain or function is not an output column
	}
	if a, isAgg, reason := parseAggregateOpts(expr, "", timeCol, opts); isAgg && reason == "" {
		for i, qa := range q.Aggs {
			if sameAgg(qa, a) {
				return q.posOf(OutAgg, i), true
			}
		}
		return 0, false
	}
	if len(q.Exprs) > 0 {
		if text, ok := renderExpr(expr); ok {
			for i, e := range q.Exprs {
				if e.SQL == text {
					return q.posOf(OutExpr, i), true
				}
			}
		}
	}
	return 0, false
}

// sameAgg reports whether two aggregates compute the same value (alias aside):
// kind, column, rank, and for conditional ones the predicate and both branches.
func sameAgg(a, b Aggregate) bool {
	return a.Kind == b.Kind && a.Col == b.Col && a.P == b.P && a.Cond == b.Cond &&
		a.ThenCol == b.ThenCol && a.ThenK == b.ThenK && a.ElseK == b.ElseK && a.ElseNull == b.ElseNull && a.FromCount == b.FromCount
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
	node, reason := serializeStatement(sql)
	if node == nil {
		return nil, reason
	}
	if node["type"] != "SELECT_NODE" {
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

// serializeStatement returns the root query node of the single statement in sql
// (any node type), or a reason it can't be parsed.
func serializeStatement(sql string) (map[string]any, string) {
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
	if node == nil {
		return nil, "not a query"
	}
	return node, ""
}

func parseSelectNode(node map[string]any, timeCol, defaultDB string, opts ParseOpts) (QueryShape, bool, string) {
	q := QueryShape{TimeCol: timeCol}

	// Default-decline: anything outside the allow-list runs from source.
	if reason := validateSelectNode(node); reason != "" {
		return QueryShape{}, false, reason
	}

	// FROM must be a single base table; JOIN/subquery/VALUES change semantics.
	src, reason := parseSource(node, defaultDB)
	if reason != "" {
		return QueryShape{}, false, reason
	}
	q.Source = src

	// SELECT list -> grain (time bucket), dims, aggregates.
	grain, bucketAlias, dims, dimAliases, exprs, aggs, out, reason := parseSelectList(node, timeCol, opts)
	if reason != "" {
		return QueryShape{}, false, reason
	}
	q.Grain = grain
	q.BucketAlias = bucketAlias
	q.Dims = dims
	for i, a := range dimAliases {
		if a != "" && a != dims[i] {
			q.DimAliases = dimAliases
			break
		}
	}
	q.Exprs = exprs
	q.Aggs = aggs
	q.Out = out
	// The select bucket's printed form identifies it in GROUP BY / ORDER BY:
	// a bucket of another grain or another function (time_bucket vs date_trunc)
	// is a different expression.
	for p, it := range out {
		if it.Kind == OutBucket {
			item, _ := listOf(node["select_list"])[p].(map[string]any)
			text, ok := renderExpr(item)
			if !ok {
				return QueryShape{}, false, "time bucket not printable"
			}
			q.bucketSQL = text
		}
	}
	q.Limit = limitOf(node)

	// ORDER BY must be reproduced exactly: it decides the row order of the result
	// and, under a LIMIT, which rows are kept.
	if !parseOrderBy(node, &q, timeCol, opts) {
		return QueryShape{}, false, "ORDER BY target not reproducible"
	}

	// WHERE -> time range (peeled off) + post-agg filters.
	lo, hi, filters, exprFilters, reason := parseWhere(node, timeCol, opts)
	if reason != "" {
		return QueryShape{}, false, reason
	}
	q.TimeLo = lo
	q.TimeHi = hi
	q.Filters = filters
	q.ExprFilters = exprFilters

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
	if reason := checkGroupBy(node, timeCol, q.bucketSQL, out, dims, exprs); reason != "" {
		return QueryShape{}, false, reason
	}
	return q, true, ""
}

// --- FROM --------------------------------------------------------------------

func parseSource(node map[string]any, defaultDB string) (string, string) {
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
	// Qualify with the explicit schema if the user wrote db.table; else the
	// request's database header; else "default".
	if schema := str(ft["schema_name"]); schema != "" {
		return schema + "." + table, ""
	}
	if defaultDB != "" {
		return defaultDB + "." + table, ""
	}
	return "default." + table, ""
}

// --- SELECT list -------------------------------------------------------------

func parseSelectList(node map[string]any, timeCol string, opts ParseOpts) (grain, bucketAlias string, dims, dimAliases []string, exprs []DimExpr, aggs []Aggregate, out []OutItem, reason string) {
	list, _ := node["select_list"].([]any)
	if len(list) == 0 {
		return "", "", nil, nil, nil, nil, nil, "empty select list"
	}
	for _, raw := range list {
		item, _ := raw.(map[string]any)
		alias := str(item["alias"])

		if g, ok := timeBucketGrain(item, timeCol); ok {
			if grain != "" {
				return "", "", nil, nil, nil, nil, nil, "multiple time buckets"
			}
			grain = g
			bucketAlias = alias
			if alias == "" {
				// The source names an unaliased bucket by its printed form; the
				// epoch placeholder has no faithful printed form, so decline.
				name, ok := renderExpr(item)
				if !ok || strings.HasPrefix(g, "secs:") {
					return "", "", nil, nil, nil, nil, nil, "unaliased time bucket"
				}
				bucketAlias = name
			}
			out = append(out, OutItem{Kind: OutBucket})
			continue
		}
		if col, ok := bareColumn(item); ok {
			if col == timeCol {
				// Selecting the raw time column (no bucket) would make every row a
				// distinct group — not an aggregate shape we materialize.
				return "", "", nil, nil, nil, nil, nil, "raw time column in select"
			}
			out = append(out, OutItem{Kind: OutDim, I: len(dims)})
			dims = append(dims, col)
			dimAliases = append(dimAliases, alias)
			continue
		}
		// A scalar expression over dims is a computed group key (opt-in). Checked
		// before the aggregate parse, which refuses every unknown function.
		if opts.DimExprs {
			if e, ok := dimExprOf(item, timeCol); ok {
				out = append(out, OutItem{Kind: OutExpr, I: len(exprs)})
				exprs = append(exprs, e)
				continue
			}
		}
		if a, ok, r := parseAggregateOpts(item, alias, timeCol, opts); r != "" {
			return "", "", nil, nil, nil, nil, nil, r
		} else if ok {
			// An unaliased aggregate is named by its printed form at source
			// (count(*) -> "count_star()"); use that name, never a made-up one.
			if a.Alias == "" {
				name, ok := renderExpr(item)
				if !ok || name == "" {
					return "", "", nil, nil, nil, nil, nil, "unaliased aggregate without a printable name"
				}
				a.Alias = name
			}
			out = append(out, OutItem{Kind: OutAgg, I: len(aggs)})
			aggs = append(aggs, a)
			continue
		}
		return "", "", nil, nil, nil, nil, nil, "unsupported select expression: " + describeExpr(item)
	}
	if len(aggs) == 0 {
		return "", "", nil, nil, nil, nil, nil, "no aggregate in select list"
	}
	return grain, bucketAlias, dims, dimAliases, exprs, aggs, out, ""
}

// timeBucketGrain recognises date_trunc('<grain>', timeCol) and
// time_bucket(INTERVAL '<n unit>', timeCol) over the time column.
func timeBucketGrain(item map[string]any, timeCol string) (string, bool) {
	if item["class"] != "FUNCTION" {
		return "", false
	}
	fn := str(item["function_name"])
	args, _ := item["children"].([]any)
	if str(item["schema"]) != "" || str(item["catalog"]) != "" {
		return "", false
	}
	switch fn {
	case epochPlaceholder:
		if len(args) != 2 {
			return "", false
		}
		if col, ok := bareColumnExpr(arg(args, 0)); !ok || col != timeCol {
			return "", false
		}
		if n, ok := intConst(arg(args, 1)); ok && n > 0 {
			return secGrain(n), true
		}
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
	return parseAggregateOpts(item, alias, "", ParseOpts{})
}

// parseAggregateOpts is parseAggregate with the optional parser capabilities;
// timeCol is needed only by them (a dim predicate must not read it).
func parseAggregateOpts(item map[string]any, alias, timeCol string, opts ParseOpts) (Aggregate, bool, string) {
	if item["class"] != "FUNCTION" {
		return Aggregate{}, false, ""
	}
	fn := str(item["function_name"])
	args, _ := item["children"].([]any)
	distinct, _ := item["distinct"].(bool)
	if str(item["schema"]) != "" || str(item["catalog"]) != "" {
		return Aggregate{}, true, "qualified aggregate not supported"
	}
	// Exactly the arguments the cube reproduces: count(*) none, count/sum/min/
	// max/avg one (count(DISTINCT a, b) and max(x, n) decline), a percentile two;
	// DISTINCT only on count (sum/avg DISTINCT decline).
	wantArgs := map[string]int{"count_star": 0, "count": 1, "sum": 1, "min": 1, "max": 1, "avg": 1, "quantile_cont": 2, "percentile_cont": 2}
	if n, known := wantArgs[fn]; known && len(args) != n {
		return Aggregate{}, true, fmt.Sprintf("%s() with %d arguments not supported", fn, len(args))
	}
	if distinct && fn != "count" {
		return Aggregate{}, true, fn + "(DISTINCT) not supported"
	}
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
		if distinct && opts.DimExprs {
			if a, ok := condDistinctOf(arg(args, 0), alias, timeCol); ok {
				return a, true, ""
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
	case "approx_quantile":
		// Returns the column's own type at source (BIGINT for an integer
		// column); the cube's KLL estimate is a DOUBLE.
		return Aggregate{}, true, "approx_quantile not supported (use quantile_cont)"
	case "quantile_cont", "percentile_cont", "quantile", "quantile_disc", "percentile_disc", "median":
		// Percentiles are not served from cubes: the KLL read is a discrete,
		// approximate quantile (quantile_cont interpolates), and sketches built
		// over an integer column cannot be decoded. No dashboard gets one from
		// a cube today, so they run from source, exactly.
		return Aggregate{}, true, fn + "() is not served from cubes (runs from source)"
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
	} else if k, ok := numericConst(thenExpr); ok {
		a.ThenK = k
	} else {
		return Aggregate{}, false, "sum(CASE) THEN must be a numeric constant or a column"
	}
	if isNullConst(elseExpr) {
		// No ELSE (or ELSE NULL): rows failing the predicate add nothing, and a
		// group where no row passes sums to NULL, not 0.
		a.ElseNull = true
	} else if k, ok := numericConst(elseExpr); ok {
		a.ElseK = k
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
		if col, isCol := bareColumnExpr(left); isCol {
			if l, ok := constLit(right); ok {
				return fmt.Sprintf("%s %s %s", quoteIdent(col), op, l.sql()), []string{col}, true
			}
		}
		if col, isCol := bareColumnExpr(right); isCol {
			if l, ok := constLit(left); ok {
				return fmt.Sprintf("%s %s %s", quoteIdent(col), flipCompare(op), l.sql()), []string{col}, true
			}
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
			l, ok := constLit(c)
			if !ok {
				return "", nil, false
			}
			vals = append(vals, l.sql())
		}
		kw := "IN"
		if typ == "COMPARE_NOT_IN" {
			kw = "NOT IN"
		}
		return fmt.Sprintf("%s %s (%s)", quoteIdent(col), kw, strings.Join(vals, ", ")), []string{col}, true

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
		return fmt.Sprintf("%s %s", quoteIdent(col), kw), []string{col}, true
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

// numericConst returns the exact SQL text of a numeric constant (INTEGER,
// BIGINT, DECIMAL, DOUBLE, ...). A string constant is NOT numeric even when its
// text parses as a number ('5', 'nan'): the old check accepted any string
// strconv.ParseFloat took, and 'nan' / 'inf' were then emitted as identifiers.
func numericConst(n map[string]any) (string, bool) {
	l, ok := constLit(n)
	if !ok || l.Str || l.V == "NULL" {
		return "", false
	}
	return l.V, true
}

// maxExactInt bounds the magnitudes a JSON number (decoded as float64) carries
// unambiguously (2^53 itself may be a rounded 2^53+1); larger ones decline.
const maxExactInt = 1 << 53

// constLit converts a constant node to a typed Lit, preserving the constant's
// type: VARCHAR stays a string; integer types render as integers; DECIMAL keeps
// its scale (1.10 stays 1.10, a DECIMAL(3,2)); DOUBLE renders in exponent form,
// which DuckDB parses back as DOUBLE; NULL stays NULL. Any other type, or a
// value that cannot be carried exactly (an integer beyond 2^53, a wide DECIMAL
// serialized as a 128-bit object), returns ok=false so the query is declined
// and runs against source.
func constLit(node map[string]any) (Lit, bool) {
	if !isConst(node) {
		return Lit{}, false
	}
	val, _ := node["value"].(map[string]any)
	if val == nil {
		return Lit{}, false
	}
	if isNull, _ := val["is_null"].(bool); isNull {
		return NumLit("NULL"), true
	}
	ti, _ := val["type"].(map[string]any)
	switch str(ti["id"]) {
	case "VARCHAR":
		s, ok := val["value"].(string)
		return StrLit(s), ok
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT":
		f, ok := val["value"].(float64)
		if !ok || f != math.Trunc(f) || math.Abs(f) >= maxExactInt {
			return Lit{}, false
		}
		return NumLit(strconv.FormatInt(int64(f), 10)), true
	case "DECIMAL":
		f, ok := val["value"].(float64)
		info, _ := ti["type_info"].(map[string]any)
		scale, okScale := info["scale"].(float64)
		if !ok || !okScale || f != math.Trunc(f) || math.Abs(f) >= maxExactInt || scale < 0 || scale > 38 || scale != math.Trunc(scale) {
			return Lit{}, false
		}
		return NumLit(decimalText(int64(f), int(scale))), true
	case "DOUBLE":
		f, ok := val["value"].(float64)
		if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
			return Lit{}, false
		}
		return NumLit(strconv.FormatFloat(f, 'e', -1, 64)), true
	}
	return Lit{}, false
}

// decimalText renders a DECIMAL's unscaled integer with its scale, e.g.
// (110, 2) -> "1.10", (-5, 3) -> "-0.005".
func decimalText(unscaled int64, scale int) string {
	neg := unscaled < 0
	if neg {
		unscaled = -unscaled
	}
	s := strconv.FormatInt(unscaled, 10)
	if scale > 0 {
		if len(s) <= scale {
			s = strings.Repeat("0", scale-len(s)+1) + s
		}
		s = s[:len(s)-scale] + "." + s[len(s)-scale:]
	}
	if neg {
		s = "-" + s
	}
	return s
}

// --- WHERE -------------------------------------------------------------------

// parseWhere peels the two-sided time range (timeCol >= lo AND timeCol < hi) off
// the top-level AND conjunction and converts every remaining predicate into a
// Filter. The time range must be a clean >=/< pair so it maps onto the cube's
// half-open bucket model; other shapes (>, <=, BETWEEN, one-sided) are rejected.
func parseWhere(node map[string]any, timeCol string, opts ParseOpts) (lo, hi string, filters []Filter, exprFilters []ExprFilter, reason string) {
	where, _ := node["where_clause"].(map[string]any)
	if where == nil {
		return "", "", nil, nil, "missing time range (no WHERE)"
	}
	for _, pred := range conjuncts(where) {
		if bound, val, ok := timeBound(pred, timeCol); ok {
			switch bound {
			case ">=":
				if lo != "" {
					return "", "", nil, nil, "duplicate lower time bound"
				}
				lo = val
			case "<":
				if hi != "" {
					return "", "", nil, nil, "duplicate upper time bound"
				}
				hi = val
			default:
				return "", "", nil, nil, "time bound must be >= and < (got " + bound + ")"
			}
			continue
		}
		// A non-time predicate that still references the time column can't be a
		// dimension filter we re-apply post-agg.
		if mentionsColumn(pred, timeCol) {
			return "", "", nil, nil, "unsupported predicate on time column"
		}
		f, ok := parseFilter(pred)
		if !ok {
			if opts.DimExprs {
				if ef, ok := exprFilterOf(pred, timeCol); ok {
					exprFilters = append(exprFilters, ef)
					continue
				}
			}
			return "", "", nil, nil, "unsupported WHERE predicate: " + describeExpr(pred)
		}
		filters = append(filters, f...)
	}
	if lo == "" || hi == "" {
		return "", "", nil, nil, "WHERE lacks a two-sided time range"
	}
	return lo, hi, filters, exprFilters, ""
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

// timestampLiteral extracts a time bound from a TIMESTAMP/TIMESTAMPTZ/DATE cast
// over a constant (the form DuckDB produces for typed literals) or from a bare
// string constant (the Grafana plugin's $__timeFilter emits `time >= 'RFC3339'`).
//
// The literal text is never passed through: it must parse as a timestamp and the
// bound is RE-RENDERED from the parsed value (canonTS, UTC — the engine's pinned
// TimeZone). The emitted SQL splices bounds into TIMESTAMPTZ '...' literals and
// runs after the API's validator and RBAC, so passing the request's text through
// let a crafted literal close the quote and append arbitrary SQL.
func timestampLiteral(node map[string]any) (string, bool) {
	var body string
	switch node["class"] {
	case "CAST":
		ct, _ := node["cast_type"].(map[string]any)
		child, _ := node["child"].(map[string]any)
		if child["class"] != "CONSTANT" {
			return "", false
		}
		body = constString(child)
		switch str(ct["id"]) {
		case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE":
		case "DATE":
			// A DATE is midnight of that day: accept only the bare date form, so a
			// time-of-day DuckDB would drop is never kept.
			t, err := time.Parse("2006-01-02", strings.TrimSpace(body))
			if err != nil {
				return "", false
			}
			return canonTS(t), true
		default:
			return "", false
		}
	case "CONSTANT":
		body = constString(node)
	default:
		return "", false
	}
	t, ok := parseTS(body)
	if !ok {
		return "", false
	}
	return canonTS(t), true
}

// parseFilter converts one WHERE predicate into Filter(s). It returns multiple
// filters only for the same-column OR case (os=a OR os=b -> IN). Anything not
// faithfully representable in the flat Filter model returns ok=false.
func parseFilter(pred map[string]any) ([]Filter, bool) {
	switch pred["type"] {
	case "COMPARE_EQUAL", "COMPARE_NOTEQUAL", "COMPARE_DISTINCT_FROM", "COMPARE_NOT_DISTINCT_FROM":
		col, lit, ok := colLit(pred)
		if !ok {
			return nil, false
		}
		// IS [NOT] DISTINCT FROM is NULL-safe and keeps its own operator: as
		// <> it would drop the NULL rows the source query counts.
		op := map[any]FilterOp{
			"COMPARE_EQUAL": OpEq, "COMPARE_NOTEQUAL": OpNe,
			"COMPARE_DISTINCT_FROM": OpDistinct, "COMPARE_NOT_DISTINCT_FROM": OpNotDistinct,
		}[pred["type"]]
		return []Filter{{Col: col, Op: op, Values: []Lit{lit}}}, true

	case "COMPARE_IN", "COMPARE_NOT_IN":
		kids := childList(pred)
		col, ok := bareColumnExpr(arg(toAny(kids), 0))
		if !ok || len(kids) < 2 {
			return nil, false
		}
		vals := make([]Lit, 0, len(kids)-1)
		for _, c := range kids[1:] {
			l, ok := constLit(c)
			if !ok {
				return nil, false
			}
			vals = append(vals, l)
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
		var vals []Lit
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

// colLit extracts (column, typed constant) from a binary comparison in either
// operand order.
func colLit(pred map[string]any) (col string, lit Lit, ok bool) {
	left, _ := pred["left"].(map[string]any)
	right, _ := pred["right"].(map[string]any)
	if c, isCol := bareColumnExpr(left); isCol {
		if l, ok := constLit(right); ok {
			return c, l, true
		}
	}
	if c, isCol := bareColumnExpr(right); isCol {
		if l, ok := constLit(left); ok {
			return c, l, true
		}
	}
	return "", Lit{}, false
}

// --- GROUP BY validation -----------------------------------------------------

// checkGroupBy verifies the GROUP BY keys are exactly the select list's group
// keys (time bucket, dims, computed keys) — as sets, so a key selected twice may
// be grouped once — with every GROUP BY entry resolved to one key: a position
// names that select item (never an aggregate); a bare name is the COLUMN of that
// name, as DuckDB binds a GROUP BY name to a column before an alias (so
// `region AS r ... GROUP BY r` and `region AS tag ... GROUP BY tag` decline); any
// other expression must be the time bucket or repeat a computed key. A grouped
// column that is not selected, or a selected key that is not grouped, declines,
// and so do GROUPING SETS / ROLLUP / CUBE and GROUP BY ALL.
func checkGroupBy(node map[string]any, timeCol, bucketSQL string, out []OutItem, dims []string, exprs []DimExpr) string {
	groups, _ := node["group_expressions"].([]any)
	if h := str(node["aggregate_handling"]); h != "" && h != "STANDARD_HANDLING" {
		return "GROUP BY ALL not supported"
	}
	if sets, _ := node["group_sets"].([]any); len(sets) > 1 {
		return "GROUPING SETS / ROLLUP / CUBE not supported"
	} else if len(sets) == 1 {
		if only, _ := sets[0].([]any); len(only) != len(groups) {
			return "GROUPING SETS / ROLLUP / CUBE not supported"
		}
	}
	// A time bucket is keyed by its printed form: date_trunc('day', time),
	// date_trunc('hour', time) and time_bucket(INTERVAL '1 hour', time) are
	// different group keys.
	bucketKey := func(text string) string { return "\x00bucket:" + text }
	keyOf := func(it OutItem) string {
		switch it.Kind {
		case OutBucket:
			return bucketKey(bucketSQL)
		case OutDim:
			return quoteIdent(dims[it.I])
		case OutExpr:
			return exprs[it.I].SQL
		}
		return ""
	}
	selected := map[string]bool{}
	for _, it := range out {
		if it.Kind != OutAgg {
			selected[keyOf(it)] = true
		}
	}
	grouped := map[string]bool{}
	for _, raw := range groups {
		g, _ := raw.(map[string]any)
		switch {
		case str(g["class"]) == "CONSTANT":
			i, ok := intConst(g)
			if !ok || i < 1 || i > len(out) {
				return "unresolvable positional GROUP BY"
			}
			if out[i-1].Kind == OutAgg {
				return "GROUP BY position names an aggregate"
			}
			grouped[keyOf(out[i-1])] = true
		default:
			if col, ok := bareColumn(g); ok {
				grouped[quoteIdent(col)] = true
				continue
			}
			if _, ok := timeBucketGrain(g, timeCol); ok {
				text, ok := renderExpr(g)
				if !ok {
					return "time bucket not printable"
				}
				grouped[bucketKey(text)] = true
				continue
			}
			text, ok := "", false
			if len(exprs) > 0 {
				text, ok = renderExpr(g)
			}
			if !ok || !selected[text] {
				return "GROUP BY has an expression not in the shape"
			}
			grouped[text] = true
		}
	}
	for k := range grouped {
		if !selected[k] {
			return "GROUP BY groups by " + k + ", which is not a selected group column"
		}
	}
	for k := range selected {
		if !grouped[k] {
			if k == bucketKey(bucketSQL) {
				return "time bucket present in SELECT but not GROUP BY"
			}
			return "selected group column " + k + " is not grouped"
		}
	}
	return ""
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
