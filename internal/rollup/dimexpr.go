package rollup

import (
	"encoding/json"
	"math"
	"strings"
	"sync"
)

// Dimension expressions (opt-in, [rollup] dim_expressions): a scalar expression
// built only from stored dimensions, constants and deterministic functions can be
// evaluated on cube rows instead of source rows. Every cube row aggregates source
// rows that share identical values of every stored dim, so such an expression has
// ONE value per cube row — the value it has on each of those source rows. Hence:
//
//   - a WHERE conjunct over dims selects exactly the cube rows that aggregate the
//     source rows it selects (applied post-aggregation, like a plain Filter);
//   - a select-list key over dims (CASE WHEN error_message ILIKE … END,
//     LEFT(error_message, 140), lib = 'x') groups cube rows exactly as it groups
//     source rows, and merging their store columns (counts summed, sketches
//     unioned) gives the source aggregate of each group.
//
// That holds for any deterministic expression of the dims, so the gate is an
// allow-list of node kinds and functions (no time column, aggregate, window,
// subquery, parameter or volatile function). Anything else declines and the
// query runs from source, as it always did.

// DimExpr is a computed group key: a scalar expression over stored dims in the
// select list, rendered back to SQL by DuckDB.
type DimExpr struct {
	SQL   string   // the expression (no alias), DuckDB-rendered
	Alias string   // output column name (the source query's name for it)
	Cols  []string // dims it reads (drive coverage)
}

// ExprFilter is a WHERE conjunct over stored dims that the flat Filter model
// cannot hold (ILIKE, OR across dims, a CASE of constants, COALESCE/CAST, …).
type ExprFilter struct {
	SQL  string   // the predicate, DuckDB-rendered
	Cols []string // dims it reads (drive coverage)
}

// ParseOpts selects optional parser capabilities. The zero value is the
// original parser.
type ParseOpts struct {
	// DimExprs accepts scalar expressions over stored dims as group keys and
	// WHERE conjuncts (see the package comment above DimExpr).
	DimExprs bool
}

// dimExprFuncs are the scalar functions (and operator functions, by DuckDB's
// internal name) a dimension expression may call. All are deterministic.
var dimExprFuncs = map[string]bool{
	"~~": true, "!~~": true, "~~*": true, "!~~*": true, "~~~": true, "!~~~": true, // LIKE, NOT LIKE, ILIKE, NOT ILIKE, GLOB
	"like_escape": true, "not_like_escape": true, "ilike_escape": true, "not_ilike_escape": true,
	"lower": true, "upper": true, "lcase": true, "ucase": true,
	"left": true, "right": true, "substr": true, "substring": true,
	"length": true, "strlen": true, "trim": true, "ltrim": true, "rtrim": true,
	"replace": true, "concat": true, "concat_ws": true, "||": true,
	"starts_with": true, "ends_with": true, "prefix": true, "suffix": true, "contains": true,
	"strpos": true, "instr": true, "split_part": true,
	"regexp_replace": true, "regexp_matches": true, "regexp_full_match": true, "regexp_extract": true,
	"nullif": true, "coalesce": true, "ifnull": true, "greatest": true, "least": true,
	"+": true, "-": true, "*": true, "/": true, "//": true, "%": true, "abs": true,
}

// dimExprOperators are the OPERATOR node types a dimension expression may hold.
var dimExprOperators = map[string]bool{
	"OPERATOR_NOT": true, "OPERATOR_IS_NULL": true, "OPERATOR_IS_NOT_NULL": true,
	"COMPARE_IN": true, "COMPARE_NOT_IN": true, "OPERATOR_COALESCE": true,
}

// dimExprCols reports whether node is a dimension expression and returns the
// columns it reads (deduplicated, in first-seen order). ok=false for anything
// outside the allow-list, a reference to the time column or a qualified column,
// and an expression that reads no column at all.
func dimExprCols(node map[string]any, timeCol string) ([]string, bool) {
	var cols []string
	seen := map[string]bool{}
	if !walkDimExpr(node, timeCol, func(c string) {
		if !seen[c] {
			seen[c] = true
			cols = append(cols, c)
		}
	}) || len(cols) == 0 {
		return nil, false
	}
	return cols, true
}

func walkDimExpr(n map[string]any, timeCol string, col func(string)) bool {
	if n == nil {
		return false
	}
	kids := func(nodes ...any) bool {
		for _, k := range nodes {
			m, _ := k.(map[string]any)
			if !walkDimExpr(m, timeCol, col) {
				return false
			}
		}
		return true
	}
	switch str(n["class"]) {
	case "COLUMN_REF":
		c, ok := bareColumn(n)
		if !ok || c == timeCol {
			return false
		}
		col(c)
		return true
	case "CONSTANT":
		return exactConst(n)
	case "CAST":
		return kids(n["child"])
	case "COMPARISON":
		return kids(n["left"], n["right"])
	case "BETWEEN":
		return kids(n["input"], n["lower"], n["upper"])
	case "CONJUNCTION":
		t := str(n["type"])
		if t != "CONJUNCTION_AND" && t != "CONJUNCTION_OR" {
			return false
		}
		return kids(toAny(childList(n))...)
	case "OPERATOR":
		if !dimExprOperators[str(n["type"])] {
			return false
		}
		return kids(toAny(childList(n))...)
	case "CASE":
		checks, _ := n["case_checks"].([]any)
		if len(checks) == 0 {
			return false
		}
		for _, raw := range checks {
			chk, _ := raw.(map[string]any)
			if chk == nil || !kids(chk["when_expr"], chk["then_expr"]) {
				return false
			}
		}
		return kids(n["else_expr"])
	case "FUNCTION":
		if !dimExprFuncs[strings.ToLower(str(n["function_name"]))] ||
			str(n["schema"]) != "" || str(n["catalog"]) != "" || n["filter"] != nil {
			return false
		}
		if d, _ := n["distinct"].(bool); d {
			return false
		}
		if ob, _ := n["order_bys"].(map[string]any); ob != nil {
			if o, _ := ob["orders"].([]any); len(o) > 0 {
				return false
			}
		}
		return kids(toAny(childList(n))...)
	}
	return false
}

// renderExpr renders one expression node back to SQL. A bare column renders as
// its quoted identifier; anything else goes through DuckDB's own printer
// (json_deserialize_sql over a one-item SELECT), so the text is rebuilt from the
// parse tree — never copied from the request — and every literal and identifier
// is re-escaped by the engine that will run it. The node's alias is dropped.
func renderExpr(node map[string]any) (string, bool) {
	if c, ok := bareColumn(node); ok {
		return quoteIdent(c), true
	}
	tmpl, ok := renderTemplate()
	if !ok {
		return "", false
	}
	raw, err := json.Marshal(node)
	if err != nil {
		return "", false
	}
	var expr map[string]any
	if err := json.Unmarshal(raw, &expr); err != nil {
		return "", false
	}
	expr["alias"] = ""
	// query_location is a uint64 that does not survive the float64 JSON round
	// trip (DuckDB then refuses the tree); it only places error carets.
	dropQueryLocations(expr)
	var root map[string]any
	if err := json.Unmarshal(tmpl, &root); err != nil {
		return "", false
	}
	dropQueryLocations(root)
	stmts, _ := root["statements"].([]any)
	stmt, _ := stmts[0].(map[string]any)
	sel, _ := stmt["node"].(map[string]any)
	sel["select_list"] = []any{expr}
	js, err := json.Marshal(root)
	if err != nil {
		return "", false
	}
	db, err := parseConn()
	if err != nil {
		return "", false
	}
	var out string
	if err := db.QueryRow("SELECT json_deserialize_sql(" + sqlStringLit(string(js)) + "::JSON)").Scan(&out); err != nil {
		return "", false
	}
	const prefix = "SELECT "
	if !strings.HasPrefix(out, prefix) {
		return "", false
	}
	return strings.TrimSpace(out[len(prefix):]), true
}

// dropQueryLocations deletes every query_location key under v, in place.
func dropQueryLocations(v any) {
	switch x := v.(type) {
	case map[string]any:
		delete(x, "query_location")
		for _, e := range x {
			dropQueryLocations(e)
		}
	case []any:
		for _, e := range x {
			dropQueryLocations(e)
		}
	}
}

// exactConst reports whether a constant node survives the float64 JSON round
// trip renderExpr makes: strings, NULLs, booleans and numbers within ±2^53. A
// larger integer would be re-emitted rounded, so it is refused instead.
func exactConst(n map[string]any) bool {
	val, _ := n["value"].(map[string]any)
	if val == nil {
		return false
	}
	switch v := val["value"].(type) {
	case nil, string, bool:
		return true
	case float64:
		return math.Abs(v) <= 1<<53
	}
	return false
}

func sqlStringLit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

var (
	renderTmplOnce sync.Once
	renderTmpl     []byte
)

// renderTemplate is the serialized `SELECT 1` statement whose select list
// renderExpr swaps for the expression to print.
func renderTemplate() ([]byte, bool) {
	renderTmplOnce.Do(func() {
		db, err := parseConn()
		if err != nil {
			return
		}
		var js string
		if err := db.QueryRow("SELECT json_serialize_sql('SELECT 1')::VARCHAR").Scan(&js); err != nil {
			return
		}
		var root map[string]any
		if json.Unmarshal([]byte(js), &root) != nil {
			return
		}
		if stmts, _ := root["statements"].([]any); len(stmts) != 1 {
			return
		}
		renderTmpl = []byte(js)
	})
	return renderTmpl, renderTmpl != nil
}

// dimExprOf turns a select item into a computed group key, or ok=false.
func dimExprOf(item map[string]any, timeCol string) (DimExpr, bool) {
	cols, ok := dimExprCols(item, timeCol)
	if !ok {
		return DimExpr{}, false
	}
	sqlText, ok := renderExpr(item)
	if !ok {
		return DimExpr{}, false
	}
	alias := str(item["alias"])
	if alias == "" {
		// The source query names an unaliased expression by its printed form,
		// which is exactly what DuckDB rendered.
		alias = sqlText
	}
	return DimExpr{SQL: sqlText, Alias: alias, Cols: cols}, true
}

// exprFilterOf turns a WHERE conjunct into a post-aggregation predicate over
// dims, or ok=false.
func exprFilterOf(pred map[string]any, timeCol string) (ExprFilter, bool) {
	cols, ok := dimExprCols(pred, timeCol)
	if !ok {
		return ExprFilter{}, false
	}
	sqlText, ok := renderExpr(pred)
	if !ok {
		return ExprFilter{}, false
	}
	return ExprFilter{SQL: sqlText, Cols: cols}, true
}

// condDistinctOf recognises COUNT(DISTINCT CASE WHEN <dim predicate> THEN col
// [ELSE NULL] END): one WHEN, a bare column THEN, no other ELSE. The predicate
// has one value per cube row, so the union of the col sketches of the cube rows
// where it holds is the distinct count of the source rows where it holds.
func condDistinctOf(child map[string]any, alias, timeCol string) (Aggregate, bool) {
	if child == nil || str(child["class"]) != "CASE" {
		return Aggregate{}, false
	}
	checks, _ := child["case_checks"].([]any)
	if len(checks) != 1 {
		return Aggregate{}, false
	}
	chk, _ := checks[0].(map[string]any)
	whenExpr, _ := chk["when_expr"].(map[string]any)
	thenExpr, _ := chk["then_expr"].(map[string]any)
	elseExpr, _ := child["else_expr"].(map[string]any)
	col, ok := bareColumnExpr(thenExpr)
	if !ok || col == timeCol || (elseExpr != nil && !isNullConst(elseExpr)) {
		return Aggregate{}, false
	}
	pred, ok := exprFilterOf(whenExpr, timeCol)
	if !ok {
		return Aggregate{}, false
	}
	return Aggregate{Kind: AggCountDistinct, Col: col, Alias: alias, Cond: pred.SQL, CondCols: pred.Cols}, true
}
