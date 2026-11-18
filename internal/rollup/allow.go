package rollup

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Default-decline. The router serves a query only when every part of its parse
// tree is something the cube read reproduces exactly. validateSelectNode checks
// the SELECT node and every expression in it against an allow-list of node
// classes, fields, modifiers and argument counts BEFORE any interpretation, and
// declines on anything else — an unknown class, an unknown field, or a known
// field set to a non-default value (DISTINCT ON, OFFSET, LIMIT n%, TABLESAMPLE,
// FROM t AS e(cols), FILTER, ordered aggregates, …). A DuckDB upgrade that adds
// a field therefore declines (runs from source) instead of being silently
// ignored. The interpreting code then adds its own, narrower checks.

// selectNodeFields are the SELECT_NODE fields the router understands.
var selectNodeFields = keySet("type", "modifiers", "cte_map", "select_list", "from_table", "where_clause",
	"group_expressions", "group_sets", "aggregate_handling", "having", "sample", "qualify")

// baseTableFields are the BASE_TABLE fields; only the name parts and an alias
// may be set.
var baseTableFields = keySet("type", "alias", "at_clause", "catalog_name", "column_name_alias", "query_location",
	"sample", "schema_name", "table_name")

// exprFields lists, per expression class, the fields it may carry (on top of
// class, type, alias and query_location, which every expression has).
var exprFields = map[string]map[string]bool{
	"COLUMN_REF":  keySet("column_names"),
	"CONSTANT":    keySet("value"),
	"FUNCTION":    keySet("function_name", "schema", "catalog", "children", "filter", "order_bys", "distinct", "is_operator", "export_state"),
	"CAST":        keySet("child", "cast_type", "try_cast"),
	"COMPARISON":  keySet("left", "right"),
	"CONJUNCTION": keySet("children"),
	"OPERATOR":    keySet("children"),
	"CASE":        keySet("case_checks", "else_expr"),
	"BETWEEN":     keySet("input", "lower", "upper"),
}

var commonExprFields = keySet("class", "type", "alias", "query_location")

func keySet(ks ...string) map[string]bool {
	m := make(map[string]bool, len(ks))
	for _, k := range ks {
		m[k] = true
	}
	return m
}

// validateSelectNode returns "" when node uses only allow-listed constructs,
// else the reason it declines.
func validateSelectNode(node map[string]any) string {
	for k, v := range node {
		if !selectNodeFields[k] {
			return "unsupported SELECT field " + k
		}
		switch k {
		case "having", "qualify", "sample":
			if v != nil {
				return strings.ToUpper(k) + " not supported"
			}
		case "aggregate_handling":
			if str(v) != "STANDARD_HANDLING" {
				return "GROUP BY ALL not supported"
			}
		case "cte_map":
			if cm, _ := v.(map[string]any); cm != nil {
				if m, _ := cm["map"].([]any); len(m) > 0 {
					return "CTE not supported"
				}
			}
		}
	}
	if str(node["type"]) != "SELECT_NODE" {
		return "not a plain SELECT"
	}
	if r := validateFrom(node["from_table"]); r != "" {
		return r
	}
	if r := validateModifiers(node["modifiers"]); r != "" {
		return r
	}
	var exprs []any
	exprs = append(exprs, listOf(node["select_list"])...)
	exprs = append(exprs, listOf(node["group_expressions"])...)
	if w := node["where_clause"]; w != nil {
		exprs = append(exprs, w)
	}
	for _, raw := range listOf(node["modifiers"]) {
		m, _ := raw.(map[string]any)
		for _, o := range listOf(m["orders"]) {
			om, _ := o.(map[string]any)
			exprs = append(exprs, om["expression"])
		}
	}
	for _, e := range exprs {
		em, _ := e.(map[string]any)
		if r := validateExpr(em); r != "" {
			return r
		}
	}
	return ""
}

func validateFrom(v any) string {
	ft, _ := v.(map[string]any)
	if ft == nil {
		return "no FROM clause"
	}
	if str(ft["type"]) != "BASE_TABLE" {
		return "FROM is not a single base table (" + str(ft["type"]) + ")"
	}
	for k, val := range ft {
		if !baseTableFields[k] {
			return "unsupported FROM field " + k
		}
		switch k {
		case "sample", "at_clause":
			if val != nil {
				return "FROM " + k + " not supported"
			}
		case "column_name_alias":
			if len(listOf(val)) > 0 {
				return "FROM column aliases not supported"
			}
		case "catalog_name":
			if str(val) != "" {
				return "catalog-qualified table not supported"
			}
		}
	}
	return ""
}

// validateModifiers allows an ORDER BY and a LIMIT of a positive integer
// constant without OFFSET; DISTINCT / DISTINCT ON, LIMIT n%, LIMIT 0, an
// expression or decimal LIMIT and any OFFSET decline.
func validateModifiers(v any) string {
	for _, raw := range listOf(v) {
		m, _ := raw.(map[string]any)
		switch str(m["type"]) {
		case "ORDER_MODIFIER":
			for k := range m {
				if k != "type" && k != "orders" {
					return "unsupported ORDER BY field " + k
				}
			}
			for _, o := range listOf(m["orders"]) {
				om, _ := o.(map[string]any)
				for k := range om {
					if k != "type" && k != "null_order" && k != "expression" {
						return "unsupported ORDER BY field " + k
					}
				}
				switch str(om["type"]) {
				case "ORDER_DEFAULT", "ASCENDING", "DESCENDING":
				default:
					return "unsupported ORDER BY direction"
				}
				switch str(om["null_order"]) {
				case "ORDER_DEFAULT", "NULLS FIRST", "NULLS LAST":
				default:
					return "unsupported ORDER BY NULLS order"
				}
			}
		case "LIMIT_MODIFIER":
			for k := range m {
				if k != "type" && k != "limit" && k != "offset" {
					return "unsupported LIMIT field " + k
				}
			}
			if m["offset"] != nil {
				return "OFFSET not supported"
			}
			lim, _ := m["limit"].(map[string]any)
			if n, ok := intConst(lim); !ok || n < 1 {
				return "LIMIT must be a positive integer constant"
			}
		default:
			return str(m["type"]) + " not supported"
		}
	}
	return ""
}

// validateExpr checks one expression tree: known classes only, known fields
// only, and the optional fields at their defaults (no FILTER, no ordered
// aggregate, no export_state, a built-in function).
func validateExpr(n map[string]any) string {
	if n == nil {
		return "missing expression"
	}
	class := str(n["class"])
	fields, ok := exprFields[class]
	if !ok {
		return "unsupported expression " + class
	}
	for k := range n {
		if !commonExprFields[k] && !fields[k] {
			return fmt.Sprintf("unsupported %s field %s", class, k)
		}
	}
	sub := func(vs ...any) string {
		for _, v := range vs {
			m, _ := v.(map[string]any)
			if r := validateExpr(m); r != "" {
				return r
			}
		}
		return ""
	}
	switch class {
	case "COLUMN_REF":
		if len(listOf(n["column_names"])) == 0 {
			return "empty column reference"
		}
	case "FUNCTION":
		if n["filter"] != nil {
			return "aggregate with FILTER not supported"
		}
		if e, _ := n["export_state"].(bool); e {
			return "EXPORT_STATE not supported"
		}
		if ob, _ := n["order_bys"].(map[string]any); ob != nil && len(listOf(ob["orders"])) > 0 {
			return "ordered aggregate not supported"
		}
		if str(n["catalog"]) != "" || (str(n["schema"]) != "" && str(n["schema"]) != "main") {
			return "qualified function not supported"
		}
		return sub(listOf(n["children"])...)
	case "CAST":
		return sub(n["child"])
	case "COMPARISON":
		return sub(n["left"], n["right"])
	case "CONJUNCTION", "OPERATOR":
		return sub(listOf(n["children"])...)
	case "BETWEEN":
		return sub(n["input"], n["lower"], n["upper"])
	case "CASE":
		checks := listOf(n["case_checks"])
		if len(checks) == 0 {
			return "CASE without WHEN"
		}
		for _, raw := range checks {
			c, _ := raw.(map[string]any)
			for k := range c {
				if k != "when_expr" && k != "then_expr" {
					return "unsupported CASE field " + k
				}
			}
			if r := sub(c["when_expr"], c["then_expr"]); r != "" {
				return r
			}
		}
		return sub(n["else_expr"])
	}
	return ""
}

// intConst returns an integer-TYPED, non-NULL constant's value: a string
// ('2'), decimal (0.2, 2.6) or NULL constant is not an integer, even where
// DuckDB would accept or reject it in that position.
func intConst(n map[string]any) (int, bool) {
	if n == nil || str(n["class"]) != "CONSTANT" {
		return 0, false
	}
	val, _ := n["value"].(map[string]any)
	if val == nil {
		return 0, false
	}
	if isNull, _ := val["is_null"].(bool); isNull {
		return 0, false
	}
	ti, _ := val["type"].(map[string]any)
	switch str(ti["id"]) {
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT":
	default:
		return 0, false
	}
	f, ok := val["value"].(float64)
	if !ok || f != float64(int64(f)) || f > 1<<31 || f < -(1<<31) {
		return 0, false
	}
	return int(f), true
}

func listOf(v any) []any {
	l, _ := v.([]any)
	return l
}

// sameTree reports whether two expression trees are identical apart from
// query_location and the top-level alias.
func sameTree(a, b map[string]any) bool {
	ca, cb := cloneTree(a), cloneTree(b)
	if ca == nil || cb == nil {
		return false
	}
	ca["alias"], cb["alias"] = "", ""
	dropQueryLocations(ca)
	dropQueryLocations(cb)
	ja, err1 := json.Marshal(ca)
	jb, err2 := json.Marshal(cb)
	return err1 == nil && err2 == nil && string(ja) == string(jb)
}

func cloneTree(n map[string]any) map[string]any {
	raw, err := json.Marshal(n)
	if err != nil {
		return nil
	}
	var out map[string]any
	if json.Unmarshal(raw, &out) != nil {
		return nil
	}
	return out
}
