package rollup

import (
	"sort"
	"strings"
)

// CTE-base rewriting lets a complex dashboard query roll up by serving only its
// inner base aggregation from a cube and leaving the lightweight outer SQL (CASE
// relabeling, TopN, ratios, …) to DuckDB. The supported shape is:
//
//	WITH base AS ( <a top-level-servable aggregate over the source table> )
//	     [, more CTEs …]
//	<final SELECT that processes `base`>
//
// Only the FIRST CTE is rewritten — by convention the heavy base aggregation.
// Its column aliases are preserved (the emit re-uses the shape's bucket/agg
// aliases), so the outer SQL still resolves. Anything we can't confidently
// rewrite returns handled=false → the query runs unchanged against source.
//
// The rewritten statement is executed AS-IS: the API does no storage-path
// conversion on it. So the rewrite is only valid when nothing outside the base
// body reads a real table — every other relation (later CTEs, the final SELECT,
// joins, subqueries at any depth) must be a CTE in scope. Otherwise a bare table
// name survives into the executed SQL and fails with a Catalog Error.

// tryRewriteCTEBase attempts the rewrite. handled=false means "not this pattern"
// (caller falls through to source); a handled Decision may still be unserved.
// headerDB threads the request's database into the base-CTE parse, same as the
// top-level path — the base records to the workload, so it must resolve an
// unqualified table to the database the query actually runs against.
func (r *Router) tryRewriteCTEBase(sql, headerDB string, record, bestEffort bool) (Decision, bool) {
	head, body, tail, ok := firstCTEBody(sql)
	if !ok {
		return Decision{}, false
	}
	// It IS a WITH query, so once we're here, report why the base can or can't roll
	// up (handled=true) rather than falling back to the generic "CTE not supported".
	//
	// First make sure the rest of the statement reads only CTEs; such a query can
	// never be served this way, so it is not recorded to the workload either.
	if rel, why := foreignRelationOutsideBase(sql); why != "" {
		return Decision{Reason: "cte base: " + why}, true
	} else if rel != "" {
		return Decision{Reason: "cte base: other relations read a table (" + rel + ")"}, true
	}
	shape, parsed, reason := ParseWithDB(body, r.TimeCol, headerDB)
	if !parsed {
		return Decision{Reason: "cte base: " + reason}, true
	}
	if record && r.OnQuery != nil {
		r.OnQuery(shape)
	}
	out, cube, served, why := r.serveShape(shape, bestEffort)
	if !served {
		return Decision{Reason: "cte base: " + why}, true
	}
	// Splice the cube-read in place of the original base CTE body. head ends with
	// the opening "(", tail begins with the matching ")".
	rewritten := head + out + tail
	return Decision{Served: true, SQL: rewritten, Cube: cube + " (cte)"}, true
}

// foreignRelationOutsideBase parses the whole statement and returns the first
// relation outside the base (first) CTE's body that is not a CTE in scope —
// a base table, a schema-qualified name, or a SHOW/DESCRIBE reference — or ""
// when every relation is a CTE. why is non-empty when the statement can't be
// checked (it is then declined too).
func foreignRelationOutsideBase(sql string) (rel, why string) {
	root, reason := serializeStatement(sql)
	if root == nil {
		return "", reason
	}
	cm, _ := root["cte_map"].(map[string]any)
	entries, _ := cm["map"].([]any)
	if len(entries) == 0 {
		return "", "no CTE in the parsed statement"
	}
	// The base body is replaced wholesale by cube SQL: drop it from the walk. Its
	// name stays in scope for everything else.
	first, _ := entries[0].(map[string]any)
	val, _ := first["value"].(map[string]any)
	if val == nil {
		return "", "unexpected CTE layout"
	}
	delete(val, "query")
	return foreignRelation(root, nil), ""
}

// foreignRelation walks a serialized query tree and returns the first relation
// that is not a CTE name in scope ("" when there is none). A node carrying a
// cte_map brings its CTE names into scope for its whole subtree (its own CTE
// bodies included, so a later CTE may read an earlier one); names never leak
// out of the subtree that defines them. Table functions are allowed (they read
// no table by name, and run identically on the source path), but their
// arguments are walked like everything else.
func foreignRelation(v any, scope map[string]bool) string {
	switch x := v.(type) {
	case []any:
		for _, e := range x {
			if r := foreignRelation(e, scope); r != "" {
				return r
			}
		}
	case map[string]any:
		if cm, ok := x["cte_map"].(map[string]any); ok {
			if entries, _ := cm["map"].([]any); len(entries) > 0 {
				inner := make(map[string]bool, len(scope)+len(entries))
				for k := range scope {
					inner[k] = true
				}
				for _, raw := range entries {
					if e, _ := raw.(map[string]any); e != nil {
						inner[strings.ToLower(str(e["key"]))] = true
					}
				}
				scope = inner
			}
		}
		switch str(x["type"]) {
		case "BASE_TABLE":
			name := str(x["table_name"])
			if str(x["schema_name"]) == "" && str(x["catalog_name"]) == "" && scope[strings.ToLower(name)] {
				return ""
			}
			parts := []string{}
			for _, p := range []string{str(x["catalog_name"]), str(x["schema_name"]), name} {
				if p != "" {
					parts = append(parts, p)
				}
			}
			return strings.Join(parts, ".")
		case "SHOW_REF":
			return "SHOW/DESCRIBE " + str(x["table_name"])
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys) // deterministic: the reported relation is stable
		for _, k := range keys {
			if r := foreignRelation(x[k], scope); r != "" {
				return r
			}
		}
	}
	return ""
}

// firstCTEBody splits `WITH name AS ( BODY ) REST` into head=`WITH … (`,
// body=`BODY`, tail=`) REST`, balancing parentheses while skipping single-quoted
// string literals and `--` line comments. Returns ok=false when the input isn't a
// WITH-query or the parentheses don't balance. (Does not handle ” escapes inside
// strings — such a query simply isn't rewritten and runs against source.)
func firstCTEBody(sql string) (head, body, tail string, ok bool) {
	i := 0
	for i < len(sql) && isSpace(sql[i]) {
		i++
	}
	if i+4 > len(sql) || !strings.EqualFold(sql[i:i+4], "WITH") {
		return "", "", "", false
	}
	open := indexTopLevel(sql, i, '(')
	if open < 0 {
		return "", "", "", false
	}
	depth := 0
	for j := open; j < len(sql); j++ {
		switch sql[j] {
		case '\'':
			j = skipString(sql, j)
		case '-':
			if j+1 < len(sql) && sql[j+1] == '-' {
				j = skipLineComment(sql, j)
			}
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return sql[:open+1], sql[open+1 : j], sql[j:], true
			}
		}
	}
	return "", "", "", false
}

// indexTopLevel returns the index of the first occurrence of ch at/after start,
// skipping string literals and line comments.
func indexTopLevel(sql string, start int, ch byte) int {
	for j := start; j < len(sql); j++ {
		switch sql[j] {
		case '\'':
			j = skipString(sql, j)
		case '-':
			if j+1 < len(sql) && sql[j+1] == '-' {
				j = skipLineComment(sql, j)
			}
		case ch:
			return j
		}
	}
	return -1
}

// skipString returns the index of the closing quote of the string literal that
// opens at i (sql[i]=='\”). If unterminated, returns the last index.
func skipString(sql string, i int) int {
	for j := i + 1; j < len(sql); j++ {
		if sql[j] == '\'' {
			return j
		}
	}
	return len(sql) - 1
}

// skipLineComment returns the index of the newline ending the `--` comment at i,
// or the last index if the comment runs to EOF.
func skipLineComment(sql string, i int) int {
	for j := i; j < len(sql); j++ {
		if sql[j] == '\n' {
			return j
		}
	}
	return len(sql) - 1
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
