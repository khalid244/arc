package rollup

import "strings"

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
