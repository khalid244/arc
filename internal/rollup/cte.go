package rollup

import (
	"sort"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// RewriteCTEs walks the WITH clause of originalSQL and applies rewriteFn to
// each CTE body. The fn is typically the same plan+rewrite pipeline used for
// top-level queries — when a CTE body is `SELECT … FROM <table> WHERE …`,
// it qualifies for the hybrid rewrite even when the OUTER query references
// only the CTE name(s).
//
// Returns the SQL with rewritten CTE bodies spliced in (or originalSQL
// unchanged if there are no CTEs or none rewrote).
//
// rewriteFn signature: (innerSQL) → (resultSQL, rewritten). When `rewritten`
// is false the body is left untouched. Recursive: if a CTE body itself has
// CTEs the same fn handles them.
//
// Anchored on the CTE name's AST location, then paren-balanced over the
// body. Whitespace before `AS` and before `(` is tolerated. We do NOT splice
// outer-query content; only CTE bodies are touched.
func RewriteCTEs(originalSQL string, rewriteFn func(string) (string, bool)) string {
	tree, err := pg.Parse(originalSQL)
	if err != nil {
		return originalSQL
	}
	if len(tree.GetStmts()) == 0 {
		return originalSQL
	}
	sel := tree.GetStmts()[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return originalSQL
	}
	with := sel.GetWithClause()
	if with == nil {
		return originalSQL
	}

	type sub struct {
		start, end int
		text       string
	}
	var subs []sub

	for _, c := range with.GetCtes() {
		e := c.GetCommonTableExpr()
		if e == nil {
			continue
		}
		name := e.GetCtename()
		if name == "" {
			continue
		}

		// CommonTableExpr.Location points at the start of the CTE name.
		nameStart := int(e.GetLocation())
		if nameStart < 0 || nameStart+len(name) > len(originalSQL) {
			continue
		}
		afterName := nameStart + len(name)

		// Skip whitespace, then require "AS".
		i := skipSpace(originalSQL, afterName)
		if i+2 > len(originalSQL) || !strings.EqualFold(originalSQL[i:i+2], "AS") {
			continue
		}
		i += 2
		// Skip whitespace, optional NOT/MATERIALIZED keywords (DuckDB accepts them),
		// then require `(`.
		i = skipSpace(originalSQL, i)
		// Handle optional `[NOT] MATERIALIZED` between AS and `(`.
		for _, kw := range []string{"NOT MATERIALIZED", "MATERIALIZED"} {
			if i+len(kw) <= len(originalSQL) && strings.EqualFold(originalSQL[i:i+len(kw)], kw) {
				i = skipSpace(originalSQL, i+len(kw))
				break
			}
		}
		if i >= len(originalSQL) || originalSQL[i] != '(' {
			continue
		}
		parenEnd, ok := findCallEnd(originalSQL, i)
		if !ok {
			continue
		}

		// Body lives between the `(` and the matching `)` — exclusive of both.
		bodyStart := i + 1
		bodyEnd := parenEnd - 1
		body := originalSQL[bodyStart:bodyEnd]

		// Trim leading/trailing whitespace from the body for the recursive
		// rewrite, but splice back into the SAME byte range so surrounding
		// whitespace is preserved.
		trimmed := strings.TrimSpace(body)
		rewritten, ok := rewriteFn(trimmed)
		if !ok || rewritten == trimmed {
			continue
		}
		subs = append(subs, sub{
			start: bodyStart,
			end:   bodyEnd,
			text:  "\n" + rewritten + "\n",
		})
	}

	if len(subs) == 0 {
		return originalSQL
	}
	// Apply substitutions in reverse to keep earlier byte offsets valid.
	sort.Slice(subs, func(i, j int) bool { return subs[i].start > subs[j].start })
	out := originalSQL
	for _, s := range subs {
		if s.start < 0 || s.end > len(out) || s.start >= s.end {
			continue
		}
		out = out[:s.start] + s.text + out[s.end:]
	}
	return out
}

// skipSpace returns the next non-whitespace index ≥ start.
func skipSpace(s string, start int) int {
	i := start
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

// findCallEnd finds the byte offset just past the closing paren of a paren
// group that starts at start. Tracks nested parens and skips over string
// literals so quoted parens don't confuse the count.
func findCallEnd(sql string, start int) (int, bool) {
	i := start
	for i < len(sql) && sql[i] != '(' {
		i++
	}
	if i >= len(sql) {
		return 0, false
	}
	depth := 0
	for i < len(sql) {
		c := sql[i]
		switch c {
		case '\'':
			// Skip string literal (including doubled '' escapes).
			i++
			for i < len(sql) {
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1, true
			}
		}
		i++
	}
	return 0, false
}
