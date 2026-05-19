package tiered

import (
	"fmt"
	"strings"
	"time"
)

// Expr is any SQL expression that knows how to render itself under a ColMode.
// Construction-time validation against the mode rejects rollup-only column
// names (cnt, sum_*, *_class, etc.) when used in SourceMode.
type Expr interface {
	sql(mode ColMode) (string, error)
}

type colNode struct{ name string }

func (c colNode) sql(mode ColMode) (string, error) {
	if mode == SourceMode && isRollupOnlyColumn(c.name) {
		return "", fmt.Errorf("source-mode SELECT cannot reference rollup-only column %q", c.name)
	}
	return c.name, nil
}

type starNode struct{}

func (starNode) sql(ColMode) (string, error) { return "*", nil }

type funcNode struct {
	name string
	args []Expr
}

func (f funcNode) sql(mode ColMode) (string, error) {
	parts := make([]string, len(f.args))
	for i, a := range f.args {
		s, err := a.sql(mode)
		if err != nil {
			return "", err
		}
		parts[i] = s
	}
	return f.name + "(" + strings.Join(parts, ", ") + ")", nil
}

// From is anything that can appear after FROM.
type From interface {
	fromSQL() (string, error)
}

type readParquetNode struct{ paths []string }

func (r readParquetNode) fromSQL() (string, error) {
	q := make([]string, len(r.paths))
	for i, p := range r.paths {
		q[i] = "'" + strings.ReplaceAll(p, "'", "''") + "'"
	}
	return "read_parquet([" + strings.Join(q, ", ") + "])", nil
}

type tableNode struct{ name string }

func (t tableNode) fromSQL() (string, error) { return t.name, nil }

type fromCTENode struct{ name string }

func (f fromCTENode) fromSQL() (string, error) { return f.name, nil }

type subQueryUnionNode struct {
	parts []*SelectStmt
}

func (s subQueryUnionNode) fromSQL() (string, error) {
	out := make([]string, len(s.parts))
	for i, p := range s.parts {
		ps, err := p.Build()
		if err != nil {
			return "", err
		}
		out[i] = ps
	}
	return "(" + strings.Join(out, " UNION ALL ") + ")", nil
}

type projection struct {
	expr  Expr
	alias string
}

type orderItem struct {
	expr Expr
	desc bool
}

// SelectStmt is a typed-mode SELECT builder.
type SelectStmt struct {
	mode        ColMode
	projections []projection
	from        From
	where       []Expr
	groupBy     []Expr
	having      []Expr
	orderBy     []orderItem
	limit       int // 0 = no limit
}

// NewSelect starts a SELECT scoped to a ColMode. Every column reference added
// to this SELECT is validated against the mode at Build() time.
func NewSelect(mode ColMode) *SelectStmt { return &SelectStmt{mode: mode} }

func (s *SelectStmt) Project(e Expr, alias string) *SelectStmt {
	s.projections = append(s.projections, projection{e, alias})
	return s
}

func (s *SelectStmt) From(f From) *SelectStmt {
	s.from = f
	return s
}

func (s *SelectStmt) Where(e Expr) *SelectStmt {
	s.where = append(s.where, e)
	return s
}

func (s *SelectStmt) GroupBy(exprs ...Expr) *SelectStmt {
	s.groupBy = append(s.groupBy, exprs...)
	return s
}

func (s *SelectStmt) Having(e Expr) *SelectStmt {
	s.having = append(s.having, e)
	return s
}

func (s *SelectStmt) OrderByExpr(e Expr, desc bool) *SelectStmt {
	s.orderBy = append(s.orderBy, orderItem{e, desc})
	return s
}

func (s *SelectStmt) Limit(n int) *SelectStmt {
	s.limit = n
	return s
}

func (s *SelectStmt) Build() (string, error) {
	var b strings.Builder
	b.WriteString("SELECT")
	for i, p := range s.projections {
		es, err := p.expr.sql(s.mode)
		if err != nil {
			return "", err
		}
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("\n  ")
		b.WriteString(es)
		if p.alias != "" {
			b.WriteString(" AS ")
			b.WriteString(p.alias)
		}
	}
	if s.from != nil {
		fs, err := s.from.fromSQL()
		if err != nil {
			return "", err
		}
		b.WriteString("\nFROM ")
		b.WriteString(fs)
	}
	if len(s.where) > 0 {
		whereParts := make([]string, len(s.where))
		for i, w := range s.where {
			ws, err := w.sql(s.mode)
			if err != nil {
				return "", err
			}
			whereParts[i] = ws
		}
		b.WriteString("\nWHERE ")
		b.WriteString(strings.Join(whereParts, " AND "))
	}
	if len(s.groupBy) > 0 {
		gbParts := make([]string, len(s.groupBy))
		for i, g := range s.groupBy {
			gs, err := g.sql(s.mode)
			if err != nil {
				return "", err
			}
			gbParts[i] = gs
		}
		b.WriteString("\nGROUP BY ")
		b.WriteString(strings.Join(gbParts, ", "))
	}
	if len(s.having) > 0 {
		havingParts := make([]string, len(s.having))
		for i, h := range s.having {
			hs, err := h.sql(s.mode)
			if err != nil {
				return "", err
			}
			havingParts[i] = hs
		}
		b.WriteString("\nHAVING ")
		b.WriteString(strings.Join(havingParts, " AND "))
	}
	if len(s.orderBy) > 0 {
		obParts := make([]string, len(s.orderBy))
		for i, o := range s.orderBy {
			os, err := o.expr.sql(s.mode)
			if err != nil {
				return "", err
			}
			if o.desc {
				os += " DESC"
			}
			obParts[i] = os
		}
		b.WriteString("\nORDER BY ")
		b.WriteString(strings.Join(obParts, ", "))
	}
	if s.limit > 0 {
		fmt.Fprintf(&b, "\nLIMIT %d", s.limit)
	}
	return b.String(), nil
}

type binOpNode struct {
	op    string
	l, r  Expr
}

func (b binOpNode) sql(mode ColMode) (string, error) {
	ls, err := b.l.sql(mode)
	if err != nil {
		return "", err
	}
	rs, err := b.r.sql(mode)
	if err != nil {
		return "", err
	}
	return ls + " " + b.op + " " + rs, nil
}

type inNode struct {
	col    Expr
	values []string
	neg    bool
}

func (i inNode) sql(mode ColMode) (string, error) {
	cs, err := i.col.sql(mode)
	if err != nil {
		return "", err
	}
	quoted := make([]string, len(i.values))
	for k, v := range i.values {
		quoted[k] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	op := "IN"
	if i.neg {
		op = "NOT IN"
	}
	return cs + " " + op + " (" + strings.Join(quoted, ", ") + ")", nil
}

type andNode struct{ parts []Expr }

func (a andNode) sql(mode ColMode) (string, error) {
	out := make([]string, len(a.parts))
	for i, p := range a.parts {
		s, err := p.sql(mode)
		if err != nil {
			return "", err
		}
		out[i] = s
	}
	return strings.Join(out, " AND "), nil
}

type timestampLitNode struct{ t time.Time }

func (l timestampLitNode) sql(ColMode) (string, error) {
	return fmt.Sprintf("TIMESTAMP '%s'", l.t.UTC().Format("2006-01-02 15:04:05+00")), nil
}

type rawNode struct{ s string }

func (r rawNode) sql(ColMode) (string, error) { return r.s, nil }

// BinOp builds "<l> <op> <r>" with mode-validated operands.
func BinOp(op string, l, r Expr) Expr { return binOpNode{op, l, r} }

// In builds "<col> IN (...)" — set neg=true for NOT IN.
func In(col Expr, values []string, neg bool) Expr { return inNode{col, values, neg} }

// And joins predicates with " AND ".
func And(parts ...Expr) Expr { return andNode{parts} }

// TimestampLit renders a TIMESTAMP '...' literal in UTC.
func TimestampLit(t time.Time) Expr { return timestampLitNode{t} }

// Raw is the escape hatch for SQL fragments the IR doesn't otherwise model
// (aggregate inner-CTE fragments built by aggInnerFragment, etc.). The
// caller takes responsibility for mode-correctness; the IR cannot validate.
func Raw(s string) Expr { return rawNode{s} }

// Col references a column by name.
func Col(name string) Expr { return colNode{name} }

// Star is the * in COUNT(*).
func Star() Expr { return starNode{} }

// FuncExpr is a SQL function call: FuncExpr("SUM", Col("cnt")).
func FuncExpr(name string, args ...Expr) Expr { return funcNode{name, args} }

// ReadParquet is a read_parquet([...]) FROM source.
func ReadParquet(paths []string) From { return readParquetNode{paths} }

// Table is a bare table-name FROM source.
func Table(name string) From { return tableNode{name} }

// FromCTE references a named CTE from the enclosing Statement.
func FromCTE(name string) From { return fromCTENode{name} }

// SubQueryUnionAll renders (sel1 UNION ALL sel2 UNION ALL ...) inline as a FROM source.
func SubQueryUnionAll(parts ...*SelectStmt) From {
	return subQueryUnionNode{parts}
}

type namedCTE struct {
	name string
	body *SelectStmt
}

// Statement is the top-level SQL: optional preamble statements,
// optional WITH-CTE bindings, and a main SELECT body.
type Statement struct {
	setup []string
	ctes  []namedCTE
	body  *SelectStmt
}

func NewStatement() *Statement { return &Statement{} }

// Setup appends a preamble statement (e.g. "SET TimeZone = 'UTC'") rendered
// verbatim with a trailing ";\n" before the WITH/SELECT body.
func (s *Statement) Setup(line string) *Statement {
	s.setup = append(s.setup, line)
	return s
}

// WithCTE binds a SELECT as a named CTE rendered "WITH name AS (...)" /
// ", name AS (...)" in declaration order.
func (s *Statement) WithCTE(name string, body *SelectStmt) *Statement {
	s.ctes = append(s.ctes, namedCTE{name, body})
	return s
}

// Body sets the outer SELECT.
func (s *Statement) Body(sel *SelectStmt) *Statement {
	s.body = sel
	return s
}

func (s *Statement) Build() (string, error) {
	var b strings.Builder
	for _, line := range s.setup {
		b.WriteString(line)
		b.WriteString(";\n")
	}
	for i, c := range s.ctes {
		if i == 0 {
			b.WriteString("WITH ")
		} else {
			b.WriteString(", ")
		}
		b.WriteString(c.name)
		b.WriteString(" AS (\n")
		body, err := c.body.Build()
		if err != nil {
			return "", err
		}
		b.WriteString(body)
		b.WriteString("\n)")
	}
	if len(s.ctes) > 0 {
		b.WriteString("\n")
	}
	if s.body != nil {
		body, err := s.body.Build()
		if err != nil {
			return "", err
		}
		b.WriteString(body)
	}
	return b.String(), nil
}

// isRollupOnlyColumn names columns that exist only in the rollup-CTE schema.
// Referencing one in SourceMode means the fragment has the wrong column world
// (which is exactly the "cnt not found" production bug). The check is a
// structural rule, not a registry — adding a new rollup-only column requires
// extending this list (and the rollup builder).
func isRollupOnlyColumn(name string) bool {
	switch name {
	case "cnt", "bucket":
		return true
	}
	for _, p := range []string{"sum_", "min_", "max_", "hll_", "kll_", "cnt_"} {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return strings.HasSuffix(name, "_class")
}
