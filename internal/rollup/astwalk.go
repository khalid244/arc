package rollup

import (
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// joinFuncName returns the dotted function name (e.g. "schema.func") from a
// FuncCall's funcname node list. Lowercasing is the caller's responsibility.
func joinFuncName(parts []*pg.Node) string {
	pieces := []string{}
	for _, p := range parts {
		if s := p.GetString_(); s != nil {
			pieces = append(pieces, s.GetSval())
		}
	}
	return strings.Join(pieces, ".")
}

// columnName returns the last segment of a ColumnRef's qualified name (i.e.,
// the bare column identifier, dropping any table/schema qualifiers). Empty
// when the ColumnRef has no String fields (e.g., a `*` reference).
func columnName(c *pg.ColumnRef) string {
	pieces := []string{}
	for _, f := range c.GetFields() {
		if s := f.GetString_(); s != nil {
			pieces = append(pieces, s.GetSval())
		}
	}
	if len(pieces) == 0 {
		return ""
	}
	return pieces[len(pieces)-1]
}

// walkFuncCalls walks n and calls visit on every FuncCall encountered,
// descending through the expression node types we care about. Used by the
// translatability guard, the nested-aggregate detector in emit.go, and the
// rewriter's aggregate enumeration.
func walkFuncCalls(n *pg.Node, visit func(*pg.FuncCall)) {
	if n == nil {
		return
	}
	if rt := n.GetResTarget(); rt != nil {
		walkFuncCalls(rt.GetVal(), visit)
		return
	}
	if fc := n.GetFuncCall(); fc != nil {
		visit(fc)
		for _, a := range fc.GetArgs() {
			walkFuncCalls(a, visit)
		}
		if af := fc.GetAggFilter(); af != nil {
			walkFuncCalls(af, visit)
		}
		for _, o := range fc.GetAggOrder() {
			walkFuncCalls(o, visit)
		}
		return
	}
	if ax := n.GetAExpr(); ax != nil {
		walkFuncCalls(ax.GetLexpr(), visit)
		walkFuncCalls(ax.GetRexpr(), visit)
		return
	}
	if be := n.GetBoolExpr(); be != nil {
		for _, a := range be.GetArgs() {
			walkFuncCalls(a, visit)
		}
		return
	}
	if ce := n.GetCaseExpr(); ce != nil {
		walkFuncCalls(ce.GetArg(), visit)
		walkFuncCalls(ce.GetDefresult(), visit)
		for _, a := range ce.GetArgs() {
			walkFuncCalls(a, visit)
		}
		return
	}
	if cw := n.GetCaseWhen(); cw != nil {
		walkFuncCalls(cw.GetExpr(), visit)
		walkFuncCalls(cw.GetResult(), visit)
		return
	}
	if cl := n.GetCoalesceExpr(); cl != nil {
		for _, a := range cl.GetArgs() {
			walkFuncCalls(a, visit)
		}
		return
	}
	if tc := n.GetTypeCast(); tc != nil {
		walkFuncCalls(tc.GetArg(), visit)
		return
	}
	if sb := n.GetSortBy(); sb != nil {
		walkFuncCalls(sb.GetNode(), visit)
		return
	}
}
