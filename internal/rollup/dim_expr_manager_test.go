package rollup

import "testing"

// The Manager hands [rollup] dim_expressions to every Router it publishes;
// unset, its routers keep the original parser.
func TestManagerRouterCarriesDimExpressions(t *testing.T) {
	for _, on := range []bool{false, true} {
		m := testManager(newFakeStorage())
		m.cfg.DimExpressions = on
		m.mu.Lock()
		m.rebuildRouterLocked()
		m.mu.Unlock()
		if got := m.Router().DimExprs; got != on {
			t.Errorf("DimExpressions=%v: router DimExprs=%v", on, got)
		}
	}
}
