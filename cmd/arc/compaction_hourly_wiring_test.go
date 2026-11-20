package main

// The compaction.hourly_* keys reach the manager only through the
// compaction.ManagerConfig literal in main(), which no test runs. This pins
// that literal in main.go itself: a dropped field silently falls back to the
// daily sizing.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestMainWiresHourlyCompactionSizing(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var literals []*ast.CompositeLit
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if sel, ok := lit.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "ManagerConfig" {
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "compaction" {
				literals = append(literals, lit)
			}
		}
		return true
	})
	if len(literals) != 1 {
		t.Fatalf("main.go has %d compaction.ManagerConfig literals, want 1", len(literals))
	}
	wired := map[string]string{}
	for _, elt := range literals[0].Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		// cfg.Compaction.<Field>
		if sel, ok := kv.Value.(*ast.SelectorExpr); ok {
			if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "Compaction" {
				if root, ok := inner.X.(*ast.Ident); ok && root.Name == "cfg" {
					wired[key.Name] = sel.Sel.Name
				}
			}
		}
	}
	for _, field := range []string{"HourlyMaxConcurrent", "HourlyMemoryLimit", "HourlyThreads", "HourlyMaxTempDirectorySize"} {
		if got := wired[field]; got != field {
			t.Errorf("compaction.ManagerConfig.%s = cfg.Compaction.%q, want cfg.Compaction.%s", field, got, field)
		}
	}
}
