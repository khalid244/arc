package main

// compaction.manifest_recovery_input_check_concurrency and
// compaction.input_delete_retries reach the compaction Manager only through
// the ManagerConfig literal in main(). Dropping either line compiles and every
// other test passes while production silently runs the default, so this pins
// the wiring in main.go itself.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestCompactionManagerConfigWiresRecoveryKeys(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ManifestRecoveryInputCheckConcurrency", "InputDeleteRetries"}

	literals := 0
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := lit.Type.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "ManagerConfig" {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "compaction" {
			return true
		}
		literals++
		values := map[string]string{}
		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok {
				continue
			}
			values[key.Name] = exprString(kv.Value)
		}
		for _, field := range want {
			if got, wantVal := values[field], "cfg.Compaction."+field; got != wantVal {
				t.Errorf("%s: compaction.ManagerConfig.%s = %q, want %q",
					fset.Position(lit.Pos()), field, got, wantVal)
			}
		}
		return true
	})
	if literals != 1 {
		t.Fatalf("found %d compaction.ManagerConfig literals in main.go, want 1", literals)
	}
}

// exprString renders a selector chain like cfg.Compaction.X; anything else is
// returned as "?" so a mismatch is reported rather than accepted.
func exprString(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return exprString(v.X) + "." + v.Sel.Name
	}
	return "?"
}
