package tiered

import (
	"strings"
	"testing"
	"time"
)

// Every clause that accepts an Expr must enforce mode validation. If any
// clause skips the check, a future edit could let a rollup-only column
// leak into a SourceMode SELECT (the original "cnt not found" bug class).
func TestIR_ModeValidation_AllClauses(t *testing.T) {
	rollupOnly := []string{"cnt", "sum_x", "min_x", "max_x", "hll_x", "kll_x", "cnt_x", "site_class", "bucket"}

	clauses := map[string]func(col string) *SelectStmt{
		"SELECT": func(col string) *SelectStmt {
			return NewSelect(SourceMode).Project(Col(col), "v").From(Table("events"))
		},
		"WHERE": func(col string) *SelectStmt {
			return NewSelect(SourceMode).
				Project(Col("site"), "site").From(Table("events")).
				Where(BinOp("=", Col(col), Raw("'x'")))
		},
		"GROUP BY": func(col string) *SelectStmt {
			return NewSelect(SourceMode).
				Project(Col("site"), "site").From(Table("events")).
				GroupBy(Col(col))
		},
		"HAVING": func(col string) *SelectStmt {
			return NewSelect(SourceMode).
				Project(Col("site"), "site").From(Table("events")).
				GroupBy(Col("site")).
				Having(BinOp(">", Col(col), Raw("0")))
		},
		"ORDER BY": func(col string) *SelectStmt {
			return NewSelect(SourceMode).
				Project(Col("site"), "site").From(Table("events")).
				OrderByExpr(Col(col), true).Limit(5)
		},
	}

	for clauseName, mk := range clauses {
		for _, col := range rollupOnly {
			t.Run(clauseName+"/"+col, func(t *testing.T) {
				_, err := mk(col).Build()
				if err == nil {
					t.Fatalf("expected mode-validation error in clause %s for column %q", clauseName, col)
				}
				if !strings.Contains(err.Error(), col) {
					t.Fatalf("err message should mention column %q, got: %v", col, err)
				}
			})
		}
	}
}

// And the converse — a typical valid source-mode SELECT involving every
// clause must Build cleanly. Catches over-zealous validation.
func TestIR_ModeValidation_TypicalSourceSelectBuilds(t *testing.T) {
	s := NewSelect(SourceMode).
		Project(Col("site"), "site").
		Project(FuncExpr("COUNT", Star()), "v").
		From(Table("events")).
		Where(BinOp(">=", Col("time"), TimestampLit(time.Now()))).
		Where(In(Col("site"), []string{"a", "b"}, false)).
		GroupBy(Col("site")).
		Having(BinOp(">", FuncExpr("COUNT", Star()), Raw("10"))).
		OrderByExpr(FuncExpr("COUNT", Star()), true).
		Limit(50)
	if _, err := s.Build(); err != nil {
		t.Fatalf("typical source SELECT failed: %v", err)
	}
}
