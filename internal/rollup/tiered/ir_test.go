package tiered

import (
	"strings"
	"testing"
	"time"
)

func TestIR_BasicSelectRenders(t *testing.T) {
	s := NewSelect(RollupMode).
		Project(Col("bucket"), "_bkt").
		From(ReadParquet([]string{"a.parquet"}))
	sql, err := s.Build()
	if err != nil {
		t.Fatalf("Build returned err: %v", err)
	}
	if !strings.Contains(sql, "SELECT") {
		t.Fatalf("expected SELECT in sql: %q", sql)
	}
	if !strings.Contains(sql, "bucket AS _bkt") {
		t.Fatalf("expected projection in sql: %q", sql)
	}
	if !strings.Contains(sql, "read_parquet(['a.parquet'])") {
		t.Fatalf("expected from in sql: %q", sql)
	}
}

func TestIR_SourceModeRejectsRollupOnlyColumn(t *testing.T) {
	cases := []string{"cnt", "sum_x", "min_x", "max_x", "hll_x", "kll_x", "cnt_x", "site_class", "bucket"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			s := NewSelect(SourceMode).
				Project(Col(name), "v").
				From(Table("events"))
			_, err := s.Build()
			if err == nil {
				t.Fatalf("expected error referencing rollup-only column %q in SourceMode", name)
			}
		})
	}
}

func TestIR_SourceModeAllowsArbitrarySourceColumn(t *testing.T) {
	s := NewSelect(SourceMode).
		Project(Col("site"), "site").
		Project(FuncExpr("COUNT", Star()), "v").
		From(Table("events"))
	sql, err := s.Build()
	if err != nil {
		t.Fatalf("Build returned err: %v", err)
	}
	if !strings.Contains(sql, "COUNT(*)") {
		t.Fatalf("expected COUNT(*) in sql: %q", sql)
	}
}

func TestIR_RollupModeAllowsCnt(t *testing.T) {
	s := NewSelect(RollupMode).
		Project(FuncExpr("SUM", Col("cnt")), "v").
		From(ReadParquet([]string{"a.parquet"}))
	sql, err := s.Build()
	if err != nil {
		t.Fatalf("Build returned err: %v", err)
	}
	if !strings.Contains(sql, "SUM(cnt)") {
		t.Fatalf("expected SUM(cnt) in sql: %q", sql)
	}
}

func TestIR_BinOpRenders(t *testing.T) {
	ts := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	e := BinOp(">=", Col("bucket"), TimestampLit(ts))
	sql, err := e.sql(RollupMode)
	if err != nil {
		t.Fatalf("sql err: %v", err)
	}
	if !strings.Contains(sql, "bucket >=") {
		t.Fatalf("expected operator in sql: %q", sql)
	}
	if !strings.Contains(sql, "TIMESTAMP '2026-05-10") {
		t.Fatalf("expected timestamp literal in sql: %q", sql)
	}
}

func TestIR_InRenders(t *testing.T) {
	e := In(Col("site_class"), []string{"a", "b'c"}, false)
	sql, err := e.sql(RollupMode)
	if err != nil {
		t.Fatalf("sql err: %v", err)
	}
	if sql != "site_class IN ('a', 'b''c')" {
		t.Fatalf("unexpected sql: %q", sql)
	}
}

func TestIR_NotInRenders(t *testing.T) {
	e := In(Col("site_class"), []string{"x"}, true)
	sql, err := e.sql(RollupMode)
	if err != nil {
		t.Fatalf("sql err: %v", err)
	}
	if sql != "site_class NOT IN ('x')" {
		t.Fatalf("unexpected sql: %q", sql)
	}
}

func TestIR_AndJoinsWithSpaces(t *testing.T) {
	e := And(
		BinOp(">=", Col("bucket"), TimestampLit(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))),
		BinOp("<", Col("bucket"), TimestampLit(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))),
		In(Col("site_class"), []string{"a"}, false),
	)
	sql, err := e.sql(RollupMode)
	if err != nil {
		t.Fatalf("sql err: %v", err)
	}
	expectedTokens := []string{"bucket >=", "AND", "bucket <", "AND", "site_class IN"}
	for _, tok := range expectedTokens {
		if !strings.Contains(sql, tok) {
			t.Fatalf("expected %q in sql: %q", tok, sql)
		}
	}
}

func TestIR_AndValidatesEachOperandMode(t *testing.T) {
	// In SourceMode, embedding a rollup-only column inside AND must error.
	e := And(
		BinOp(">=", Col("time"), TimestampLit(time.Now())),
		In(Col("site_class"), []string{"a"}, false), // rollup-only
	)
	if _, err := e.sql(SourceMode); err == nil {
		t.Fatalf("expected error for site_class in SourceMode")
	}
}

func TestIR_RawEscapeHatch(t *testing.T) {
	e := Raw("CASE WHEN x THEN 1 ELSE 0 END")
	sql, err := e.sql(SourceMode)
	if err != nil {
		t.Fatalf("sql err: %v", err)
	}
	if sql != "CASE WHEN x THEN 1 ELSE 0 END" {
		t.Fatalf("unexpected sql: %q", sql)
	}
}

func TestIR_SelectWithWhereAndGroupAndOrderAndLimit(t *testing.T) {
	s := NewSelect(RollupMode).
		Project(Col("bucket"), "_bkt").
		Project(Col("site_class"), "site").
		Project(FuncExpr("SUM", Col("cnt")), "value").
		From(ReadParquet([]string{"a.parquet"})).
		Where(BinOp(">=", Col("bucket"), TimestampLit(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)))).
		Where(In(Col("site_class"), []string{"a"}, false)).
		GroupBy(Col("bucket"), Col("site_class")).
		OrderByExpr(Col("bucket"), false).
		Limit(10)
	sql, err := s.Build()
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	for _, tok := range []string{"WHERE", "bucket >=", "AND site_class IN", "GROUP BY bucket, site_class", "ORDER BY bucket", "LIMIT 10"} {
		if !strings.Contains(sql, tok) {
			t.Fatalf("expected %q in sql:\n%s", tok, sql)
		}
	}
}

func TestIR_OrderByDesc(t *testing.T) {
	s := NewSelect(RollupMode).
		Project(FuncExpr("SUM", Col("cnt")), "v").
		From(ReadParquet([]string{"a.parquet"})).
		OrderByExpr(Col("cnt"), true).
		Limit(5)
	sql, err := s.Build()
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	if !strings.Contains(sql, "ORDER BY cnt DESC") {
		t.Fatalf("expected ORDER BY cnt DESC: %s", sql)
	}
	if !strings.Contains(sql, "LIMIT 5") {
		t.Fatalf("expected LIMIT 5: %s", sql)
	}
}

func TestIR_Having(t *testing.T) {
	s := NewSelect(RollupMode).
		Project(Col("site_class"), "site").
		Project(FuncExpr("SUM", Col("cnt")), "v").
		From(ReadParquet([]string{"a.parquet"})).
		GroupBy(Col("site_class")).
		Having(BinOp(">", FuncExpr("SUM", Col("cnt")), Raw("10")))
	sql, err := s.Build()
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	if !strings.Contains(sql, "HAVING SUM(cnt) > 10") {
		t.Fatalf("expected HAVING in sql: %s", sql)
	}
}

func TestIR_WhereValidatesAgainstMode(t *testing.T) {
	// site_class in SourceMode WHERE must error.
	s := NewSelect(SourceMode).
		Project(Col("site"), "site").
		From(Table("events")).
		Where(In(Col("site_class"), []string{"a"}, false))
	if _, err := s.Build(); err == nil {
		t.Fatalf("expected error referencing site_class in SourceMode")
	}
}

func TestIR_FromCTE(t *testing.T) {
	s := NewSelect(RollupMode).
		Project(Col("_bkt"), "time").
		From(FromCTE("rollup"))
	sql, err := s.Build()
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	if !strings.Contains(sql, "FROM rollup") {
		t.Fatalf("expected FROM rollup: %s", sql)
	}
}

func TestIR_UnionAllSubquery(t *testing.T) {
	a := NewSelect(RollupMode).Project(Star(), "").From(FromCTE("rollup"))
	b := NewSelect(RollupMode).Project(Star(), "").From(FromCTE("fresh"))
	main := NewSelect(RollupMode).
		Project(Col("_bkt"), "time").
		From(SubQueryUnionAll(a, b))
	sql, err := main.Build()
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	if !strings.Contains(sql, "FROM (SELECT") || !strings.Contains(sql, "UNION ALL SELECT") {
		t.Fatalf("expected UNION ALL subquery: %s", sql)
	}
}

func TestIR_StatementWithSetupAndCTEs(t *testing.T) {
	rollupCTE := NewSelect(RollupMode).
		Project(FuncExpr("SUM", Col("cnt")), "v").
		From(ReadParquet([]string{"a.parquet"})).
		GroupBy(Col("bucket"))
	main := NewSelect(RollupMode).
		Project(Col("v"), "value").
		From(FromCTE("rollup"))
	stmt := NewStatement().
		Setup("SET TimeZone = 'UTC'").
		WithCTE("rollup", rollupCTE).
		Body(main)
	sql, err := stmt.Build()
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	for _, tok := range []string{"SET TimeZone = 'UTC';", "WITH rollup AS (", "SUM(cnt)", "FROM rollup"} {
		if !strings.Contains(sql, tok) {
			t.Fatalf("expected %q in:\n%s", tok, sql)
		}
	}
}

func TestIR_StatementMultipleCTEs(t *testing.T) {
	cte1 := NewSelect(RollupMode).Project(Col("cnt"), "c").From(ReadParquet([]string{"a"}))
	cte2 := NewSelect(SourceMode).Project(FuncExpr("COUNT", Star()), "c").From(Table("events"))
	main := NewSelect(RollupMode).
		Project(Col("c"), "value").
		From(SubQueryUnionAll(
			NewSelect(RollupMode).Project(Star(), "").From(FromCTE("rollup")),
			NewSelect(RollupMode).Project(Star(), "").From(FromCTE("fresh")),
		))
	stmt := NewStatement().
		WithCTE("rollup", cte1).
		WithCTE("fresh", cte2).
		Body(main)
	sql, err := stmt.Build()
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	for _, tok := range []string{"WITH rollup AS (", ", fresh AS (", "UNION ALL"} {
		if !strings.Contains(sql, tok) {
			t.Fatalf("expected %q in:\n%s", tok, sql)
		}
	}
}

func TestIR_StatementPropagatesCTEModeError(t *testing.T) {
	// A source-mode CTE that references cnt must surface its error at statement build.
	badCTE := NewSelect(SourceMode).
		Project(FuncExpr("SUM", Col("cnt")), "v").
		From(Table("events"))
	stmt := NewStatement().
		WithCTE("fresh", badCTE).
		Body(NewSelect(RollupMode).Project(Col("v"), "v").From(FromCTE("fresh")))
	if _, err := stmt.Build(); err == nil {
		t.Fatalf("expected mode error for cnt in SourceMode CTE")
	}
}
