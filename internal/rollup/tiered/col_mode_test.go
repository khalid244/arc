package tiered

import (
	"strings"
	"testing"
)

// Tests for the single dim/agg-translation helper that bridges
// RollupMode (pre-aggregated rollup files: <dim>_class, cnt, sum_x …)
// and SourceMode (raw rows: <dim>, x …). Centralising this translation
// makes the "forgot to translate one site" bug class structurally
// impossible — every column reference goes through these helpers.

// ---- dimClassExpr ----

func TestDimClassExpr_RollupMode_JustColumn(t *testing.T) {
	got := dimClassExpr(RollupMode, "site", nil)
	if got != "site_class" {
		t.Errorf("rollup mode should be `site_class`, got %q", got)
	}
}

func TestDimClassExpr_RollupMode_IgnoresKeptValues(t *testing.T) {
	got := dimClassExpr(RollupMode, "site", []string{"a", "b"})
	if got != "site_class" {
		t.Errorf("rollup mode should be `site_class` regardless of kept_values, got %q", got)
	}
}

func TestDimClassExpr_SourceMode_NoKeptValues_CoalescesNull(t *testing.T) {
	got := dimClassExpr(SourceMode, "site", nil)
	want := "COALESCE(site, '_null_')"
	if got != want {
		t.Errorf("source mode w/o kept_values should COALESCE: want %q got %q", want, got)
	}
}

func TestDimClassExpr_SourceMode_WithKeptValues_CaseWhen(t *testing.T) {
	got := dimClassExpr(SourceMode, "site", []string{"youtu.be", "www.instagram.com"})
	// Must classify: NULL → _null_, kept → kept value, else → _other_
	if !strings.Contains(got, "CASE WHEN site IS NULL THEN '_null_'") {
		t.Errorf("missing null branch: %s", got)
	}
	if !strings.Contains(got, "WHEN site IN ('youtu.be','www.instagram.com') THEN site") {
		t.Errorf("missing kept branch: %s", got)
	}
	if !strings.Contains(got, "ELSE '_other_'") {
		t.Errorf("missing other branch: %s", got)
	}
}

func TestDimClassExpr_SourceMode_EscapesSingleQuotesInKeptValues(t *testing.T) {
	got := dimClassExpr(SourceMode, "site", []string{"O'Brien"})
	if !strings.Contains(got, "'O''Brien'") {
		t.Errorf("kept value with single quote not escaped: %s", got)
	}
}

// ---- dimFilterCol ----
// In a WHERE clause, the rollup-mode reference is `<dim>_class` and the
// source-mode reference is just `<dim>`. (Filter values themselves may
// need separate classification — that's a different helper.)

func TestDimFilterCol_RollupMode(t *testing.T) {
	if got := dimFilterCol(RollupMode, "site"); got != "site_class" {
		t.Errorf("rollup mode filter col should be `site_class`, got %q", got)
	}
}

func TestDimFilterCol_SourceMode(t *testing.T) {
	if got := dimFilterCol(SourceMode, "site"); got != "site" {
		t.Errorf("source mode filter col should be `site`, got %q", got)
	}
}

// ---- aggInnerFragment ----
// Inner fragment for COUNT(*) — RollupMode uses pre-aggregated `cnt`,
// SourceMode counts rows directly.

func TestAggInnerFragment_CountStar_RollupMode(t *testing.T) {
	inner, ok := aggInnerFragment(RollupMode, Aggregate{Kind: AggCountStar}, 0)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if inner != "SUM(cnt) AS _agg_0" {
		t.Errorf("rollup COUNT(*) inner should sum pre-aggregated cnt, got %q", inner)
	}
}

func TestAggInnerFragment_CountStar_SourceMode(t *testing.T) {
	inner, ok := aggInnerFragment(SourceMode, Aggregate{Kind: AggCountStar}, 0)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if inner != "COUNT(*) AS _agg_0" {
		t.Errorf("source COUNT(*) inner should COUNT raw rows, got %q", inner)
	}
}

func TestAggInnerFragment_Sum_RollupMode(t *testing.T) {
	inner, ok := aggInnerFragment(RollupMode, Aggregate{Kind: AggSum, Column: "duration"}, 0)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if inner != "SUM(sum_duration) AS _agg_0" {
		t.Errorf("rollup SUM should reference sum_<col>, got %q", inner)
	}
}

func TestAggInnerFragment_Sum_SourceMode(t *testing.T) {
	inner, ok := aggInnerFragment(SourceMode, Aggregate{Kind: AggSum, Column: "duration"}, 0)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if inner != "SUM(duration) AS _agg_0" {
		t.Errorf("source SUM should sum raw col, got %q", inner)
	}
}

func TestAggInnerFragment_Avg_BothModes_TwoColumns(t *testing.T) {
	// AVG needs sum + count both sides so the outer can divide.
	rollupInner, _ := aggInnerFragment(RollupMode, Aggregate{Kind: AggAvg, Column: "x"}, 0)
	if !strings.Contains(rollupInner, "SUM(sum_x)") || !strings.Contains(rollupInner, "SUM(cnt_x)") {
		t.Errorf("rollup AVG inner needs sum_x + cnt_x: %s", rollupInner)
	}
	sourceInner, _ := aggInnerFragment(SourceMode, Aggregate{Kind: AggAvg, Column: "x"}, 0)
	if !strings.Contains(sourceInner, "SUM(x)") || !strings.Contains(sourceInner, "COUNT(x)") {
		t.Errorf("source AVG inner needs SUM(x) + COUNT(x): %s", sourceInner)
	}
}

func TestAggInnerFragment_MinMax_RollupMode(t *testing.T) {
	minInner, _ := aggInnerFragment(RollupMode, Aggregate{Kind: AggMin, Column: "x"}, 0)
	if minInner != "MIN(min_x) AS _agg_0" {
		t.Errorf("rollup MIN should be MIN(min_x): %s", minInner)
	}
	maxInner, _ := aggInnerFragment(RollupMode, Aggregate{Kind: AggMax, Column: "x"}, 0)
	if maxInner != "MAX(max_x) AS _agg_0" {
		t.Errorf("rollup MAX should be MAX(max_x): %s", maxInner)
	}
}

func TestAggInnerFragment_MinMax_SourceMode(t *testing.T) {
	minInner, _ := aggInnerFragment(SourceMode, Aggregate{Kind: AggMin, Column: "x"}, 0)
	if minInner != "MIN(x) AS _agg_0" {
		t.Errorf("source MIN should be MIN(x): %s", minInner)
	}
	maxInner, _ := aggInnerFragment(SourceMode, Aggregate{Kind: AggMax, Column: "x"}, 0)
	if maxInner != "MAX(x) AS _agg_0" {
		t.Errorf("source MAX should be MAX(x): %s", maxInner)
	}
}

func TestAggInnerFragment_Sketches_OnlyRollupMode(t *testing.T) {
	// CountDistinct/Quantile need pre-built sketch blobs. Source mode
	// can't synthesise them exactly so the helper must refuse.
	if _, ok := aggInnerFragment(SourceMode, Aggregate{Kind: AggCountDistinct, Column: "id"}, 0); ok {
		t.Error("source mode should refuse AggCountDistinct (no pre-built sketch)")
	}
	if _, ok := aggInnerFragment(SourceMode, Aggregate{Kind: AggQuantile, Column: "x", Quantile: 0.95}, 0); ok {
		t.Error("source mode should refuse AggQuantile (no pre-built sketch)")
	}
	// Rollup mode supports them (existing behaviour preserved).
	if _, ok := aggInnerFragment(RollupMode, Aggregate{Kind: AggCountDistinct, Column: "id"}, 0); !ok {
		t.Error("rollup mode must support AggCountDistinct")
	}
}

func TestAggInnerFragment_MissingColumn_Refuses(t *testing.T) {
	// SUM/AVG/MIN/MAX/COUNT(col) all need a column; refuse if empty.
	for _, kind := range []AggKind{AggCount, AggSum, AggAvg, AggMin, AggMax} {
		if _, ok := aggInnerFragment(RollupMode, Aggregate{Kind: kind}, 0); ok {
			t.Errorf("kind=%v with empty column should refuse in rollup mode", kind)
		}
		if _, ok := aggInnerFragment(SourceMode, Aggregate{Kind: kind}, 0); ok {
			t.Errorf("kind=%v with empty column should refuse in source mode", kind)
		}
	}
}
