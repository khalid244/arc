package tiered

import (
	"strings"
	"testing"
)

func TestTranslate_CountStar(t *testing.T) {
	a := Aggregate{Kind: AggCountStar}
	inner, outer, ok := TranslateAggregate(a, 0, "sketch")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if inner != "SUM(cnt) AS _agg_0" {
		t.Fatalf("inner: got %q", inner)
	}
	if outer != "CAST(SUM(_agg_0) AS BIGINT)" {
		t.Fatalf("outer: got %q", outer)
	}
}

func TestTranslate_Count(t *testing.T) {
	a := Aggregate{Kind: AggCount, Column: "country"}
	inner, outer, ok := TranslateAggregate(a, 0, "sketch")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if inner != "SUM(cnt_country) AS _agg_0" {
		t.Fatalf("inner: got %q", inner)
	}
	if outer != "CAST(SUM(_agg_0) AS BIGINT)" {
		t.Fatalf("outer: got %q", outer)
	}
}

func TestTranslate_Sum(t *testing.T) {
	a := Aggregate{Kind: AggSum, Column: "duration_seconds"}
	inner, outer, ok := TranslateAggregate(a, 0, "sketch")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if inner != "SUM(sum_duration_seconds) AS _agg_0" {
		t.Fatalf("inner: got %q", inner)
	}
	if outer != "SUM(_agg_0)" {
		t.Fatalf("outer: got %q", outer)
	}
}

func TestTranslate_Avg(t *testing.T) {
	a := Aggregate{Kind: AggAvg, Column: "duration_seconds"}
	inner, outer, ok := TranslateAggregate(a, 0, "sketch")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if !strings.Contains(inner, "SUM(sum_duration_seconds) AS _agg_0_sum") {
		t.Fatalf("inner missing sum col: %q", inner)
	}
	if !strings.Contains(inner, "SUM(cnt_duration_seconds) AS _agg_0_cnt") {
		t.Fatalf("inner missing cnt col: %q", inner)
	}
	if outer != "SUM(_agg_0_sum) / NULLIF(SUM(_agg_0_cnt), 0)" {
		t.Fatalf("outer: got %q", outer)
	}
}

func TestTranslate_Min(t *testing.T) {
	a := Aggregate{Kind: AggMin, Column: "duration_seconds"}
	inner, outer, ok := TranslateAggregate(a, 0, "sketch")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if inner != "MIN(min_duration_seconds) AS _agg_0" {
		t.Fatalf("inner: got %q", inner)
	}
	if outer != "MIN(_agg_0)" {
		t.Fatalf("outer: got %q", outer)
	}
}

func TestTranslate_Max(t *testing.T) {
	a := Aggregate{Kind: AggMax, Column: "duration_seconds"}
	inner, outer, ok := TranslateAggregate(a, 0, "sketch")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if inner != "MAX(max_duration_seconds) AS _agg_0" {
		t.Fatalf("inner: got %q", inner)
	}
	if outer != "MAX(_agg_0)" {
		t.Fatalf("outer: got %q", outer)
	}
}

func TestTranslate_CountDistinct_SketchVariant(t *testing.T) {
	a := Aggregate{Kind: AggCountDistinct, Column: "device_id"}
	inner, outer, ok := TranslateAggregate(a, 0, "sketch")
	if !ok {
		t.Fatal("expected ok=true for sketch variant")
	}
	if inner != "datasketch_hll_union(14, CAST(hll_device_id AS sketch_hll)) AS _agg_0" {
		t.Fatalf("inner: got %q", inner)
	}
	if outer != "CAST(datasketch_hll_estimate(datasketch_hll_union(14, CAST(_agg_0 AS sketch_hll))) AS BIGINT)" {
		t.Fatalf("outer: got %q", outer)
	}
}

func TestTranslate_CountDistinct_AllVariant_Refused(t *testing.T) {
	a := Aggregate{Kind: AggCountDistinct, Column: "device_id"}
	_, _, ok := TranslateAggregate(a, 0, "all")
	if ok {
		t.Fatal("expected ok=false for all variant")
	}
}

func TestTranslate_Quantile_SketchVariant(t *testing.T) {
	a := Aggregate{Kind: AggQuantile, Column: "duration_seconds", Quantile: 0.95}
	inner, outer, ok := TranslateAggregate(a, 0, "sketch")
	if !ok {
		t.Fatal("expected ok=true for sketch variant")
	}
	if inner != "datasketch_kll(200, CAST(kll_duration_seconds AS sketch_kll_double)) AS _agg_0" {
		t.Fatalf("inner: got %q", inner)
	}
	if outer != "datasketch_kll_quantile(datasketch_kll(200, CAST(_agg_0 AS sketch_kll_double)), 0.95::DOUBLE, false)" {
		t.Fatalf("outer: got %q", outer)
	}
}

func TestTranslate_Quantile_AllVariant_Refused(t *testing.T) {
	a := Aggregate{Kind: AggQuantile, Column: "duration_seconds", Quantile: 0.95}
	_, _, ok := TranslateAggregate(a, 0, "all")
	if ok {
		t.Fatal("expected ok=false for all variant")
	}
}

func TestTranslate_OutputAlias(t *testing.T) {
	a := Aggregate{Kind: AggCountStar, OutputAlias: "my_total"}
	_, outer, ok := TranslateAggregate(a, 0, "sketch")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if !strings.HasSuffix(outer, " AS my_total") {
		t.Fatalf("outer should end with ' AS my_total': %q", outer)
	}
}

func TestTranslate_NestedExprMultiply(t *testing.T) {
	a := Aggregate{Kind: AggSum, Column: "duration_seconds", OuterExpr: "_agg * 100"}
	_, outer, ok := TranslateAggregate(a, 0, "sketch")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if outer != "(SUM(_agg_0)) * 100" {
		t.Fatalf("outer: got %q", outer)
	}
}

func TestTranslate_AvgWithNestedExpr(t *testing.T) {
	a := Aggregate{Kind: AggAvg, Column: "duration_seconds", OuterExpr: "_agg * 100"}
	_, outer, ok := TranslateAggregate(a, 0, "sketch")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if outer != "(SUM(_agg_0_sum) / NULLIF(SUM(_agg_0_cnt), 0)) * 100" {
		t.Fatalf("outer: got %q", outer)
	}
}
