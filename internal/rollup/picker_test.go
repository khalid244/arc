package rollup

import (
	"testing"
	"time"
)

func TestPickBestVariant_DailyPlainAgg(t *testing.T) {
	specs := []RollupSpec{
		{Name: "x__y__1d", BucketInterval: 24 * time.Hour, KeepDimensions: []string{"a", "b"}, Aggregations: []Aggregation{{SourceColumn: "v", Functions: []AggFunction{AggCount, AggSum}}}},
		{Name: "x__y__sketch_1d", BucketInterval: 24 * time.Hour},
	}
	q := QueryShape{
		BucketGrain:      24 * time.Hour,
		NeededDims:       []string{"a"},
		NeededAggregates: []NeededAgg{{Op: "SUM", Column: "v"}},
	}
	got := PickBestVariant(specs, q)
	if got == nil || got.Name != "x__y__1d" {
		t.Errorf("picked = %v, want x__y__1d", got)
	}
}

func TestPickBestVariant_SketchSingleDim(t *testing.T) {
	hll := &SketchConfig{HLLLgK: 12}
	specs := []RollupSpec{
		{Name: "x__y__1d", BucketInterval: 24 * time.Hour, KeepDimensions: []string{"country"}},
		{Name: "x__y__by_country__1d", BucketInterval: 24 * time.Hour, KeepDimensions: []string{"country"}, Aggregations: []Aggregation{{SourceColumn: "device_id", Functions: []AggFunction{AggHLL}, SketchConfig: hll}}},
	}
	q := QueryShape{
		BucketGrain:      24 * time.Hour,
		NeededDims:       []string{"country"},
		NeededAggregates: []NeededAgg{{Op: "COUNT_DISTINCT", Column: "device_id"}},
	}
	got := PickBestVariant(specs, q)
	if got == nil || got.Name != "x__y__by_country__1d" {
		t.Errorf("picked = %v, want x__y__by_country__1d", got)
	}
}

func TestPickBestVariant_GlobalSketch(t *testing.T) {
	hll := &SketchConfig{HLLLgK: 12}
	specs := []RollupSpec{
		{Name: "x__y__sketch_1d", BucketInterval: 24 * time.Hour, Aggregations: []Aggregation{{SourceColumn: "device_id", Functions: []AggFunction{AggHLL}, SketchConfig: hll}}},
	}
	q := QueryShape{
		BucketGrain:      24 * time.Hour,
		NeededDims:       nil,
		NeededAggregates: []NeededAgg{{Op: "COUNT_DISTINCT", Column: "device_id"}},
	}
	got := PickBestVariant(specs, q)
	if got == nil || got.Name != "x__y__sketch_1d" {
		t.Errorf("picked = %v, want x__y__sketch_1d", got)
	}
}

func TestPickBestVariant_NoFit_ReturnsNil(t *testing.T) {
	specs := []RollupSpec{
		{Name: "x__y__1d", BucketInterval: 24 * time.Hour, KeepDimensions: []string{"a"}},
	}
	q := QueryShape{
		BucketGrain: time.Minute,
		NeededDims:  []string{"a"},
	}
	if got := PickBestVariant(specs, q); got != nil {
		t.Errorf("expected nil for minute-grain query, got %v", got.Name)
	}
}
