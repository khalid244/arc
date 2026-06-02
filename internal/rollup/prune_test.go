package rollup

import "testing"

// TestPrunedToColumns pins the schema-drift adaptation: a per-dim cube is skipped
// when its dimension is absent that day, aggregates over absent columns are
// dropped, and count(*) / present-column aggregates survive.
func TestPrunedToColumns(t *testing.T) {
	spec := CubeSpec{
		Source: "default.events",
		Grain:  "hour",
		Dims:   []string{"site"},
		Aggs: []Aggregate{
			{Kind: AggCount},               // count(*) — no source column
			{Kind: AggSum, Col: "bytes"},   // needs "bytes"
			{Kind: AggMax, Col: "latency"}, // needs "latency"
		},
	}

	// dim present, all agg columns present → unchanged
	if got, ok := spec.prunedToColumns(map[string]bool{"site": true, "bytes": true, "latency": true}); !ok || len(got.Aggs) != 3 {
		t.Fatalf("all-present: ok=%v aggs=%d, want ok=true aggs=3", ok, len(got.Aggs))
	}

	// dimension absent → skip the whole cube for this day
	if _, ok := spec.prunedToColumns(map[string]bool{"bytes": true, "latency": true}); ok {
		t.Fatal("dim absent: expected ok=false (skip cube-day)")
	}

	// one agg column absent → that agg dropped, count(*) + present agg kept
	got, ok := spec.prunedToColumns(map[string]bool{"site": true, "bytes": true})
	if !ok || len(got.Aggs) != 2 {
		t.Fatalf("partial: ok=%v aggs=%d, want ok=true aggs=2", ok, len(got.Aggs))
	}
	for _, a := range got.Aggs {
		if a.Kind == AggMax {
			t.Fatal("partial: AggMax over absent 'latency' should have been dropped")
		}
	}

	// per-dim cube carrying only count(*): dim present → builds; dim absent → skip
	perDim := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"image_files_count"}, Aggs: []Aggregate{{Kind: AggCount}}}
	if _, ok := perDim.prunedToColumns(map[string]bool{"image_files_count": true}); !ok {
		t.Fatal("per-dim present: expected ok=true")
	}
	if _, ok := perDim.prunedToColumns(map[string]bool{"other": true}); ok {
		t.Fatal("per-dim absent: expected ok=false")
	}
}
