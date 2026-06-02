package rollup

import "testing"

// Pure (no-DB) tests for the coverage matcher and SQL generators. These run in
// milliseconds and gate the integration suite.

func shape(grain string, dims []string, aggs []Aggregate, fs []Filter) QueryShape {
	return QueryShape{Source: "default.downloads", TimeCol: "time", Grain: grain,
		Dims: dims, Aggs: aggs, Filters: fs, TimeLo: "2025-12-28 00:00:00+00", TimeHi: "2025-12-29 00:00:00+00"}
}

func TestCoverage_DimSubset(t *testing.T) {
	cube := CubeSpec{Source: "default.downloads", Grain: "hour",
		Dims: []string{"status", "region"}, Aggs: []Aggregate{{Kind: AggCount}}}
	cases := []struct {
		name string
		q    QueryShape
		want bool
	}{
		{"subset dims", shape("hour", []string{"status"}, []Aggregate{{Kind: AggCount}}, nil), true},
		{"all dims", shape("hour", []string{"status", "region"}, []Aggregate{{Kind: AggCount}}, nil), true},
		{"filter col stored", shape("hour", nil, []Aggregate{{Kind: AggCount}}, []Filter{{Col: "region", Op: OpEq, Values: []string{"x"}}}), true},
		{"dim not stored", shape("hour", []string{"city"}, []Aggregate{{Kind: AggCount}}, nil), false},
		{"filter col not stored", shape("hour", nil, []Aggregate{{Kind: AggCount}}, []Filter{{Col: "city", Op: OpEq, Values: []string{"x"}}}), false},
		{"coarser grain ok", shape("day", []string{"status"}, []Aggregate{{Kind: AggCount}}, nil), true},
		{"finer grain rejected", shape("minute", []string{"status"}, []Aggregate{{Kind: AggCount}}, nil), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, reason := cube.Covers(c.q)
			if got != c.want {
				t.Fatalf("Covers=%v want %v (reason=%q)", got, c.want, reason)
			}
		})
	}
}

func TestCoverage_AggDerivable(t *testing.T) {
	// avg needs sum+count; a cube storing only _cnt can't serve avg.
	cntOnly := CubeSpec{Source: "default.downloads", Grain: "hour", Aggs: []Aggregate{{Kind: AggCount}}}
	if ok, _ := cntOnly.Covers(shape("hour", nil, []Aggregate{{Kind: AggAvg, Col: "d"}}, nil)); ok {
		t.Fatal("count-only cube must not serve AVG")
	}
	// A cube with the matching HLL sketch CAN serve COUNT(DISTINCT) — the v2 regression fix.
	withTheta := CubeSpec{Source: "default.downloads", Grain: "hour",
		Aggs: []Aggregate{{Kind: AggCountDistinct, Col: "device_id"}}}
	if ok, r := withTheta.Covers(shape("hour", nil, []Aggregate{{Kind: AggCountDistinct, Col: "device_id"}}, nil)); !ok {
		t.Fatalf("HLL cube must serve COUNT(DISTINCT): %s", r)
	}
}

func TestCoverage_PickNarrowest(t *testing.T) {
	coarse := CubeSpec{Source: "default.downloads", Grain: "hour", Aggs: []Aggregate{{Kind: AggCount}}}
	rich := CubeSpec{Source: "default.downloads", Grain: "hour",
		Dims: []string{"status", "region", "tag"}, Aggs: []Aggregate{{Kind: AggCount}}}
	cubes := []CubeSpec{rich, coarse}
	// A dimensionless query must pick the coarse cube (fewest dims).
	got := PickNarrowest(cubes, shape("hour", nil, []Aggregate{{Kind: AggCount}}, nil))
	if got == nil || len(got.Dims) != 0 {
		t.Fatalf("expected coarse cube, got %+v", got)
	}
	// A query on status must pick rich (coarse doesn't store status).
	got = PickNarrowest(cubes, shape("hour", []string{"status"}, []Aggregate{{Kind: AggCount}}, nil))
	if got == nil || len(got.Dims) != 3 {
		t.Fatalf("expected rich cube, got %+v", got)
	}
}

func TestStoreCols_Dedup(t *testing.T) {
	// avg(d) + count(d) share _cnt_d; avg also adds _sum_d. Expect 2 store cols.
	s := CubeSpec{Aggs: []Aggregate{{Kind: AggAvg, Col: "d"}, {Kind: AggCountCol, Col: "d"}}}
	if got := len(s.orderedStoreCols()); got != 2 {
		t.Fatalf("expected 2 deduped store cols, got %d: %v", got, s.orderedStoreCols())
	}
}

func TestAlign(t *testing.T) {
	lo, _ := parseTS("2025-12-28 00:30:00+00")
	if up := alignUp(lo, "hour"); fmtTS(up) != "2025-12-28 01:00:00+00" {
		t.Fatalf("alignUp hour = %s", fmtTS(up))
	}
	on, _ := parseTS("2025-12-28 01:00:00+00")
	if up := alignUp(on, "hour"); !up.Equal(on) {
		t.Fatalf("alignUp of aligned must be identity, got %s", fmtTS(up))
	}
	w, _ := parseTS("2025-12-28 18:45:00+00")
	if dn := alignDown(w, "hour"); fmtTS(dn) != "2025-12-28 18:00:00+00" {
		t.Fatalf("alignDown hour = %s", fmtTS(dn))
	}
}
