package rollup

import (
	"fmt"
	"strconv"
	"strings"
)

// grainSeconds maps a calendar grain to a comparable magnitude for divisibility.
// Calendar grains aren't fixed seconds, but their *nesting* is total
// (hour ⊂ day ⊂ week ⊂ month), which is all coverage needs: a cube at a finer
// grain can always be re-bucketed up to a coarser query grain via date_trunc,
// and date_trunc('Q', date_trunc('G', t)) == date_trunc('Q', t) when G ⊆ Q.
var grainRank = map[string]int{
	"":       0, // total — coarsest; any cube grain rolls up into it
	"month":  1,
	"week":   2,
	"day":    3,
	"hour":   4,
	"minute": 5,
}

// grainSeconds returns the fixed width of a grain in seconds. Named calendar
// grains map to their nominal width; "secs:N" (Grafana epoch buckets) to N;
// "" (total) to 0; "month" has no fixed width (ok=false → use the nesting rank).
func grainSeconds(g string) (int, bool) {
	switch g {
	case "":
		return 0, true
	case "minute":
		return 60, true
	case "hour":
		return 3600, true
	case "day":
		return 86400, true
	case "week":
		return 604800, true
	}
	if strings.HasPrefix(g, "secs:") {
		if n, err := strconv.Atoi(g[len("secs:"):]); err == nil && n > 0 {
			return n, true
		}
	}
	return 0, false
}

// grainDivides reports whether a cube at cubeGrain can serve a query at
// queryGrain: the cube's buckets must tile evenly into the query's buckets.
func grainDivides(cubeGrain, queryGrain string) bool {
	if queryGrain == "" {
		// A grand total rolls up any finite cube grain (drop the time bucket).
		return true
	}
	cs, cok := grainSeconds(cubeGrain)
	qs, qok := grainSeconds(queryGrain)
	if cok && qok && cs > 0 && qs > 0 {
		return qs%cs == 0
	}
	// Calendar grains without a fixed width (e.g. month) fall back to nesting rank.
	cg, okc := grainRank[cubeGrain]
	qg, okq := grainRank[queryGrain]
	return okc && okq && cg >= qg
}

// requiredDims is the set of columns the cube must physically store to serve a
// shape: its group-by dimensions plus any post-aggregation filter columns.
func (q QueryShape) requiredDims() []string {
	seen := map[string]bool{}
	var out []string
	add := func(c string) {
		if c != "" && !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}
	for _, d := range q.Dims {
		add(d)
	}
	for _, f := range q.Filters {
		add(f.Col)
	}
	// A conditional aggregate's CASE predicate must be evaluable on cube rows, so
	// every column it references has to be a stored dimension too.
	for _, a := range q.Aggs {
		for _, c := range a.CondCols {
			add(c)
		}
	}
	return out
}

// storeColSet returns the physical store columns a cube provides.
func (s CubeSpec) storeColSet() map[string]bool {
	m := map[string]bool{}
	for _, sc := range s.orderedStoreCols() {
		m[sc[0]] = true
	}
	return m
}

// aggDerivable reports whether a query aggregate can be computed from the cube's
// store columns. This is where the v2 regression is fixed: HLL/KLL sketch aggs
// ARE derivable when the cube stores the matching sketch column.
func (s CubeSpec) aggDerivable(a Aggregate) bool {
	have := s.storeColSet()
	for _, sc := range a.storeCols() {
		if !have[sc[0]] {
			return false
		}
	}
	return true
}

// Covers reports whether cube s can serve query q, and if not, a reason code.
// A cube covers a query iff: same source; cube grain nests inside query grain;
// every required dim is stored; every aggregate is derivable.
func (s CubeSpec) Covers(q QueryShape) (bool, string) {
	if s.Source != q.Source {
		return false, "source_mismatch"
	}
	if !grainDivides(s.Grain, q.Grain) {
		return false, fmt.Sprintf("grain_mismatch(cube=%q query=%q)", s.Grain, q.Grain)
	}
	have := s.dimSet()
	for _, d := range q.requiredDims() {
		if !have[d] {
			return false, fmt.Sprintf("dim_not_stored(%s)", d)
		}
	}
	for _, a := range q.Aggs {
		if !s.aggDerivable(a) {
			return false, fmt.Sprintf("agg_not_derivable(%s)", a.Alias)
		}
	}
	return true, ""
}

// coversExceptGrain reports whether s would cover q if grain were ignored — i.e.
// the dims and aggregates all match. Used to distinguish a "your time bucket is
// finer than the cube" miss (actionable: widen the range) from a genuine
// dimension/aggregate gap.
func (s CubeSpec) coversExceptGrain(q QueryShape) bool {
	if s.Source != q.Source {
		return false
	}
	have := s.dimSet()
	for _, d := range q.requiredDims() {
		if !have[d] {
			return false
		}
	}
	for _, a := range q.Aggs {
		if !s.aggDerivable(a) {
			return false
		}
	}
	return true
}

func (s CubeSpec) dimSet() map[string]bool {
	m := make(map[string]bool, len(s.Dims))
	for _, d := range s.Dims {
		m[d] = true
	}
	return m
}

// PickNarrowest selects the covering cube with the fewest dimensions (cheapest to
// scan/re-aggregate). Returns nil when none cover.
func PickNarrowest(cubes []CubeSpec, q QueryShape) *CubeSpec {
	var best *CubeSpec
	for i := range cubes {
		if ok, _ := cubes[i].Covers(q); !ok {
			continue
		}
		if best == nil || len(cubes[i].Dims) < len(best.Dims) {
			best = &cubes[i]
		}
	}
	return best
}
