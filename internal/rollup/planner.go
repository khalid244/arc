package rollup

import "sort"

// CardinalityFunc reports the approximate distinct cardinality of a dimension on
// a source. In production this is measured from the actual cube build (or a cheap
// probe), never pre-sampled from raw data — which is what made prior auto-
// classifiers drift. In tests it is injected.
type CardinalityFunc func(source, dim string) int

// PlanConfig is the entire (optional) configuration surface of Rollup.
type PlanConfig struct {
	// MaxDimCard: a dimension at or below this cardinality is eligible for the
	// shared dim-rich cube; above it, a queried dim gets its own narrow cube so
	// the shared cube never explodes. Default 1024.
	MaxDimCard int
	// MinCount: minimum observed frequency for a shape to earn materialization.
	MinCount int
}

func (c PlanConfig) withDefaults() PlanConfig {
	if c.MaxDimCard == 0 {
		c.MaxDimCard = 1024
	}
	if c.MinCount == 0 {
		c.MinCount = 1
	}
	return c
}

// Plan turns an observed workload into the minimal set of cubes that covers it.
//
// Strategy:
//   - finest queried grain becomes the cube grain (coarser queries roll up);
//   - all low-cardinality required dims fold into one shared dim-rich cube
//     carrying the union of all aggregates;
//   - a shape requiring a high-cardinality dim gets a dedicated narrow cube
//     (its required dims + its aggregates) so the shared cube stays small;
//   - a shape using a percentile (KLL — lossy under many-cell merges) gets a
//     dedicated cube at its own dimensionality for accuracy, unless the dim-rich
//     cube already matches that dimensionality.
//
// Cubes are only emitted for shapes that are actually queried, so storage is
// never paid for unused materialization.
func Plan(w *Workload, card CardinalityFunc, cfg PlanConfig) []CubeSpec {
	cfg = cfg.withDefaults()
	hot := w.Hot(cfg.MinCount)

	// Group by source.
	bySource := map[string][]QueryShape{}
	var sources []string
	for _, e := range hot {
		s := e.Shape.Source
		if _, ok := bySource[s]; !ok {
			sources = append(sources, s)
		}
		bySource[s] = append(bySource[s], e.Shape)
	}
	sort.Strings(sources)

	var cubes []CubeSpec
	seen := map[string]bool{}
	emit := func(c CubeSpec) {
		k := cubeKeyOf(c)
		if !seen[k] {
			seen[k] = true
			cubes = append(cubes, c)
		}
	}

	for _, src := range sources {
		shapes := bySource[src]
		grain := finestGrain(shapes)

		// Classify dims and collect the aggregate union.
		lowSet := map[string]bool{}
		var allAggs []Aggregate
		aggSeen := map[string]bool{}
		for _, q := range shapes {
			for _, d := range q.requiredDims() {
				if card(src, d) <= cfg.MaxDimCard {
					lowSet[d] = true
				}
			}
			for _, a := range q.Aggs {
				if k := aggKey(a); !aggSeen[k] {
					aggSeen[k] = true
					allAggs = append(allAggs, a)
				}
			}
		}
		lowDims := sortedKeys(lowSet)

		// Shared dim-rich cube over all low-card dims.
		emit(CubeSpec{Source: src, Grain: grain, Dims: lowDims, Aggs: allAggs})

		// Per-shape specialized cubes.
		for _, q := range shapes {
			req := q.requiredDims()
			hasHigh := false
			for _, d := range req {
				if card(src, d) > cfg.MaxDimCard {
					hasHigh = true
					break
				}
			}
			if hasHigh {
				emit(CubeSpec{Source: src, Grain: grain, Dims: sortedCopy(req), Aggs: q.Aggs})
				continue
			}
			if shapeHasPercentile(q) && len(req) < len(lowDims) {
				emit(CubeSpec{Source: src, Grain: grain, Dims: sortedCopy(req), Aggs: q.Aggs})
			}
		}
	}
	return cubes
}

func shapeHasPercentile(q QueryShape) bool {
	for _, a := range q.Aggs {
		if a.Kind == AggPercentile {
			return true
		}
	}
	return false
}

// finestGrain returns the finest non-total grain in the workload (the grain the
// cube is built at); defaults to "hour" when every query is a grand total.
func finestGrain(shapes []QueryShape) string {
	best := "hour"
	bestRank := grainRank["hour"]
	for _, q := range shapes {
		if q.Grain == "" {
			continue
		}
		if r := grainRank[q.Grain]; r > bestRank {
			best, bestRank = q.Grain, r
		}
	}
	return best
}

func cubeKeyOf(c CubeSpec) string {
	return c.Source + "|" + c.Grain + "|" + join(c.Dims) + "|" + aggsKey(c.Aggs)
}

func aggsKey(aggs []Aggregate) string {
	ks := make([]string, len(aggs))
	for i, a := range aggs {
		ks[i] = aggKey(a)
	}
	sort.Strings(ks)
	return join(ks)
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedCopy(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

func join(s []string) string {
	out := ""
	for i, x := range s {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out
}
