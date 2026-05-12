package rollup

import (
	"time"
)

// QueryShape is the minimum info the picker needs about a parsed query.
type QueryShape struct {
	BucketGrain      time.Duration // smallest time-bucket grouping; 0 if none
	NeededDims       []string      // dim columns referenced in SELECT/WHERE/GROUP BY
	NeededAggregates []NeededAgg
}

// NeededAgg describes one aggregate the query needs answered.
type NeededAgg struct {
	Op     string // "COUNT", "SUM", "MIN", "MAX", "AVG", "COUNT_DISTINCT", "PERCENTILE_CONT"
	Column string
}

func (q QueryShape) usesSketch() bool {
	for _, a := range q.NeededAggregates {
		if a.Op == "COUNT_DISTINCT" || a.Op == "PERCENTILE_CONT" {
			return true
		}
	}
	return false
}

// PickBestVariant returns the deterministic best-fit spec, or nil if none fit.
// Priority list documented in docs/superpowers/specs/2026-05-12-rollup-rework-design.md
// under "Guard 3: variant pick":
//  1. Query bucket ≥ 1d, no sketches → __1d (dim-rich)
//  2. Query bucket ≥ 1d, sketch + exactly 1 kept dim → __by_<dim>__1d
//  3. Query bucket ≥ 1d, sketch + no kept dim → __sketch_1d
//  4. Query bucket ≥ 1h → __1h
//  5. Else → nil (no variant fits)
//
// Every priority calls specCovers (dims AND aggregates) — a variant that
// covers the dims but lacks the required pre-agg column (e.g. picking
// __1d for a query that wants SUM(amount) when the variant only carries
// __row_count) must NOT be picked, otherwise EmitMergeOnRead errors at
// translation time and the rewriter returns refusal anyway — but with
// wasted work.
func PickBestVariant(specs []RollupSpec, q QueryShape) *RollupSpec {
	day := 24 * time.Hour
	hour := time.Hour

	if q.BucketGrain >= day {
		if !q.usesSketch() {
			if s := findCoveringSpec(specs, day, q); s != nil {
				return s
			}
		} else if len(q.NeededDims) == 1 {
			if s := findPerDimSpec(specs, q.NeededDims[0], q); s != nil {
				return s
			}
		} else if len(q.NeededDims) == 0 {
			if s := findSpecBySuffix(specs, "__sketch_1d", q); s != nil {
				return s
			}
		}
		// Sketch + multi-dim, or per-dim/sketch_1d miss: fall through to dim-rich 1d
		if s := findCoveringSpec(specs, day, q); s != nil {
			return s
		}
	}
	if q.BucketGrain >= hour && q.BucketGrain < day {
		if s := findCoveringSpec(specs, hour, q); s != nil {
			return s
		}
	}
	return nil
}

// findCoveringSpec returns the first spec at the given bucket interval that
// fully covers the query (dims and aggregates).
func findCoveringSpec(specs []RollupSpec, interval time.Duration, q QueryShape) *RollupSpec {
	for i := range specs {
		s := &specs[i]
		if s.BucketInterval != interval {
			continue
		}
		if !specCovers(s, q) {
			continue
		}
		return s
	}
	return nil
}

// findPerDimSpec returns the per-dim variant for `dim` if it covers the
// query. Matches the spec by name (case-sensitive — names are computed
// from the actual column name in inference, so picker callers must pass
// the column name as the parser observed it).
func findPerDimSpec(specs []RollupSpec, dim string, q QueryShape) *RollupSpec {
	suffix := "__by_" + dim + "__1d"
	return findSpecBySuffix(specs, suffix, q)
}

func findSpecBySuffix(specs []RollupSpec, suffix string, q QueryShape) *RollupSpec {
	for i := range specs {
		if len(specs[i].Name) >= len(suffix) && specs[i].Name[len(specs[i].Name)-len(suffix):] == suffix {
			if specCovers(&specs[i], q) {
				return &specs[i]
			}
		}
	}
	return nil
}

func dimsCovered(keep, needed []string) bool {
	have := map[string]bool{}
	for _, k := range keep {
		have[k] = true
	}
	for _, n := range needed {
		if !have[n] {
			return false
		}
	}
	return true
}

func hasSketchAgg(s *RollupSpec) bool {
	for _, a := range s.Aggregations {
		for _, f := range a.Functions {
			if f == AggHLL || f == AggTDigest {
				return true
			}
		}
	}
	return false
}

func specCovers(s *RollupSpec, q QueryShape) bool {
	if !dimsCovered(s.KeepDimensions, q.NeededDims) {
		return false
	}
	for _, a := range q.NeededAggregates {
		if !aggSatisfied(s, a) {
			return false
		}
	}
	return true
}

// aggSatisfied checks whether spec s can answer the NeededAgg a.
func aggSatisfied(s *RollupSpec, a NeededAgg) bool {
	for _, ag := range s.Aggregations {
		if ag.SourceColumn != a.Column {
			continue
		}
		for _, f := range ag.Functions {
			switch {
			case a.Op == "COUNT_DISTINCT" && f == AggHLL:
				return true
			case a.Op == "PERCENTILE_CONT" && f == AggTDigest:
				return true
			case a.Op == "SUM" && f == AggSum:
				return true
			case a.Op == "MIN" && f == AggMin:
				return true
			case a.Op == "MAX" && f == AggMax:
				return true
			case a.Op == "COUNT" && (f == AggCount || f == AggSum):
				return true
			case a.Op == "AVG" && f == AggSum:
				return true
			}
		}
	}
	if a.Op == "COUNT" {
		return true // satisfied by __row_count
	}
	return false
}
