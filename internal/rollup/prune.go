package rollup

// prunedToColumns adapts a cube to the columns physically present in one day's
// source. Source schemas drift over time — sparse event properties (posthog,
// crashlytics, …) appear and disappear — so a column the classifier saw in a
// recent-sample profile can be entirely absent from an older day's Parquet. Even
// with read_parquet(union_by_name=true), a column present in NONE of a day's files
// is not in _rollup_day, and the build COPY referencing it fails with a DuckDB
// Binder Error ("Referenced column … not found"), which used to drop the whole
// cube-day.
//
// So per day: aggregates whose input column is missing are dropped (merge-on-read
// union_by_name NULL-fills the gap across days). A missing DIMENSION column can't
// be dropped without changing the cube's identity, so ok is false — the caller
// skips that cube for that day. That's correct: a dimension that did not exist that
// day has nothing to group by, and the cube simply has no rows for it then.
func (s CubeSpec) prunedToColumns(present map[string]bool) (CubeSpec, bool) {
	for _, d := range s.Dims {
		if !present[d] {
			return CubeSpec{}, false
		}
	}
	out := s
	out.Aggs = make([]Aggregate, 0, len(s.Aggs))
	for _, a := range s.Aggs {
		if a.Kind != AggCount && a.Col != "" && !present[a.Col] {
			continue
		}
		if a.ThenCol != "" && !present[a.ThenCol] {
			continue
		}
		out.Aggs = append(out.Aggs, a)
	}
	return out, true
}
