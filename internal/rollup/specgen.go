package rollup

import (
	"time"
)

// Fixed sketch precision. Industry defaults (Apache DataSketches HLL,
// t-digest library). Not user-tunable; escape hatch deferred to future iteration.
// See docs/superpowers/specs/2026-05-12-rollup-rework-design.md, "Fixed
// sketch precision" subsection.
const (
	defaultHLLLgK   = 12  // ~1.6% error, ~2.5 KB in-memory
	defaultTDigestK = 100 // matches t-digest library default
)

// defaultSketchConfig returns a pointer to the default SketchConfig.
// Allocated per-call so each Aggregation owns its own pointer (avoiding
// shared-state aliasing if the config is later mutated downstream).
func defaultSketchConfig() *SketchConfig {
	return &SketchConfig{HLLLgK: defaultHLLLgK, TDigestK: defaultTDigestK}
}

func attachSketchConfig(fns []AggFunction) *SketchConfig {
	for _, f := range fns {
		if f == AggHLL || f == AggTDigest {
			return defaultSketchConfig()
		}
	}
	return nil
}

// GenerateSpecs builds the deterministic list of RollupSpec for a table given
// its inferred schema. Always emits three kinds of daily variants:
//
//   - dim-rich __1d:     all low-card dims as kept; metrics with counters +
//     t-digest; HLL sketches on identity columns
//   - no-dim __sketch_1d: global HLL/t-digest only (one row per day)
//   - per-dim __by_<dim>__1d: one variant per RoleDim column (low-card +
//     user force-kept high-card via keep_columns)
//
// No hourly tier. No mode knob. The fixed shape costs ~6% of source on the
// validation dataset.
func GenerateSpecs(db, table string, schema InferredSchema) []RollupSpec {
	// lowCardDims feeds the dim-rich cross-product variant; allDims feeds
	// per-dim variants. HighCard dims (force-kept via keep_columns) go to
	// allDims only — adding them to dim-rich would explode the row count.
	var lowCardDims, allDims []string
	var metrics []ClassifiedColumn
	var sketches []ClassifiedColumn
	for _, c := range schema.Columns {
		switch c.Role {
		case RoleDim:
			if !c.HighCard {
				lowCardDims = append(lowCardDims, c.Name)
			}
			allDims = append(allDims, c.Name)
		case RoleMetric:
			metrics = append(metrics, c)
		case RoleSketch:
			sketches = append(sketches, c)
		}
	}

	var specs []RollupSpec

	// Dim-rich daily.
	dimDaily := buildDimRichSpec(db, table, schema.TimeColumn, lowCardDims, metrics, sketches, 24*time.Hour, "1d")
	specs = append(specs, dimDaily)

	// No-dim sketch daily (HLL-only; t-digest is excluded due to a DuckDB
	// SIGSEGV — see buildSketchSpec). Present iff at least one HLL-sketched
	// column exists; pure t-digest tables don't need this variant since
	// percentile queries without GROUP BY fall back to source anyway.
	if len(sketches) > 0 {
		specs = append(specs, buildSketchSpec(db, table, schema.TimeColumn, metrics, sketches, 24*time.Hour, "sketch_1d"))
	}

	// Per-dim sketch daily. Iterates allDims (includes high-card columns) so
	// users get a single-dim variant for force-kept columns without exploding
	// the dim-rich variant.
	if len(sketches) > 0 || hasTDigest(metrics) {
		for _, d := range allDims {
			specs = append(specs, buildPerDimSketchSpec(db, table, schema.TimeColumn, d, metrics, sketches, 24*time.Hour))
		}
	}

	return specs
}

func buildDimRichSpec(db, table, timeCol string, dims []string, metrics, sketches []ClassifiedColumn, interval time.Duration, suffix string) RollupSpec {
	s := RollupSpec{
		Name:           db + "__" + table + "__" + suffix,
		Database:       db,
		SourceTable:    table,
		KeyTable:       table, // dim-rich: <table>__<interval>
		BucketColumn:   timeCol,
		BucketInterval: interval,
		KeepDimensions: append([]string{}, dims...),
	}
	for _, m := range metrics {
		fns := metricFunctions(m)
		s.Aggregations = append(s.Aggregations, Aggregation{
			SourceColumn: m.Name,
			Functions:    fns,
			SketchConfig: attachSketchConfig(fns),
		})
	}
	for _, k := range sketches {
		fns := []AggFunction{AggHLL}
		s.Aggregations = append(s.Aggregations, Aggregation{
			SourceColumn: k.Name,
			Functions:    fns,
			SketchConfig: attachSketchConfig(fns),
		})
	}
	return s
}

func buildSketchSpec(db, table, timeCol string, metrics, sketches []ClassifiedColumn, interval time.Duration, suffix string) RollupSpec {
	s := RollupSpec{
		Name:           db + "__" + table + "__" + suffix,
		Database:       db,
		SourceTable:    table,
		KeyTable:       table + "_sketch", // distinct dir vs dim-rich: <table>_sketch__<interval>
		BucketColumn:   timeCol,
		BucketInterval: interval,
		KeepDimensions: nil, // no dims
	}
	// NOTE: deliberately NOT emitting t-digest here. The no-dim variant has
	// only one row per bucket (e.g. 14 rows for a 14-day window) and DuckDB's
	// datasketches t-digest aggregator triggers a SIGSEGV when merging that
	// few sketches in a no-GROUP-BY position (cgo NULL-deref, upstream bug).
	// Percentile queries with no dim grouping fall back to source — fine, the
	// dim-rich and per-dim variants serve the dim-grouped percentile shape.
	_ = metrics
	for _, k := range sketches {
		fns := []AggFunction{AggHLL}
		s.Aggregations = append(s.Aggregations, Aggregation{
			SourceColumn: k.Name,
			Functions:    fns,
			SketchConfig: attachSketchConfig(fns),
		})
	}
	return s
}

func buildPerDimSketchSpec(db, table, timeCol, dim string, metrics, sketches []ClassifiedColumn, interval time.Duration) RollupSpec {
	s := RollupSpec{
		Name:           db + "__" + table + "__by_" + dim + "__1d",
		Database:       db,
		SourceTable:    table,
		KeyTable:       table + "_by_" + dim, // distinct dir per dim: <table>_by_<dim>__<interval>
		BucketColumn:   timeCol,
		BucketInterval: interval,
		KeepDimensions: []string{dim},
	}
	for _, m := range metrics {
		if m.TDigest {
			fns := []AggFunction{AggTDigest}
			s.Aggregations = append(s.Aggregations, Aggregation{
				SourceColumn: m.Name,
				Functions:    fns,
				SketchConfig: attachSketchConfig(fns),
			})
		}
	}
	for _, k := range sketches {
		fns := []AggFunction{AggHLL}
		s.Aggregations = append(s.Aggregations, Aggregation{
			SourceColumn: k.Name,
			Functions:    fns,
			SketchConfig: attachSketchConfig(fns),
		})
	}
	return s
}

func metricFunctions(c ClassifiedColumn) []AggFunction {
	out := []AggFunction{AggCount, AggSum, AggMin, AggMax}
	if c.TDigest {
		out = append(out, AggTDigest)
	}
	return out
}

func hasTDigest(metrics []ClassifiedColumn) bool {
	for _, m := range metrics {
		if m.TDigest {
			return true
		}
	}
	return false
}
