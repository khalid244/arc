package rollup

import (
	"strings"
	"testing"
	"time"
)

// parseSelect lives in guards_test.go; reuse it here.

func dummyGlob(string, string) string { return "/data/default/x/**/*.parquet" }

// testBoundary is "now truncated to day" for emitter tests: a fixed instant
// the assertions can reference deterministically. Picked to be late enough
// that all test fixtures' tr.Hi values land before it (rollup CTE owns the
// whole range; fresh CTE collapses to empty) so the emitted SQL is stable.
var testBoundary = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

func TestEmitMergeOnRead_DailyCountDistinct(t *testing.T) {
	sql := `SELECT country, COUNT(DISTINCT device_id) FROM downloads
            WHERE time >= TIMESTAMP '2025-05-12' AND time < TIMESTAMP '2026-05-12'
            GROUP BY country`
	sel := parseSelect(t, sql)
	spec := &RollupSpec{
		Name:           "default__downloads__by_country__1d",
		Database:       "default",
		SourceTable:    "downloads",
		KeyTable:       "downloads_by_country",
		BucketColumn:   "time",
		BucketInterval: 24 * time.Hour,
		KeepDimensions: []string{"country"},
		Aggregations: []Aggregation{
			{SourceColumn: "device_id", Functions: []AggFunction{AggHLL}, SketchConfig: &SketchConfig{HLLLgK: 12}},
		},
	}
	tr := TimeRange{Lo: time.Date(2025, 5, 12, 0, 0, 0, 0, time.UTC), Hi: time.Date(2026, 5, 12, 0, 0, 0, 0, time.UTC)}
	out, err := EmitMergeOnRead(sel, spec, tr, testBoundary, dummyGlob, nil)
	if err != nil {
		t.Fatalf("emit error: %v", err)
	}
	for _, expect := range []string{
		"WITH rollup AS",
		"FROM read_parquet",
		"by_country__1d",
		"fresh AS",
		"FROM downloads",
		"UNION ALL",
		"datasketch_hll_estimate(datasketch_hll_union(12, device_id__hll::sketch_hll))",
		// Boundary is now a precomputed timestamp literal (Go-side truncation),
		// not a date_trunc(NOW()) call — see emit.go for rationale.
		"TIMESTAMP '",
	} {
		if !strings.Contains(out, expect) {
			t.Errorf("missing %q in:\n%s", expect, out)
		}
	}
}

// Bug 1: the rollup CTE must read the parquet's actual bucket column,
// `bucket`, and alias it to the variant's bucket column name so UNION ALL
// columns line up with the fresh branch.
func TestEmitMergeOnRead_RollupCTEUsesBucketColumn(t *testing.T) {
	sql := `SELECT country, SUM(latency_ms) FROM downloads
            WHERE time >= TIMESTAMP '2026-05-01' AND time < TIMESTAMP '2026-05-08'
            GROUP BY country`
	sel := parseSelect(t, sql)
	spec := &RollupSpec{
		Name: "v", Database: "default", SourceTable: "downloads",
		BucketColumn: "time", BucketInterval: 24 * time.Hour,
		KeepDimensions: []string{"country"},
		Aggregations: []Aggregation{
			{SourceColumn: "latency_ms", Functions: []AggFunction{AggSum, AggCount}},
		},
	}
	tr := TimeRange{Lo: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), Hi: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)}
	out, err := EmitMergeOnRead(sel, spec, tr, testBoundary, dummyGlob, nil)
	if err != nil {
		t.Fatalf("emit error: %v", err)
	}
	// The rollup CTE must alias bucket to the variant's bucket column.
	if !strings.Contains(out, "bucket AS time") {
		t.Errorf("expected `bucket AS time` in rollup CTE projection:\n%s", out)
	}
	// And the WHERE on the rollup CTE must filter on `bucket`, not `time`.
	if !strings.Contains(out, "WHERE bucket >=") {
		t.Errorf("expected rollup CTE to filter on `bucket`:\n%s", out)
	}
}

// Bug 2: HLL/tdigest SQL must use the right function names with casts and
// K parameters — NOT the non-existent `datasketch_tdigest_merge`.
func TestEmitMergeOnRead_SketchMergeSQL(t *testing.T) {
	sql := `SELECT country, COUNT(DISTINCT device_id), percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)
            FROM downloads
            WHERE time >= TIMESTAMP '2026-05-01' AND time < TIMESTAMP '2026-05-08'
            GROUP BY country`
	sel := parseSelect(t, sql)
	spec := &RollupSpec{
		Name: "v", Database: "default", SourceTable: "downloads",
		BucketColumn: "time", BucketInterval: 24 * time.Hour,
		KeepDimensions: []string{"country"},
		Aggregations: []Aggregation{
			{SourceColumn: "device_id", Functions: []AggFunction{AggHLL}, SketchConfig: &SketchConfig{HLLLgK: 12}},
			{SourceColumn: "latency_ms", Functions: []AggFunction{AggTDigest}, SketchConfig: &SketchConfig{TDigestK: 100}},
		},
	}
	tr := TimeRange{Lo: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), Hi: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)}
	out, err := EmitMergeOnRead(sel, spec, tr, testBoundary, dummyGlob, nil)
	if err != nil {
		t.Fatalf("emit error: %v", err)
	}
	// HLL: cast + K argument required.
	if !strings.Contains(out, "device_id__hll::sketch_hll") {
		t.Errorf("expected `device_id__hll::sketch_hll` cast:\n%s", out)
	}
	if !strings.Contains(out, "datasketch_hll_union(12,") {
		t.Errorf("expected HLL union with K=12 argument:\n%s", out)
	}
	// t-digest: the right function is `datasketch_tdigest`, not `_merge`.
	if strings.Contains(out, "datasketch_tdigest_merge") {
		t.Errorf("emitter used non-existent datasketch_tdigest_merge:\n%s", out)
	}
	if !strings.Contains(out, "datasketch_tdigest(100, latency_ms__tdigest::sketch_tdigest_double)") {
		t.Errorf("expected `datasketch_tdigest(100, latency_ms__tdigest::sketch_tdigest_double)`:\n%s", out)
	}
	if !strings.Contains(out, "datasketch_tdigest_quantile(") {
		t.Errorf("expected `datasketch_tdigest_quantile`:\n%s", out)
	}
}

// Bug 3: the rollup branch must cast its BLOB sketch columns to sketch_hll/
// sketch_tdigest_double so the UNION ALL with the fresh branch (which
// produces sketch_hll/sketch_tdigest_double via datasketch_hll/tdigest) is
// type-safe.
func TestEmitMergeOnRead_SketchTypesAlignAcrossUnion(t *testing.T) {
	sql := `SELECT country, COUNT(DISTINCT device_id) FROM downloads
            WHERE time >= TIMESTAMP '2026-05-01' AND time < TIMESTAMP '2026-05-08'
            GROUP BY country`
	sel := parseSelect(t, sql)
	spec := &RollupSpec{
		Name: "v", Database: "default", SourceTable: "downloads",
		BucketColumn: "time", BucketInterval: 24 * time.Hour,
		KeepDimensions: []string{"country"},
		Aggregations: []Aggregation{
			{SourceColumn: "device_id", Functions: []AggFunction{AggHLL}, SketchConfig: &SketchConfig{HLLLgK: 12}},
		},
	}
	tr := TimeRange{Lo: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), Hi: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)}
	out, err := EmitMergeOnRead(sel, spec, tr, testBoundary, dummyGlob, nil)
	if err != nil {
		t.Fatalf("emit error: %v", err)
	}
	// The rollup branch must cast device_id__hll to sketch_hll.
	if !strings.Contains(out, "device_id__hll::sketch_hll AS device_id__hll") {
		t.Errorf("rollup CTE missing sketch_hll cast:\n%s", out)
	}
	// The fresh branch produces sketch_hll via datasketch_hll(K, col).
	if !strings.Contains(out, "datasketch_hll(12, device_id) AS device_id__hll") {
		t.Errorf("fresh CTE missing datasketch_hll(K, col) projection:\n%s", out)
	}
}

// Bug 4: outer GROUP BY must mirror the user's actual GROUP BY clause —
// not the variant's KeepDimensions, which may carry more dims.
func TestEmitMergeOnRead_OuterGroupByMatchesUserClause(t *testing.T) {
	// User groups only by country; variant keeps (country, region).
	sql := `SELECT country, SUM(latency_ms) FROM downloads
            WHERE time >= TIMESTAMP '2026-05-01' AND time < TIMESTAMP '2026-05-08'
            GROUP BY country`
	sel := parseSelect(t, sql)
	spec := &RollupSpec{
		Name: "v", Database: "default", SourceTable: "downloads",
		BucketColumn: "time", BucketInterval: 24 * time.Hour,
		KeepDimensions: []string{"country", "region"},
		Aggregations: []Aggregation{
			{SourceColumn: "latency_ms", Functions: []AggFunction{AggSum}},
		},
	}
	tr := TimeRange{Lo: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), Hi: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)}
	out, err := EmitMergeOnRead(sel, spec, tr, testBoundary, dummyGlob, nil)
	if err != nil {
		t.Fatalf("emit error: %v", err)
	}
	// Locate the outer GROUP BY. The fresh CTE's GROUP BY uses the bucket
	// column AND the variant's KeepDimensions; that's fine. Only the trailing
	// outer-SELECT GROUP BY needs to mirror the user.
	// We split on "merged" since the outer GROUP BY follows the alias.
	idx := strings.LastIndex(out, "merged")
	if idx < 0 {
		t.Fatalf("output missing `merged` marker:\n%s", out)
	}
	tail := out[idx:]
	if !strings.Contains(tail, "GROUP BY country") {
		t.Errorf("outer GROUP BY should be `country` only:\n%s", out)
	}
	if strings.Contains(tail, "GROUP BY country, region") {
		t.Errorf("outer GROUP BY incorrectly includes `region`:\n%s", out)
	}
}

// Bug 5: outer GROUP BY must preserve time-bucket expressions / aliases
// when the user groups on `date_trunc(...)`.
func TestEmitMergeOnRead_OuterGroupByPreservesTimeBucket(t *testing.T) {
	sql := `SELECT date_trunc('day', time) AS day, SUM(latency_ms)
            FROM downloads
            WHERE time >= TIMESTAMP '2026-05-01' AND time < TIMESTAMP '2026-05-08'
            GROUP BY day`
	sel := parseSelect(t, sql)
	spec := &RollupSpec{
		Name: "v", Database: "default", SourceTable: "downloads",
		BucketColumn: "time", BucketInterval: 24 * time.Hour,
		KeepDimensions: []string{},
		Aggregations: []Aggregation{
			{SourceColumn: "latency_ms", Functions: []AggFunction{AggSum}},
		},
	}
	tr := TimeRange{Lo: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), Hi: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)}
	out, err := EmitMergeOnRead(sel, spec, tr, testBoundary, dummyGlob, nil)
	if err != nil {
		t.Fatalf("emit error: %v", err)
	}
	idx := strings.LastIndex(out, "merged")
	if idx < 0 {
		t.Fatalf("output missing `merged` marker:\n%s", out)
	}
	tail := out[idx:]
	if !strings.Contains(tail, "GROUP BY day") {
		t.Errorf("outer GROUP BY should mirror user `GROUP BY day`:\n%s", out)
	}
}

// Bug 6: nested aggregates (e.g. `100 * AVG(x)`) must NOT pass through
// verbatim. The fallback is refusal so the caller falls back to source.
func TestEmitMergeOnRead_RefusesNestedAggregate(t *testing.T) {
	sql := `SELECT country, 100 * AVG(latency_ms) FROM downloads
            WHERE time >= TIMESTAMP '2026-05-01' AND time < TIMESTAMP '2026-05-08'
            GROUP BY country`
	sel := parseSelect(t, sql)
	spec := &RollupSpec{
		Name: "v", Database: "default", SourceTable: "downloads",
		BucketColumn: "time", BucketInterval: 24 * time.Hour,
		KeepDimensions: []string{"country"},
		Aggregations: []Aggregation{
			{SourceColumn: "latency_ms", Functions: []AggFunction{AggSum, AggCount}},
		},
	}
	tr := TimeRange{Lo: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), Hi: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)}
	_, err := EmitMergeOnRead(sel, spec, tr, testBoundary, dummyGlob, nil)
	if err == nil {
		t.Errorf("expected error refusing nested aggregate; got success")
	}
}

// Bug 7: SELECT * must be refused because the merged view exposes pre-agg
// columns, not the source schema.
func TestEmitMergeOnRead_RefusesSelectStar(t *testing.T) {
	sql := `SELECT * FROM downloads
            WHERE time >= TIMESTAMP '2026-05-01' AND time < TIMESTAMP '2026-05-08'`
	sel := parseSelect(t, sql)
	spec := &RollupSpec{
		Name: "v", Database: "default", SourceTable: "downloads",
		BucketColumn: "time", BucketInterval: 24 * time.Hour,
		Aggregations: []Aggregation{
			{SourceColumn: "latency_ms", Functions: []AggFunction{AggSum}},
		},
	}
	tr := TimeRange{Lo: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), Hi: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)}
	_, err := EmitMergeOnRead(sel, spec, tr, testBoundary, dummyGlob, nil)
	if err == nil {
		t.Errorf("expected error refusing SELECT *; got success")
	}
}

// Cleanup-defense: HLL/tdigest aggregation with nil SketchConfig must
// error (not silently emit a default K).
func TestEmitMergeOnRead_RefusesMissingSketchConfig(t *testing.T) {
	sql := `SELECT country, COUNT(DISTINCT device_id) FROM downloads
            WHERE time >= TIMESTAMP '2026-05-01' AND time < TIMESTAMP '2026-05-08'
            GROUP BY country`
	sel := parseSelect(t, sql)
	spec := &RollupSpec{
		Name: "v", Database: "default", SourceTable: "downloads",
		BucketColumn: "time", BucketInterval: 24 * time.Hour,
		KeepDimensions: []string{"country"},
		Aggregations: []Aggregation{
			// SketchConfig deliberately nil.
			{SourceColumn: "device_id", Functions: []AggFunction{AggHLL}},
		},
	}
	tr := TimeRange{Lo: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), Hi: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)}
	_, err := EmitMergeOnRead(sel, spec, tr, testBoundary, dummyGlob, nil)
	if err == nil {
		t.Errorf("expected error for HLL with nil SketchConfig; got success")
	}
}
