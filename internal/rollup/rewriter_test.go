package rollup

import (
	"context"
	"strings"
	"testing"
	"time"
)

// newRewriteTestRegistry returns a registry with one hourly variant plus a
// dim-rich daily variant, both rooted at the "d.events" source table.
func newRewriteTestRegistry() *Registry {
	specs := []RollupSpec{
		{
			Name:           "d__events__1h",
			Database:       "d",
			SourceTable:    "events",
			BucketColumn:   "ts",
			BucketInterval: time.Hour,
			KeepDimensions: []string{"service"},
			Aggregations: []Aggregation{
				{SourceColumn: "latency_ms", Functions: []AggFunction{AggSum, AggCount, AggMin, AggMax, AggTDigest}, SketchConfig: &SketchConfig{HLLLgK: 12, TDigestK: 200}},
				{SourceColumn: "user_id", Functions: []AggFunction{AggHLL}, SketchConfig: &SketchConfig{HLLLgK: 12, TDigestK: 200}},
			},
		},
		{
			Name:           "d__events__1d",
			Database:       "d",
			SourceTable:    "events",
			BucketColumn:   "ts",
			BucketInterval: 24 * time.Hour,
			KeepDimensions: []string{"service"},
			Aggregations: []Aggregation{
				{SourceColumn: "latency_ms", Functions: []AggFunction{AggSum, AggCount, AggMin, AggMax, AggTDigest}, SketchConfig: &SketchConfig{HLLLgK: 12, TDigestK: 200}},
				{SourceColumn: "user_id", Functions: []AggFunction{AggHLL}, SketchConfig: &SketchConfig{HLLLgK: 12, TDigestK: 200}},
			},
		},
	}
	return NewRegistry(specs)
}

func TestRewrite_DailyAvgRewritesToMergeOnRead(t *testing.T) {
	sql := `SELECT date_trunc('day', ts) AS d, AVG(latency_ms) FROM d.events ` +
		`WHERE ts >= TIMESTAMP '2026-04-10 00:00:00' AND ts < TIMESTAMP '2026-05-10 00:00:00' GROUP BY 1`
	out, ok := Rewrite(context.Background(), sql, newRewriteTestRegistry(), nil, nil, nil)
	if !ok {
		t.Fatalf("expected rewrite, got refusal\n--- SQL ---\n%s", sql)
	}
	for _, want := range []string{
		"WITH rollup AS",
		"fresh AS",
		"SUM(latency_ms__sum) / NULLIF(SUM(latency_ms__count)",
		"UNION ALL",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rewritten SQL missing %q\n--- SQL ---\n%s", want, out)
		}
	}
}

func TestRewrite_RefusesWhenNoTimeFilter(t *testing.T) {
	sql := `SELECT service, SUM(latency_ms) FROM d.events GROUP BY service`
	out, ok := Rewrite(context.Background(), sql, newRewriteTestRegistry(), nil, nil, nil)
	if ok {
		t.Fatalf("expected refusal for missing time filter, got rewrite:\n%s", out)
	}
	if out != sql {
		t.Errorf("expected original SQL on refusal, got:\n%s", out)
	}
}

func TestRewrite_RefusesOpenEndedTimeFilter(t *testing.T) {
	sql := `SELECT SUM(latency_ms) FROM d.events WHERE ts >= TIMESTAMP '2026-04-10 00:00:00'`
	if _, ok := Rewrite(context.Background(), sql, newRewriteTestRegistry(), nil, nil, nil); ok {
		t.Error("expected refusal for one-sided time range")
	}
}

func TestRewrite_RefusesUnknownTable(t *testing.T) {
	sql := `SELECT SUM(latency_ms) FROM d.unknown ` +
		`WHERE ts >= TIMESTAMP '2026-04-10 00:00:00' AND ts < TIMESTAMP '2026-05-10 00:00:00'`
	if _, ok := Rewrite(context.Background(), sql, newRewriteTestRegistry(), nil, nil, nil); ok {
		t.Error("expected refusal for unregistered table")
	}
}

func TestRewrite_RefusesJoin(t *testing.T) {
	sql := `SELECT SUM(a.latency_ms) FROM d.events a JOIN d.other b ON a.id = b.id ` +
		`WHERE a.ts >= TIMESTAMP '2026-04-10 00:00:00' AND a.ts < TIMESTAMP '2026-05-10 00:00:00'`
	if _, ok := Rewrite(context.Background(), sql, newRewriteTestRegistry(), nil, nil, nil); ok {
		t.Error("expected refusal for join (multi-table FROM)")
	}
}

func TestRewrite_RefusesUntranslatableAggregate(t *testing.T) {
	// stddev is in the known-DuckDB-aggregates list but not in the v2
	// translation table; AllAggregatesTranslatable must reject it.
	sql := `SELECT stddev(latency_ms) FROM d.events ` +
		`WHERE ts >= TIMESTAMP '2026-04-10 00:00:00' AND ts < TIMESTAMP '2026-05-10 00:00:00'`
	if _, ok := Rewrite(context.Background(), sql, newRewriteTestRegistry(), nil, nil, nil); ok {
		t.Error("expected refusal for stddev aggregate")
	}
}

// fakeWMReader implements WMReader for tests. Returns the canned Watermark
// when name matches, an empty Watermark otherwise.
type fakeWMReader struct {
	wms map[string]Watermark
}

func (f *fakeWMReader) Get(_ context.Context, rollupName string) (Watermark, error) {
	return f.wms[rollupName], nil
}

// TestRewrite_RefusesWhenWatermarkZero pins the cold-start guard: when the
// builder hasn't written a single bucket yet for the picked variant, the
// emitted SQL would `read_parquet('<glob>')` against an empty directory
// and DuckDB would raise "No files found" with no fallback. Refusing in
// the rewriter routes those queries to source instead.
func TestRewrite_RefusesWhenWatermarkZero(t *testing.T) {
	sql := `SELECT date_trunc('day', ts) AS d, COUNT(*) FROM d.events ` +
		`WHERE ts >= TIMESTAMP '2026-04-10 00:00:00' AND ts < TIMESTAMP '2026-05-10 00:00:00' GROUP BY 1`
	wm := &fakeWMReader{wms: map[string]Watermark{}}
	out, ok := Rewrite(context.Background(), sql, newRewriteTestRegistry(), wm, nil, nil)
	if ok {
		t.Errorf("expected refusal for zero watermark, got rewrite:\n%s", out)
	}
}

// TestRewrite_RefusesBucketColEquality pins the residue guard: a non-range
// predicate on the bucket column (e.g. `ts = X`) would slip past the
// time-filter guard's bound recovery and survive into the rollup CTE's
// WHERE, which references `bucket` not `ts` (rename happens at SELECT-
// time). Refusing keeps the emitter from producing SQL that errors.
func TestRewrite_RefusesBucketColEquality(t *testing.T) {
	sql := `SELECT COUNT(*) FROM d.events ` +
		`WHERE ts >= TIMESTAMP '2026-04-10 00:00:00' AND ts < TIMESTAMP '2026-05-10 00:00:00' AND ts = TIMESTAMP '2026-05-01 00:00:00'`
	wm := &fakeWMReader{wms: map[string]Watermark{
		"d__events__1d": {Rollup: "d__events__1d", Watermark: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC), BucketInterval: 24 * time.Hour},
		"d__events__1h": {Rollup: "d__events__1h", Watermark: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC), BucketInterval: time.Hour},
	}}
	out, ok := Rewrite(context.Background(), sql, newRewriteTestRegistry(), wm, nil, nil)
	if ok {
		t.Errorf("expected refusal for bucket-col equality, got rewrite:\n%s", out)
	}
}

func TestRewrite_HourlyVariantPickedForHourlyGrouping(t *testing.T) {
	sql := `SELECT date_trunc('hour', ts), SUM(latency_ms) FROM d.events ` +
		`WHERE ts >= TIMESTAMP '2026-05-09 00:00:00' AND ts < TIMESTAMP '2026-05-10 00:00:00' GROUP BY 1`
	out, ok := Rewrite(context.Background(), sql, newRewriteTestRegistry(), nil, nil, nil)
	if !ok {
		t.Fatalf("expected rewrite, got refusal\n--- SQL ---\n%s", sql)
	}
	if !strings.Contains(out, "events__1h") {
		t.Errorf("expected hourly variant (events__1h) in rewrite, got:\n%s", out)
	}
}
