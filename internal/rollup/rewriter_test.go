package rollup

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
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
			Kind:           RollupKindAll,
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
			Kind:           RollupKindAll,
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

// fakeWMReader implements WMReader for tests. Keyed by storagePath (the same
// key the production WatermarkStore uses) so tests catch any drift between
// the production write key and the rewriter read key.
type fakeWMReader struct {
	wms map[string]Watermark
}

func (f *fakeWMReader) Get(_ context.Context, storagePath string) (Watermark, error) {
	return f.wms[storagePath], nil
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
		"d/events/all/1d": {Rollup: "d__events__1d", StoragePath: "d/events/all/1d", Watermark: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC), BucketInterval: 24 * time.Hour},
		"d/events/all/1h": {Rollup: "d__events__1h", StoragePath: "d/events/all/1h", Watermark: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC), BucketInterval: time.Hour},
	}}
	out, ok := Rewrite(context.Background(), sql, newRewriteTestRegistry(), wm, nil, nil)
	if ok {
		t.Errorf("expected refusal for bucket-col equality, got rewrite:\n%s", out)
	}
}

// TestRewrite_WatermarkStoreRoundTrip pins C1+C2: a watermark written via
// the real WatermarkStore (keyed by storagePath) must be visible to the
// rewriter's cold-start guard. This catches the failure mode where the
// builder writes by storagePath but the rewriter reads by spec.Name (or
// vice-versa) — every Get returns zero and the rewriter refuses every
// query in production despite the builder running healthily.
func TestRewrite_WatermarkStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	store := NewWatermarkStore(backend)
	reg := newRewriteTestRegistry()
	specs := reg.ForTable("d", "events")
	if len(specs) == 0 {
		t.Fatalf("test registry has no specs for d.events")
	}
	// Write a watermark for both variants using the storagePath the
	// builder uses in production.
	wmTime := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	for _, s := range specs {
		if err := store.Put(context.Background(), Watermark{
			Rollup:         s.Name,
			StoragePath:    s.StoragePath(),
			BucketInterval: s.BucketInterval,
			Watermark:      wmTime,
		}); err != nil {
			t.Fatalf("put %s: %v", s.Name, err)
		}
	}

	sql := `SELECT date_trunc('day', ts) AS d, COUNT(*) FROM d.events ` +
		`WHERE ts >= TIMESTAMP '2026-04-10 00:00:00' AND ts < TIMESTAMP '2026-05-09 00:00:00' GROUP BY 1`
	out, ok := Rewrite(context.Background(), sql, reg, store, nil, nil)
	if !ok {
		t.Fatalf("expected rewrite ok=true when watermark is present in WatermarkStore, got refusal (sql:\n%s)", sql)
	}
	if !strings.Contains(out, "WITH rollup AS") {
		t.Errorf("rewritten SQL missing rollup CTE:\n%s", out)
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
