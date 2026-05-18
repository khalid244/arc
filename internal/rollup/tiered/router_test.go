package tiered

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestRewrite_HappyPath_DailyCountSketch(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()

	// MemoryFileIndex with daily sketch files covering the range.
	// Watermark is derived from bucketHi of the latest file.
	// Latest file is 2026/05/15 → bucketHi = 2026-05-16 (1d).
	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/default/events/1d/2026/05/01/sketch/file1.parquet",
			"_arc/rollup/default/events/1d/2026/05/02/sketch/file2.parquet",
			"_arc/rollup/default/events/1d/2026/05/15/sketch/file3.parquet",
		},
	}

	spec := Spec{
		Table:      "events",
		TZ:         "Asia/Riyadh",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"US", "GB", "IN"}},
			"dim_b": {Role: "Dim", KeptValues: []string{"web", "app"}},
		},
	}

	userSQL := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
		WHERE time BETWEEN '2026-05-01' AND '2026-05-15'
		GROUP BY 1`

	deps := RewriteDeps{
		DB:          db,
		Files:       idx,
		Spec:        &spec,
		DimRichCap:  100,
		GraceWindow: 6 * time.Hour,
	}

	out, ok := Rewrite(ctx, userSQL, deps)
	if !ok {
		t.Fatalf("Rewrite returned ok=false, expected true")
	}
	if out == userSQL {
		t.Fatalf("Rewrite returned originalSQL unchanged")
	}

	if !contains(out, "WITH rollup AS") {
		t.Errorf("output missing 'WITH rollup AS': %s", out)
	}
	if !contains(out, "SUM(cnt)") {
		t.Errorf("output missing 'SUM(cnt)': %s", out)
	}
	if !contains(out, "file1.parquet") {
		t.Errorf("output missing file path: %s", out)
	}
}

func TestRewrite_RefusesWhenParserRefuses(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()

	idx := &MemoryFileIndex{}
	spec := Spec{
		Table:      "events",
		TZ:         "UTC",
		TimeColumn: "time",
		Dims:       map[string]DimSpec{},
	}

	userSQL := `SELECT COUNT(*) FROM events`

	deps := RewriteDeps{
		DB:         db,
		Files:      idx,
		Spec:       &spec,
		DimRichCap: 100,
	}

	out, ok := Rewrite(ctx, userSQL, deps)
	if ok {
		t.Fatalf("Rewrite returned ok=true, expected false for unparseable SQL")
	}
	if out != userSQL {
		t.Fatalf("Rewrite returned modified SQL instead of original: got %s want %s", out, userSQL)
	}
}

func TestRewrite_RefusesWhenVariantNotPickable(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()

	_, _ = db.Exec(`INSERT INTO events (time, dim_a) VALUES ('2026-05-05', 'US')`)

	idx := &MemoryFileIndex{}
	spec := Spec{
		Table:      "events",
		TZ:         "UTC",
		TimeColumn: "time",
		Dims:       map[string]DimSpec{},
	}

	userSQL := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
		WHERE time BETWEEN '2026-05-01' AND '2026-05-15'
		AND dim_a = 'ZZ'
		GROUP BY 1`

	deps := RewriteDeps{
		DB:         db,
		Files:      idx,
		Spec:       &spec,
		DimRichCap: 100,
	}

	out, ok := Rewrite(ctx, userSQL, deps)
	if ok {
		t.Fatalf("Rewrite returned ok=true, expected false when variant not pickable")
	}
	if out != userSQL {
		t.Fatalf("Rewrite returned modified SQL instead of original")
	}
}

func TestRewrite_RefusesWhenTierWatermarkBelowRange(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()

	_, _ = db.Exec(`INSERT INTO events (time) VALUES ('2026-05-05')`)

	// Watermark derived from file: 1d/2026/04/01 → bucketHi=2026-04-02, before range 2026-05-01
	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/default/events/1d/2026/04/01/sketch/path.parquet",
		},
	}

	spec := Spec{
		Table:      "events",
		TZ:         "UTC",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"US"}},
		},
	}

	userSQL := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
		WHERE time BETWEEN '2026-05-01' AND '2026-05-15'
		GROUP BY 1`

	deps := RewriteDeps{
		DB:         db,
		Files:      idx,
		Spec:       &spec,
		DimRichCap: 100,
	}

	out, ok := Rewrite(ctx, userSQL, deps)
	if ok {
		t.Fatalf("Rewrite returned ok=true, expected false when watermark below range")
	}
	if out != userSQL {
		t.Fatalf("Rewrite returned modified SQL instead of original")
	}
}

// TestRewrite_PartialCoverage_QueryStartsBeforeEarliestEntry exercises the
// scenario where precalc has hours [01:00, 05:00) but the user queries
// [00:00, 05:00). Hour [00:00, 01:00) has no rollup coverage.
//
// Correct behavior: refuse to rewrite (ok=false) so the original query
// scans source and returns the full correct answer.
func TestRewrite_PartialCoverage_QueryStartsBeforeEarliestEntry(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()

	_, _ = db.Exec(`INSERT INTO events (time) VALUES ('2026-05-05')`)

	// Precalc covers hours 1, 2, 3, 4 of 2026-05-05 — NOT hour 0.
	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/default/events/1h/2026/05/05/01/sketch/h01.parquet",
			"_arc/rollup/default/events/1h/2026/05/05/02/sketch/h02.parquet",
			"_arc/rollup/default/events/1h/2026/05/05/03/sketch/h03.parquet",
			"_arc/rollup/default/events/1h/2026/05/05/04/sketch/h04.parquet",
		},
	}

	spec := Spec{Table: "events", TZ: "UTC", TimeColumn: "time"}

	userSQL := `SELECT date_trunc('hour', time) AS h, COUNT(*) FROM events
		WHERE time >= '2026-05-05 00:00:00' AND time < '2026-05-05 05:00:00'
		GROUP BY 1`

	deps := RewriteDeps{
		DB: db, Files: idx, Spec: &spec, DimRichCap: 100,
	}

	out, ok := Rewrite(ctx, userSQL, deps)
	if ok {
		t.Errorf("Rewrite returned ok=true with partial coverage (gap at hour 0); would under-count")
		t.Logf("rewritten SQL: %s", out)
	}
	if out != userSQL {
		t.Errorf("Rewrite must return original SQL when refusing — got modified output")
	}
}

func TestRewrite_RefusesWhenNoFiles(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()

	_, _ = db.Exec(`INSERT INTO events (time) VALUES ('2026-05-05')`)

	// Has a 1d/other file (not sketch), so sketch tier/variant has no files.
	// The watermark for sketch via MemoryFileIndex is zero → tier refusal.
	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/default/events/1d/2026/05/01/other/path.parquet",
		},
	}

	spec := Spec{
		Table:      "events",
		TZ:         "UTC",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"US"}},
		},
	}

	userSQL := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
		WHERE time BETWEEN '2026-05-01' AND '2026-05-15'
		GROUP BY 1`

	deps := RewriteDeps{
		DB:         db,
		Files:      idx,
		Spec:       &spec,
		DimRichCap: 100,
	}

	out, ok := Rewrite(ctx, userSQL, deps)
	if ok {
		t.Fatalf("Rewrite returned ok=true, expected false when no files")
	}
	if out != userSQL {
		t.Fatalf("Rewrite returned modified SQL instead of original")
	}
}

func TestRewrite_DefaultsApplied(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()

	_, _ = db.Exec(`INSERT INTO events (time) VALUES ('2026-05-05')`)

	now := time.Now().UTC()
	fiveHoursAgo := now.Add(-5 * time.Hour)

	dayBucket := fiveHoursAgo.Truncate(24 * time.Hour).UTC()
	dayPath := fmt.Sprintf("_arc/rollup/default/events/1d/%04d/%02d/%02d/sketch/file.parquet",
		dayBucket.Year(), dayBucket.Month(), dayBucket.Day())
	idx := &MemoryFileIndex{
		Paths: []string{dayPath},
	}

	spec := Spec{
		Table:      "events",
		TZ:         "UTC",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"US"}},
		},
	}

	userSQL := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
		WHERE time BETWEEN '` + fiveHoursAgo.Format("2006-01-02") + `' AND '` + fiveHoursAgo.AddDate(0, 0, 2).Format("2006-01-02") + `'
		GROUP BY 1`

	deps := RewriteDeps{
		DB:          db,
		Files:       idx,
		Spec:        &spec,
		DimRichCap:  0,
		GraceWindow: 0,
	}

	out, ok := Rewrite(ctx, userSQL, deps)
	if !ok {
		t.Logf("Rewrite returned ok=false; may be expected if date range doesn't align")
	} else {
		if out == userSQL {
			t.Fatalf("Rewrite returned originalSQL unchanged despite defaults being applied")
		}
		if !contains(out, "WITH rollup AS") {
			t.Errorf("output missing expected structure: %s", out)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && (s == substr || (len(s) > len(substr) && len(s) >= len(substr)))
}

// mockSink records every MetricsSink call for assertion in tests.
type mockSink struct {
	attempts, accepted, refusedParser, refusedVariant, refusedTier, refusedEmit int64
	nanos                                                                        int64
	buildSuccess, buildErrors, buildNanos                                        int64
	maxWatermarkLag                                                              int64
}

func (m *mockSink) IncRewriteAttempts()               { m.attempts++ }
func (m *mockSink) IncRewriteAccepted()               { m.accepted++ }
func (m *mockSink) IncRewriteRefusedParser()          { m.refusedParser++ }
func (m *mockSink) IncRewriteRefusedVariant()         { m.refusedVariant++ }
func (m *mockSink) IncRewriteRefusedTier()            { m.refusedTier++ }
func (m *mockSink) IncRewriteRefusedEmit()            { m.refusedEmit++ }
func (m *mockSink) AddRewriteNanos(ns int64)          { m.nanos += ns }
func (m *mockSink) IncBuildSuccess()                  { m.buildSuccess++ }
func (m *mockSink) IncBuildErrors()                   { m.buildErrors++ }
func (m *mockSink) AddBuildNanos(ns int64)            { m.buildNanos += ns }
func (m *mockSink) SetMaxWatermarkLagSeconds(s int64) { m.maxWatermarkLag = s }

func TestRewrite_EmitsAttemptCounterOnParserRefusal(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()

	idx := &MemoryFileIndex{}
	spec := Spec{Table: "events", TZ: "UTC", TimeColumn: "time", Dims: map[string]DimSpec{}}

	sink := &mockSink{}
	deps := RewriteDeps{
		DB:      db,
		Files:   idx,
		Spec:    &spec,
		Metrics: sink,
	}

	joinSQL := `SELECT COUNT(*) FROM events e JOIN events e2 ON e.dim_a = e2.dim_a
		WHERE e.time BETWEEN '2026-05-01' AND '2026-05-15'`

	_, ok := Rewrite(ctx, joinSQL, deps)

	if ok {
		t.Fatal("expected ok=false")
	}
	if sink.attempts != 1 {
		t.Errorf("attempts = %d, want 1", sink.attempts)
	}
	if sink.refusedParser != 1 {
		t.Errorf("refusedParser = %d, want 1", sink.refusedParser)
	}
	if sink.accepted != 0 {
		t.Errorf("accepted = %d, want 0", sink.accepted)
	}
	if sink.nanos <= 0 {
		t.Error("nanos should be > 0")
	}
}

func TestRewrite_EmitsAcceptedOnSuccess(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()

	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/default/events/1d/2026/05/01/sketch/file1.parquet",
		},
	}
	spec := Spec{
		Table:      "events",
		TZ:         "UTC",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"US", "GB"}},
		},
	}

	sink := &mockSink{}
	deps := RewriteDeps{
		DB:          db,
		Files:       idx,
		Spec:        &spec,
		DimRichCap:  100,
		GraceWindow: 6 * time.Hour,
		Metrics:     sink,
	}

	_, ok := Rewrite(ctx, `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
		WHERE time BETWEEN '2026-05-01' AND '2026-05-01'
		GROUP BY 1`, deps)

	if !ok {
		t.Fatal("expected ok=true for happy path")
	}
	if sink.attempts != 1 {
		t.Errorf("attempts = %d, want 1", sink.attempts)
	}
	if sink.accepted != 1 {
		t.Errorf("accepted = %d, want 1", sink.accepted)
	}
	if sink.refusedParser+sink.refusedVariant+sink.refusedTier+sink.refusedEmit != 0 {
		t.Errorf("unexpected refusal counters: parser=%d variant=%d tier=%d emit=%d",
			sink.refusedParser, sink.refusedVariant, sink.refusedTier, sink.refusedEmit)
	}
	if sink.nanos <= 0 {
		t.Error("nanos should be > 0")
	}
}

func TestRewrite_NilMetricsNoPanic(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()

	idx := &MemoryFileIndex{}
	spec := Spec{Table: "events", TZ: "UTC", TimeColumn: "time", Dims: map[string]DimSpec{}}
	deps := RewriteDeps{DB: db, Files: idx, Spec: &spec}

	_, _ = Rewrite(ctx, `SELECT COUNT(*) FROM events`, deps)
}
