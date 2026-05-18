package tiered

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestPreClassifyCardinalities_ReturnsApproxDistinct(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, dim_a VARCHAR, dim_b VARCHAR)`)
	db.Exec(`INSERT INTO evt SELECT '2026-05-10 00:00:00+00'::TIMESTAMPTZ,
             'a' || (i % 3), 'b' || (i % 50) FROM range(1000) t(i)`)

	s := &Scheduler{Publisher: &Publisher{DB: db}, Logger: zerolog.Nop()}
	got := preClassifyCardinalities(ctx, db, "SELECT * FROM evt", []string{"dim_a", "dim_b"})
	if got["dim_a"] < 2 || got["dim_a"] > 5 {
		t.Errorf("dim_a approx_distinct = %d, want ~3", got["dim_a"])
	}
	if got["dim_b"] < 45 || got["dim_b"] > 55 {
		t.Errorf("dim_b approx_distinct = %d, want ~50", got["dim_b"])
	}
	_ = s
}

// newTestScheduler sets up a Scheduler backed by local storage + in-memory DuckDB.
// The caller supplies:
//   - sourceWM: the value the SourceWatermark stub returns for every table
//   - tables: the table names to register
//   - specs: per-table Spec (written into the SpecStore)
//   - manifests: per-table *Manifest to pre-seed (nil = start fresh)
func newTestScheduler(t *testing.T, sourceWM map[string]time.Time, tables []string, specs map[string]Spec, manifests map[string]*Manifest) (*Scheduler, *ManifestStore) {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES ('2026-01-01 00:30:00+00', 1.0)`); err != nil {
		t.Fatal(err)
	}

	specStore := NewSpecStore(backend)
	manifestStore := NewManifestStore(backend)

	ctx := context.Background()
	for _, table := range tables {
		spec, ok := specs[table]
		if !ok {
			continue
		}
		if err := specStore.Put(ctx, table, spec); err != nil {
			t.Fatalf("put spec %s: %v", table, err)
		}
		if m, ok := manifests[table]; ok && m != nil {
			if err := manifestStore.Put(ctx, table, m); err != nil {
				t.Fatalf("seed manifest %s: %v", table, err)
			}
		}
	}

	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		Manifests:   manifestStore,
		LocalTmpDir: filepath.Join(dir, "_tmp"),
	}

	buildArgs := make(map[string]BuildArgs)
	for _, table := range tables {
		spec := specs[table]
		buildArgs[table] = BuildArgs{
			Source:     "evt",
			TimeColumn: spec.TimeColumn,
			MetricCols: []MetricCol{{Name: "m", Numeric: true}},
		}
	}

	sched := &Scheduler{
		Publisher:     pub,
		SpecStore:     specStore,
		ManifestStore: manifestStore,
		Tables:        tables,
		Tiers:         []Tier{Tier1h, Tier1d, Tier1w, Tier1mo},
		Variants:      []string{"sketch"},
		GraceWindow:   15 * time.Minute,
		BuildArgsFor:  buildArgs,
		Logger:        zerolog.Nop(),
		SourceWatermark: func(ctx context.Context, table string) (time.Time, error) {
			return sourceWM[table], nil
		},
	}

	return sched, manifestStore
}

// TestScheduler_BuildsOne1hBucketWhenSealed verifies that when the source
// watermark is 2026-05-01 02:00 (grace 15m), exactly the bucket [01:00,02:00)
// is sealed and built; [02:00,03:00) is not.
func TestScheduler_BuildsOne1hBucketWhenSealed(t *testing.T) {
	ctx := context.Background()
	table := "default.events"

	// Pre-seed manifest with 1h watermark at 01:00 so the scheduler starts
	// from that point (otherwise it starts from 2026-01-01 and builds ~2800 buckets).
	seedManifest := &Manifest{
		Table:      table,
		Generation: 1,
		Watermarks: map[string]time.Time{
			"1h.sketch": time.Date(2026, 5, 1, 1, 0, 0, 0, time.UTC),
		},
	}

	// Source WM at 02:00 — bucket [01:00,02:00) ends at 02:00.
	// Sealed condition: bucket_end + grace ≤ effectiveMax → 02:00 + 15m = 02:15 > 02:00 → NOT sealed.
	// We need source WM at 02:15 or later to seal it.
	// Per spec: "bucket_end + GraceWindow ≤ Now" but effectiveMax = sourceWatermark for 1h.
	// Let's set to 02:15 exactly → sealed. 02:00 bucket end + 15m grace = 02:15 ≤ 02:15 ✓
	srcWM := time.Date(2026, 5, 1, 2, 15, 0, 0, time.UTC)

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, ms := newTestScheduler(t,
		map[string]time.Time{table: srcWM},
		[]string{table},
		map[string]Spec{table: spec},
		map[string]*Manifest{table: seedManifest},
	)

	sched.runOnce(ctx)

	m, err := ms.Get(ctx, table)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}

	wm := m.Watermark("1h", "sketch")
	want := time.Date(2026, 5, 1, 2, 0, 0, 0, time.UTC)
	if !wm.Equal(want) {
		t.Errorf("1h watermark = %v, want %v", wm, want)
	}

	// Confirm exactly one new entry was added (the seed had none, we added generation).
	entries1h := m.FilesForTierVariant("1h", "sketch")
	if len(entries1h) != 1 {
		t.Errorf("expected 1 manifest entry for 1h/sketch, got %d", len(entries1h))
	}

	// Bucket [02:00,03:00) should NOT be sealed — watermark should not advance past 02:00.
	if wm.After(want) {
		t.Errorf("watermark advanced past expected sealed bucket: %v", wm)
	}
}

// TestScheduler_GatesHigherTierOnLowerWatermark verifies that 1d does not
// build until 1h has covered the full day.
func TestScheduler_GatesHigherTierOnLowerWatermark(t *testing.T) {
	ctx := context.Background()
	table := "events"

	// Source WM at 23:59 — 1h can build many buckets but not a full day's worth.
	// After one tick: 1h watermark ≤ 23:00 (maxBucketsPerTick=24 from 00:00),
	// so 1d cannot build [00:00, 24:00) because 1h doesn't cover it.
	// Seed watermark at 00:00 so scheduler starts there.
	// Seed both 1h and 1d watermarks at the start of the day under test.
	// This ensures the next bucket for 1d is [2026-05-01, 2026-05-02),
	// which needs effectiveMax ≥ 2026-05-02 00:15 to seal — but 1h WM
	// will only reach ~2026-05-01 23:00 after one tick (24 buckets from 00:00,
	// src 23:59 with grace 15m seals up to bucket ending ≤ 23:44).
	seedManifest := &Manifest{
		Table:      table,
		Generation: 1,
		Watermarks: map[string]time.Time{
			"1h.sketch": time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
			"1d.sketch": time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	srcWM := time.Date(2026, 5, 1, 23, 59, 0, 0, time.UTC)

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, ms := newTestScheduler(t,
		map[string]time.Time{table: srcWM},
		[]string{table},
		map[string]Spec{table: spec},
		map[string]*Manifest{table: seedManifest},
	)

	sched.runOnce(ctx)

	m, err := ms.Get(ctx, table)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}

	wm1h := m.Watermark("1h", "sketch")
	if wm1h.IsZero() {
		t.Fatal("1h watermark should have advanced")
	}

	// 1d should NOT have built: effectiveMax for 1d = min(srcWM=23:59, 1h WM≈23:xx).
	// The next 1d bucket [2026-05-01 00:00, 2026-05-02 00:00) ends at 2026-05-02 00:00;
	// adding 15m grace gives 2026-05-02 00:15 which is after effectiveMax (~23:xx).
	entries1d := m.FilesForTierVariant("1d", "sketch")
	if len(entries1d) != 0 {
		t.Errorf("1d should be gated on 1h watermark; got %d entries", len(entries1d))
	}
}

// TestScheduler_StopsAtCapPerTick verifies that at most 24 buckets are built
// per tier per variant per tick even when a large backlog exists.
func TestScheduler_StopsAtCapPerTick(t *testing.T) {
	ctx := context.Background()
	table := "events"

	// Source WM far in the future — lots of 1h buckets could be built.
	// Seed at 2026-05-01 00:00 so the scheduler starts there.
	seedManifest := &Manifest{
		Table:      table,
		Generation: 1,
		Watermarks: map[string]time.Time{
			"1h.sketch": time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	srcWM := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, ms := newTestScheduler(t,
		map[string]time.Time{table: srcWM},
		[]string{table},
		map[string]Spec{table: spec},
		map[string]*Manifest{table: seedManifest},
	)

	sched.runOnce(ctx)

	m, err := ms.Get(ctx, table)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}

	entries1h := m.FilesForTierVariant("1h", "sketch")
	if len(entries1h) != 24 {
		t.Errorf("expected exactly 24 buckets (cap), got %d", len(entries1h))
	}
}

// TestScheduler_NoBuildsWhenSourceBehindWatermark verifies that when the
// source watermark equals the existing 1h manifest watermark, no new buckets
// are built.
func TestScheduler_NoBuildsWhenSourceBehindWatermark(t *testing.T) {
	ctx := context.Background()
	table := "events"

	wm := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	seedManifest := &Manifest{
		Table:      table,
		Generation: 1,
		Watermarks: map[string]time.Time{
			"1h.sketch": wm,
		},
	}

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, ms := newTestScheduler(t,
		map[string]time.Time{table: wm}, // src == watermark
		[]string{table},
		map[string]Spec{table: spec},
		map[string]*Manifest{table: seedManifest},
	)

	sched.runOnce(ctx)

	m, err := ms.Get(ctx, table)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}

	entries := m.FilesForTierVariant("1h", "sketch")
	if len(entries) != 0 {
		t.Errorf("expected no new builds, got %d entries", len(entries))
	}
}

// TestScheduler_DimVariantGatesOnSameVariantFinerWatermark verifies that
// 1d.by_dim_a builds when 1h.by_dim_a is advanced, even when 1h.sketch is
// still at zero — i.e. each variant gates on its own finer-tier watermark.
func TestScheduler_DimVariantGatesOnSameVariantFinerWatermark(t *testing.T) {
	ctx := context.Background()
	table := "events"

	// 1h.by_dim_a watermark covers a full day (2026-05-01 00:00 → 2026-05-02 00:00).
	// 1h.sketch is absent (zero).
	// Source WM is far enough ahead that both 1d.by_dim_a and 1d.sketch could
	// theoretically build — but 1d.sketch must be gated on 1h.sketch (zero), so
	// it should not build.
	seedManifest := &Manifest{
		Table:      table,
		Generation: 1,
		Watermarks: map[string]time.Time{
			"1h.by_dim_a": time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC),
			"1d.by_dim_a": time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
			"1d.sketch":   time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	srcWM := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)

	spec := Spec{
		Table:      table,
		TZ:         "UTC",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"x"}, EffectiveCard: 1},
		},
	}

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, m DOUBLE, dim_a VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES ('2026-05-01 00:30:00+00', 1.0, 'x')`); err != nil {
		t.Fatal(err)
	}

	specStore := NewSpecStore(backend)
	manifestStore := NewManifestStore(backend)
	if err := specStore.Put(ctx, table, spec); err != nil {
		t.Fatalf("put spec: %v", err)
	}
	if err := manifestStore.Put(ctx, table, seedManifest); err != nil {
		t.Fatalf("seed manifest: %v", err)
	}

	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		Manifests:   manifestStore,
		LocalTmpDir: filepath.Join(dir, "_tmp"),
	}

	sched := &Scheduler{
		Publisher:     pub,
		SpecStore:     specStore,
		ManifestStore: manifestStore,
		Tables:        []string{table},
		Tiers:         []Tier{Tier1h, Tier1d, Tier1w, Tier1mo},
		GraceWindow:   15 * time.Minute,
		BuildArgsFor: map[string]BuildArgs{
			table: {
				Source:     "evt",
				TimeColumn: "time",
				MetricCols: []MetricCol{{Name: "m", Numeric: true}},
			},
		},
		Logger: zerolog.Nop(),
		SourceWatermark: func(_ context.Context, _ string) (time.Time, error) {
			return srcWM, nil
		},
	}

	sched.runOnce(ctx)

	m, err := manifestStore.Get(ctx, table)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}

	// 1d.by_dim_a should have built (1h.by_dim_a watermark covers the full day).
	entries1dDim := m.FilesForTierVariant("1d", "by_dim_a")
	if len(entries1dDim) == 0 {
		t.Errorf("expected 1d.by_dim_a to build (gated on 1h.by_dim_a), got 0 entries")
	}

	// 1d.sketch should NOT have built (1h.sketch watermark is zero).
	entries1dSketch := m.FilesForTierVariant("1d", "sketch")
	if len(entries1dSketch) != 0 {
		t.Errorf("expected 1d.sketch to be gated on 1h.sketch (zero); got %d entries", len(entries1dSketch))
	}
}

// TestScheduler_GracefullySkipsTableWithMissingSpec verifies that the
// scheduler logs a warning and skips a table with no spec, while still
// building other tables that have specs.
func TestScheduler_GracefullySkipsTableWithMissingSpec(t *testing.T) {
	ctx := context.Background()
	goodTable := "good"
	badTable := "x"

	// Seed good table's watermark at 01:00 so it can build one bucket at src 02:15.
	seedManifest := &Manifest{
		Table:      goodTable,
		Generation: 1,
		Watermarks: map[string]time.Time{
			"1h.sketch": time.Date(2026, 5, 1, 1, 0, 0, 0, time.UTC),
		},
	}
	srcWM := time.Date(2026, 5, 1, 2, 15, 0, 0, time.UTC)

	spec := Spec{Table: goodTable, TZ: "UTC", TimeColumn: "time"}

	// Only register spec for goodTable; badTable has none.
	sched, ms := newTestScheduler(t,
		map[string]time.Time{goodTable: srcWM, badTable: srcWM},
		[]string{badTable, goodTable},
		map[string]Spec{goodTable: spec},
		map[string]*Manifest{goodTable: seedManifest},
	)

	// Should not panic or crash.
	sched.runOnce(ctx)

	// Good table should have advanced.
	m, err := ms.Get(ctx, goodTable)
	if err != nil {
		t.Fatalf("get manifest for good table: %v", err)
	}
	wm := m.Watermark("1h", "sketch")
	want := time.Date(2026, 5, 1, 2, 0, 0, 0, time.UTC)
	if !wm.Equal(want) {
		t.Errorf("good table 1h watermark = %v, want %v", wm, want)
	}
}

// TestScheduler_MetricsWatermarkLag verifies that SetMaxWatermarkLagSeconds is
// called after each tickTable and reflects the worst now-watermark lag.
func TestScheduler_MetricsWatermarkLag(t *testing.T) {
	ctx := context.Background()
	table := "default.events"

	// fixedNow is 2 hours ahead of wmTime so the expected lag is ~7200s.
	// Seed all four tier watermarks at wmTime so the scheduler finds nothing
	// to build and the watermarks remain stable throughout the tick.
	wmTime := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	fixedNow := wmTime.Add(2 * time.Hour) // 10:00

	seedManifest := &Manifest{
		Table:      table,
		Generation: 1,
		Watermarks: map[string]time.Time{
			"1h.sketch":  wmTime,
			"1d.sketch":  wmTime,
			"1w.sketch":  wmTime,
			"1mo.sketch": wmTime,
		},
	}

	// srcWM just ahead of fixedNow so no bucket qualifies as sealed
	// (nextEnd + grace > effectiveMax for any next bucket from wmTime).
	srcWM := fixedNow.Add(time.Minute)

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, _ := newTestScheduler(t,
		map[string]time.Time{table: srcWM},
		[]string{table},
		map[string]Spec{table: spec},
		map[string]*Manifest{table: seedManifest},
	)

	sink := &mockSink{}
	sched.Metrics = sink
	sched.Now = func() time.Time { return fixedNow }

	sched.runOnce(ctx)

	if sink.maxWatermarkLag <= 0 {
		t.Errorf("maxWatermarkLag = %d, want > 0", sink.maxWatermarkLag)
	}
	// All four watermarks are at wmTime, lag = 2h = 7200s.
	if sink.maxWatermarkLag < 7000 || sink.maxWatermarkLag > 7400 {
		t.Errorf("maxWatermarkLag = %d seconds, expected ~7200 (2h)", sink.maxWatermarkLag)
	}
}

func TestScheduler_AutoClassifiesOnMissingSpec(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, dim_a VARCHAR, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES
		('2026-05-10 00:00:00+00','val_a',1.0),
		('2026-05-10 01:00:00+00','val_b',2.0)`); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	specStore := NewSpecStore(backend)
	manStore := NewManifestStore(backend)
	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		Manifests:   manStore,
		LocalTmpDir: filepath.Join(dir, "_tmp"),
	}

	s := &Scheduler{
		Publisher:     pub,
		SpecStore:     specStore,
		ManifestStore: manStore,
		Tables:        []string{"test.evt"},
		Tiers:         []Tier{Tier1h},
		BuildArgsFor: map[string]BuildArgs{
			"test.evt": {Source: "evt", TimeColumn: "time"},
		},
		ClassifierConfigFor: map[string]ClassifierConfig{
			"test.evt": {Source: "SELECT * FROM evt", DimColumns: []string{"dim_a"}},
		},
		Now: func() time.Time { return time.Date(2026, 5, 10, 2, 0, 0, 0, time.UTC) },
		SourceWatermark: func(ctx context.Context, table string) (time.Time, error) {
			return time.Date(2026, 5, 10, 2, 0, 0, 0, time.UTC), nil
		},
		DimRichCap: 100,
		Logger:     zerolog.Nop(),
	}

	if _, err := specStore.Get(ctx, "test.evt"); err == nil {
		t.Fatal("expected spec missing before tick")
	}

	s.runOnce(ctx)

	got, err := specStore.Get(ctx, "test.evt")
	if err != nil {
		t.Fatalf("spec should exist after auto-classify: %v", err)
	}
	if _, ok := got.Dims["dim_a"]; !ok {
		t.Errorf("spec missing dim_a after auto-classify: %+v", got.Dims)
	}
}

func TestScheduler_NoAutoClassifyWithoutConfig(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	specStore := NewSpecStore(backend)
	manStore := NewManifestStore(backend)

	s := &Scheduler{
		Publisher:           &Publisher{},
		SpecStore:           specStore,
		ManifestStore:       manStore,
		Tables:              []string{"unconfigured.tbl"},
		Tiers:               []Tier{Tier1h},
		BuildArgsFor:        map[string]BuildArgs{},
		ClassifierConfigFor: map[string]ClassifierConfig{},
		Now:                 func() time.Time { return time.Now() },
		SourceWatermark: func(ctx context.Context, table string) (time.Time, error) {
			return time.Now(), nil
		},
		Logger: zerolog.Nop(),
	}

	s.runOnce(ctx)

	if _, err := specStore.Get(ctx, "unconfigured.tbl"); err == nil {
		t.Error("spec should still be missing")
	}
}

func TestScheduler_DiscoverStringColumns(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, dim_a VARCHAR, dim_b VARCHAR, x DOUBLE, user_id VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES ('2026-05-10 00:00:00+00','va','vb',1.0,'u1')`); err != nil {
		t.Fatal(err)
	}

	backend, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	pub := &Publisher{DB: db, Backend: backend, Manifests: NewManifestStore(backend), BuilderVersion: "test"}
	s := &Scheduler{Publisher: pub, Logger: zerolog.Nop()}

	got, err := s.discoverStringColumns(ctx, "SELECT * FROM evt", []string{"user_id"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"dim_a": true, "dim_b": true}
	if len(got) != 2 {
		t.Errorf("got %v, want 2 cols (dim_a, dim_b)", got)
	}
	for _, c := range got {
		if !want[c] {
			t.Errorf("unexpected column %q in result", c)
		}
	}
}

func TestScheduler_AutoClassifyDerivesWhenConfigEmpty(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, dim_a VARCHAR, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES ('2026-05-10 00:00:00+00','va',1.0)`); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	specStore := NewSpecStore(backend)
	manStore := NewManifestStore(backend)
	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		Manifests:   manStore,
		LocalTmpDir: filepath.Join(dir, "_tmp"),
	}

	s := &Scheduler{
		Publisher:     pub,
		SpecStore:     specStore,
		ManifestStore: manStore,
		Tables:        []string{"test.evt"},
		Tiers:         []Tier{Tier1h},
		BuildArgsFor: map[string]BuildArgs{
			"test.evt": {Source: "evt", TimeColumn: "time"},
		},
		ClassifierConfigFor: map[string]ClassifierConfig{
			"test.evt": {Source: "SELECT * FROM evt"},
		},
		Now: func() time.Time { return time.Date(2026, 5, 10, 2, 0, 0, 0, time.UTC) },
		SourceWatermark: func(ctx context.Context, table string) (time.Time, error) {
			return time.Date(2026, 5, 10, 2, 0, 0, 0, time.UTC), nil
		},
		DimRichCap: 100,
		Logger:     zerolog.Nop(),
	}

	s.runOnce(ctx)

	got, err := specStore.Get(ctx, "test.evt")
	if err != nil {
		t.Fatalf("spec missing after auto-derive: %v", err)
	}
	if _, ok := got.Dims["dim_a"]; !ok {
		t.Errorf("auto-discovery should have included dim_a")
	}
}

func TestScheduler_AutoDiscoverSkipsForceSketch(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, dim_a VARCHAR, user_id VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES ('2026-05-10 00:00:00+00','x','u1')`); err != nil {
		t.Fatal(err)
	}

	backend, _ := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	pub := &Publisher{DB: db, Backend: backend, Manifests: NewManifestStore(backend), BuilderVersion: "test"}
	s := &Scheduler{Publisher: pub, Logger: zerolog.Nop()}

	got, err := s.discoverStringColumns(ctx, "SELECT * FROM evt", []string{"user_id"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range got {
		if c == "user_id" {
			t.Error("user_id should be skipped via the skip list")
		}
	}
}

func TestScheduler_AutoClassifyForceSketchExcludedFromDiscovery(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, dim_a VARCHAR, user_id VARCHAR, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES ('2026-05-10 00:00:00+00','va','u1',1.0)`); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	specStore := NewSpecStore(backend)
	manStore := NewManifestStore(backend)
	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		Manifests:   manStore,
		LocalTmpDir: filepath.Join(dir, "_tmp"),
	}

	s := &Scheduler{
		Publisher:     pub,
		SpecStore:     specStore,
		ManifestStore: manStore,
		Tables:        []string{"test.evt"},
		Tiers:         []Tier{Tier1h},
		BuildArgsFor: map[string]BuildArgs{
			"test.evt": {Source: "evt", TimeColumn: "time"},
		},
		ClassifierConfigFor: map[string]ClassifierConfig{
			"test.evt": {
				Source:      "SELECT * FROM evt",
				ForceSketch: []string{"user_id"},
			},
		},
		Now: func() time.Time { return time.Date(2026, 5, 10, 2, 0, 0, 0, time.UTC) },
		SourceWatermark: func(ctx context.Context, table string) (time.Time, error) {
			return time.Date(2026, 5, 10, 2, 0, 0, 0, time.UTC), nil
		},
		DimRichCap: 100,
		Logger:     zerolog.Nop(),
	}

	s.runOnce(ctx)

	got, err := specStore.Get(ctx, "test.evt")
	if err != nil {
		t.Fatalf("spec missing after auto-classify: %v", err)
	}
	// user_id must appear as Sketch (set by ForceSketch post-processing), not as a classified Dim.
	d, ok := got.Dims["user_id"]
	if !ok {
		t.Fatal("user_id should be in spec (as Sketch via ForceSketch)")
	}
	if d.Role != "Sketch" {
		t.Errorf("user_id.Role = %q, want Sketch", d.Role)
	}
	// dim_a should have been classified normally.
	if _, ok := got.Dims["dim_a"]; !ok {
		t.Errorf("dim_a should be in spec after auto-classify")
	}
}

func TestBuildDateScopedSource_BuildsPerDayGlobs(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	got := buildDateScopedSource("bucket", "default", "downloads", 3, now, "FALLBACK")
	for _, want := range []string{
		"'s3://bucket/default/downloads/2026/05/18/**/*.parquet'",
		"'s3://bucket/default/downloads/2026/05/17/**/*.parquet'",
		"'s3://bucket/default/downloads/2026/05/16/**/*.parquet'",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
	if !strings.Contains(got, "union_by_name=true") {
		t.Error("missing union_by_name=true")
	}
}

func TestBuildDateScopedSource_FallsBackOnEmptyBucket(t *testing.T) {
	got := buildDateScopedSource("", "default", "downloads", 3, time.Now(), "FALLBACK")
	if got != "FALLBACK" {
		t.Errorf("got %q want FALLBACK", got)
	}
}
