package tiered

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

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
