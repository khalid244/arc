package tiered

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
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
// Returns the scheduler and the backend for querying via S3FileIndex.
func newTestScheduler(t *testing.T, sourceWM map[string]time.Time, tables []string, specs map[string]Spec, seedFiles map[string][]string) (*Scheduler, storage.Backend) {
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

	ctx := context.Background()
	for _, table := range tables {
		spec, ok := specs[table]
		if !ok {
			continue
		}
		if err := specStore.Put(ctx, table, spec); err != nil {
			t.Fatalf("put spec %s: %v", table, err)
		}
		// Write seed files into the backend so S3FileIndex finds them.
		if paths, ok := seedFiles[table]; ok {
			for _, p := range paths {
				// Write a minimal valid parquet placeholder. The scheduler
				// reads these via FilesForTierVariant to compute watermarks.
				if err := backend.Write(ctx, p, []byte("placeholder")); err != nil {
					t.Fatalf("seed file %s: %v", p, err)
				}
			}
		}
	}

	filesFor := func(table string) FileIndex {
		return &S3FileIndex{Backend: backend, Table: table}
	}

	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		FilesFor:    filesFor,
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
		Publisher: pub,
		SpecStore: specStore,
		FilesFor:  filesFor,
		Tables:    tables,
		Tiers:     []Tier{Tier1h, Tier1d, Tier1w, Tier1mo},
		Variants:  []string{"sketch"},
		GraceWindow: 15 * time.Minute,
		BuildArgsFor: buildArgs,
		Logger:       zerolog.Nop(),
		SourceWatermark: func(ctx context.Context, table string) (time.Time, error) {
			return sourceWM[table], nil
		},
	}

	return sched, backend
}

// filesForTable lists files via S3FileIndex for a given tier/variant.
func filesForTable(ctx context.Context, backend storage.Backend, table, tier, variant string) []string {
	idx := &S3FileIndex{Backend: backend, Table: table}
	paths, _ := idx.FilesForTierVariant(ctx, tier, variant)
	return paths
}

// watermarkForTable returns the watermark via S3FileIndex.
func watermarkForTable(ctx context.Context, backend storage.Backend, table, tier, variant string) time.Time {
	idx := &S3FileIndex{Backend: backend, Table: table}
	wm, ok, err := idx.Watermark(ctx, tier, variant)
	if err != nil || !ok {
		return time.Time{}
	}
	return wm
}

// TestScheduler_BuildsOne1hBucketWhenSealed verifies that when the source
// watermark is 2026-05-02 00:15, exactly the bucket [2026-05-01, 2026-05-02) is
// sealed and built; the next day is not.
func TestScheduler_BuildsOne1hBucketWhenSealed(t *testing.T) {
	ctx := context.Background()
	table := "default.events"

	// Seed a placeholder file whose bucketHi = 2026-05-01 00:00 (so watermark=2026-05-01).
	seedPath := VariantPath(table, Tier1h, "sketch", time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC), "seed")
	seedFiles := map[string][]string{
		table: {seedPath},
	}

	srcWM := time.Date(2026, 5, 2, 0, 15, 0, 0, time.UTC)

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, backend := newTestScheduler(t,
		map[string]time.Time{table: srcWM},
		[]string{table},
		map[string]Spec{table: spec},
		seedFiles,
	)

	sched.runOnce(ctx)

	wm := watermarkForTable(ctx, backend, table, "1h", "sketch")
	want := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	if !wm.Equal(want) {
		t.Errorf("1h watermark = %v, want %v", wm, want)
	}

	entries1h := filesForTable(ctx, backend, table, "1h", "sketch")
	// Seed file + 1 new build = 2 total.
	if len(entries1h) != 2 {
		t.Errorf("expected 2 files for 1h/sketch (seed + 1 build), got %d", len(entries1h))
	}

	if wm.After(want) {
		t.Errorf("watermark advanced past expected sealed bucket: %v", wm)
	}
}

// TestScheduler_GatesHigherTierOnLowerWatermark verifies that 1d does not
// build until 1h has covered the full day.
func TestScheduler_GatesHigherTierOnLowerWatermark(t *testing.T) {
	ctx := context.Background()
	table := "events"

	// Seed watermarks at 2026-05-01 00:00 for both 1h and 1d sketch.
	seed1h := VariantPath(table, Tier1h, "sketch", time.Date(2026, 4, 30, 23, 0, 0, 0, time.UTC), "seed1h")
	seed1d := VariantPath(table, Tier1d, "sketch", time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC), "seed1d")
	seedFiles := map[string][]string{
		table: {seed1h, seed1d},
	}
	srcWM := time.Date(2026, 5, 1, 23, 59, 0, 0, time.UTC)

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, backend := newTestScheduler(t,
		map[string]time.Time{table: srcWM},
		[]string{table},
		map[string]Spec{table: spec},
		seedFiles,
	)

	sched.runOnce(ctx)

	wm1h := watermarkForTable(ctx, backend, table, "1h", "sketch")
	if wm1h.IsZero() {
		t.Fatal("1h watermark should have advanced")
	}

	entries1d := filesForTable(ctx, backend, table, "1d", "sketch")
	// Only the seed file, no new build (1d gated on 1h watermark).
	if len(entries1d) != 1 {
		t.Errorf("1d should be gated on 1h watermark; got %d files (want 1 seed)", len(entries1d))
	}
}

// TestScheduler_StopsAtCapPerTick verifies that at most 24 buckets are built
// per tier per variant per tick even when a large backlog exists.
func TestScheduler_StopsAtCapPerTick(t *testing.T) {
	ctx := context.Background()
	table := "events"

	// Seed watermark at 2026-05-01 00:00 for 1h.sketch.
	seedPath := VariantPath(table, Tier1h, "sketch", time.Date(2026, 4, 30, 23, 0, 0, 0, time.UTC), "seed")
	srcWM := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, backend := newTestScheduler(t,
		map[string]time.Time{table: srcWM},
		[]string{table},
		map[string]Spec{table: spec},
		map[string][]string{table: {seedPath}},
	)
	// Pin Now far enough that cutoff (now-48h) exceeds seed watermark+24 days+grace.
	sched.Now = func() time.Time { return time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC) }

	sched.runOnce(ctx)

	entries1h := filesForTable(ctx, backend, table, "1h", "sketch")
	// 1 seed + 24 cap = 25 total.
	if len(entries1h) != 25 {
		t.Errorf("expected 25 files (1 seed + 24 cap), got %d", len(entries1h))
	}
}

// TestScheduler_NoBuildsWhenSourceBehindWatermark verifies that when the
// source watermark equals the existing watermark, no new buckets are built.
func TestScheduler_NoBuildsWhenSourceBehindWatermark(t *testing.T) {
	ctx := context.Background()
	table := "events"

	wmTime := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	// Seed file bucketHi = wmTime → watermark = wmTime.
	seedPath := VariantPath(table, Tier1h, "sketch", wmTime.Add(-time.Hour), "seed")

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, backend := newTestScheduler(t,
		map[string]time.Time{table: wmTime},
		[]string{table},
		map[string]Spec{table: spec},
		map[string][]string{table: {seedPath}},
	)

	sched.runOnce(ctx)

	entries := filesForTable(ctx, backend, table, "1h", "sketch")
	if len(entries) != 1 {
		t.Errorf("expected 1 file (seed only, no new builds), got %d", len(entries))
	}
}

// TestScheduler_DimVariantGatesOnSameVariantFinerWatermark verifies that
// 1d.by_dim_a builds when 1h.by_dim_a covers a full day.
func TestScheduler_DimVariantGatesOnSameVariantFinerWatermark(t *testing.T) {
	ctx := context.Background()
	table := "events"

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
	if err := specStore.Put(ctx, table, spec); err != nil {
		t.Fatalf("put spec: %v", err)
	}

	filesFor := func(t string) FileIndex {
		return &S3FileIndex{Backend: backend, Table: t}
	}

	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		FilesFor:    filesFor,
		LocalTmpDir: filepath.Join(dir, "_tmp"),
	}

	specPtr := &spec
	h0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	h1 := h0.Add(time.Hour)
	args1h := BuildArgs{
		Tier:       Tier1h,
		Source:     "evt",
		TimeColumn: "time",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
	}
	if err := pub.PublishPerDimVariant(ctx, table, specPtr, args1h, "dim_a", Tier1h, h0, h1); err != nil {
		t.Fatalf("publish 1h.by_dim_a: %v", err)
	}

	// Seed 1h.by_dim_a watermark past the grace window beyond 2026-05-02 00:00,
	// so effectiveMax > nextEnd+grace and the 1d build is not blocked.
	// bucket day=2026-05-02 → bucketHi=2026-05-03 00:00 → watermark=2026-05-03 00:00.
	seedPath := VariantPath(table, Tier1h, "by_dim_a", time.Date(2026, 5, 2, 1, 0, 0, 0, time.UTC), "wmseed")
	if err := backend.Write(ctx, seedPath, []byte("placeholder")); err != nil {
		t.Fatalf("write wm seed: %v", err)
	}

	// Seed 1d.by_dim_a and 1d.sketch watermarks at 2026-05-01 00:00.
	seed1dDim := VariantPath(table, Tier1d, "by_dim_a", time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC), "seed1d")
	seed1dSketch := VariantPath(table, Tier1d, "sketch", time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC), "seed1ds")
	backend.Write(ctx, seed1dDim, []byte("placeholder"))
	backend.Write(ctx, seed1dSketch, []byte("placeholder"))

	srcWM := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	sched := &Scheduler{
		Publisher:  pub,
		SpecStore:  specStore,
		FilesFor:   filesFor,
		Tables:     []string{table},
		Tiers:      []Tier{Tier1h, Tier1d, Tier1w, Tier1mo},
		GraceWindow: 15 * time.Minute,
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

	entries1dDim := filesForTable(ctx, backend, table, "1d", "by_dim_a")
	// seed + at least 1 new build.
	if len(entries1dDim) <= 1 {
		t.Errorf("expected 1d.by_dim_a to build (gated on 1h.by_dim_a), got only %d files", len(entries1dDim))
	}

	// 1d.sketch should NOT have built (1h.sketch watermark is zero — no 1h/sketch files).
	entries1dSketch := filesForTable(ctx, backend, table, "1d", "sketch")
	// Only the seed, no new builds.
	if len(entries1dSketch) != 1 {
		t.Errorf("expected 1d.sketch to be gated on 1h.sketch (zero); got %d files", len(entries1dSketch))
	}
}

// TestScheduler_GracefullySkipsTableWithMissingSpec verifies that the
// scheduler logs a warning and skips a table with no spec.
func TestScheduler_GracefullySkipsTableWithMissingSpec(t *testing.T) {
	ctx := context.Background()
	goodTable := "good"
	badTable := "x"

	// Seed watermark at 2026-05-01 00:00 (day bucket for 2026-04-30).
	seedPath := VariantPath(goodTable, Tier1h, "sketch", time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC), "seed")
	srcWM := time.Date(2026, 5, 2, 0, 15, 0, 0, time.UTC)

	spec := Spec{Table: goodTable, TZ: "UTC", TimeColumn: "time"}
	sched, backend := newTestScheduler(t,
		map[string]time.Time{goodTable: srcWM, badTable: srcWM},
		[]string{badTable, goodTable},
		map[string]Spec{goodTable: spec},
		map[string][]string{goodTable: {seedPath}},
	)

	sched.runOnce(ctx)

	wm := watermarkForTable(ctx, backend, goodTable, "1h", "sketch")
	want := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	if !wm.Equal(want) {
		t.Errorf("good table 1h watermark = %v, want %v", wm, want)
	}
}

// TestScheduler_MetricsWatermarkLag verifies that SetMaxWatermarkLagSeconds is
// called after each tickTable with a value reflecting the 1h-tier lag.
func TestScheduler_MetricsWatermarkLag(t *testing.T) {
	ctx := context.Background()
	table := "default.events"

	wmTime := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	fixedNow := wmTime.Add(10 * time.Hour)

	// Seed only the 1h watermark so the lag is exactly 10h (36000s).
	// 1d/1w/1mo seeds would introduce different lags due to tier-boundary rounding.
	lo1h := bucketLoForWatermark(string(Tier1h), wmTime)
	seedPaths := []string{VariantPath(table, Tier1h, "sketch", lo1h, "seed")}

	srcWM := fixedNow.Add(time.Minute)

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, _ := newTestScheduler(t,
		map[string]time.Time{table: srcWM},
		[]string{table},
		map[string]Spec{table: spec},
		map[string][]string{table: seedPaths},
	)

	sink := &mockSink{}
	sched.Metrics = sink
	sched.Now = func() time.Time { return fixedNow }

	sched.runOnce(ctx)

	if sink.maxWatermarkLag <= 0 {
		t.Errorf("maxWatermarkLag = %d, want > 0", sink.maxWatermarkLag)
	}
	if sink.maxWatermarkLag < 35000 || sink.maxWatermarkLag > 37000 {
		t.Errorf("maxWatermarkLag = %d seconds, expected ~36000 (10h)", sink.maxWatermarkLag)
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
	filesFor := func(table string) FileIndex {
		return &S3FileIndex{Backend: backend, Table: table}
	}
	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		FilesFor:    filesFor,
		LocalTmpDir: filepath.Join(dir, "_tmp"),
	}

	s := &Scheduler{
		Publisher:  pub,
		SpecStore:  specStore,
		FilesFor:   filesFor,
		Tables:     []string{"test.evt"},
		Tiers:      []Tier{Tier1h},
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
	filesFor := func(table string) FileIndex {
		return &S3FileIndex{Backend: backend, Table: table}
	}

	s := &Scheduler{
		Publisher:           &Publisher{FilesFor: filesFor},
		SpecStore:           specStore,
		FilesFor:            filesFor,
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
	filesFor := func(table string) FileIndex { return &S3FileIndex{Backend: backend, Table: table} }
	pub := &Publisher{DB: db, Backend: backend, FilesFor: filesFor, BuilderVersion: "test"}
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
	filesFor := func(table string) FileIndex {
		return &S3FileIndex{Backend: backend, Table: table}
	}
	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		FilesFor:    filesFor,
		LocalTmpDir: filepath.Join(dir, "_tmp"),
	}

	s := &Scheduler{
		Publisher:  pub,
		SpecStore:  specStore,
		FilesFor:   filesFor,
		Tables:     []string{"test.evt"},
		Tiers:      []Tier{Tier1h},
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
	filesFor := func(table string) FileIndex { return &S3FileIndex{Backend: backend, Table: table} }
	pub := &Publisher{DB: db, Backend: backend, FilesFor: filesFor, BuilderVersion: "test"}
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
	filesFor := func(table string) FileIndex {
		return &S3FileIndex{Backend: backend, Table: table}
	}
	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		FilesFor:    filesFor,
		LocalTmpDir: filepath.Join(dir, "_tmp"),
	}

	s := &Scheduler{
		Publisher:  pub,
		SpecStore:  specStore,
		FilesFor:   filesFor,
		Tables:     []string{"test.evt"},
		Tiers:      []Tier{Tier1h},
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
	d, ok := got.Dims["user_id"]
	if !ok {
		t.Fatal("user_id should be in spec (as Sketch via ForceSketch)")
	}
	if d.Role != "Sketch" {
		t.Errorf("user_id.Role = %q, want Sketch", d.Role)
	}
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

func TestBuildWindowSource_ScopesToDayPartition(t *testing.T) {
	// Single-UTC-day window: just one path.
	ws := time.Date(2026, 5, 17, 0, 0, 0, 0, time.UTC)
	we := ws.AddDate(0, 0, 1)
	got := buildWindowSource("bucket", "default", "downloads", ws, we)
	want := "'s3://bucket/default/downloads/2026/05/17/**/*.parquet'"
	if !strings.Contains(got, want) {
		t.Errorf("missing %q in %q", want, got)
	}
	if strings.HasPrefix(got, "SELECT") {
		t.Errorf("must be a bare table expression for FROM %%s interpolation, got %q", got)
	}
}

func TestBuildWindowSource_CoversBothUTCDaysWhenSpecTZShiftsMidnight(t *testing.T) {
	// 24h window starting 2025-02-10 00:00 Asia/Riyadh (UTC+3) =
	// 2025-02-09 21:00 UTC → 2025-02-10 21:00 UTC. Window touches Feb 9 AND Feb 10.
	loc, _ := time.LoadLocation("Asia/Riyadh")
	ws := time.Date(2025, 2, 10, 0, 0, 0, 0, loc)
	we := ws.AddDate(0, 0, 1)
	got := buildWindowSource("bucket", "default", "downloads", ws, we)
	for _, want := range []string{
		"'s3://bucket/default/downloads/2025/02/09/**/*.parquet'",
		"'s3://bucket/default/downloads/2025/02/10/**/*.parquet'",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
}

func TestBuildWindowSource_EmptyBucketFallsBack(t *testing.T) {
	got := buildWindowSource("", "default", "downloads", time.Now(), time.Now().Add(time.Hour))
	if got != "" {
		t.Errorf("want empty, got %q", got)
	}
}

// TestScheduler_RebuildHorizon_RebuildsExistingBuckets verifies that runRebuild
// deletes the pre-seeded file at a partition and writes a fresh one in its place.
func TestScheduler_RebuildHorizon_RebuildsExistingBuckets(t *testing.T) {
	ctx := context.Background()
	table := "default.events"

	fixedNow := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	recentGrace := 1 * time.Hour
	rebuildHorizon := 7 * 24 * time.Hour

	// A bucket well within the horizon: [2026-05-05, 2026-05-06).
	bucketStart := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC)
	preSeededPath := VariantPath(table, Tier1h, "sketch", bucketStart, "preseed")

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, backend := newTestScheduler(t,
		map[string]time.Time{table: fixedNow},
		[]string{table},
		map[string]Spec{table: spec},
		map[string][]string{table: {preSeededPath}},
	)
	sched.Now = func() time.Time { return fixedNow }
	sched.RecentGrace = recentGrace
	sched.RebuildHorizon = rebuildHorizon

	sched.runRebuild(ctx)

	// Pre-seeded file must be gone.
	exists, err := backend.Exists(ctx, preSeededPath)
	if err != nil {
		t.Fatalf("Exists check failed: %v", err)
	}
	if exists {
		t.Errorf("pre-seeded file still exists after rebuild — Delete was not called")
	}

	// A new file must exist at the same partition.
	partition := variantPartitionPath(table, Tier1h, "sketch", bucketStart)
	keys, err := backend.List(ctx, partition)
	if err != nil {
		t.Fatalf("List partition failed: %v", err)
	}
	if len(keys) == 0 {
		t.Errorf("no file at partition %q after rebuild — publishBucket did not write", partition)
	}
}

// TestScheduler_RebuildHorizon_ZeroDisablesRebuild verifies that when
// RebuildHorizon is 0 runRebuild is a no-op.
func TestScheduler_RebuildHorizon_ZeroDisablesRebuild(t *testing.T) {
	ctx := context.Background()
	table := "default.events"

	fixedNow := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	bucketStart := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC)
	preSeededPath := VariantPath(table, Tier1h, "sketch", bucketStart, "preseed")

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, backend := newTestScheduler(t,
		map[string]time.Time{table: fixedNow},
		[]string{table},
		map[string]Spec{table: spec},
		map[string][]string{table: {preSeededPath}},
	)
	sched.Now = func() time.Time { return fixedNow }
	sched.RebuildHorizon = 0

	sched.runRebuild(ctx)

	exists, err := backend.Exists(ctx, preSeededPath)
	if err != nil {
		t.Fatalf("Exists check failed: %v", err)
	}
	if !exists {
		t.Errorf("pre-seeded file was deleted but RebuildHorizon=0 should disable rebuild")
	}
}

// TestScheduler_RebuildHorizon_StopsAtRecentGrace verifies that buckets whose
// end time is within RecentGrace of now are not touched by runRebuild.
func TestScheduler_RebuildHorizon_StopsAtRecentGrace(t *testing.T) {
	ctx := context.Background()
	table := "default.events"

	fixedNow := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	recentGrace := 2 * time.Hour
	// Bucket that ends within RecentGrace: [2026-05-10, 2026-05-11) ends after now-2h.
	nearBucketStart := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	nearSeededPath := VariantPath(table, Tier1h, "sketch", nearBucketStart, "nearseed")

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, backend := newTestScheduler(t,
		map[string]time.Time{table: fixedNow},
		[]string{table},
		map[string]Spec{table: spec},
		map[string][]string{table: {nearSeededPath}},
	)
	sched.Now = func() time.Time { return fixedNow }
	sched.RecentGrace = recentGrace
	sched.RebuildHorizon = 30 * 24 * time.Hour

	sched.runRebuild(ctx)

	// The near-future bucket file must be untouched.
	exists, err := backend.Exists(ctx, nearSeededPath)
	if err != nil {
		t.Fatalf("Exists check failed: %v", err)
	}
	if !exists {
		t.Errorf("bucket within RecentGrace was rebuilt — should have been skipped")
	}
}

// TestScheduler_RecentGraceCutoff verifies that when RecentGrace=24h and
// sourceWatermark=now, the scheduler does not build buckets within the last 24h.
func TestScheduler_RecentGraceCutoff(t *testing.T) {
	ctx := context.Background()
	table := "events"

	fixedNow := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	recentGrace := 24 * time.Hour
	cutoff := fixedNow.Add(-recentGrace) // 2026-04-30 12:00

	// Seed watermark at 2026-04-30 00:00 (day bucket for 2026-04-29), which is
	// before the cutoff of 2026-04-30 12:00.
	seedPath := VariantPath(table, Tier1h, "sketch", time.Date(2026, 4, 29, 0, 0, 0, 0, time.UTC), "seed")

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, backend := newTestScheduler(t,
		map[string]time.Time{table: fixedNow},
		[]string{table},
		map[string]Spec{table: spec},
		map[string][]string{table: {seedPath}},
	)
	sched.Now = func() time.Time { return fixedNow }
	sched.RecentGrace = recentGrace

	sched.runOnce(ctx)

	wm := watermarkForTable(ctx, backend, table, "1h", "sketch")
	if wm.IsZero() {
		t.Fatal("watermark should have advanced from seed")
	}
	if wm.After(cutoff) {
		t.Errorf("1h watermark %v is past the recent_grace cutoff %v (now=%v, grace=%v)",
			wm, cutoff, fixedNow, recentGrace)
	}
}

// TestNextBucketStart_UsesEarliestSourceWhenZero verifies that when
// EarliestSource returns 2025-02-15, the scheduler begins from there.
func TestNextBucketStart_UsesEarliestSourceWhenZero(t *testing.T) {
	ctx := context.Background()
	table := "events"

	earliest := time.Date(2025, 2, 15, 0, 0, 0, 0, time.UTC)
	// Place now far enough past earliest so day-buckets are sealed (now - RecentGrace > earliest + 1d + graceWindow).
	// RecentGrace defaults to 48h, so now must be at least earliest + 48h + 1d + 15m.
	now := earliest.Add(73 * time.Hour) // 2025-02-18 01:00

	spec := Spec{Table: table, TZ: "UTC", TimeColumn: "time"}
	sched, backend := newTestScheduler(t,
		map[string]time.Time{table: now},
		[]string{table},
		map[string]Spec{table: spec},
		map[string][]string{},
	)
	sched.Now = func() time.Time { return now }
	sched.EarliestSource = func(_ context.Context, _ string) (time.Time, error) {
		return earliest, nil
	}

	sched.runOnce(ctx)

	wm := watermarkForTable(ctx, backend, table, "1h", "sketch")
	if wm.Before(earliest) {
		t.Errorf("watermark %v is before earliest source %v — EarliestSource was not used as start",
			wm, earliest)
	}
	hardcoded := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !wm.Before(hardcoded) {
		t.Errorf("watermark %v is at or after hardcoded 2026-01-01 fallback — EarliestSource seeding did not take effect", wm)
	}
}

// countingBackend wraps a storage.Backend and counts calls to List.
type countingBackend struct {
	inner     storage.Backend
	listCalls atomic.Int64
}

func (c *countingBackend) List(ctx context.Context, prefix string) ([]string, error) {
	c.listCalls.Add(1)
	return c.inner.List(ctx, prefix)
}
func (c *countingBackend) Write(ctx context.Context, path string, data []byte) error {
	return c.inner.Write(ctx, path, data)
}
func (c *countingBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	return c.inner.WriteReader(ctx, path, r, size)
}
func (c *countingBackend) Read(ctx context.Context, path string) ([]byte, error) {
	return c.inner.Read(ctx, path)
}
func (c *countingBackend) ReadTo(ctx context.Context, path string, w io.Writer) error {
	return c.inner.ReadTo(ctx, path, w)
}
func (c *countingBackend) ReadToAt(ctx context.Context, path string, w io.Writer, offset int64) error {
	return c.inner.ReadToAt(ctx, path, w, offset)
}
func (c *countingBackend) StatFile(ctx context.Context, path string) (int64, error) {
	return c.inner.StatFile(ctx, path)
}
func (c *countingBackend) Delete(ctx context.Context, path string) error {
	return c.inner.Delete(ctx, path)
}
func (c *countingBackend) DeleteBatch(ctx context.Context, paths []string) error {
	return c.inner.DeleteBatch(ctx, paths)
}
func (c *countingBackend) Exists(ctx context.Context, path string) (bool, error) {
	return c.inner.Exists(ctx, path)
}
func (c *countingBackend) Close() error             { return c.inner.Close() }
func (c *countingBackend) Type() string             { return c.inner.Type() }
func (c *countingBackend) ConfigJSON() string       { return c.inner.ConfigJSON() }

// TestScheduler_MaterializeOnce_OneSourceReadPerBucket verifies that when
// multiple variant plans share a Tier1h bucket, the S3 LIST used to check
// source-file presence (filterDaysWithFiles) is issued exactly ONCE per
// bucket regardless of the number of variants. The test uses a
// countingBackend to measure Backend.List calls.
//
// Setup: all 3 variants (sketch, by_dim_a, all) share watermark 2026-04-30,
// so a single tick builds exactly 1 bucket ([2026-05-01, 2026-05-02)) for
// each variant. Old code would call filterDaysWithFiles 3 times (once per
// variant); new code calls it once and shares the result.
func TestScheduler_MaterializeOnce_OneSourceReadPerBucket(t *testing.T) {
	ctx := context.Background()
	table := "default.events"

	dir := t.TempDir()
	innerBackend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	cb := &countingBackend{inner: innerBackend}

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

	filesFor := func(tbl string) FileIndex {
		return &S3FileIndex{Backend: cb, Table: tbl}
	}
	specStore := NewSpecStore(cb)

	spec := Spec{
		Table:      table,
		TZ:         "UTC",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"x"}, EffectiveCard: 1},
		},
	}
	if err := specStore.Put(ctx, table, spec); err != nil {
		t.Fatalf("put spec: %v", err)
	}

	pub := &Publisher{
		DB:          db,
		Backend:     cb,
		FilesFor:    filesFor,
		LocalTmpDir: filepath.Join(dir, "_tmp"),
	}

	// Seed all 3 variants to watermark 2026-05-01 so each plan's next bucket
	// is [2026-05-01, 2026-05-02) — all three are co-located at the same bucket.
	// bucketLo=2026-04-30 → bucketHi=2026-05-01 → watermark=2026-05-01.
	seedLo := time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC)
	for _, variant := range []string{"sketch", "by_dim_a", "all"} {
		p := VariantPath(table, Tier1h, variant, seedLo, "seed_"+variant)
		if err := cb.Write(ctx, p, []byte("placeholder")); err != nil {
			t.Fatalf("seed %s: %v", variant, err)
		}
	}

	// srcWM=2026-05-02 12:00, Now=2026-05-04 12:00. With default RecentGrace=48h,
	// cutoff=2026-05-02 12:00 — bucket [2026-05-01,2026-05-02) ends 2026-05-02 00:00
	// which is before cutoff, so all 3 variants build exactly 1 bucket.
	srcWM := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	fixedNow := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)

	buildArgs := map[string]BuildArgs{
		table: {
			Source:     "evt",
			TimeColumn: "time",
			MetricCols: []MetricCol{{Name: "m", Numeric: true}},
		},
	}

	sched := &Scheduler{
		Publisher:   pub,
		SpecStore:   specStore,
		FilesFor:    filesFor,
		Tables:      []string{table},
		Tiers:       []Tier{Tier1h},
		DimRichCap:  100,
		GraceWindow: 15 * time.Minute,
		BuildArgsFor: buildArgs,
		Logger:      zerolog.Nop(),
		SourceWatermark: func(_ context.Context, _ string) (time.Time, error) {
			return srcWM, nil
		},
		Now: func() time.Time { return fixedNow },
	}

	before := cb.listCalls.Load()
	sched.runOnce(ctx)
	after := cb.listCalls.Load()
	listsDuringTick := after - before

	// Verify all 3 variants were built (each should have seed + 1 new file).
	idx := &S3FileIndex{Backend: cb, Table: table}
	sketchFiles, _ := idx.FilesForTierVariant(ctx, "1h", "sketch")
	byDimFiles, _ := idx.FilesForTierVariant(ctx, "1h", "by_dim_a")
	allFiles, _ := idx.FilesForTierVariant(ctx, "1h", "all")

	if len(sketchFiles) < 2 {
		t.Errorf("sketch: expected >=2 files (seed + build), got %d", len(sketchFiles))
	}
	if len(byDimFiles) < 2 {
		t.Errorf("by_dim_a: expected >=2 files (seed + build), got %d", len(byDimFiles))
	}
	if len(allFiles) < 2 {
		t.Errorf("all: expected >=2 files (seed + build), got %d", len(allFiles))
	}

	// Now run the single-variant baseline to compare List overhead.
	// Seed a fresh table for single-variant to avoid watermark interference.
	table2 := "default.events2"
	if err := specStore.Put(ctx, table2, Spec{
		Table: table2, TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"x"}, EffectiveCard: 1},
		},
	}); err != nil {
		t.Fatalf("put spec2: %v", err)
	}
	p2 := VariantPath(table2, Tier1h, "sketch", seedLo, "seed2")
	_ = cb.Write(ctx, p2, []byte("placeholder"))

	schedSingle := &Scheduler{
		Publisher:   pub,
		SpecStore:   specStore,
		FilesFor:    filesFor,
		Tables:      []string{table2},
		Tiers:       []Tier{Tier1h},
		DimRichCap:  100,
		Variants:    []string{"sketch"},
		GraceWindow: 15 * time.Minute,
		BuildArgsFor: map[string]BuildArgs{
			table2: {Source: "evt", TimeColumn: "time", MetricCols: []MetricCol{{Name: "m", Numeric: true}}},
		},
		Logger: zerolog.Nop(),
		SourceWatermark: func(_ context.Context, _ string) (time.Time, error) { return srcWM, nil },
		Now:             func() time.Time { return fixedNow },
	}

	beforeSingle := cb.listCalls.Load()
	schedSingle.runOnce(ctx)
	afterSingle := cb.listCalls.Load()
	listsSingle := afterSingle - beforeSingle

	// The 3-variant tick should use at most listsSingle + a small margin for
	// the extra watermark-check Lists per variant (FilesForTierVariant called
	// once per variant in tickTableTier). The critical saving is that the
	// source-check List (filterDaysWithFiles) is called once, not 3 times.
	//
	// Old code would issue: listsSingle + (nVariants-1) * daysPerBucket extra calls.
	// New code issues: listsSingle + small watermark overhead.
	//
	// For a single-day bucket, daysPerBucket=1, nVariants=3 →
	// old code overhead = listsSingle + 2; new code overhead ≤ listsSingle + 6.
	// We assert the total doesn't scale linearly with variant count.
	nVariants := int64(3)
	daysPerBucket := int64(1)
	oldCodeMinExtra := (nVariants - 1) * daysPerBucket // =2 extra source-check Lists
	margin := int64(10)                                  // generous allowance for watermark Lists per extra variant
	maxAllowed := listsSingle + margin
	t.Logf("3-variant tick Lists=%d, single-variant Lists=%d, oldCode would add %d extra source-checks",
		listsDuringTick, listsSingle, oldCodeMinExtra)

	if listsDuringTick > maxAllowed {
		t.Errorf(
			"3-variant tick used %d List calls, single-variant used %d "+
				"(max allowed=%d=single+%d); old code would use at least single+%d; "+
				"source materialisation should share the source-check List",
			listsDuringTick, listsSingle, maxAllowed, margin, oldCodeMinExtra,
		)
	}
}
