package tiered

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestPublisher_PublishVariant_AddsManifestEntry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES ('2026-05-10 00:00:00+00', 1)`); err != nil {
		t.Fatal(err)
	}

	spec := &Spec{Table: "default.events", TZ: "UTC", TimeColumn: "time"}
	sh, _ := spec.SchemaHash()
	pub := &Publisher{
		DB:             db,
		Backend:        backend,
		Manifests:      NewManifestStore(backend),
		BuilderVersion: "v_test",
		LocalTmpDir:    filepath.Join(dir, "_tmp_build"),
	}
	args := BuildArgs{
		Tier:       Tier1h,
		Source:     "evt",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
	}
	bucketLo := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	bucketHi := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC)
	if err := pub.PublishSketchVariant(ctx, "default.events", spec, args, Tier1h, "sketch", bucketLo, bucketHi); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	m, err := pub.Manifests.Get(ctx, "default.events")
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	if len(m.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(m.Entries))
	}
	e := m.Entries[0]
	_, parsedTier, parsedVariant, _, _, parsedOk := ParseVariantPath(e.Path)
	if !parsedOk || parsedTier != "1h" || parsedVariant != "sketch" {
		t.Errorf("entry path does not encode 1h/sketch: path=%q parsed=%v tier=%q variant=%q", e.Path, parsedOk, parsedTier, parsedVariant)
	}
	if e.SchemaHash != sh {
		t.Errorf("schema_hash = %q, want %q", e.SchemaHash, sh)
	}
	// Read the parquet from final path: must exist.
	final, err := backend.Read(ctx, e.Path)
	if err != nil {
		t.Fatalf("read final %s: %v", e.Path, err)
	}
	if len(final) == 0 {
		t.Error("final file is empty")
	}
}

func TestPublisher_MetricsOnSuccess(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())
	db, _ := OpenWithDataSketches("UTC")
	defer db.Close()
	db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, m DOUBLE)`)
	db.Exec(`INSERT INTO evt VALUES ('2026-05-10 00:00:00+00', 1)`)

	sink := &mockSink{}
	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		Manifests:   NewManifestStore(backend),
		LocalTmpDir: t.TempDir(),
		Metrics:     sink,
	}
	spec := &Spec{Table: "t", TZ: "UTC", TimeColumn: "time"}
	args := BuildArgs{Tier: Tier1h, Source: "evt", MetricCols: []MetricCol{{Name: "m", Numeric: true}}}
	t1 := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)

	if err := pub.PublishSketchVariant(ctx, "t", spec, args, Tier1h, "sketch", t1, t1.Add(time.Hour)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if sink.buildSuccess != 1 {
		t.Errorf("buildSuccess = %d, want 1", sink.buildSuccess)
	}
	if sink.buildErrors != 0 {
		t.Errorf("buildErrors = %d, want 0", sink.buildErrors)
	}
	if sink.buildNanos <= 0 {
		t.Error("buildNanos should be > 0")
	}
}

func TestPublisher_MetricsOnBuildError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())
	db, _ := OpenWithDataSketches("UTC")
	defer db.Close()
	// No table created — build will fail.

	sink := &mockSink{}
	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		Manifests:   NewManifestStore(backend),
		LocalTmpDir: t.TempDir(),
		Metrics:     sink,
	}
	spec := &Spec{Table: "t", TZ: "UTC", TimeColumn: "time"}
	args := BuildArgs{Tier: Tier1h, Source: "no_such_table", MetricCols: []MetricCol{{Name: "m", Numeric: true}}}
	t1 := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)

	err := pub.PublishSketchVariant(ctx, "t", spec, args, Tier1h, "sketch", t1, t1.Add(time.Hour))
	if err == nil {
		t.Fatal("expected error when table does not exist")
	}
	if sink.buildErrors != 1 {
		t.Errorf("buildErrors = %d, want 1", sink.buildErrors)
	}
	if sink.buildSuccess != 0 {
		t.Errorf("buildSuccess = %d, want 0", sink.buildSuccess)
	}
}

// TestPublisher_HigherTierReadsPrecalc verifies that for tier > Tier1h the
// publisher reads finer-tier manifest entries rather than the raw source.
// Two hourly tables are built (one row each, different hours), producing two
// 1h/sketch parquets. Then a 1d bucket is published — it must roll them up
// via the manifest (total cnt=2), not re-read the raw source table which is
// replaced with unrelated data before the 1d build.
func TestPublisher_HigherTierReadsPrecalc(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spec := &Spec{Table: "default.events", TZ: "UTC", TimeColumn: "time"}
	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		Manifests:   NewManifestStore(backend),
		LocalTmpDir: filepath.Join(dir, "_tmp_build"),
	}

	h0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h1 := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	h2 := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)

	if _, err := db.Exec(`CREATE TABLE h0_data (time TIMESTAMPTZ, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO h0_data VALUES ('2026-01-01 00:30:00+00', 1.0)`); err != nil {
		t.Fatal(err)
	}
	if err := pub.PublishSketchVariant(ctx, "default.events", spec,
		BuildArgs{Tier: Tier1h, Source: "h0_data", MetricCols: []MetricCol{{Name: "m", Numeric: true}}},
		Tier1h, "sketch", h0, h1); err != nil {
		t.Fatalf("publish 1h bucket 0: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE h1_data (time TIMESTAMPTZ, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO h1_data VALUES ('2026-01-01 01:30:00+00', 2.0)`); err != nil {
		t.Fatal(err)
	}
	if err := pub.PublishSketchVariant(ctx, "default.events", spec,
		BuildArgs{Tier: Tier1h, Source: "h1_data", MetricCols: []MetricCol{{Name: "m", Numeric: true}}},
		Tier1h, "sketch", h1, h2); err != nil {
		t.Fatalf("publish 1h bucket 1: %v", err)
	}

	m, err := pub.Manifests.Get(ctx, "default.events")
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	if len(m.FilesForTierVariant("1h", "sketch")) != 2 {
		t.Fatalf("expected 2 1h entries, got %d", len(m.FilesForTierVariant("1h", "sketch")))
	}

	dayLo := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	dayHi := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

	if _, err := db.Exec(`CREATE TABLE unrelated (time TIMESTAMPTZ, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO unrelated VALUES ('2025-06-01 00:30:00+00', 99.0)`); err != nil {
		t.Fatal(err)
	}
	args1d := BuildArgs{
		Tier:       Tier1d,
		Source:     "unrelated",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
	}
	if err := pub.PublishSketchVariant(ctx, "default.events", spec, args1d, Tier1d, "sketch", dayLo, dayHi); err != nil {
		t.Fatalf("publish 1d bucket: %v", err)
	}

	m2, err := pub.Manifests.Get(ctx, "default.events")
	if err != nil {
		t.Fatalf("get manifest after 1d: %v", err)
	}
	dailyPaths := m2.FilesForTierVariant("1d", "sketch")
	if len(dailyPaths) != 1 {
		t.Fatalf("expected 1 daily entry, got %d", len(dailyPaths))
	}

	localPath := filepath.Join(dir, "daily_check.parquet")
	data, err := backend.Read(ctx, dailyPaths[0])
	if err != nil {
		t.Fatalf("read daily parquet from backend: %v", err)
	}
	if err := os.WriteFile(localPath, data, 0o644); err != nil {
		t.Fatalf("write local copy: %v", err)
	}

	var totalCnt int64
	if err := db.QueryRowContext(ctx, `SELECT SUM(cnt) FROM read_parquet('`+localPath+`')`).Scan(&totalCnt); err != nil {
		t.Fatalf("query daily parquet: %v", err)
	}
	if totalCnt != 2 {
		t.Errorf("daily rollup cnt = %d, want 2 (from 1h precalc, not unrelated source)", totalCnt)
	}
}

func TestPublisher_HigherTierSkipsWhenNoFinerEntries(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, m DOUBLE)`)
	db.Exec(`INSERT INTO evt VALUES ('2026-01-01 00:30:00+00', 1.0)`)

	spec := &Spec{Table: "t", TZ: "UTC", TimeColumn: "time"}
	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		Manifests:   NewManifestStore(backend),
		LocalTmpDir: t.TempDir(),
	}

	dayLo := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	dayHi := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	args := BuildArgs{Tier: Tier1d, Source: "evt", MetricCols: []MetricCol{{Name: "m", Numeric: true}}}

	err = pub.PublishSketchVariant(ctx, "t", spec, args, Tier1d, "sketch", dayLo, dayHi)
	if err != nil {
		t.Fatalf("expected nil when no finer entries, got: %v", err)
	}
	m, _ := pub.Manifests.Get(ctx, "t")
	if m != nil && len(m.FilesForTierVariant("1d", "sketch")) != 0 {
		t.Error("expected no 1d entries when no 1h finer entries exist")
	}
}

func TestPublisher_PublishVariant_RetriesOnStaleManifest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())
	db, _ := OpenWithDataSketches("UTC")
	defer db.Close()
	db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, m DOUBLE)`)
	db.Exec(`INSERT INTO evt VALUES ('2026-05-10 00:00:00+00', 1)`)

	pub := &Publisher{
		DB: db, Backend: backend, Manifests: NewManifestStore(backend),
		BuilderVersion: "v_test", LocalTmpDir: filepath.Join(dir, "_tmp"),
	}
	spec := &Spec{Table: "t", TZ: "UTC", TimeColumn: "time"}
	args := BuildArgs{Tier: Tier1h, Source: "evt", MetricCols: []MetricCol{{Name: "m", Numeric: true}}}
	t1 := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)

	// Two sequential publishes — both should succeed (the publisher's retry handles the generation increment internally).
	if err := pub.PublishSketchVariant(ctx, "t", spec, args, Tier1h, "sketch", t1, t1.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := pub.PublishSketchVariant(ctx, "t", spec, args, Tier1h, "sketch", t1.Add(time.Hour), t1.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	m, _ := pub.Manifests.Get(ctx, "t")
	if len(m.Entries) != 2 {
		t.Errorf("expected 2 entries after 2 publishes, got %d", len(m.Entries))
	}
}
