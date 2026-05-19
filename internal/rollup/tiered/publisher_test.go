package tiered

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func newTestPublisher(t *testing.T, dir string, backend storage.Backend, db interface {
	Exec(string, ...interface{}) (interface{}, error)
}) *Publisher {
	t.Helper()
	return nil
}

func makeFilesFor(backend storage.Backend) func(string) FileIndex {
	return func(table string) FileIndex {
		return &S3FileIndex{Backend: backend, Table: table}
	}
}

func TestPublisher_PublishVariant_WritesFile(t *testing.T) {
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
	pub := &Publisher{
		DB:             db,
		Backend:        backend,
		FilesFor:       makeFilesFor(backend),
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

	idx := &S3FileIndex{Backend: backend, Table: "default.events"}
	paths, err := idx.FilesForTierVariant(ctx, "1h", "sketch")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("expected 1 file in S3, got %d", len(paths))
	}
	_, parsedTier, parsedVariant, _, _, parsedOk := ParseVariantPath(paths[0])
	if !parsedOk || parsedTier != "1h" || parsedVariant != "sketch" {
		t.Errorf("path does not encode 1h/sketch: path=%q tier=%q variant=%q", paths[0], parsedTier, parsedVariant)
	}
	final, err := backend.Read(ctx, paths[0])
	if err != nil {
		t.Fatalf("read final %s: %v", paths[0], err)
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
		FilesFor:    makeFilesFor(backend),
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

	sink := &mockSink{}
	pub := &Publisher{
		DB:          db,
		Backend:     backend,
		FilesFor:    makeFilesFor(backend),
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


func TestPublisher_TwoSequentialPublishesSucceed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())
	db, _ := OpenWithDataSketches("UTC")
	defer db.Close()
	db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, m DOUBLE)`)
	db.Exec(`INSERT INTO evt VALUES ('2026-05-10 00:00:00+00', 1)`)

	pub := &Publisher{
		DB: db, Backend: backend, FilesFor: makeFilesFor(backend),
		BuilderVersion: "v_test", LocalTmpDir: filepath.Join(dir, "_tmp"),
	}
	spec := &Spec{Table: "t", TZ: "UTC", TimeColumn: "time"}
	args := BuildArgs{Tier: Tier1h, Source: "evt", MetricCols: []MetricCol{{Name: "m", Numeric: true}}}
	t1 := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)

	if err := pub.PublishSketchVariant(ctx, "t", spec, args, Tier1h, "sketch", t1, t1.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := pub.PublishSketchVariant(ctx, "t", spec, args, Tier1h, "sketch", t1.Add(time.Hour), t1.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	idx := &S3FileIndex{Backend: backend, Table: "t"}
	paths, _ := idx.FilesForTierVariant(ctx, "1h", "sketch")
	if len(paths) != 2 {
		t.Errorf("expected 2 files after 2 publishes, got %d", len(paths))
	}
}
