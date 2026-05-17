package tiered

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestFileSchemaHash_ReadsStampedHash(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES ('2026-05-10 00:00:00+00', 1.0)`); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(t.TempDir(), "x.parquet")
	b := &Builder{
		DB:             db,
		HLLLgK:         14,
		KLLk:           200,
		SchemaHash:     "deadbeefcafe",
		TierTZ:         "UTC",
		BuilderVersion: "v1",
		BucketLo:       time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
		BucketHi:       time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC),
	}
	if err := b.BuildSketchVariant(ctx, BuildArgs{
		Tier:       Tier1h,
		Source:     "evt",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
	}, out); err != nil {
		t.Fatal(err)
	}

	got, err := FileSchemaHash(ctx, db, out)
	if err != nil {
		t.Fatal(err)
	}
	if got != "deadbeefcafe" {
		t.Errorf("got %q want %q", got, "deadbeefcafe")
	}
}

func TestFileSchemaHash_MissingKVReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES ('2026-05-10 00:00:00+00', 1.0)`); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "no_kv.parquet")
	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	if err := b.BuildSketchVariant(ctx, BuildArgs{
		Tier:       Tier1h,
		Source:     "evt",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
	}, out); err != nil {
		t.Fatal(err)
	}
	got, err := FileSchemaHash(ctx, db, out)
	if err != nil {
		t.Fatalf("err on no-kv file: %v", err)
	}
	if got != "" {
		t.Errorf("expected empty string for missing KV, got %q", got)
	}
}
