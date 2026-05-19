package tiered

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Scalar property test: `SELECT COUNT(*) FROM events WHERE …` with no
// GROUP BY and no time bucket. Exercises the "no-bucket" scalar path
// in emit (the `BucketArg == ""` branch with `emitScalarAggregate`).
func TestProperty_ScalarCountStar(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	keptSites := []string{"a.example", "b.example", "c.example"}
	day := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	for hour := 0; hour < 24; hour++ {
		t0 := day.Add(time.Duration(hour) * time.Hour)
		for i, s := range keptSites {
			n := 2 + hour + i
			for j := 0; j < n; j++ {
				ts := t0.Add(time.Duration(j) * time.Minute)
				if _, err := db.Exec(`INSERT INTO events VALUES (?, ?)`, ts, s); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	tmp := t.TempDir()
	spec := &Spec{
		Table:      "test.events",
		TZ:         "UTC",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"site": {Role: "PerDim", KeptValues: keptSites, EffectiveCard: 3},
		},
	}
	specHash, _ := spec.SchemaHash()

	tierDir := filepath.Join(tmp, "_arc/rollup/test/events/1h/2026/05/15")
	_ = os.MkdirAll(tierDir, 0o755)
	bld := &Builder{
		DB: db, HLLLgK: 14, KLLk: 200, SchemaHash: specHash, TierTZ: "UTC", BuilderVersion: "test",
		BucketLo: day, BucketHi: day.Add(24 * time.Hour),
	}
	produced, err := bld.BuildAllVariants(ctx, BuildArgs{Tier: Tier1h, Source: "events"}, spec, 100, tierDir)
	if err != nil {
		t.Fatal(err)
	}
	sketchSrc := produced["sketch"]
	variantDir := filepath.Join(tierDir, "sketch")
	_ = os.MkdirAll(variantDir, 0o755)
	sketchFinal := filepath.Join(variantDir, "f1.parquet")
	_ = os.Rename(sketchSrc, sketchFinal)

	idx := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/test/events/1h/2026/05/15/sketch/f1.parquet",
	}}

	// Scalar count with no dim filter → sketch variant.
	_ = keptSites // kept around for the dataset, unused in this query
	userSQL := `SELECT COUNT(*) AS c FROM events
		 WHERE time >= TIMESTAMP '2026-05-15 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-16 00:00:00+00'`

	truth := runRowSet(t, db, userSQL)

	var refusalLog strings.Builder
	logger := zerolog.New(&refusalLog).With().Timestamp().Logger()
	rewritten, ok := Rewrite(ctx, userSQL, RewriteDeps{
		DB: db, Files: idx, Spec: spec, DimRichCap: 100, GraceWindow: 0,
		Logger: logger, StoragePrefix: tmp + "/",
	})
	if !ok {
		t.Fatalf("router refused; expected acceptance.\nrefusal log: %s", refusalLog.String())
	}
	got := runRowSet(t, db, rewritten)
	if !equalRowSets(truth, got) {
		t.Errorf("scalar count mismatch.\ntruth: %v\ngot:   %v", truth, got)
	}
}
