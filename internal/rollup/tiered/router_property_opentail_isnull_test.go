package tiered

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Open-tail + IS NOT NULL: rollup day classifies NULL → '_null_'
// (because _null_ is in KeptValues), then the open-tail source day
// must apply the same semantics via a CASE expression. The fresh CTE
// runs in SourceMode where the col_mode helper synthesises the
// classification — this test pins that bridge.
func TestProperty_OpenTail_IsNotNullFilter(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR)`); err != nil {
		t.Fatal(err)
	}

	day1 := time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	for _, dayStart := range []time.Time{day1, day2} {
		for hour := 0; hour < 24; hour++ {
			t0 := dayStart.Add(time.Duration(hour) * time.Hour)
			// 2 kept rows ("a") + 1 null per hour per day.
			for j := 0; j < 2; j++ {
				if _, err := db.Exec(`INSERT INTO events VALUES (?, 'a')`, t0.Add(time.Duration(j)*time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Exec(`INSERT INTO events VALUES (?, NULL)`, t0.Add(2*time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
	}

	tmp := t.TempDir()
	spec := &Spec{
		Table:      "test.events",
		TZ:         "UTC",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"site": {Role: "PerDim", KeptValues: []string{"a", "_null_"}, EffectiveCard: 2},
		},
	}
	specHash, _ := spec.SchemaHash()

	// Roll up day1 only — day2 stays as source.
	tierDir := filepath.Join(tmp, "_arc/rollup/test/events/1h/2026/05/14")
	if err := os.MkdirAll(tierDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bld := &Builder{
		DB: db, HLLLgK: 14, KLLk: 200, SchemaHash: specHash,
		TierTZ: "UTC", BuilderVersion: "test",
		BucketLo: day1, BucketHi: day2,
	}
	filteredSrc := fmt.Sprintf(
		"(SELECT * FROM events WHERE time >= TIMESTAMP '%s' AND time < TIMESTAMP '%s') AS day",
		day1.Format("2006-01-02 15:04:05+00"),
		day2.Format("2006-01-02 15:04:05+00"))
	produced, err := bld.BuildAllVariants(ctx, BuildArgs{Tier: Tier1h, Source: filteredSrc}, spec, 100, tierDir)
	if err != nil {
		t.Fatal(err)
	}
	bySrc := produced["by_site"]
	variantDir := filepath.Join(tierDir, "by_site")
	if err := os.MkdirAll(variantDir, 0o755); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(variantDir, "f.parquet")
	if err := os.Rename(bySrc, final); err != nil {
		t.Fatal(err)
	}
	idx := &MemoryFileIndex{Paths: []string{"_arc/rollup/test/events/1h/2026/05/14/by_site/f.parquet"}}

	userSQL := `SELECT date_trunc('hour', time) AS h, COUNT(*) AS c FROM events
		 WHERE time >= TIMESTAMP '2026-05-14 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-16 00:00:00+00'
		   AND site IS NOT NULL
		 GROUP BY 1`

	truth := runRowSet(t, db, userSQL)

	var refusalLog strings.Builder
	logger := zerolog.New(&refusalLog).With().Timestamp().Logger()
	rewritten, ok := Rewrite(ctx, userSQL, RewriteDeps{
		DB: db, Files: idx, Spec: spec, DimRichCap: 100, GraceWindow: 0,
		Logger: logger, StoragePrefix: tmp + "/",
	})
	if !ok {
		t.Fatalf("router refused (expected acceptance with open tail).\nrefusal log: %s", refusalLog.String())
	}
	if !strings.Contains(rewritten, "fresh AS") {
		t.Fatalf("expected open-tail fresh CTE in rewrite:\n%s", rewritten)
	}

	got := runRowSet(t, db, rewritten)
	if !equalRowSets(truth, got) {
		t.Errorf("row sets differ (open-tail IS NOT NULL).\nrewritten:\n%s\nsource truth (%d rows):\n%s\nrollup (%d rows):\n%s",
			rewritten, len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}

// Open-tail + NOT IN filter — different from existing closed-window
// NotIn test in that the source-side CTE must also apply NOT IN against
// the raw column (not the _class column). This was the second bug class.
func TestProperty_OpenTail_NotInFilter(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR)`); err != nil {
		t.Fatal(err)
	}

	day1 := time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	for _, dayStart := range []time.Time{day1, day2} {
		for hour := 0; hour < 24; hour++ {
			t0 := dayStart.Add(time.Duration(hour) * time.Hour)
			for _, s := range []string{"a", "b", "c"} {
				if _, err := db.Exec(`INSERT INTO events VALUES (?, ?)`, t0, s); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	tmp := t.TempDir()
	spec := &Spec{
		Table: "test.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"site": {Role: "PerDim", KeptValues: []string{"a", "b", "c"}, EffectiveCard: 3},
		},
	}
	specHash, _ := spec.SchemaHash()
	tierDir := filepath.Join(tmp, "_arc/rollup/test/events/1h/2026/05/14")
	if err := os.MkdirAll(tierDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bld := &Builder{
		DB: db, HLLLgK: 14, KLLk: 200, SchemaHash: specHash,
		TierTZ: "UTC", BuilderVersion: "test",
		BucketLo: day1, BucketHi: day2,
	}
	filteredSrc := fmt.Sprintf(
		"(SELECT * FROM events WHERE time >= TIMESTAMP '%s' AND time < TIMESTAMP '%s') AS day",
		day1.Format("2006-01-02 15:04:05+00"),
		day2.Format("2006-01-02 15:04:05+00"))
	produced, err := bld.BuildAllVariants(ctx, BuildArgs{Tier: Tier1h, Source: filteredSrc}, spec, 100, tierDir)
	if err != nil {
		t.Fatal(err)
	}
	bySrc := produced["by_site"]
	variantDir := filepath.Join(tierDir, "by_site")
	if err := os.MkdirAll(variantDir, 0o755); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(variantDir, "f.parquet")
	if err := os.Rename(bySrc, final); err != nil {
		t.Fatal(err)
	}
	idx := &MemoryFileIndex{Paths: []string{"_arc/rollup/test/events/1h/2026/05/14/by_site/f.parquet"}}

	userSQL := `SELECT date_trunc('hour', time) AS h, COUNT(*) AS c FROM events
		 WHERE time >= TIMESTAMP '2026-05-14 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-16 00:00:00+00'
		   AND site NOT IN ('b')
		 GROUP BY 1`

	truth := runRowSet(t, db, userSQL)
	rewritten := mustRewriteAccept(t, ctx, db, idx, spec, tmp, userSQL)
	if !strings.Contains(rewritten, "fresh AS") {
		t.Fatalf("expected open-tail fresh CTE: %s", rewritten)
	}
	got := runRowSet(t, db, rewritten)
	if !equalRowSets(truth, got) {
		t.Errorf("row sets differ.\nrewritten:\n%s\nsource truth (%d rows):\n%s\nrollup (%d rows):\n%s",
			rewritten, len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}
