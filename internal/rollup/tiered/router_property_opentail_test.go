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

// Open-tail property test: query window extends past the most recent
// rollup file, so the emitter must produce a UNION ALL of the rollup
// CTE (pre-aggregated) and the fresh-source CTE (raw rows from the
// source table) — using SourceMode aggregate/dim translation. Result
// must equal the same query against source-only.
//
// This is the exact shape that caused the `Referenced column "cnt"`
// and `Referenced column "site_class"` outages this session. Locking
// it in here means a regression fails CI rather than prod.

func TestProperty_OpenTail_CountByHourSite_FreshSourceCTE(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	keptSites := []string{"youtu.be", "www.instagram.com", "youtube.com"}

	// Two-day dataset: 5/14 (rolled up) + 5/15 (will be the open tail).
	day1 := time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	for _, dayStart := range []time.Time{day1, day2} {
		for hour := 0; hour < 24; hour++ {
			t0 := dayStart.Add(time.Duration(hour) * time.Hour)
			for i, s := range keptSites {
				n := 3 + hour + i*2
				for j := 0; j < n; j++ {
					ts := t0.Add(time.Duration(j) * time.Minute)
					if _, err := db.Exec(`INSERT INTO events VALUES (?, ?)`, ts, s); err != nil {
						t.Fatal(err)
					}
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
	specHash, err := spec.SchemaHash()
	if err != nil {
		t.Fatal(err)
	}

	// Build the 1h rollup for day1 only. Source rows for day2 stay
	// uncompacted — the emitter must read them from the events table
	// via the open-tail fresh CTE.
	tierDir := filepath.Join(tmp, "_arc/rollup/test/events/1h/2026/05/14")
	if err := os.MkdirAll(tierDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bld := &Builder{
		DB:             db,
		HLLLgK:         14,
		KLLk:           200,
		SchemaHash:     specHash,
		TierTZ:         "UTC",
		BuilderVersion: "test",
		BucketLo:       day1,
		BucketHi:       day2,
	}
	produced, err := bld.BuildAllVariants(ctx, BuildArgs{Tier: Tier1h, Source: "events"}, spec, 100, tierDir)
	if err != nil {
		t.Fatalf("BuildAllVariants: %v", err)
	}
	bySrc := produced["by_site"]
	variantDir := filepath.Join(tierDir, "by_site")
	if err := os.MkdirAll(variantDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bySiteFinal := filepath.Join(variantDir, "f1.parquet")
	if err := os.Rename(bySrc, bySiteFinal); err != nil {
		t.Fatal(err)
	}

	idx := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/test/events/1h/2026/05/14/by_site/f1.parquet",
	}}

	// Query covers both days. With GraceWindow=0 and the rollup's
	// watermark at 5/15 00:00, PickTier returns TailLo = watermark
	// = 5/15 → rollup serves [5/14, 5/15), fresh source [5/15, 5/16).
	userSQL := fmt.Sprintf(
		`SELECT date_trunc('hour', time) AS h, site, COUNT(*) AS c FROM events
		 WHERE time >= TIMESTAMP '2026-05-14 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-16 00:00:00+00'
		   AND site IN (%s)
		 GROUP BY 1, 2`,
		quoteList(keptSites))

	truth := runRowSet(t, db, userSQL)

	var refusalLog strings.Builder
	logger := zerolog.New(&refusalLog).With().Timestamp().Logger()
	rewritten, ok := Rewrite(ctx, userSQL, RewriteDeps{
		DB:            db,
		Files:         idx,
		Spec:          spec,
		DimRichCap:    100,
		GraceWindow:   0,
		Logger:        logger,
		StoragePrefix: tmp + "/",
	})
	if !ok {
		t.Fatalf("router refused (expected acceptance with open tail).\nrefusal log: %s", refusalLog.String())
	}
	if !strings.Contains(rewritten, "fresh AS") {
		t.Fatalf("expected open-tail fresh CTE in rewrite:\n%s", rewritten)
	}
	if !strings.Contains(rewritten, "UNION ALL") {
		t.Fatalf("expected UNION ALL between rollup and fresh CTEs:\n%s", rewritten)
	}
	// Bug-class assertions: fresh CTE must NOT reference rollup-only
	// columns. These were the production-outage signatures.
	freshStart := strings.Index(rewritten, ", fresh AS")
	freshBlock := rewritten[freshStart:]
	if end := strings.Index(freshBlock, "\n)"); end != -1 {
		freshBlock = freshBlock[:end]
	}
	if strings.Contains(freshBlock, "SUM(cnt)") {
		t.Errorf("fresh CTE must use COUNT(*) not SUM(cnt):\n%s", freshBlock)
	}
	if strings.Contains(freshBlock, "site_class IN (") {
		t.Errorf("fresh CTE filter must use `site` not `site_class`:\n%s", freshBlock)
	}

	got := runRowSet(t, db, rewritten)
	if !equalRowSets(truth, got) {
		t.Errorf("row sets differ (open-tail).\nsource truth (%d rows):\n%s\nrewritten (%d rows):\n%s",
			len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}
