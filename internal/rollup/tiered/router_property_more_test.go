package tiered

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestProperty_MultiDay_CountByHour exercises the file-window code path
// (FilesForTierVariantWindow) — the rewrite must include only files
// that overlap the query range, and the result must match source.
func TestProperty_MultiDay_CountByHour(t *testing.T) {
	ctx := context.Background()
	db, spec, idx, tmp := setupKept3DayDataset(t, ctx, []string{"a.site", "b.site", "c.site"}, 3)

	userSQL := fmt.Sprintf(
		`SELECT date_trunc('hour', time) AS h, site, COUNT(*) AS c FROM events
		 WHERE time >= TIMESTAMP '2026-05-14 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-17 00:00:00+00'
		   AND site IN (%s)
		 GROUP BY 1, 2`,
		quoteList([]string{"a.site", "b.site", "c.site"}))

	truth := runRowSet(t, db, userSQL)
	rewritten := mustRewriteAccept(t, ctx, db, idx, spec, tmp, userSQL)
	got := runRowSet(t, db, rewritten)
	if !equalRowSets(truth, got) {
		t.Errorf("multi-day mismatch.\ntruth (%d): %s\ngot   (%d): %s",
			len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}

// TestProperty_NotInFilter — `site NOT IN (…)` must match source.
func TestProperty_NotInFilter(t *testing.T) {
	ctx := context.Background()
	keep := []string{"a.site", "b.site", "c.site", "d.site"}
	db, spec, idx, tmp := setupKept3DayDataset(t, ctx, keep, 1)

	userSQL := fmt.Sprintf(
		`SELECT date_trunc('hour', time) AS h, site, COUNT(*) AS c FROM events
		 WHERE time >= TIMESTAMP '2026-05-14 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-15 00:00:00+00'
		   AND site NOT IN (%s)
		 GROUP BY 1, 2`,
		quoteList([]string{"a.site"})) // exclude one

	truth := runRowSet(t, db, userSQL)
	rewritten := mustRewriteAccept(t, ctx, db, idx, spec, tmp, userSQL)
	got := runRowSet(t, db, rewritten)
	if !equalRowSets(truth, got) {
		t.Errorf("NOT IN mismatch.\ntruth (%d): %s\ngot   (%d): %s",
			len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}

// TestProperty_TwoDayWindow_PartialFirstDay — query starts mid-day, so
// the first day's rollup must be filtered by `bucket >= timeLo`. The
// rewrite must drop hours before timeLo even though they're in the file.
func TestProperty_TwoDayWindow_PartialFirstDay(t *testing.T) {
	ctx := context.Background()
	keep := []string{"a.site", "b.site", "c.site"}
	db, spec, idx, tmp := setupKept3DayDataset(t, ctx, keep, 2)

	// 5/14 12:00 → 5/16 00:00 — partial first day, full second day.
	userSQL := fmt.Sprintf(
		`SELECT date_trunc('hour', time) AS h, site, COUNT(*) AS c FROM events
		 WHERE time >= TIMESTAMP '2026-05-14 12:00:00+00'
		   AND time <  TIMESTAMP '2026-05-16 00:00:00+00'
		   AND site IN (%s)
		 GROUP BY 1, 2`,
		quoteList(keep))

	truth := runRowSet(t, db, userSQL)
	rewritten := mustRewriteAccept(t, ctx, db, idx, spec, tmp, userSQL)
	got := runRowSet(t, db, rewritten)
	if !equalRowSets(truth, got) {
		t.Errorf("partial-first-day mismatch.\ntruth (%d): %s\ngot   (%d): %s",
			len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}

// --- shared setup ---

// setupKept3DayDataset seeds 5/14, 5/15, 5/16 (one row per minute per
// site per hour, varying counts), builds 1h rollups for `nDays` of
// them (starting from 5/14), and returns everything wired for a
// router property test. The spec uses only kept_values so source and
// rollup classifications match.
func setupKept3DayDataset(t *testing.T, ctx context.Context, keptSites []string, nDays int) (*sql.DB, *Spec, *MemoryFileIndex, string) {
	t.Helper()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR)`); err != nil {
		t.Fatal(err)
	}

	days := []time.Time{
		time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
	}
	for _, dayStart := range days {
		for hour := 0; hour < 24; hour++ {
			t0 := dayStart.Add(time.Duration(hour) * time.Hour)
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
	}

	tmp := t.TempDir()
	spec := &Spec{
		Table:      "test.events",
		TZ:         "UTC",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"site": {Role: "PerDim", KeptValues: keptSites, EffectiveCard: len(keptSites)},
		},
	}
	specHash, _ := spec.SchemaHash()

	paths := []string{}
	for i := 0; i < nDays && i < len(days); i++ {
		d := days[i]
		ymd := d.Format("2006/01/02")
		tierDir := filepath.Join(tmp, "_arc/rollup/test/events/1h", ymd)
		if err := os.MkdirAll(tierDir, 0o755); err != nil {
			t.Fatal(err)
		}
		bld := &Builder{
			DB: db, HLLLgK: 14, KLLk: 200, SchemaHash: specHash,
			TierTZ: "UTC", BuilderVersion: "test",
			BucketLo: d, BucketHi: d.Add(24 * time.Hour),
		}
		// Builder's BucketLo/BucketHi are metadata only; filter the
		// source via an inline subquery so per-day rollups only see
		// that day's rows.
		filteredSource := fmt.Sprintf(
			"(SELECT * FROM events WHERE time >= TIMESTAMP '%s' AND time < TIMESTAMP '%s') AS day",
			d.Format("2006-01-02 15:04:05+00"),
			d.Add(24*time.Hour).Format("2006-01-02 15:04:05+00"))
		produced, err := bld.BuildAllVariants(ctx, BuildArgs{Tier: Tier1h, Source: filteredSource}, spec, 100, tierDir)
		if err != nil {
			t.Fatal(err)
		}
		bySrc := produced["by_site"]
		variantDir := filepath.Join(tierDir, "by_site")
		_ = os.MkdirAll(variantDir, 0o755)
		final := filepath.Join(variantDir, fmt.Sprintf("f%d.parquet", i))
		if err := os.Rename(bySrc, final); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, fmt.Sprintf("_arc/rollup/test/events/1h/%s/by_site/f%d.parquet", ymd, i))
	}
	idx := &MemoryFileIndex{Paths: paths}
	return db, spec, idx, tmp
}

func mustRewriteAccept(t *testing.T, ctx context.Context, db *sql.DB, idx *MemoryFileIndex, spec *Spec, tmp, userSQL string) string {
	t.Helper()
	var refusalLog strings.Builder
	logger := zerolog.New(&refusalLog).With().Timestamp().Logger()
	rewritten, ok := Rewrite(ctx, userSQL, RewriteDeps{
		DB: db, Files: idx, Spec: spec, DimRichCap: 100, GraceWindow: 0,
		Logger: logger, StoragePrefix: tmp + "/",
	})
	if !ok {
		t.Fatalf("router refused (expected acceptance).\nrefusal log: %s\nSQL:\n%s",
			refusalLog.String(), userSQL)
	}
	return rewritten
}
