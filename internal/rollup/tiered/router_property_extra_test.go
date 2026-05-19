package tiered

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The exact panel shape from the system reminder — kept-values IN filter
// and an explicit ORDER BY time ASC. Verifies the IR-emitted SQL preserves
// the user's intended row order semantics for time-series Grafana panels.
func TestProperty_GrafanaPanel_OrderByTimeAsc(t *testing.T) {
	ctx := context.Background()
	keptSites := []string{"youtu.be", "www.instagram.com", "youtube.com"}
	db, spec, idx, tmp := setupKept3DayDataset(t, ctx, keptSites, 1)

	userSQL := fmt.Sprintf(
		`SELECT date_trunc('hour', time) AS time, site, COUNT(*) AS value
		 FROM events
		 WHERE time >= TIMESTAMP '2026-05-14 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-15 00:00:00+00'
		   AND site IN (%s)
		 GROUP BY 1, 2
		 ORDER BY time ASC`,
		quoteList(keptSites))

	truth := runRowSet(t, db, userSQL)
	rewritten := mustRewriteAccept(t, ctx, db, idx, spec, tmp, userSQL)
	got := runRowSet(t, db, rewritten)

	if !equalRowSets(truth, got) {
		t.Errorf("row sets differ.\nsource truth (%d rows):\n%s\nrollup result (%d rows):\n%s",
			len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}

// Two GROUP BY dims — exercises the dim-set ordering in both the rollup
// CTE projection and the outer SELECT. site + country together.
func TestProperty_TwoGroupByDims(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR, country VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	day := mustTime("2026-05-15")
	sites := []string{"a", "b"}
	countries := []string{"US", "DE"}
	for hour := 0; hour < 12; hour++ {
		t0 := day.Add(time.Duration(hour) * time.Hour)
		for _, s := range sites {
			for _, c := range countries {
				if _, err := db.Exec(`INSERT INTO events VALUES (?, ?, ?)`, t0, s, c); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	tmp := t.TempDir()
	spec := &Spec{
		Table: "test.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"site":    {Role: "Dim", KeptValues: sites, EffectiveCard: 2},
			"country": {Role: "Dim", KeptValues: countries, EffectiveCard: 2},
		},
	}
	specHash, _ := spec.SchemaHash()
	tierDir := filepath.Join(tmp, "_arc/rollup/test/events/1h/2026/05/15")
	if err := os.MkdirAll(tierDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bld := &Builder{
		DB: db, HLLLgK: 14, KLLk: 200, SchemaHash: specHash,
		TierTZ: "UTC", BuilderVersion: "test",
		BucketLo: day, BucketHi: day.AddDate(0, 0, 1),
	}
	produced, err := bld.BuildAllVariants(ctx, BuildArgs{Tier: Tier1h, Source: "events"}, spec, 100, tierDir)
	if err != nil {
		t.Fatal(err)
	}
	// PickVariant returns "all" when 2 dims are involved — needs the
	// "all" parquet variant. The builder emits both per-dim and "all".
	allSrc := produced["all"]
	if allSrc == "" {
		t.Fatalf("expected 'all' variant in produced %v", produced)
	}
	variantDir := filepath.Join(tierDir, "all")
	if err := os.MkdirAll(variantDir, 0o755); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(variantDir, "f.parquet")
	if err := os.Rename(allSrc, final); err != nil {
		t.Fatal(err)
	}
	idx := &MemoryFileIndex{Paths: []string{"_arc/rollup/test/events/1h/2026/05/15/all/f.parquet"}}

	userSQL := fmt.Sprintf(
		`SELECT date_trunc('hour', time) AS h, site, country, COUNT(*) AS c FROM events
		 WHERE time >= TIMESTAMP '2026-05-15 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-16 00:00:00+00'
		   AND site IN (%s) AND country IN (%s)
		 GROUP BY 1, 2, 3`,
		quoteList(sites), quoteList(countries))

	truth := runRowSet(t, db, userSQL)
	rewritten := mustRewriteAccept(t, ctx, db, idx, spec, tmp, userSQL)
	got := runRowSet(t, db, rewritten)

	if !equalRowSets(truth, got) {
		t.Errorf("row sets differ.\nsource truth (%d rows):\n%s\nrollup result (%d rows):\n%s",
			len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}

// COUNT(*) + SUM(value) + AVG(value) in one SELECT — exercises the
// buildAggFragments loop, AVG's split into sum/cnt pair, and outer-projection
// ordering across multiple aggregates.
func TestProperty_MultipleAggregatesInOneQuery(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR, value DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	day := mustTime("2026-05-15")
	sites := []string{"a", "b"}
	for hour := 0; hour < 12; hour++ {
		t0 := day.Add(time.Duration(hour) * time.Hour)
		for i, s := range sites {
			n := 3 + hour
			for j := 0; j < n; j++ {
				v := float64(hour*10 + i + j)
				if _, err := db.Exec(`INSERT INTO events VALUES (?, ?, ?)`, t0, s, v); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	tmp := t.TempDir()
	spec := &Spec{
		Table: "test.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"site": {Role: "PerDim", KeptValues: sites, EffectiveCard: 2},
		},
	}
	specHash, _ := spec.SchemaHash()
	tierDir := filepath.Join(tmp, "_arc/rollup/test/events/1h/2026/05/15")
	if err := os.MkdirAll(tierDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bld := &Builder{
		DB: db, HLLLgK: 14, KLLk: 200, SchemaHash: specHash,
		TierTZ: "UTC", BuilderVersion: "test",
		BucketLo: day, BucketHi: day.AddDate(0, 0, 1),
	}
	produced, err := bld.BuildAllVariants(ctx, BuildArgs{
		Tier: Tier1h, Source: "events",
		MetricCols: []MetricCol{{Name: "value", Numeric: true}},
	}, spec, 100, tierDir)
	if err != nil {
		t.Fatal(err)
	}
	bySrc := produced["by_site"]
	if bySrc == "" {
		t.Fatalf("expected by_site variant; got %v", produced)
	}
	variantDir := filepath.Join(tierDir, "by_site")
	if err := os.MkdirAll(variantDir, 0o755); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(variantDir, "f.parquet")
	if err := os.Rename(bySrc, final); err != nil {
		t.Fatal(err)
	}
	idx := &MemoryFileIndex{Paths: []string{"_arc/rollup/test/events/1h/2026/05/15/by_site/f.parquet"}}

	userSQL := fmt.Sprintf(
		`SELECT date_trunc('hour', time) AS h, site,
		        COUNT(*) AS c, SUM(value) AS s, AVG(value) AS a
		 FROM events
		 WHERE time >= TIMESTAMP '2026-05-15 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-16 00:00:00+00'
		   AND site IN (%s)
		 GROUP BY 1, 2`,
		quoteList(sites))

	truth := runRowSet(t, db, userSQL)
	rewritten := mustRewriteAccept(t, ctx, db, idx, spec, tmp, userSQL)
	got := runRowSet(t, db, rewritten)

	if !equalRowSets(truth, got) {
		t.Errorf("row sets differ.\nsource truth (%d rows):\n%s\nrollup result (%d rows):\n%s",
			len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}

// IS NULL / IS NOT NULL filter against a dim column. Parser maps to the
// `_null_` sentinel that the rollup builder writes for missing values.
func TestProperty_IsNullFilter(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	day := mustTime("2026-05-15")
	for hour := 0; hour < 12; hour++ {
		t0 := day.Add(time.Duration(hour) * time.Hour)
		if _, err := db.Exec(`INSERT INTO events VALUES (?, 'a')`, t0); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO events VALUES (?, NULL)`, t0); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO events VALUES (?, 'a')`, t0); err != nil {
			t.Fatal(err)
		}
	}

	tmp := t.TempDir()
	spec := &Spec{
		Table: "test.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			// _null_ must be declared as kept so the builder writes the
			// sentinel rather than bucketing NULL into _OTHER_. Without
			// this, IS NULL/IS NOT NULL queries cannot be served accurately
			// from the rollup — the router would need to refuse the
			// rewrite, but currently doesn't (separate refinement).
			"site": {Role: "PerDim", KeptValues: []string{"a", "_null_"}, EffectiveCard: 2},
		},
	}
	specHash, _ := spec.SchemaHash()
	tierDir := filepath.Join(tmp, "_arc/rollup/test/events/1h/2026/05/15")
	if err := os.MkdirAll(tierDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bld := &Builder{
		DB: db, HLLLgK: 14, KLLk: 200, SchemaHash: specHash,
		TierTZ: "UTC", BuilderVersion: "test",
		BucketLo: day, BucketHi: day.AddDate(0, 0, 1),
	}
	produced, err := bld.BuildAllVariants(ctx, BuildArgs{Tier: Tier1h, Source: "events"}, spec, 100, tierDir)
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
	idx := &MemoryFileIndex{Paths: []string{"_arc/rollup/test/events/1h/2026/05/15/by_site/f.parquet"}}

	for _, predicate := range []string{"site IS NULL", "site IS NOT NULL"} {
		t.Run(predicate, func(t *testing.T) {
			userSQL := fmt.Sprintf(
				`SELECT date_trunc('hour', time) AS h, COUNT(*) AS c FROM events
				 WHERE time >= TIMESTAMP '2026-05-15 00:00:00+00'
				   AND time <  TIMESTAMP '2026-05-16 00:00:00+00'
				   AND %s
				 GROUP BY 1`,
				predicate)
			truth := runRowSet(t, db, userSQL)
			rewritten := mustRewriteAccept(t, ctx, db, idx, spec, tmp, userSQL)
			got := runRowSet(t, db, rewritten)
			if !equalRowSets(truth, got) {
				t.Errorf("row sets differ.\nuser sql:\n%s\nrewritten sql:\n%s\nsource truth (%d rows):\n%s\nrollup result (%d rows):\n%s",
					userSQL, rewritten, len(truth), formatRowSet(truth), len(got), formatRowSet(got))
			}
		})
	}
}
