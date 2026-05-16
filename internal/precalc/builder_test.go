package precalc

import (
	"context"
	"path/filepath"
	"testing"
)

func TestBuilder_BuildSketchVariant_Synthetic(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, x DOUBLE, id VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES
		('2026-05-10 00:00:00+00', 1.0, 'a'),
		('2026-05-10 00:30:00+00', 2.0, 'b'),
		('2026-05-10 01:00:00+00', 3.0, 'a')`); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(t.TempDir(), "sketch.parquet")
	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	err = b.BuildSketchVariant(ctx, BuildArgs{
		Tier:       Tier1h,
		Source:     "evt",
		MetricCols: []MetricCol{{Name: "x", Numeric: true}},
		HLLCols:    []string{"id"},
	}, out)
	if err != nil {
		t.Fatalf("BuildSketchVariant: %v", err)
	}

	// Round-trip: read the parquet, verify 2 rows (2 hourly buckets), total cnt=3
	var rows int
	var totalCnt int
	err = db.QueryRow(`SELECT COUNT(*), SUM(cnt) FROM read_parquet('` + out + `')`).Scan(&rows, &totalCnt)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if totalCnt != 3 {
		t.Errorf("total cnt = %d, want 3", totalCnt)
	}
}

func TestBuilder_SketchVariant_RealDownloads(t *testing.T) {
	skipIfNoTestData(t)
	ctx := context.Background()
	db, err := OpenWithDataSketches("Asia/Riyadh")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	out := filepath.Join(t.TempDir(), "sketch.parquet")
	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	err = b.BuildSketchVariant(ctx, BuildArgs{
		Tier:       Tier1h,
		Source:     "read_parquet('" + testDataGlob + "')",
		MetricCols: []MetricCol{{Name: "duration_seconds", Numeric: true}, {Name: "response", Numeric: true}},
		HLLCols:    []string{"device_id", "ip", "url", "title"},
		KLLCols:    []string{"duration_seconds", "response"},
	}, out)
	if err != nil {
		t.Fatal(err)
	}

	var precalcTotal, rawTotal int64
	if err := db.QueryRow(`SELECT SUM(cnt) FROM read_parquet('` + out + `')`).Scan(&precalcTotal); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM read_parquet('` + testDataGlob + `')`).Scan(&rawTotal); err != nil {
		t.Fatal(err)
	}
	if precalcTotal != rawTotal {
		t.Errorf("precalc sketch total %d != raw total %d", precalcTotal, rawTotal)
	}
}

func TestBuilder_PerDimVariant_RealDownloads(t *testing.T) {
	skipIfNoTestData(t)
	ctx := context.Background()
	db, err := OpenWithDataSketches("Asia/Riyadh")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spec := &Spec{
		Dims: map[string]DimSpec{
			"site": {Role: "Dim", KeptValues: []string{"youtu.be", "m.youtube.com", "www.instagram.com"}},
		},
	}
	out := filepath.Join(t.TempDir(), "by_site.parquet")
	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	err = b.BuildPerDimVariant(ctx, BuildArgs{
		Tier:       Tier1h,
		Source:     "read_parquet('" + testDataGlob + "')",
		MetricCols: []MetricCol{{Name: "duration_seconds", Numeric: true}},
		HLLCols:    []string{"device_id"},
	}, spec, "site", out)
	if err != nil {
		t.Fatal(err)
	}

	var precalcTotal, rawTotal int64
	if err := db.QueryRow(`SELECT SUM(cnt) FROM read_parquet('` + out + `')`).Scan(&precalcTotal); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM read_parquet('` + testDataGlob + `')`).Scan(&rawTotal); err != nil {
		t.Fatal(err)
	}
	if precalcTotal != rawTotal {
		t.Errorf("by_site total %d != raw total %d", precalcTotal, rawTotal)
	}

	var youtuCnt, otherCnt int64
	db.QueryRow(`SELECT SUM(cnt) FROM read_parquet('` + out + `') WHERE site_class = 'youtu.be'`).Scan(&youtuCnt)
	db.QueryRow(`SELECT SUM(cnt) FROM read_parquet('` + out + `') WHERE site_class = '_OTHER_'`).Scan(&otherCnt)
	if youtuCnt == 0 {
		t.Error("youtu.be cnt is 0, want positive")
	}
	if otherCnt == 0 {
		t.Error("_OTHER_ cnt is 0, want positive")
	}
	t.Logf("precalcTotal=%d rawTotal=%d youtuCnt=%d otherCnt=%d", precalcTotal, rawTotal, youtuCnt, otherCnt)
}

func TestBuilder_DimRichVariant_RealDownloads(t *testing.T) {
	skipIfNoTestData(t)
	ctx := context.Background()
	db, err := OpenWithDataSketches("Asia/Riyadh")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spec, err := Classify(ctx, db, ClassifyOpts{
		Source:            "SELECT * FROM read_parquet('" + testDataGlob + "')",
		TimeColumn:        "time",
		DimColumns:        []string{"site", "country", "os", "vpn", "status", "tag", "app_version", "os_version"},
		CoverageThreshold: 0.99,
		DimRichCap:        100,
		Table:             "default.downloads",
		TZ:                "Asia/Riyadh",
	})
	if err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(t.TempDir(), "all.parquet")
	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	if err := b.BuildDimRichVariant(ctx, BuildArgs{
		Tier:       Tier1h,
		Source:     "read_parquet('" + testDataGlob + "')",
		MetricCols: []MetricCol{{Name: "duration_seconds", Numeric: true}, {Name: "response", Numeric: true}},
	}, &spec, 100, out); err != nil {
		t.Fatal(err)
	}

	var precalcTotal, rawTotal int64
	db.QueryRow(`SELECT SUM(cnt) FROM read_parquet('` + out + `')`).Scan(&precalcTotal)
	db.QueryRow(`SELECT COUNT(*) FROM read_parquet('` + testDataGlob + `')`).Scan(&rawTotal)
	if precalcTotal != rawTotal {
		t.Errorf("dim-rich total %d != raw total %d", precalcTotal, rawTotal)
	}
}

func TestBuilder_HierarchicalRollup_TotalCntConsistent(t *testing.T) {
	skipIfNoTestData(t)
	ctx := context.Background()
	db, err := OpenWithDataSketches("Asia/Riyadh")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tmp := t.TempDir()
	hourly := filepath.Join(tmp, "1h_sketch.parquet")
	daily := filepath.Join(tmp, "1d_sketch.parquet")
	weekly := filepath.Join(tmp, "1w_sketch.parquet")
	monthly := filepath.Join(tmp, "1mo_sketch.parquet")

	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}

	// 1h from raw
	if err := b.BuildSketchVariant(ctx, BuildArgs{
		Tier: Tier1h, Source: "read_parquet('" + testDataGlob + "')",
		MetricCols: []MetricCol{{Name: "duration_seconds", Numeric: true}},
		HLLCols: []string{"device_id"}, KLLCols: []string{"duration_seconds"},
	}, hourly); err != nil {
		t.Fatal(err)
	}
	// 1d from 1h
	if err := b.RollupSketchVariant(ctx, RollupArgs{
		TargetTier: Tier1d, SourcePath: hourly,
		MetricCols: []MetricCol{{Name: "duration_seconds", Numeric: true}},
		HLLCols: []string{"device_id"}, KLLCols: []string{"duration_seconds"},
	}, daily); err != nil {
		t.Fatal(err)
	}
	// 1w from 1d
	if err := b.RollupSketchVariant(ctx, RollupArgs{
		TargetTier: Tier1w, SourcePath: daily,
		MetricCols: []MetricCol{{Name: "duration_seconds", Numeric: true}},
		HLLCols: []string{"device_id"}, KLLCols: []string{"duration_seconds"},
	}, weekly); err != nil {
		t.Fatal(err)
	}
	// 1mo from 1w
	if err := b.RollupSketchVariant(ctx, RollupArgs{
		TargetTier: Tier1mo, SourcePath: weekly,
		MetricCols: []MetricCol{{Name: "duration_seconds", Numeric: true}},
		HLLCols: []string{"device_id"}, KLLCols: []string{"duration_seconds"},
	}, monthly); err != nil {
		t.Fatal(err)
	}

	totals := map[string]int64{}
	for tier, path := range map[string]string{"1h": hourly, "1d": daily, "1w": weekly, "1mo": monthly} {
		var v int64
		if err := db.QueryRow(`SELECT CAST(SUM(cnt) AS BIGINT) FROM read_parquet('` + path + `')`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		totals[tier] = v
	}
	t.Logf("tier totals: 1h=%d 1d=%d 1w=%d 1mo=%d", totals["1h"], totals["1d"], totals["1w"], totals["1mo"])
	for _, tier := range []string{"1d", "1w", "1mo"} {
		if totals[tier] != totals["1h"] {
			t.Errorf("tier %s total %d != 1h total %d", tier, totals[tier], totals["1h"])
		}
	}
}
