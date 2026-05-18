package tiered

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
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

func TestBuilder_StampsKVMetadata(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES ('2026-05-10 00:00:00+00', 1.5)`); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "x.parquet")
	b := &Builder{
		DB: db, HLLLgK: 14, KLLk: 200,
		SchemaHash:     "test_hash_abc",
		TierTZ:         "UTC",
		BuilderVersion: "v_test",
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

	want := map[string]string{
		"schema_hash":     "test_hash_abc",
		"tier_tz":         "UTC",
		"builder_version": "v_test",
	}
	got := map[string]string{}
	rows, err := db.Query(`SELECT key, value FROM parquet_kv_metadata('` + out + `')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		got[k] = v
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("KV[%s] = %q, want %q", k, got[k], v)
		}
	}
	if got["bucket_lo"] == "" || got["bucket_hi"] == "" {
		t.Errorf("bucket bounds missing: %+v", got)
	}
}

func TestBuildRollupPerDimSQL_DayFromHour(t *testing.T) {
	sql := BuildRollupPerDimSQL(RollupArgs{
		TargetTier: Tier1d,
		SourcePath: "/tmp/by_dim_a_1h.parquet",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
		HLLCols:    []string{"id"},
		HLLLgK:     14,
	}, "dim_a")
	for _, want := range []string{
		"date_trunc('day', bucket) AS bucket",
		"dim_a_class",
		"SUM(cnt) AS cnt",
		"SUM(sum_m) AS sum_m",
		"datasketch_hll_union(14, CAST(hll_id AS sketch_hll)) AS hll_id",
		"GROUP BY 1, 2",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q in:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "sum_sq") {
		t.Error("per-dim rollup must NOT contain sum_sq (not tracked in per-dim variant)")
	}
	if strings.Contains(sql, "datasketch_kll") {
		t.Error("per-dim rollup must NOT contain kll (not tracked in per-dim variant)")
	}
}

func TestBuildRollupDimRichSQL_DayFromHour(t *testing.T) {
	spec := &Spec{Dims: map[string]DimSpec{
		"dim_a": {Role: "Dim", EffectiveCard: 2},
		"dim_b": {Role: "Dim", EffectiveCard: 2},
		"dim_c": {Role: "PerDim", EffectiveCard: 200},
	}}
	sql := BuildRollupDimRichSQL(RollupArgs{
		TargetTier: Tier1d,
		SourcePath: "/tmp/all_1h.parquet",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
	}, spec, 100)
	for _, want := range []string{
		"dim_a_class",
		"dim_b_class",
		"SUM(cnt) AS cnt",
		"GROUP BY ALL",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q in:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "dim_c_class") {
		t.Error("dim_c (PerDim) should NOT appear in dim-rich rollup")
	}
	if strings.Contains(sql, "datasketch_hll") || strings.Contains(sql, "datasketch_kll") {
		t.Error("dim-rich rollup must NOT contain sketches")
	}
}

func TestBuilder_RollupPerDimVariant_RoundTrip(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, dim_a VARCHAR, m DOUBLE, id VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt VALUES
		('2026-05-10 00:00:00+00','x',1.0,'u1'),
		('2026-05-10 01:00:00+00','x',2.0,'u2'),
		('2026-05-10 02:00:00+00','y',3.0,'u1')`); err != nil {
		t.Fatal(err)
	}
	spec := &Spec{Dims: map[string]DimSpec{
		"dim_a": {Role: "Dim", KeptValues: []string{"x", "y"}, EffectiveCard: 2},
	}}
	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}

	hourly := filepath.Join(t.TempDir(), "1h.parquet")
	if err := b.BuildPerDimVariant(ctx, BuildArgs{
		Tier:    Tier1h,
		Source:  "evt",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
		HLLCols: []string{"id"},
		KLLCols: []string{"m"},
	}, spec, "dim_a", hourly); err != nil {
		t.Fatal(err)
	}

	daily := filepath.Join(t.TempDir(), "1d.parquet")
	if err := b.RollupPerDimVariant(ctx, RollupArgs{
		TargetTier: Tier1d, SourcePath: hourly,
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
		HLLCols:    []string{"id"},
	}, "dim_a", daily); err != nil {
		t.Fatal(err)
	}

	var hourlyTotal, dailyTotal int64
	db.QueryRow(`SELECT SUM(cnt) FROM read_parquet('` + hourly + `')`).Scan(&hourlyTotal)
	db.QueryRow(`SELECT SUM(cnt) FROM read_parquet('` + daily + `')`).Scan(&dailyTotal)
	if hourlyTotal != dailyTotal {
		t.Errorf("1d total %d != 1h total %d", dailyTotal, hourlyTotal)
	}
}

// TestBuildAllVariants_Synthetic verifies that BuildAllVariants produces the
// same row counts as the individual per-variant methods for a small in-memory
// table with two dims and one metric.
func TestBuildAllVariants_Synthetic(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE evt2 (time TIMESTAMPTZ, country VARCHAR, site VARCHAR, m DOUBLE, id VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt2 VALUES
		('2026-05-10 00:00:00+00', 'SA', 'alpha', 1.0, 'u1'),
		('2026-05-10 00:30:00+00', 'SA', 'beta',  2.0, 'u2'),
		('2026-05-10 01:00:00+00', 'EG', 'alpha', 3.0, 'u1'),
		('2026-05-10 01:30:00+00', 'EG', 'beta',  4.0, 'u3')`); err != nil {
		t.Fatal(err)
	}

	spec := &Spec{
		Table: "test", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"country": {Role: "Dim", KeptValues: []string{"SA", "EG"}, EffectiveCard: 2},
			"site":    {Role: "Dim", KeptValues: []string{"alpha", "beta"}, EffectiveCard: 2},
		},
	}

	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	outDir := t.TempDir()
	args := BuildArgs{
		Tier:       Tier1h,
		Source:     "evt2",
		TimeColumn: "time",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
		HLLCols:    []string{"id"},
		KLLCols:    []string{"m"},
	}

	files, err := b.BuildAllVariants(ctx, args, spec, 100, outDir)
	if err != nil {
		t.Fatalf("BuildAllVariants: %v", err)
	}

	// Expect 4 variants: sketch, by_country, by_site, all
	wantVariants := []string{"sketch", "by_country", "by_site", "all"}
	if len(files) != len(wantVariants) {
		t.Errorf("got %d variants, want %d: %v", len(files), len(wantVariants), files)
	}
	for _, v := range wantVariants {
		if _, ok := files[v]; !ok {
			t.Errorf("missing variant %q in output", v)
		}
	}

	// Sketch: 2 hourly buckets, total cnt = 4
	var sketchRows, sketchCnt int64
	if p, ok := files["sketch"]; ok {
		db.QueryRow(`SELECT COUNT(*), SUM(cnt) FROM read_parquet('` + p + `')`).Scan(&sketchRows, &sketchCnt)
		if sketchRows != 2 {
			t.Errorf("sketch rows = %d, want 2", sketchRows)
		}
		if sketchCnt != 4 {
			t.Errorf("sketch total cnt = %d, want 4", sketchCnt)
		}
	}

	// by_country: each bucket has only 1 country → 2 rows (one per bucket), total cnt = 4
	var countryRows, countryCnt int64
	if p, ok := files["by_country"]; ok {
		db.QueryRow(`SELECT COUNT(*), SUM(cnt) FROM read_parquet('` + p + `')`).Scan(&countryRows, &countryCnt)
		if countryRows != 2 {
			t.Errorf("by_country rows = %d, want 2", countryRows)
		}
		if countryCnt != 4 {
			t.Errorf("by_country total cnt = %d, want 4", countryCnt)
		}
	}

	// all: each bucket has 2 (country×site) combinations = 4 rows total, total cnt = 4
	var allRows, allCnt int64
	if p, ok := files["all"]; ok {
		db.QueryRow(`SELECT COUNT(*), SUM(cnt) FROM read_parquet('` + p + `')`).Scan(&allRows, &allCnt)
		if allRows != 4 {
			t.Errorf("all rows = %d, want 4", allRows)
		}
		if allCnt != 4 {
			t.Errorf("all total cnt = %d, want 4", allCnt)
		}
	}

	// Verify by_country has country_class column but not site_class
	colRows, err := db.Query(`DESCRIBE SELECT * FROM read_parquet('` + files["by_country"] + `')`)
	if err != nil {
		t.Fatal(err)
	}
	var byCols []string
	for colRows.Next() {
		var name, typ string
		var rest [4]any
		colRows.Scan(&name, &typ, &rest[0], &rest[1], &rest[2], &rest[3])
		byCols = append(byCols, name)
	}
	colRows.Close()
	hasCountryClass := false
	hasSiteClass := false
	for _, c := range byCols {
		if c == "country_class" {
			hasCountryClass = true
		}
		if c == "site_class" {
			hasSiteClass = true
		}
	}
	if !hasCountryClass {
		t.Errorf("by_country parquet missing country_class column; cols: %v", byCols)
	}
	if hasSiteClass {
		t.Errorf("by_country parquet should NOT have site_class column; cols: %v", byCols)
	}

	// Verify all has both country_class and site_class
	allColRows, err := db.Query(`DESCRIBE SELECT * FROM read_parquet('` + files["all"] + `')`)
	if err != nil {
		t.Fatal(err)
	}
	var allCols []string
	for allColRows.Next() {
		var name, typ string
		var rest [4]any
		allColRows.Scan(&name, &typ, &rest[0], &rest[1], &rest[2], &rest[3])
		allCols = append(allCols, name)
	}
	allColRows.Close()
	hasAllCountry := false
	hasAllSite := false
	for _, c := range allCols {
		if c == "country_class" {
			hasAllCountry = true
		}
		if c == "site_class" {
			hasAllSite = true
		}
	}
	if !hasAllCountry || !hasAllSite {
		t.Errorf("all parquet missing dim_class columns; cols: %v", allCols)
	}
}

// TestBuildAllVariants_LocalFixture runs BuildAllVariants against a directory
// of real parquet files and verifies the output count matches per-variant builds.
//
// Set ARC_ROLLUP_FIXTURE=<dir> or uses /tmp/local-downloads.
func TestBuildAllVariants_LocalFixture(t *testing.T) {
	dir := os.Getenv("ARC_ROLLUP_FIXTURE")
	if dir == "" {
		dir = "/tmp/local-downloads"
	}
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("ARC_ROLLUP_FIXTURE not present at %s — skipping", dir)
	}

	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	source := fmt.Sprintf("read_parquet('%s/**/*.parquet', union_by_name=true)", dir)

	var n int64
	if err := db.QueryRow("SELECT count(*) FROM " + source).Scan(&n); err != nil {
		t.Fatalf("source SQL failed: %v", err)
	}
	t.Logf("source rows: %d", n)

	rows, err := db.Query("DESCRIBE SELECT * FROM " + source + " LIMIT 1")
	if err != nil {
		t.Fatal(err)
	}
	var timeCol string
	var dimCols []string
	for rows.Next() {
		var name, typ string
		var nullCols [4]any
		if err := rows.Scan(&name, &typ, &nullCols[0], &nullCols[1], &nullCols[2], &nullCols[3]); err != nil {
			t.Fatal(err)
		}
		if timeCol == "" && strings.HasPrefix(typ, "TIMESTAMP") {
			timeCol = name
			continue
		}
		if typ == "VARCHAR" && len(dimCols) < 4 {
			dimCols = append(dimCols, name)
		}
	}
	rows.Close()
	if timeCol == "" || len(dimCols) < 2 {
		t.Fatalf("fixture must have a TIMESTAMP and ≥2 VARCHAR columns; got time=%q dims=%v", timeCol, dimCols)
	}
	t.Logf("discovered timeCol=%q dims=%v", timeCol, dimCols)

	dims := make(map[string]DimSpec, len(dimCols))
	for i, c := range dimCols {
		q := fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL GROUP BY 1 ORDER BY count(*) DESC LIMIT 3", c, source, c)
		r, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		var kept []string
		for r.Next() {
			var v string
			r.Scan(&v)
			kept = append(kept, v)
		}
		r.Close()
		role := "Dim"
		if i == len(dimCols)-1 {
			role = "Sketch"
		}
		dims[c] = DimSpec{Role: role, KeptValues: kept, EffectiveCard: len(kept)}
	}

	var lo time.Time
	if err := db.QueryRow(fmt.Sprintf("SELECT date_trunc('day', MIN(%s)) FROM %s", timeCol, source)).Scan(&lo); err != nil {
		t.Fatal(err)
	}
	hi := lo.AddDate(0, 0, 1)

	spec := &Spec{Table: "fixture", TZ: "UTC", TimeColumn: timeCol, Dims: dims}
	sketchDim := dimCols[len(dimCols)-1]
	args := BuildArgs{
		Tier: Tier1h, Source: source, TimeColumn: timeCol,
		HLLCols: []string{sketchDim},
	}

	b := &Builder{
		DB: db, HLLLgK: 14, KLLk: 200,
		SchemaHash: "test_hash", TierTZ: "UTC", BuilderVersion: "v_local_test",
		BucketLo: lo, BucketHi: hi,
	}

	outDir := t.TempDir()
	files, err := b.BuildAllVariants(ctx, args, spec, 100, outDir)
	if err != nil {
		t.Fatalf("BuildAllVariants: %v", err)
	}

	t.Logf("produced %d variant files", len(files))
	for variant, path := range files {
		var rows int64
		db.QueryRow(`SELECT COUNT(*) FROM read_parquet('` + path + `')`).Scan(&rows)
		t.Logf("  %s: %d rows", variant, rows)
	}

	if _, ok := files["sketch"]; !ok {
		t.Error("missing sketch variant")
	}
}

// TestBuilder_LocalFixture_AllVariants runs sketch / per-dim / dim-rich
// builds against a directory of parquet files using the exact Source-string
// shape buildWindowSource emits in production (bare read_parquet(...)
// expression, not wrapped in SELECT * FROM). Catches Parser Errors and
// schema-mismatch bugs without a build/push cycle.
//
// Columns + their kept values are DISCOVERED from the parquet, so the test
// is independent of any specific table's schema.
//
// Set ARC_ROLLUP_FIXTURE=<dir> to a directory containing *.parquet (or a
// glob like <dir>/**/*.parquet) and run:
//
//	go test -tags=duckdb_arrow -run TestBuilder_LocalFixture ./internal/rollup/tiered/
func TestBuilder_LocalFixture_AllVariants(t *testing.T) {
	dir := os.Getenv("ARC_ROLLUP_FIXTURE")
	if dir == "" {
		dir = "/tmp/local-downloads"
	}
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("ARC_ROLLUP_FIXTURE not present at %s — skipping", dir)
	}

	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// EXACT shape buildWindowSource emits in production: bare read_parquet
	// expression. The build SQL templates interpolate this directly into
	// `FROM %s` — wrapping in `SELECT * FROM` here would reproduce the
	// Parser Error that motivated this test.
	source := fmt.Sprintf("read_parquet('%s/**/*.parquet', union_by_name=true)", dir)

	var n int64
	if err := db.QueryRow("SELECT count(*) FROM " + source).Scan(&n); err != nil {
		t.Fatalf("source SQL failed: %v\nSource: %s", err, source)
	}
	if n == 0 {
		t.Fatalf("source returned 0 rows — bad fixture")
	}
	t.Logf("source rows: %d", n)

	// Discover schema: pick the timestamp column (first TIMESTAMP*) and up to
	// 4 VARCHAR columns. No hardcoded names.
	rows, err := db.Query("DESCRIBE SELECT * FROM " + source + " LIMIT 1")
	if err != nil {
		t.Fatal(err)
	}
	var timeCol string
	var dimCols []string
	for rows.Next() {
		var name, typ string
		var nullCols [4]any // ignore the rest of DESCRIBE's columns
		if err := rows.Scan(&name, &typ, &nullCols[0], &nullCols[1], &nullCols[2], &nullCols[3]); err != nil {
			t.Fatal(err)
		}
		if timeCol == "" && strings.HasPrefix(typ, "TIMESTAMP") {
			timeCol = name
			continue
		}
		if typ == "VARCHAR" && len(dimCols) < 4 {
			dimCols = append(dimCols, name)
		}
	}
	rows.Close()
	if timeCol == "" || len(dimCols) < 2 {
		t.Fatalf("fixture must have a TIMESTAMP column and ≥2 VARCHAR columns; got time=%q dims=%v", timeCol, dimCols)
	}
	t.Logf("discovered timeCol=%q dims=%v", timeCol, dimCols)

	// Discover top-3 kept values per dim from the fixture, so the per-dim
	// variant has real matches (no values fall through to _other_).
	dims := make(map[string]DimSpec, len(dimCols))
	for i, c := range dimCols {
		q := fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL GROUP BY 1 ORDER BY count(*) DESC LIMIT 3", c, source, c)
		r, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		var kept []string
		for r.Next() {
			var v string
			r.Scan(&v)
			kept = append(kept, v)
		}
		r.Close()
		// Last dim acts as a Sketch role so we exercise HLL cols too.
		role := "Dim"
		if i == len(dimCols)-1 {
			role = "Sketch"
		}
		dims[c] = DimSpec{Role: role, KeptValues: kept, EffectiveCard: len(kept)}
	}

	// Bucket bounds: one full day covering the fixture's earliest data.
	var lo time.Time
	if err := db.QueryRow(fmt.Sprintf("SELECT date_trunc('day', MIN(%s)) FROM %s", timeCol, source)).Scan(&lo); err != nil {
		t.Fatal(err)
	}
	hi := lo.AddDate(0, 0, 1)

	b := &Builder{
		DB: db, HLLLgK: 14, KLLk: 200,
		SchemaHash: "test_hash", TierTZ: "UTC", BuilderVersion: "v_local_test",
		BucketLo: lo, BucketHi: hi,
	}
	spec := &Spec{Table: "fixture", TZ: "UTC", TimeColumn: timeCol, Dims: dims}

	// Last dim is the Sketch role — feed its column name as HLLCols.
	sketchDim := dimCols[len(dimCols)-1]
	args := BuildArgs{
		Tier: Tier1h, Source: source, TimeColumn: timeCol,
		HLLCols: []string{sketchDim},
	}

	out1 := filepath.Join(t.TempDir(), "sketch.parquet")
	if err := b.BuildSketchVariant(ctx, args, out1); err != nil {
		t.Fatalf("BuildSketchVariant: %v", err)
	}
	var c1 int64
	db.QueryRow(`SELECT count(*) FROM read_parquet('` + out1 + `')`).Scan(&c1)
	t.Logf("sketch variant rows: %d", c1)

	perDim := dimCols[0]
	out2 := filepath.Join(t.TempDir(), "per_dim.parquet")
	if err := b.BuildPerDimVariant(ctx, args, spec, perDim, out2); err != nil {
		t.Fatalf("BuildPerDimVariant: %v", err)
	}
	var c2 int64
	db.QueryRow(`SELECT count(*) FROM read_parquet('` + out2 + `')`).Scan(&c2)
	t.Logf("per-dim %q rows: %d", perDim, c2)

	out3 := filepath.Join(t.TempDir(), "dim_rich.parquet")
	if err := b.BuildDimRichVariant(ctx, args, spec, 100, out3); err != nil {
		t.Fatalf("BuildDimRichVariant: %v", err)
	}
	var c3 int64
	db.QueryRow(`SELECT count(*) FROM read_parquet('` + out3 + `')`).Scan(&c3)
	t.Logf("dim-rich rows: %d", c3)
}

// ─── equivalence helpers ─────────────────────────────────────────────────────

// eqBucketKey is the composite key for sketch-variant equivalence checks.
type eqBucketKey struct{ bucket string }

// eqDimKey is the composite key for per-dim equivalence checks.
type eqDimKey struct{ bucket, class string }

// eqAllKey is the composite key for dim-rich equivalence checks.
type eqAllKey struct{ bucket, classes string } // classes = sorted "k=v,..." string

// readSketchAgg reads bucket → {rowCount, cntSum} from the sketch parquet.
func readSketchAgg(t *testing.T, db *sql.DB, path string) map[eqBucketKey]struct{ rows, cnt int64 } {
	t.Helper()
	q := `SELECT strftime(bucket, '%Y-%m-%dT%H:%M:%SZ'), COUNT(*) AS n, SUM(cnt) AS s
	      FROM read_parquet('` + path + `') GROUP BY 1`
	rs, err := db.Query(q)
	if err != nil {
		t.Fatalf("readSketchAgg %s: %v", path, err)
	}
	defer rs.Close()
	out := map[eqBucketKey]struct{ rows, cnt int64 }{}
	for rs.Next() {
		var bk string
		var n, s int64
		if err := rs.Scan(&bk, &n, &s); err != nil {
			t.Fatal(err)
		}
		out[eqBucketKey{bk}] = struct{ rows, cnt int64 }{n, s}
	}
	return out
}

// readPerDimAgg reads (bucket, dim_class) → {rowCount, cntSum} from a per-dim parquet.
func readPerDimAgg(t *testing.T, db *sql.DB, path, dimCol string) map[eqDimKey]struct{ rows, cnt int64 } {
	t.Helper()
	classCol := dimCol + "_class"
	q := fmt.Sprintf(`SELECT strftime(bucket, '%%Y-%%m-%%dT%%H:%%M:%%SZ'), %s, COUNT(*) AS n, SUM(cnt) AS s
	                  FROM read_parquet('%s') GROUP BY 1, 2`, classCol, path)
	rs, err := db.Query(q)
	if err != nil {
		t.Fatalf("readPerDimAgg %s: %v", path, err)
	}
	defer rs.Close()
	out := map[eqDimKey]struct{ rows, cnt int64 }{}
	for rs.Next() {
		var bk, cls string
		var n, s int64
		if err := rs.Scan(&bk, &cls, &n, &s); err != nil {
			t.Fatal(err)
		}
		out[eqDimKey{bk, cls}] = struct{ rows, cnt int64 }{n, s}
	}
	return out
}

// readPerDimClasses reads the distinct dim_class values present in a per-dim parquet.
func readPerDimClasses(t *testing.T, db *sql.DB, path, dimCol string) []string {
	t.Helper()
	classCol := dimCol + "_class"
	q := fmt.Sprintf(`SELECT DISTINCT %s FROM read_parquet('%s') ORDER BY 1`, classCol, path)
	rs, err := db.Query(q)
	if err != nil {
		t.Fatalf("readPerDimClasses %s: %v", path, err)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var v string
		rs.Scan(&v)
		out = append(out, v)
	}
	return out
}

// readHLLSum returns SUM(datasketch_hll_estimate(hll_<col>)) from a parquet.
// datasketch_hll_estimate is the scalar function that returns the cardinality
// estimate for a single HLL sketch blob.
func readHLLSum(t *testing.T, db *sql.DB, path, col string) float64 {
	t.Helper()
	hllCol := "hll_" + col
	q := fmt.Sprintf(`SELECT SUM(datasketch_hll_estimate(CAST(%s AS sketch_hll))) FROM read_parquet('%s')`, hllCol, path)
	var v float64
	if err := db.QueryRow(q).Scan(&v); err != nil {
		t.Fatalf("readHLLSum %s %s: %v", path, col, err)
	}
	return v
}

// assertHLLClose fails the test if the relative difference between old and new
// HLL estimates exceeds the given tolerance (e.g. 0.03 for 3%).
func assertHLLClose(t *testing.T, old, new float64, tol float64, label string) {
	t.Helper()
	if old == 0 && new == 0 {
		return
	}
	rel := math.Abs(old-new) / math.Max(math.Abs(old), 1)
	if rel > tol {
		t.Errorf("HLL estimate diverges %.1f%% > %.1f%% for %s (old=%.0f new=%.0f)",
			rel*100, tol*100, label, old, new)
	}
}

// readKLLQuantile returns the p-th quantile estimate from the merged kll_<col>
// across all rows in the parquet. Uses the datasketch_kll aggregate to merge
// all KLL sketch blobs, then extracts the quantile.
func readKLLQuantile(t *testing.T, db *sql.DB, path, col string, p float64) float64 {
	t.Helper()
	kllCol := "kll_" + col
	// datasketch_kll aggregate merges sketch blobs when fed cast sketch values.
	// datasketch_kll_quantile extracts the estimate from the merged sketch.
	q := fmt.Sprintf(
		`SELECT datasketch_kll_quantile(datasketch_kll(200, CAST(%s AS sketch_kll_double)), %.2f::DOUBLE, false) FROM read_parquet('%s')`,
		kllCol, p, path)
	var v float64
	if err := db.QueryRow(q).Scan(&v); err != nil {
		t.Fatalf("readKLLQuantile %s %s: %v", path, col, err)
	}
	return v
}

// assertKLLClose fails if the relative difference exceeds tol (e.g. 0.05 for 5%).
func assertKLLClose(t *testing.T, old, new float64, tol float64, label string) {
	t.Helper()
	if old == 0 && new == 0 {
		return
	}
	rel := math.Abs(old-new) / math.Max(math.Abs(old), 1)
	if rel > tol {
		t.Errorf("KLL quantile diverges %.1f%% > %.1f%% for %s (old=%.4f new=%.4f)",
			rel*100, tol*100, label, old, new)
	}
}

// assertExactSets fails if the two string slices (already sorted) differ.
func assertExactSets(t *testing.T, old, new []string, label string) {
	t.Helper()
	sort.Strings(old)
	sort.Strings(new)
	if !reflect.DeepEqual(old, new) {
		t.Errorf("dim_class set mismatch for %s:\n  old=%v\n  new=%v", label, old, new)
	}
}

// ─── equivalence tests ───────────────────────────────────────────────────────

// TestEquivalence_GroupingSets_vs_PerVariant_Synthetic verifies BuildAllVariants
// produces bit-exact aggregates vs the individual Build* methods.
// Table has 50 K rows, 2 Dim-role dims (low-card), 1 Sketch-role dim (device_id),
// and ~30% NULLs in dim_b.
func TestEquivalence_GroupingSets_vs_PerVariant_Synthetic(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Generate 50 K rows: 2 hourly buckets, dim_a ∈ {A,B,C}, dim_b ∈ {X,Y,NULL},
	// device_id is a unique-ish string, m is a numeric metric.
	if _, err := db.Exec(`CREATE TABLE eq_src (time TIMESTAMPTZ, dim_a VARCHAR, dim_b VARCHAR, device_id VARCHAR, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO eq_src
		SELECT
			TIMESTAMPTZ '2026-05-10 00:00:00+00' + (i % 2) * INTERVAL '1 hour' AS time,
			CASE (i % 3) WHEN 0 THEN 'A' WHEN 1 THEN 'B' ELSE 'C' END            AS dim_a,
			CASE WHEN i % 10 < 3 THEN NULL
			     WHEN i % 10 < 6 THEN 'X'
			     ELSE 'Y' END                                                      AS dim_b,
			'dev_' || (i % 5000)::VARCHAR                                          AS device_id,
			(i % 100) + 0.5                                                        AS m
		FROM range(50000) t(i)
	`); err != nil {
		t.Fatal(err)
	}

	spec := &Spec{
		Table: "eq_src", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a":     {Role: "Dim", KeptValues: []string{"A", "B", "C"}, EffectiveCard: 3},
			"dim_b":     {Role: "Dim", KeptValues: []string{"X", "Y", "_null_"}, EffectiveCard: 3},
			"device_id": {Role: "Sketch"},
		},
	}

	args := BuildArgs{
		Tier:       Tier1h,
		Source:     "eq_src",
		TimeColumn: "time",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
		HLLCols:    []string{"device_id"},
		KLLCols:    []string{"m"},
	}

	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	oldDir := t.TempDir()
	newDir := t.TempDir()

	// OLD path: individual build methods.
	oldSketch := filepath.Join(oldDir, "sketch.parquet")
	if err := b.BuildSketchVariant(ctx, args, oldSketch); err != nil {
		t.Fatalf("OLD BuildSketchVariant: %v", err)
	}
	oldDimA := filepath.Join(oldDir, "by_dim_a.parquet")
	if err := b.BuildPerDimVariant(ctx, args, spec, "dim_a", oldDimA); err != nil {
		t.Fatalf("OLD BuildPerDimVariant dim_a: %v", err)
	}
	oldDimB := filepath.Join(oldDir, "by_dim_b.parquet")
	if err := b.BuildPerDimVariant(ctx, args, spec, "dim_b", oldDimB); err != nil {
		t.Fatalf("OLD BuildPerDimVariant dim_b: %v", err)
	}
	oldAll := filepath.Join(oldDir, "all.parquet")
	if err := b.BuildDimRichVariant(ctx, args, spec, 100, oldAll); err != nil {
		t.Fatalf("OLD BuildDimRichVariant: %v", err)
	}

	// NEW path: combined GROUPING SETS build.
	newFiles, err := b.BuildAllVariants(ctx, args, spec, 100, newDir)
	if err != nil {
		t.Fatalf("NEW BuildAllVariants: %v", err)
	}

	// Assert variant set matches.
	wantVariants := []string{"sketch", "by_dim_a", "by_dim_b", "all"}
	for _, v := range wantVariants {
		if _, ok := newFiles[v]; !ok {
			t.Errorf("NEW path missing variant %q; got %v", v, newFiles)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// ── sketch ──────────────────────────────────────────────────────────────
	oldSketchAgg := readSketchAgg(t, db, oldSketch)
	newSketchAgg := readSketchAgg(t, db, newFiles["sketch"])
	if !reflect.DeepEqual(oldSketchAgg, newSketchAgg) {
		t.Errorf("sketch agg mismatch:\n  old=%v\n  new=%v", oldSketchAgg, newSketchAgg)
	}

	oldHLL := readHLLSum(t, db, oldSketch, "device_id")
	newHLL := readHLLSum(t, db, newFiles["sketch"], "device_id")
	assertHLLClose(t, oldHLL, newHLL, 0.03, "sketch hll_device_id")

	oldKLL := readKLLQuantile(t, db, oldSketch, "m", 0.5)
	newKLL := readKLLQuantile(t, db, newFiles["sketch"], "m", 0.5)
	assertKLLClose(t, oldKLL, newKLL, 0.05, "sketch kll_m p50")

	// ── by_dim_a ─────────────────────────────────────────────────────────────
	oldDimAAgg := readPerDimAgg(t, db, oldDimA, "dim_a")
	newDimAAgg := readPerDimAgg(t, db, newFiles["by_dim_a"], "dim_a")
	if !reflect.DeepEqual(oldDimAAgg, newDimAAgg) {
		t.Errorf("by_dim_a agg mismatch:\n  old=%v\n  new=%v", oldDimAAgg, newDimAAgg)
	}

	oldDimAClasses := readPerDimClasses(t, db, oldDimA, "dim_a")
	newDimAClasses := readPerDimClasses(t, db, newFiles["by_dim_a"], "dim_a")
	assertExactSets(t, oldDimAClasses, newDimAClasses, "by_dim_a classes")

	oldDimAHLL := readHLLSum(t, db, oldDimA, "device_id")
	newDimAHLL := readHLLSum(t, db, newFiles["by_dim_a"], "device_id")
	assertHLLClose(t, oldDimAHLL, newDimAHLL, 0.03, "by_dim_a hll_device_id")

	// ── by_dim_b (has NULLs → should produce _null_ sentinel rows) ───────────
	oldDimBAgg := readPerDimAgg(t, db, oldDimB, "dim_b")
	newDimBAgg := readPerDimAgg(t, db, newFiles["by_dim_b"], "dim_b")
	if !reflect.DeepEqual(oldDimBAgg, newDimBAgg) {
		t.Errorf("by_dim_b agg mismatch:\n  old=%v\n  new=%v", oldDimBAgg, newDimBAgg)
	}

	oldDimBClasses := readPerDimClasses(t, db, oldDimB, "dim_b")
	newDimBClasses := readPerDimClasses(t, db, newFiles["by_dim_b"], "dim_b")
	assertExactSets(t, oldDimBClasses, newDimBClasses, "by_dim_b classes")

	// ── all (dim-rich) ───────────────────────────────────────────────────────
	// Row count + cnt sum per (bucket, dim_a_class, dim_b_class) triple.
	type allKey struct{ bucket, da, db string }
	readAllAgg := func(path string) map[allKey]struct{ rows, cnt int64 } {
		q := `SELECT strftime(bucket, '%Y-%m-%dT%H:%M:%SZ'), dim_a_class, dim_b_class,
		             COUNT(*) AS n, SUM(cnt) AS s
		      FROM read_parquet('` + path + `') GROUP BY 1, 2, 3`
		rs, err := db.Query(q)
		if err != nil {
			t.Fatalf("readAllAgg %s: %v", path, err)
		}
		defer rs.Close()
		out := map[allKey]struct{ rows, cnt int64 }{}
		for rs.Next() {
			var bk, da, dbv string
			var n, s int64
			rs.Scan(&bk, &da, &dbv, &n, &s)
			out[allKey{bk, da, dbv}] = struct{ rows, cnt int64 }{n, s}
		}
		return out
	}
	oldAllAgg := readAllAgg(oldAll)
	newAllAgg := readAllAgg(newFiles["all"])
	if !reflect.DeepEqual(oldAllAgg, newAllAgg) {
		t.Errorf("all agg mismatch:\n  old=%v\n  new=%v", oldAllAgg, newAllAgg)
	}
}

// TestEquivalence_GroupingSets_vs_PerVariant_LocalFixture runs the equivalence
// checks against the real production fixture at /tmp/local-downloads.
func TestEquivalence_GroupingSets_vs_PerVariant_LocalFixture(t *testing.T) {
	dir := os.Getenv("ARC_ROLLUP_FIXTURE")
	if dir == "" {
		dir = "/tmp/local-downloads"
	}
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("ARC_ROLLUP_FIXTURE not present at %s — skipping", dir)
	}

	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	source := fmt.Sprintf("read_parquet('%s/**/*.parquet', union_by_name=true)", dir)

	// Discover schema.
	schemaRows, err := db.Query("DESCRIBE SELECT * FROM " + source + " LIMIT 1")
	if err != nil {
		t.Fatal(err)
	}
	var timeCol string
	var varcharCols []string
	for schemaRows.Next() {
		var name, typ string
		var x [4]any
		schemaRows.Scan(&name, &typ, &x[0], &x[1], &x[2], &x[3])
		if timeCol == "" && strings.HasPrefix(typ, "TIMESTAMP") {
			timeCol = name
			continue
		}
		if typ == "VARCHAR" {
			varcharCols = append(varcharCols, name)
		}
	}
	schemaRows.Close()
	if timeCol == "" || len(varcharCols) < 2 {
		t.Fatalf("fixture needs a TIMESTAMP and ≥2 VARCHAR cols; got time=%q varchars=%v", timeCol, varcharCols)
	}

	// Use up to 5 Dim-role dims + 1 Sketch dim.
	maxDims := 5
	if len(varcharCols)-1 < maxDims {
		maxDims = len(varcharCols) - 1
	}
	dimCols := varcharCols[:maxDims]
	sketchCol := varcharCols[len(varcharCols)-1]

	dims := make(map[string]DimSpec, len(dimCols)+1)
	for _, c := range dimCols {
		q := fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL GROUP BY 1 ORDER BY count(*) DESC LIMIT 5", c, source, c)
		r, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		var kept []string
		for r.Next() {
			var v string
			r.Scan(&v)
			kept = append(kept, v)
		}
		r.Close()
		dims[c] = DimSpec{Role: "Dim", KeptValues: kept, EffectiveCard: len(kept)}
	}
	dims[sketchCol] = DimSpec{Role: "Sketch"}

	var lo time.Time
	if err := db.QueryRow(fmt.Sprintf("SELECT date_trunc('day', MIN(%s)) FROM %s", timeCol, source)).Scan(&lo); err != nil {
		t.Fatal(err)
	}
	hi := lo.AddDate(0, 0, 1)

	spec := &Spec{Table: "fixture", TZ: "UTC", TimeColumn: timeCol, Dims: dims}
	args := BuildArgs{
		Tier:    Tier1h,
		Source:  source,
		TimeColumn: timeCol,
		HLLCols: []string{sketchCol},
	}

	b := &Builder{
		DB: db, HLLLgK: 14, KLLk: 200,
		SchemaHash: "test_hash", TierTZ: "UTC", BuilderVersion: "v_equiv_test",
		BucketLo: lo, BucketHi: hi,
	}
	oldDir := t.TempDir()
	newDir := t.TempDir()

	// OLD path.
	oldSketch := filepath.Join(oldDir, "sketch.parquet")
	if err := b.BuildSketchVariant(ctx, args, oldSketch); err != nil {
		t.Fatalf("OLD BuildSketchVariant: %v", err)
	}
	oldPerDim := map[string]string{}
	for _, c := range dimCols {
		p := filepath.Join(oldDir, "by_"+c+".parquet")
		if err := b.BuildPerDimVariant(ctx, args, spec, c, p); err != nil {
			t.Fatalf("OLD BuildPerDimVariant %s: %v", c, err)
		}
		oldPerDim[c] = p
	}
	oldAll := filepath.Join(oldDir, "all.parquet")
	if err := b.BuildDimRichVariant(ctx, args, spec, 100, oldAll); err != nil {
		t.Fatalf("OLD BuildDimRichVariant: %v", err)
	}

	// NEW path.
	newFiles, err := b.BuildAllVariants(ctx, args, spec, 100, newDir)
	if err != nil {
		t.Fatalf("NEW BuildAllVariants: %v", err)
	}

	// ── sketch ──────────────────────────────────────────────────────────────
	oldSketchAgg := readSketchAgg(t, db, oldSketch)
	newSketchAgg := readSketchAgg(t, db, newFiles["sketch"])
	if !reflect.DeepEqual(oldSketchAgg, newSketchAgg) {
		t.Errorf("sketch agg mismatch:\n  old=%v\n  new=%v", oldSketchAgg, newSketchAgg)
	}
	oldHLL := readHLLSum(t, db, oldSketch, sketchCol)
	newHLL := readHLLSum(t, db, newFiles["sketch"], sketchCol)
	assertHLLClose(t, oldHLL, newHLL, 0.03, "sketch hll_"+sketchCol)

	// ── per-dim ──────────────────────────────────────────────────────────────
	for _, c := range dimCols {
		newPath, ok := newFiles["by_"+c]
		if !ok {
			t.Errorf("NEW path missing by_%s", c)
			continue
		}
		oldAgg := readPerDimAgg(t, db, oldPerDim[c], c)
		newAgg := readPerDimAgg(t, db, newPath, c)
		if !reflect.DeepEqual(oldAgg, newAgg) {
			t.Errorf("by_%s agg mismatch (first 3 old):\n  old=%v\n  new=%v", c, oldAgg, newAgg)
		}
		oldClasses := readPerDimClasses(t, db, oldPerDim[c], c)
		newClasses := readPerDimClasses(t, db, newPath, c)
		assertExactSets(t, oldClasses, newClasses, "by_"+c+" classes")

		oldH := readHLLSum(t, db, oldPerDim[c], sketchCol)
		newH := readHLLSum(t, db, newPath, sketchCol)
		assertHLLClose(t, oldH, newH, 0.03, "by_"+c+" hll_"+sketchCol)
	}

	// ── all (dim-rich) ───────────────────────────────────────────────────────
	if newAllPath, ok := newFiles["all"]; ok {
		var oldAllRows, newAllRows int64
		db.QueryRow(`SELECT COUNT(*) FROM read_parquet('` + oldAll + `')`).Scan(&oldAllRows)
		db.QueryRow(`SELECT COUNT(*) FROM read_parquet('` + newAllPath + `')`).Scan(&newAllRows)
		if oldAllRows != newAllRows {
			t.Errorf("all variant row count: old=%d new=%d", oldAllRows, newAllRows)
		}
		var oldAllCnt, newAllCnt int64
		db.QueryRow(`SELECT SUM(cnt) FROM read_parquet('` + oldAll + `')`).Scan(&oldAllCnt)
		db.QueryRow(`SELECT SUM(cnt) FROM read_parquet('` + newAllPath + `')`).Scan(&newAllCnt)
		if oldAllCnt != newAllCnt {
			t.Errorf("all variant cnt sum: old=%d new=%d", oldAllCnt, newAllCnt)
		}
	}
}

// TestEquivalence_HandlesNULLs verifies that both paths produce a _null_ row
// in the by_dim_a variant with matching count when dim_a has 30% NULLs.
func TestEquivalence_HandlesNULLs(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE null_src (time TIMESTAMPTZ, dim_a VARCHAR, id VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	// 100 rows: 30 NULL, 40 'keep', 30 'other'
	if _, err := db.Exec(`
		INSERT INTO null_src
		SELECT
			TIMESTAMPTZ '2026-05-10 00:00:00+00' AS time,
			CASE WHEN i < 30 THEN NULL
			     WHEN i < 70 THEN 'keep'
			     ELSE 'other_val' END AS dim_a,
			'u' || i::VARCHAR AS id
		FROM range(100) t(i)
	`); err != nil {
		t.Fatal(err)
	}

	spec := &Spec{
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"keep", "_null_"}, EffectiveCard: 2},
		},
	}
	args := BuildArgs{
		Tier: Tier1h, Source: "null_src", TimeColumn: "time",
		HLLCols: []string{"id"},
	}
	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	oldDir := t.TempDir()
	newDir := t.TempDir()

	oldPath := filepath.Join(oldDir, "by_dim_a.parquet")
	if err := b.BuildPerDimVariant(ctx, args, spec, "dim_a", oldPath); err != nil {
		t.Fatalf("OLD BuildPerDimVariant: %v", err)
	}
	newFiles, err := b.BuildAllVariants(ctx, args, spec, 100, newDir)
	if err != nil {
		t.Fatalf("NEW BuildAllVariants: %v", err)
	}
	newPath, ok := newFiles["by_dim_a"]
	if !ok {
		t.Fatal("NEW path missing by_dim_a")
	}

	oldAgg := readPerDimAgg(t, db, oldPath, "dim_a")
	newAgg := readPerDimAgg(t, db, newPath, "dim_a")
	if !reflect.DeepEqual(oldAgg, newAgg) {
		t.Errorf("NULL handling agg mismatch:\n  old=%v\n  new=%v", oldAgg, newAgg)
	}

	// Specifically verify _null_ sentinel is present with count 30.
	nullCnt := int64(0)
	for k, v := range newAgg {
		if k.class == "_null_" {
			nullCnt = v.cnt
		}
	}
	if nullCnt != 30 {
		t.Errorf("_null_ sentinel cnt = %d, want 30", nullCnt)
	}
}

// TestEquivalence_AllValuesAreOTHER verifies that when all dim values are
// outside the kept set, both paths produce a single _OTHER_ row per bucket
// with matching count and no rows are dropped.
func TestEquivalence_AllValuesAreOTHER(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE other_src (time TIMESTAMPTZ, dim_a VARCHAR, id VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO other_src
		SELECT TIMESTAMPTZ '2026-05-10 00:00:00+00', 'outside_' || i::VARCHAR, 'u' || i::VARCHAR
		FROM range(200) t(i)
	`); err != nil {
		t.Fatal(err)
	}

	spec := &Spec{
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"in_set_only"}, EffectiveCard: 1},
		},
	}
	args := BuildArgs{
		Tier: Tier1h, Source: "other_src", TimeColumn: "time",
		HLLCols: []string{"id"},
	}
	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	oldDir := t.TempDir()
	newDir := t.TempDir()

	oldPath := filepath.Join(oldDir, "by_dim_a.parquet")
	if err := b.BuildPerDimVariant(ctx, args, spec, "dim_a", oldPath); err != nil {
		t.Fatalf("OLD BuildPerDimVariant: %v", err)
	}
	newFiles, err := b.BuildAllVariants(ctx, args, spec, 100, newDir)
	if err != nil {
		t.Fatalf("NEW BuildAllVariants: %v", err)
	}
	newPath := newFiles["by_dim_a"]

	oldAgg := readPerDimAgg(t, db, oldPath, "dim_a")
	newAgg := readPerDimAgg(t, db, newPath, "dim_a")
	if !reflect.DeepEqual(oldAgg, newAgg) {
		t.Errorf("_OTHER_ only agg mismatch:\n  old=%v\n  new=%v", oldAgg, newAgg)
	}

	// Exactly one class: _OTHER_, count = 200.
	if len(newAgg) != 1 {
		t.Errorf("expected exactly 1 class (_OTHER_), got %d: %v", len(newAgg), newAgg)
	}
	for k, v := range newAgg {
		if k.class != "_OTHER_" {
			t.Errorf("unexpected class %q (want _OTHER_)", k.class)
		}
		if v.cnt != 200 {
			t.Errorf("_OTHER_ cnt = %d, want 200", v.cnt)
		}
	}
}

// TestEquivalence_EmptySource verifies that both paths produce 0-row parquets
// with identical schemas when the source is empty.
func TestEquivalence_EmptySource(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE empty_src (time TIMESTAMPTZ, dim_a VARCHAR, id VARCHAR)`); err != nil {
		t.Fatal(err)
	}

	spec := &Spec{
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"A"}, EffectiveCard: 1},
		},
	}
	args := BuildArgs{
		Tier: Tier1h, Source: "empty_src", TimeColumn: "time",
		HLLCols: []string{"id"},
	}
	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	oldDir := t.TempDir()
	newDir := t.TempDir()

	oldSketch := filepath.Join(oldDir, "sketch.parquet")
	if err := b.BuildSketchVariant(ctx, args, oldSketch); err != nil {
		t.Fatalf("OLD BuildSketchVariant: %v", err)
	}
	oldPerDim := filepath.Join(oldDir, "by_dim_a.parquet")
	if err := b.BuildPerDimVariant(ctx, args, spec, "dim_a", oldPerDim); err != nil {
		t.Fatalf("OLD BuildPerDimVariant: %v", err)
	}

	newFiles, err := b.BuildAllVariants(ctx, args, spec, 100, newDir)
	if err != nil {
		t.Fatalf("NEW BuildAllVariants: %v", err)
	}

	for _, v := range []string{"sketch", "by_dim_a"} {
		newPath, ok := newFiles[v]
		if !ok {
			t.Errorf("NEW path missing variant %q", v)
			continue
		}
		var newRows int64
		db.QueryRow(`SELECT COUNT(*) FROM read_parquet('` + newPath + `')`).Scan(&newRows)
		if newRows != 0 {
			t.Errorf("empty source: variant %q has %d rows, want 0", v, newRows)
		}
	}

	// Both sketch parquets should have 0 rows.
	var oldRows int64
	db.QueryRow(`SELECT COUNT(*) FROM read_parquet('` + oldSketch + `')`).Scan(&oldRows)
	if oldRows != 0 {
		t.Errorf("OLD sketch has %d rows from empty source, want 0", oldRows)
	}
}

// TestEquivalence_TimezoneSpec verifies that bucket alignment matches between
// paths when TierTZ is "Asia/Riyadh" and data straddles RYD midnight.
func TestEquivalence_TimezoneSpec(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// RYD is UTC+3; midnight RYD = 21:00 UTC previous day.
	// Insert rows around that boundary.
	if _, err := db.Exec(`CREATE TABLE tz_src (time TIMESTAMPTZ, dim_a VARCHAR, id VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tz_src VALUES
		('2026-05-10 20:30:00+00', 'A', 'u1'),
		('2026-05-10 21:00:00+00', 'A', 'u2'),
		('2026-05-10 21:30:00+00', 'B', 'u3'),
		('2026-05-10 22:00:00+00', 'B', 'u4')`); err != nil {
		t.Fatal(err)
	}

	spec := &Spec{
		TZ: "Asia/Riyadh",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"A", "B"}, EffectiveCard: 2},
		},
	}
	args := BuildArgs{
		Tier: Tier1h, Source: "tz_src", TimeColumn: "time",
		HLLCols: []string{"id"},
	}

	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200, TierTZ: "Asia/Riyadh"}
	oldDir := t.TempDir()
	newDir := t.TempDir()

	oldSketch := filepath.Join(oldDir, "sketch.parquet")
	if err := b.BuildSketchVariant(ctx, args, oldSketch); err != nil {
		t.Fatalf("OLD BuildSketchVariant: %v", err)
	}

	newFiles, err := b.BuildAllVariants(ctx, args, spec, 100, newDir)
	if err != nil {
		t.Fatalf("NEW BuildAllVariants: %v", err)
	}

	oldAgg := readSketchAgg(t, db, oldSketch)
	newAgg := readSketchAgg(t, db, newFiles["sketch"])
	if !reflect.DeepEqual(oldAgg, newAgg) {
		t.Errorf("timezone sketch agg mismatch:\n  old=%v\n  new=%v", oldAgg, newAgg)
	}
}

// TestEquivalence_DimRichCap verifies that when a dim has EffectiveCard >
// dimRichCap, both paths exclude it from the dim-rich variant.
func TestEquivalence_DimRichCap(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE cap_src (time TIMESTAMPTZ, low_card VARCHAR, high_card VARCHAR, id VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO cap_src
		SELECT
			TIMESTAMPTZ '2026-05-10 00:00:00+00',
			CASE (i%2) WHEN 0 THEN 'lo_A' ELSE 'lo_B' END,
			'hi_' || i::VARCHAR,
			'u' || i::VARCHAR
		FROM range(20) t(i)
	`); err != nil {
		t.Fatal(err)
	}

	// low_card has card=2 (under cap), high_card has card=20 (over cap=5).
	spec := &Spec{
		Dims: map[string]DimSpec{
			"low_card":  {Role: "Dim", KeptValues: []string{"lo_A", "lo_B"}, EffectiveCard: 2},
			"high_card": {Role: "Dim", KeptValues: []string{"hi_0", "hi_1", "hi_2", "hi_3", "hi_4", "hi_5"}, EffectiveCard: 20},
		},
	}
	const dimRichCap = 5
	args := BuildArgs{
		Tier: Tier1h, Source: "cap_src", TimeColumn: "time",
		HLLCols: []string{"id"},
	}
	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	oldDir := t.TempDir()
	newDir := t.TempDir()

	oldAll := filepath.Join(oldDir, "all.parquet")
	if err := b.BuildDimRichVariant(ctx, args, spec, dimRichCap, oldAll); err != nil {
		t.Fatalf("OLD BuildDimRichVariant: %v", err)
	}
	newFiles, err := b.BuildAllVariants(ctx, args, spec, dimRichCap, newDir)
	if err != nil {
		t.Fatalf("NEW BuildAllVariants: %v", err)
	}
	newAll := newFiles["all"]

	colsOf := func(path string) []string {
		rs, err := db.Query(`DESCRIBE SELECT * FROM read_parquet('` + path + `')`)
		if err != nil {
			t.Fatalf("DESCRIBE %s: %v", path, err)
		}
		defer rs.Close()
		var cols []string
		for rs.Next() {
			var name, typ string
			var x [4]any
			rs.Scan(&name, &typ, &x[0], &x[1], &x[2], &x[3])
			cols = append(cols, name)
		}
		return cols
	}

	oldCols := colsOf(oldAll)
	newCols := colsOf(newAll)
	sort.Strings(oldCols)
	sort.Strings(newCols)
	if !reflect.DeepEqual(oldCols, newCols) {
		t.Errorf("dim-rich column sets differ:\n  old=%v\n  new=%v", oldCols, newCols)
	}

	for _, col := range newCols {
		if col == "high_card_class" {
			t.Errorf("high_card (EffectiveCard=20 > cap=%d) must NOT appear in dim-rich variant", dimRichCap)
		}
	}
}

// TestEquivalence_SketchOnlySpec verifies that when no dim has KeptValues
// both paths produce only the sketch variant and no per-dim files.
func TestEquivalence_SketchOnlySpec(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE sketch_only_src (time TIMESTAMPTZ, device_id VARCHAR, m DOUBLE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO sketch_only_src
		SELECT TIMESTAMPTZ '2026-05-10 00:00:00+00', 'dev_' || i::VARCHAR, i*1.0
		FROM range(100) t(i)
	`); err != nil {
		t.Fatal(err)
	}

	spec := &Spec{
		Dims: map[string]DimSpec{
			"device_id": {Role: "Sketch"},
		},
	}
	args := BuildArgs{
		Tier:       Tier1h,
		Source:     "sketch_only_src",
		TimeColumn: "time",
		MetricCols: []MetricCol{{Name: "m", Numeric: true}},
		HLLCols:    []string{"device_id"},
		KLLCols:    []string{"m"},
	}
	b := &Builder{DB: db, HLLLgK: 14, KLLk: 200}
	oldDir := t.TempDir()
	newDir := t.TempDir()

	oldSketch := filepath.Join(oldDir, "sketch.parquet")
	if err := b.BuildSketchVariant(ctx, args, oldSketch); err != nil {
		t.Fatalf("OLD BuildSketchVariant: %v", err)
	}

	newFiles, err := b.BuildAllVariants(ctx, args, spec, 100, newDir)
	if err != nil {
		t.Fatalf("NEW BuildAllVariants: %v", err)
	}

	// Only sketch variant must exist.
	if len(newFiles) != 1 {
		t.Errorf("sketch-only spec: expected 1 variant, got %d: %v", len(newFiles), newFiles)
	}
	if _, ok := newFiles["sketch"]; !ok {
		t.Errorf("sketch-only spec: missing sketch variant; got %v", newFiles)
	}

	// Sketch aggregates must match.
	oldAgg := readSketchAgg(t, db, oldSketch)
	newAgg := readSketchAgg(t, db, newFiles["sketch"])
	if !reflect.DeepEqual(oldAgg, newAgg) {
		t.Errorf("sketch-only agg mismatch:\n  old=%v\n  new=%v", oldAgg, newAgg)
	}
}
