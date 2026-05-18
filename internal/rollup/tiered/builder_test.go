package tiered

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
		Tier:   Tier1h,
		Source: "evt",
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

