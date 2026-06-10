package rollup

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// openLocalDuck opens a plain in-memory DuckDB (no httpfs/S3) for tests that read
// local Parquet files — no MinIO corpus required, so these always run.
func openLocalDuck(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	if _, err := db.Exec("SET TimeZone='UTC'"); err != nil {
		db.Close()
		t.Fatalf("set tz: %v", err)
	}
	return db
}

// TestBuildMonthNullFillsSchemaDrift reproduces the production scenario where a
// whole-month cube build references a dimension column that did not exist that
// month (a newer, sparse event property), and pins the NULL-fill behavior:
//   - the month BUILDS (no Binder Error, no skip): the absent dimension is stored
//     as a typed NULL group, matching what a raw union_by_name source scan would
//     return for those rows;
//   - the output carries the FULL cube schema (dim column present, typed);
//   - a cube whose dimension IS present still builds with real values.
func TestBuildMonthNullFillsSchemaDrift(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	dir := t.TempDir()

	// An "April" month whose schema has time + site, but NOT email (added later).
	src := filepath.Join(dir, "apr.parquet")
	copySrc := fmt.Sprintf(
		`COPY (SELECT TIMESTAMPTZ '2026-04-10 01:30:00' AS "time", 'web' AS site
		       UNION ALL SELECT TIMESTAMPTZ '2026-04-10 02:15:00', 'ios') TO '%s' (FORMAT PARQUET)`, src)
	if _, err := db.Exec(copySrc); err != nil {
		t.Fatalf("write source parquet: %v", err)
	}
	glob := "['" + src + "']"
	lo, hi := "2026-04-01 00:00:00", "2026-05-01 00:00:00"

	// A by-email cube over a month lacking "email" builds with a NULL email dim.
	emailSpec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"email"},
		Aggs: []Aggregate{{Kind: AggCount}}, ColTypes: map[string]string{"email": "VARCHAR"}}
	emailDest := filepath.Join(dir, "email.parquet")
	entry, err := BuildRange(db, emailSpec, glob, "time", "2026-04", lo, hi, emailDest)
	if err != nil {
		t.Fatalf("by-email month build over drifted month must NULL-fill, got: %v", err)
	}
	if entry.Rows == 0 {
		t.Fatal("by-email cube should have NULL-dim group rows")
	}
	types := describeFile(t, db, emailDest)
	if types["email"] != "VARCHAR" {
		t.Fatalf("email dim type = %q, want VARCHAR (typed NULL from ColTypes)", types["email"])
	}
	var cnt int64
	var nonNull int64
	if err := db.QueryRow("SELECT sum(_cnt)::BIGINT, count(email) FROM read_parquet('"+emailDest+"')").Scan(&cnt, &nonNull); err != nil {
		t.Fatalf("read cube: %v", err)
	}
	if cnt != 2 || nonNull != 0 {
		t.Fatalf("cube _cnt=%d nonNullEmail=%d, want 2 / 0 (all rows under the NULL email group)", cnt, nonNull)
	}

	// A by-site cube still builds with real dimension values.
	siteSpec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}}}
	siteDest := filepath.Join(dir, "site.parquet")
	sEntry, err := BuildRange(db, siteSpec, glob, "time", "2026-04", lo, hi, siteDest)
	if err != nil {
		t.Fatalf("by-site month build failed: %v", err)
	}
	if sEntry.Rows == 0 {
		t.Fatal("by-site cube should have rows")
	}
	var sites int64
	if err := db.QueryRow("SELECT count(DISTINCT site) FROM read_parquet('" + siteDest + "')").Scan(&sites); err != nil {
		t.Fatalf("read site cube: %v", err)
	}
	if sites != 2 {
		t.Fatalf("distinct sites = %d, want 2 (real dim values preserved)", sites)
	}
}

// TestBuildUsesCaseFoldedColumns (F9) — DuckDB binds identifiers
// case-insensitively and union_by_name merges `UserId`/`userId` into one column,
// so the NULL-fill presence check must case-fold: a spec column present in the
// source under different casing must be built from the REAL column (non-NULL
// values), never NULL-filled as "absent".
func TestBuildUsesCaseFoldedColumns(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	dir := t.TempDir()

	src := filepath.Join(dir, "src.parquet")
	mustExec(t, db, fmt.Sprintf(
		`COPY (SELECT TIMESTAMPTZ '2026-05-01 10:00:00' AS "time", 'u7' AS "UserId", 100::BIGINT AS "Bytes"
		       UNION ALL SELECT TIMESTAMPTZ '2026-05-01 10:30:00', 'u7', 50::BIGINT) TO '%s' (FORMAT PARQUET)`, src))

	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"userId"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Col: "bytes"}}}
	dest := filepath.Join(dir, "cube.parquet")
	if _, err := BuildDay(db, spec, "['"+src+"']", "time", "2026-05-01", dest); err != nil {
		t.Fatalf("build over differently-cased source columns: %v", err)
	}
	var user sql.NullString
	var sum sql.NullFloat64
	if err := db.QueryRow(`SELECT any_value("userId"), sum(_sum_bytes)::DOUBLE FROM read_parquet('`+dest+`')`).Scan(&user, &sum); err != nil {
		t.Fatalf("read cube: %v", err)
	}
	if !user.Valid || user.String != "u7" {
		t.Fatalf("dim userId = %v, want 'u7' — a present (differently-cased) column must NOT be NULL-filled", user)
	}
	if !sum.Valid || sum.Float64 != 150 {
		t.Fatalf("sum bytes = %v, want 150 — a present (differently-cased) metric must NOT be NULL-filled", sum)
	}
}
