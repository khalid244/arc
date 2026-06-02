package rollup

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// testEndpoint is the local MinIO that holds the 6-month downloads corpus.
const (
	testEndpoint = "localhost:9000"
	testKey      = "arcadmin"
	testSecret   = "arcpassword"
	testBucket   = "arc-test"
	// A single fully-sealed historic day used as the smallest comparison window.
	testDayGlob = "s3://arc-test/default/downloads/2025/12/28/**/*.parquet"
)

// openTestDuck opens a DuckDB connection wired to the local MinIO, with
// httpfs + datasketches loaded. Skips the whole test if MinIO is unreachable
// so unit-only runs (CI without the corpus) still pass.
func openTestDuck(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	stmts := []string{
		"INSTALL httpfs", "LOAD httpfs",
		"INSTALL datasketches FROM community", "LOAD datasketches",
		fmt.Sprintf("SET GLOBAL s3_access_key_id='%s'", testKey),
		fmt.Sprintf("SET GLOBAL s3_secret_access_key='%s'", testSecret),
		fmt.Sprintf("SET GLOBAL s3_endpoint='%s'", testEndpoint),
		"SET GLOBAL s3_url_style='path'",
		"SET GLOBAL s3_use_ssl=false",
		"SET TimeZone='UTC'", // deterministic date_trunc semantics for cube vs source
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			db.Close()
			t.Fatalf("setup %q: %v", s, err)
		}
	}
	return db
}

// corpusReachable returns false (and skips) when MinIO isn't serving the corpus.
func requireCorpus(t *testing.T, db *sql.DB) {
	t.Helper()
	if os.Getenv("ROLLUP_SKIP_CORPUS") != "" {
		t.Skip("ROLLUP_SKIP_CORPUS set")
	}
	var n int64
	if err := db.QueryRow("SELECT count(*) FROM read_parquet('" + testDayGlob + "', union_by_name=true)").Scan(&n); err != nil {
		t.Skipf("corpus unreachable (start MinIO): %v", err)
	}
	if n == 0 {
		t.Skip("corpus empty")
	}
}

func TestSmoke_CorpusReachable(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)

	var n int64
	if err := db.QueryRow("SELECT count(*) FROM read_parquet('" + testDayGlob + "', union_by_name=true)").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n < 1_000_000 {
		t.Fatalf("expected >1M rows in the 2025-12-28 day, got %d", n)
	}
	t.Logf("smoke ok: %d rows in 2025-12-28", n)
}
