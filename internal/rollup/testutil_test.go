package rollup

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// openDuckDBWithDataSketches opens an in-memory DuckDB and loads the
// datasketches community extension. Skips the test if neither LOAD nor
// INSTALL succeeds (e.g., CI without internet and without a baked image).
func openDuckDBWithDataSketches(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	if _, err := db.Exec("LOAD datasketches"); err != nil {
		if _, ierr := db.Exec("INSTALL datasketches FROM community"); ierr != nil {
			db.Close()
			t.Skipf("datasketches extension not available locally and INSTALL failed: %v", ierr)
		}
		if _, lerr := db.Exec("LOAD datasketches"); lerr != nil {
			db.Close()
			t.Skipf("datasketches LOAD failed after install: %v", lerr)
		}
	}
	return db
}
