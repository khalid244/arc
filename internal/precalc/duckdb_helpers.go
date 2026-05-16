package precalc

import (
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
)

// OpenWithDataSketches opens an in-memory DuckDB connection with the
// datasketches community extension loaded and session TimeZone set.
// All precalc code paths (builder, router) use this so timezone alignment
// is consistent.
func OpenWithDataSketches(tz string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	for _, stmt := range []string{
		"INSTALL datasketches FROM community",
		"LOAD datasketches",
		fmt.Sprintf("SET TimeZone = '%s'", tz),
	} {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("setup %q: %w", stmt, err)
		}
	}
	return db, nil
}
