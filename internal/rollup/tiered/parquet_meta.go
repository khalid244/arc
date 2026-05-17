package tiered

import (
	"context"
	"database/sql"
)

// FileSchemaHash extracts the schema_hash KV-metadata from a precalc
// parquet at the given path. Returns "" + nil error if the file has no
// schema_hash stamped (files written before the KV-metadata feature
// landed will have none — caller decides whether to treat empty as
// match-anything or refuse).
//
// Uses DuckDB's parquet_kv_metadata() table function. Caller passes the
// shared DuckDB connection (no new connection opened).
func FileSchemaHash(ctx context.Context, db *sql.DB, path string) (string, error) {
	row := db.QueryRowContext(ctx,
		"SELECT CAST(value AS VARCHAR) FROM parquet_kv_metadata(?) WHERE CAST(key AS VARCHAR) = 'schema_hash' LIMIT 1",
		path,
	)
	var v string
	err := row.Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}
