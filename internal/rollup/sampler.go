package rollup

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SourceTableResolver returns the SQL FROM expression that reads a table's
// source data — typically a `read_parquet('<glob>', union_by_name=true)` call
// for the production storage backend, or a bare table name for tests.
type SourceTableResolver func(database, table string) string

// DuckDBSampler samples column statistics from a DuckDB connection. It is the
// production-side Sampler implementation: it issues DESCRIBE + COUNT DISTINCT
// queries against a `read_parquet(...)` expression resolved per (db, table).
type DuckDBSampler struct {
	DB       *sql.DB
	Resolver SourceTableResolver
}

// NewDuckDBSampler wires a DuckDBSampler.
func NewDuckDBSampler(db *sql.DB, resolver SourceTableResolver) *DuckDBSampler {
	return &DuckDBSampler{DB: db, Resolver: resolver}
}

// SampleSourceColumns implements the Sampler interface. It runs DESCRIBE
// against the resolved FROM expression to get (name, type) for each column,
// then issues one COUNT(DISTINCT) per column to fill in cardinality. Returns
// an empty slice (not an error) when the source has zero rows or doesn't
// exist yet — the caller logs and skips.
//
// Performance note: COUNT(DISTINCT) on every column is O(N×cols). For large
// tables this can be slow; we accept that for now because schema inference
// runs once at server startup, not on every query. If this becomes a
// bottleneck, switch to TABLESAMPLE or HyperLogLog approx_count_distinct.
func (s *DuckDBSampler) SampleSourceColumns(ctx context.Context, dbName, table string) ([]ColumnStats, error) {
	if s.Resolver == nil {
		return nil, fmt.Errorf("sampler: nil resolver")
	}
	from := s.Resolver(dbName, table)
	if from == "" {
		return nil, fmt.Errorf("sampler: resolver returned empty FROM for %s.%s", dbName, table)
	}

	// DESCRIBE returns column_name, column_type, null, key, default, extra.
	describe := fmt.Sprintf("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM %s)", from)
	rows, err := s.DB.QueryContext(ctx, describe)
	if err != nil {
		return nil, fmt.Errorf("sampler describe %s.%s: %w", dbName, table, err)
	}
	type colInfo struct {
		name, typ string
	}
	var cols []colInfo
	for rows.Next() {
		var c colInfo
		if err := rows.Scan(&c.name, &c.typ); err != nil {
			rows.Close()
			return nil, fmt.Errorf("sampler scan describe row: %w", err)
		}
		cols = append(cols, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sampler iterate describe: %w", err)
	}

	stats := make([]ColumnStats, 0, len(cols))
	for _, c := range cols {
		distinct, derr := countDistinct(ctx, s.DB, from, c.name)
		if derr != nil {
			// Column-level failure (e.g. COUNT DISTINCT on an unsupported type)
			// shouldn't sink the whole table. Record 0 distinct so the column
			// gets classified as RoleDrop (or a metric, by type) and continue.
			distinct = 0
		}
		stats = append(stats, ColumnStats{
			Name:     c.name,
			Type:     c.typ,
			Distinct: distinct,
		})
	}
	return stats, nil
}

func countDistinct(ctx context.Context, db *sql.DB, from, col string) (int64, error) {
	q := fmt.Sprintf(`SELECT COUNT(DISTINCT "%s") FROM %s`, strings.ReplaceAll(col, `"`, `""`), from)
	row := db.QueryRowContext(ctx, q)
	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
