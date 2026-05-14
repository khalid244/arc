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
// inferenceSampleRows caps how many source rows we materialize into a
// temp table for cardinality estimation. 1M is large enough to resolve
// the default classification thresholds (1024 dim, 100000 sketch) with
// HLL's ~3% error margin while bounding memory and scan time. The
// REPEATABLE seed makes the sample deterministic for the same source
// data — same restart, same sample, same classifications.
const (
	inferenceSampleRows = 1_000_000
	inferenceSampleSeed = 42
	sampleTempTable     = "__rollup_inference_sample"
)

func (s *DuckDBSampler) SampleSourceColumns(ctx context.Context, dbName, table string) ([]ColumnStats, error) {
	from, cols, err := s.describe(ctx, dbName, table)
	if err != nil {
		return nil, err
	}

	// Materialize a deterministic, bounded sample ONCE so the per-column
	// distinct counts below are a single in-memory pass each instead of
	// N full source scans. USING SAMPLE ... REPEATABLE pins the sampling
	// seed so the same data produces the same sample on every restart.
	createStmt := fmt.Sprintf(
		"CREATE OR REPLACE TEMP TABLE %s AS SELECT * FROM %s USING SAMPLE reservoir(%d ROWS) REPEATABLE (%d)",
		sampleTempTable, from, inferenceSampleRows, inferenceSampleSeed,
	)
	if _, err := s.DB.ExecContext(ctx, createStmt); err != nil {
		return nil, fmt.Errorf("sampler create temp sample %s.%s: %w", dbName, table, err)
	}
	defer func() {
		_, _ = s.DB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", sampleTempTable))
	}()

	stats := make([]ColumnStats, 0, len(cols))
	for _, c := range cols {
		distinct, derr := approxCountDistinct(ctx, s.DB, sampleTempTable, c.Name)
		if derr != nil {
			// Column-level failure (e.g. unsupported type for HLL aggregator)
			// shouldn't sink the whole table. Record 0 distinct so the column
			// gets classified as RoleDrop (or a metric, by type) and continue.
			distinct = 0
		}
		stats = append(stats, ColumnStats{
			Name:     c.Name,
			Type:     c.Type,
			Distinct: distinct,
		})
	}
	return stats, nil
}

// DescribeSourceColumns returns the column shape without the per-column
// COUNT(DISTINCT) sweep. Distinct is left zero. Used by the SpecsCached
// path to verify that a table's column SET hasn't changed since the
// persisted-specs cache was written.
func (s *DuckDBSampler) DescribeSourceColumns(ctx context.Context, dbName, table string) ([]ColumnStats, error) {
	_, cols, err := s.describe(ctx, dbName, table)
	if err != nil {
		return nil, err
	}
	stats := make([]ColumnStats, 0, len(cols))
	for _, c := range cols {
		stats = append(stats, ColumnStats{Name: c.Name, Type: c.Type})
	}
	return stats, nil
}

// describe runs the shared DESCRIBE pass: resolves the FROM expression and
// returns the (name, type) list. Returns the FROM expression too so the
// caller can reuse it for COUNT(DISTINCT) calls without re-running the
// resolver.
func (s *DuckDBSampler) describe(ctx context.Context, dbName, table string) (string, []ColumnStats, error) {
	if s.Resolver == nil {
		return "", nil, fmt.Errorf("sampler: nil resolver")
	}
	from := s.Resolver(dbName, table)
	if from == "" {
		return "", nil, fmt.Errorf("sampler: resolver returned empty FROM for %s.%s", dbName, table)
	}
	q := fmt.Sprintf("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM %s)", from)
	rows, err := s.DB.QueryContext(ctx, q)
	if err != nil {
		return "", nil, fmt.Errorf("sampler describe %s.%s: %w", dbName, table, err)
	}
	defer rows.Close()
	var cols []ColumnStats
	for rows.Next() {
		var c ColumnStats
		if err := rows.Scan(&c.Name, &c.Type); err != nil {
			return "", nil, fmt.Errorf("sampler scan describe row: %w", err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return "", nil, fmt.Errorf("sampler iterate describe: %w", err)
	}
	return from, cols, nil
}

// approxCountDistinct runs HLL on `col` against `from` (which is normally
// the local TEMP TABLE materialized by SampleSourceColumns, NOT the source
// glob — running HLL over a billion-row remote parquet is the slow path we
// avoided by sampling first).
func approxCountDistinct(ctx context.Context, db *sql.DB, from, col string) (int64, error) {
	q := fmt.Sprintf(`SELECT approx_count_distinct("%s") FROM %s`, strings.ReplaceAll(col, `"`, `""`), from)
	row := db.QueryRowContext(ctx, q)
	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
