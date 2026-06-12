package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// readTagColumnsFromParquetFiles reads and unions "arc:tags" metadata from multiple Parquet files.
// Returns the union of all tag column names across files, handling schema evolution where
// newer files may have additional tags. Returns nil if no files have tag metadata.
//
// The "arc:tags" key is written to the Parquet footer by the Arrow writer during ingestion.
// Arrow schema metadata keys are stored directly as Parquet key-value metadata entries,
// so DuckDB's parquet_kv_metadata() reads them without any extra decoding.
func readTagColumnsFromParquetFiles(ctx context.Context, db *sql.DB, filePaths []string) ([]string, error) {
	tagSet := make(map[string]struct{})
	foundAny := false

	for _, filePath := range filePaths {
		tags, err := readTagColumnsFromParquet(ctx, db, filePath)
		if err != nil {
			return nil, err
		}
		if tags != nil {
			foundAny = true
			for _, tag := range tags {
				tagSet[tag] = struct{}{}
			}
		}
	}

	if !foundAny {
		return nil, nil
	}

	result := make([]string, 0, len(tagSet))
	for tag := range tagSet {
		result = append(result, tag)
	}
	sort.Strings(result)
	return result, nil
}

// readTagColumnsFromParquet reads the "arc:tags" metadata from a single Parquet file.
// Returns nil if no tag metadata is found (file written before dedup feature).
// A Parquet footer may contain more than one "arc:tags" entry (e.g. after a
// concat write); union all of them so a near-duplicate row would still be
// caught by the dedup PARTITION BY clause.
func readTagColumnsFromParquet(ctx context.Context, db *sql.DB, filePath string) ([]string, error) {
	query := fmt.Sprintf(
		`SELECT value FROM parquet_kv_metadata('%s') WHERE key = 'arc:tags'`,
		escapeSQLPath(filePath),
	)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet metadata: %w", err)
	}
	defer rows.Close()

	tagSet := make(map[string]struct{})
	foundAny := false
	for rows.Next() {
		var tagValue string
		if err := rows.Scan(&tagValue); err != nil {
			return nil, fmt.Errorf("failed to scan parquet metadata row: %w", err)
		}
		if tagValue == "" {
			continue
		}
		foundAny = true
		for _, tag := range strings.Split(tagValue, ",") {
			if tag == "" || !isValidIdentifier(tag) {
				return nil, fmt.Errorf("invalid tag column name in parquet metadata: %q", tag)
			}
			tagSet[tag] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate parquet metadata: %w", err)
	}
	if !foundAny {
		return nil, nil
	}

	tags := make([]string, 0, len(tagSet))
	for tag := range tagSet {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	return tags, nil
}

// isValidIdentifier checks if a string is a safe SQL column identifier.
// Allows alphanumeric characters, underscores, and hyphens (matching Arc's measurement name rules).
func isValidIdentifier(name string) bool {
	if len(name) == 0 {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// buildCompactionQuery builds the DuckDB COPY query for compaction.
// When tagColumns is non-empty, adds ROW_NUMBER deduplication (last-write-wins on time).
// When tagColumns is nil/empty, returns the standard compaction query (zero overhead).
func buildCompactionQuery(fileListSQL, orderByClause, outputFile string, tagColumns []string) string {
	escapedOutput := escapeSQLPath(outputFile)

	if len(tagColumns) == 0 {
		// Standard compaction — full-row dedup via DISTINCT (no tag metadata available).
		// Normalize "time" to TIMESTAMP so a partition that mixes file schemas
		// (some time=TIMESTAMP, some time=VARCHAR from a misbehaving writer)
		// still compacts instead of failing the column bind. See timeNormalizeReplace.
		// DISTINCT runs over the normalized rows, so mixed-type duplicates of the
		// same logical row collapse correctly.
		return fmt.Sprintf(`
		COPY (
			SELECT DISTINCT * REPLACE (%s) FROM read_parquet(%s, union_by_name=true)
			%s
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880
		)
	`, timeNormalizeReplace, fileListSQL, orderByClause, escapedOutput)
	}

	// Dedup compaction — keep one row per unique (tags, time) key.
	//
	// The "time" normalization MUST be materialized in a CTE that the dedup
	// window reads from. Three subtle DuckDB facts drive this structure:
	//
	//  1. A `SELECT *, ROW_NUMBER() OVER(...) ) WHERE rn=1` subquery re-expands
	//     `*` over the raw `read_parquet(union_by_name=true)` schema and mis-binds
	//     "time" as VARCHAR across many files (TIMESTAMP WITH TIME ZONE != VARCHAR
	//     plan error) — even when every file's time is a valid TIMESTAMPTZ.
	//  2. QUALIFY is evaluated BEFORE the SELECT projection, so a top-level
	//     `SELECT * REPLACE(time...) ... QUALIFY ROW_NUMBER() OVER(PARTITION BY
	//     ..., "time" ...)` runs the window over the *raw* time column, not the
	//     REPLACE-normalized one. On a mixed-type partition the same (tags,time)
	//     then renders as two different strings, lands in two window partitions,
	//     and BOTH rows survive — silent under-dedup (duplicates written, sources
	//     deleted). Worse than the loud bind error.
	//  3. On DuckDB v1.5.1 (the shipped duckdb-go-bindings v0.10501), normalizing
	//     inside the CTE via `SELECT * REPLACE(time...)` and then windowing over
	//     the replaced column trips a planner bug: ColumnBindingResolver fails
	//     with `INTERNAL Error: Failed to bind column reference "time" ...
	//     inequal types (TIMESTAMP WITH TIME ZONE != VARCHAR)` — even when every
	//     input file has identical schemas. (Fixed upstream in v1.5.2; bindings
	//     v0.10502.) The workaround that binds cleanly on BOTH versions is to
	//     project the normalized time as an appended column instead of an
	//     in-place REPLACE: `SELECT * EXCLUDE ("time"), <normalize> AS "time"`.
	//     Same rows, same names, same types — only the physical column position
	//     of "time" moves to the end, which is harmless: all Arc readers resolve
	//     parquet columns by name (read_parquet(union_by_name=true) included).
	//
	// The CTE form fixes 1 and 2: the normalization is materialized first, so
	// the window (and QUALIFY) bind against the already-normalized TIMESTAMPTZ
	// "time". The EXCLUDE/append projection fixes 3. Verified empirically: a
	// partition mixing a TIMESTAMPTZ-time file and a VARCHAR-time file dedups
	// to 1 row with this form on both v1.5.1 and v1.5.2.
	//
	// PARTITION BY all tag columns + time: rows with identical (tags, time) are
	// duplicates; one is kept. This is the dedup key, not the output order — the
	// output is sorted by the separate orderByClause (time).
	var quotedTags []string
	for _, tag := range tagColumns {
		// Double-quote escaping: DuckDB treats "" inside a quoted identifier as a literal "
		// (same as PostgreSQL). This prevents identifier breakout even if validation is bypassed.
		escaped := strings.ReplaceAll(tag, `"`, `""`)
		quotedTags = append(quotedTags, fmt.Sprintf(`"%s"`, escaped))
	}
	partitionBy := strings.Join(quotedTags, ", ")

	return fmt.Sprintf(`
		COPY (
			WITH normalized AS (
				SELECT * EXCLUDE ("time"), %s FROM read_parquet(%s, union_by_name=true)
			)
			SELECT * FROM normalized
			QUALIFY ROW_NUMBER() OVER (
				PARTITION BY %s, "time"
				ORDER BY "time" DESC
			) = 1
			%s
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880
		)
	`, timeNormalizeReplace, fileListSQL, partitionBy, orderByClause, escapedOutput)
}

// timeNormalizeReplace is the DuckDB expression that coerces a possibly-mixed-type
// "time" column to a single canonical type during compaction. It is used both as
// a `* REPLACE (...)` entry (standard branch) and as a plain aliased projection
// item after `* EXCLUDE ("time")` (dedup branch; see fact 3 in buildCompactionQuery).
// A partition can contain files where "time" is a proper timestamp and others
// where a misbehaving writer wrote it as VARCHAR; read_parquet(union_by_name=true)
// reconciles column NAMES but not conflicting TYPES, so without this the column
// bind fails (TIMESTAMP WITH TIME ZONE != VARCHAR) and the partition can never
// compact.
//
// The target type is TIMESTAMPTZ (TIMESTAMP WITH TIME ZONE), NOT naive TIMESTAMP:
// Arc's Arrow writer types time as Timestamp_us{TimeZone:"UTC"}, which DuckDB
// reads/writes as TIMESTAMP WITH TIME ZONE. Normalizing to naive TIMESTAMP would
// strip the zone and make the compacted file's time type disagree with future
// raw files (TIMESTAMPTZ) — recreating the very bind mismatch this fixes on the
// NEXT compaction cycle. Casting to TIMESTAMPTZ keeps compacted output identical
// to the normal ingest schema.
//
// COALESCE order handles every case:
//   - already TIMESTAMPTZ, or an ISO/datetime string → TRY_CAST(... AS TIMESTAMPTZ)
//   - epoch-microseconds written as a string ("1717689600000000") →
//     make_timestamptz(BIGINT micros). Arc stores time as microseconds, which is
//     exactly make_timestamptz's unit; it yields a UTC-anchored TIMESTAMPTZ.
const timeNormalizeReplace = `COALESCE(TRY_CAST("time" AS TIMESTAMPTZ), make_timestamptz(TRY_CAST("time" AS BIGINT))) AS "time"`

// countParquetRows counts total rows across Parquet files using metadata (no data scan).
// fileListSQL is a DuckDB array literal like "['file1.parquet', 'file2.parquet']".
func countParquetRows(ctx context.Context, db *sql.DB, fileListSQL string) (int64, error) {
	query := fmt.Sprintf(`SELECT SUM(num_rows) FROM parquet_metadata(%s)`, fileListSQL)
	var count int64
	err := db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count parquet rows: %w", err)
	}
	return count, nil
}
