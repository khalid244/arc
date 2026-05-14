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
		// Standard compaction — full-row dedup via DISTINCT (no tag metadata available)
		return fmt.Sprintf(`
		COPY (
			SELECT DISTINCT * FROM read_parquet(%s, union_by_name=true)
			%s
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880
		)
	`, fileListSQL, orderByClause, escapedOutput)
	}

	// Dedup compaction — use ROW_NUMBER to keep one row per unique key.
	// PARTITION BY all tag columns + time: rows with identical (tags, time) are duplicates.
	// Among duplicates, one row is kept (choice is arbitrary since all share the same time).
	// The __dedup_rn column is excluded from the output via EXCLUDE.
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
			SELECT * EXCLUDE (__dedup_rn) FROM (
				SELECT *, ROW_NUMBER() OVER (
					PARTITION BY %s, "time"
					ORDER BY "time" DESC
				) AS __dedup_rn
				FROM read_parquet(%s, union_by_name=true)
			) WHERE __dedup_rn = 1
			%s
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880
		)
	`, partitionBy, fileListSQL, orderByClause, escapedOutput)
}

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
