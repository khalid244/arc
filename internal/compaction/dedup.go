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

// dedupStagingTable is the temp table the dedup path materializes into. The
// caller (job.go) pins both statements to one connection via db.Conn (a DuckDB
// TEMP table is connection-local) and DROPs it before returning the connection
// to the pool — the DuckDB driver does not reset sessions, so a leftover temp
// table would otherwise retain a full partition's rows on a pooled connection.
// CREATE OR REPLACE additionally guards against any stale table on a reused
// connection. A fixed name is safe: TEMP tables are per-connection, so even
// concurrent jobs (distinct pinned connections) do not collide.
const dedupStagingTable = "arc_compaction_staged"

// buildCompactionQuery builds the DuckDB statement(s) for compaction. It returns
// a slice of statements to execute in order on the same connection.
//
// When tagColumns is nil/empty, returns a single standard COPY (zero overhead).
// When tagColumns is non-empty, returns two statements (CREATE OR REPLACE TEMP
// TABLE ... AS <normalized read> ; COPY <dedup window over the table>) — see the
// long comment on the dedup branch for why a temp table is required rather than
// a CTE or subquery.
func buildCompactionQuery(fileListSQL, orderByClause, outputFile string, tagColumns []string) []string {
	escapedOutput := escapeSQLPath(outputFile)

	if len(tagColumns) == 0 {
		// Standard compaction — full-row dedup via DISTINCT (no tag metadata available).
		// Normalize "time" to TIMESTAMP so a partition that mixes file schemas
		// (some time=TIMESTAMP, some time=VARCHAR from a misbehaving writer)
		// still compacts instead of failing the column bind. See timeNormalizeReplace.
		// DISTINCT runs over the normalized rows, so mixed-type duplicates of the
		// same logical row collapse correctly.
		return []string{fmt.Sprintf(`
		COPY (
			SELECT DISTINCT * REPLACE (%s) FROM read_parquet(%s, union_by_name=true)
			%s
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880
		)
	`, timeNormalizeReplace, fileListSQL, orderByClause, escapedOutput)}
	}

	// Dedup compaction — keep one row per unique (tags, time) key.
	//
	// The "time" normalization MUST be fully materialized into a typed relation
	// BEFORE the dedup window binds. This requires a real temp table, not a CTE
	// or subquery. Three DuckDB facts, all verified empirically against Arc's
	// linked DuckDB on the real 5-column cpu schema (host,time,value,cpu_idle,
	// cpu_user) — NOT a reduced fixture, which hides the bug:
	//
	//  1. A dedup window that references "time" over a many-file
	//     `read_parquet(union_by_name=true)` fails to bind with
	//     `Failed to bind column reference "time": TIMESTAMP WITH TIME ZONE !=
	//     VARCHAR` — EVEN WHEN EVERY FILE'S time IS ALREADY TIMESTAMPTZ and no
	//     VARCHAR exists anywhere. The "VARCHAR" is a phantom the binder
	//     introduces while resolving the window's column reference down through
	//     the union scan. This is what wedged production/cpu on every cycle
	//     (NOT a leftover mixed-type file — the partition was uniformly
	//     TIMESTAMPTZ). It only triggers above a column/file-count threshold, so
	//     the original #493 3-column test never saw it.
	//  2. A plain CTE does NOT fix it — DuckDB inlines the CTE (and OFFSET 0,
	//     contrary to folklore, does not reliably block the inline on the real
	//     schema; it worked on a reduced fixture and failed on real files).
	//  3. The flat no-CTE form `SELECT * REPLACE(time...) ... QUALIFY` DOES bind,
	//     but QUALIFY is evaluated before the projection, so the window runs over
	//     the RAW time. A duplicate written once as TIMESTAMPTZ and once as a
	//     VARCHAR epoch then lands in two window partitions and BOTH survive —
	//     silent under-dedup (verified: 2 rows out instead of 1).
	//
	// CREATE OR REPLACE TEMP TABLE AS forces the normalized, typed result to be
	// fully materialized first; the dedup window then binds against the table's
	// concrete TIMESTAMPTZ "time" — binds correctly (fact 1) and dedups
	// correctly (fact 3). Verified: real all-TIMESTAMPTZ partition binds; a
	// partition with an injected VARCHAR-time duplicate dedups to one row.
	// Fork note: the pre-#515 fork form (CTE + `* EXCLUDE ("time"), <expr> AS
	// "time"`) is dropped. It existed to dodge a DuckDB 1.5.1 planner bug that
	// only bit when the dedup WINDOW bound over a REPLACE projection; the window
	// now binds over the materialized temp table, so REPLACE is safe again and
	// keeps "time" in its original column position instead of moving it last.
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

	stage := fmt.Sprintf(
		`CREATE OR REPLACE TEMP TABLE %s AS SELECT * REPLACE (%s) FROM read_parquet(%s, union_by_name=true)`,
		dedupStagingTable, timeNormalizeReplace, fileListSQL)

	copyOut := fmt.Sprintf(`
		COPY (
			SELECT * FROM %s
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
	`, dedupStagingTable, partitionBy, orderByClause, escapedOutput)

	return []string{stage, copyOut}
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

// parquetFilesHaveTimeColumn reports whether the unified schema of the given
// Parquet files contains a "time" column. DESCRIBE over read_parquet reads
// footers only (no data scan), and union_by_name gives exactly the schema the
// compaction COPY will see — so "true" here guarantees the REPLACE("time")
// normalization and the default ORDER BY "time" can bind. When only SOME files
// have the column, union_by_name backfills the others with NULLs and the query
// still binds; the probe is false only when NO file has it (e.g. a dataset
// loaded outside Arc's ingest path, which always writes "time").
// fileListSQL is a DuckDB array literal or single quoted path, as built by
// Job.compactFiles.
func parquetFilesHaveTimeColumn(ctx context.Context, db *sql.DB, fileListSQL string) (bool, error) {
	query := fmt.Sprintf(
		`SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM read_parquet(%s, union_by_name=true)) WHERE column_name = 'time'`,
		fileListSQL,
	)
	var n int64
	if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return false, fmt.Errorf("failed to probe parquet schema for time column: %w", err)
	}
	return n > 0, nil
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
