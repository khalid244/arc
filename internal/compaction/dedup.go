package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
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
// concat write); union all of them so every tag narrows the exact-row dedup
// key (see buildCompactionQuery).
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

// exactDedupMarker is the Parquet footer key upstream Arc (#1232) reads as
// "deduplicate this batch to exact rows". This build deduplicates EVERY batch
// to exact rows regardless of footer metadata, so it never reads the key; it
// writes it on outputs whose batch carried no tag columns so that a build which
// does gate dedup on footer metadata keeps deduplicating them instead of
// treating them as raw ingest that is "left alone". The legacy arc:dedup_time
// key (upstream #521) is never written: builds before #1232 read it as "one
// row per time", which would drop rows that differ only in a field.
const exactDedupMarker = "arc:dedup_exact"

// dedupStagingTable is the temp table the tagged dedup path materializes into.
// The caller (job.go) pins every statement to one connection via db.Conn (a
// DuckDB TEMP table is connection-local) and DROPs it before returning the
// connection to the pool — the DuckDB driver does not reset sessions, so a
// leftover temp table would otherwise retain a full partition's rows on a
// pooled connection. CREATE OR REPLACE additionally guards against any stale
// table on a reused connection. A fixed name is safe: TEMP tables are
// per-connection, so even concurrent jobs (distinct pinned connections) do not
// collide.
const dedupStagingTable = "arc_compaction_staged"

// dedupKeysTable holds the (tags..., time) keys that occur on more than one
// staged row; same connection-local lifetime as dedupStagingTable.
const dedupKeysTable = "arc_compaction_dupkeys"

// compactionCopyOptions are the Parquet writer options every compaction output
// uses.
const compactionCopyOptions = `FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880`

// buildCompactionQuery builds the DuckDB statement(s) for compaction. It returns
// a slice of statements to execute in order on the same connection.
//
// EVERY batch is deduplicated to EXACT ROWS: a row is dropped only when another
// row of the batch is identical to it in every column — time, every tag and
// every field — after "time" is normalized (see timeNormalizeReplace). Rows
// that share their tags and time but differ in any field are different data
// and are all kept (#1232). There is deliberately no "no dedup" mode: Arc's own
// crash and retry paths (a WAL replay after an ambiguous PUT, a reorg rollback,
// an upload whose outcome was unknown) produce byte-identical duplicate rows,
// and production files carry no arc:tags footer, so gating dedup on footer
// metadata (upstream #1234's decideDedup) would leave every such duplicate in
// place forever.
//
// Two equivalent forms, chosen only for cost:
//
//   - tagColumns empty: one COPY of SELECT DISTINCT over the normalized rows.
//     This is exact-row dedup by definition, and it is the statement this
//     fork has always run on tagless (i.e. all production) data, so its memory
//     and spill profile under compaction.max_temp_directory_size is known.
//     The output is marked with exactDedupMarker.
//   - tagColumns non-empty: three statements (#1232) — stage the normalized
//     read in a temp table, collect the (tagColumns..., time) keys that occur
//     more than once, then copy every row whose key is unique as is and, for
//     the rows whose key repeats, one copy of each distinct row. The key only
//     narrows WHERE whole rows are compared; it never decides on its own that
//     two rows are duplicates, so the result is the same row set DISTINCT
//     would give, at close to the cost of the old one-row-per-key window.
//
// Neither UNION ALL nor DISTINCT keeps insertion order, so the output is
// always ordered explicitly: by the configured sort keys, else by time.
func buildCompactionQuery(fileListSQL, orderByClause, outputFile string, tagColumns []string) []string {
	escapedOutput := escapeSQLPath(outputFile)
	readSQL := sqlutil.ReadParquet(fileListSQL, "union_by_name=true")

	order := orderByClause
	if order == "" {
		order = `ORDER BY "time"`
	}

	if len(tagColumns) == 0 {
		// Whole-row DISTINCT over the normalized projection. Normalizing "time"
		// to TIMESTAMPTZ lets a partition that mixes file schemas (some
		// time=TIMESTAMP, some time=VARCHAR from a misbehaving writer) compact
		// instead of failing the column bind, and DISTINCT runs over the
		// normalized rows, so mixed-type duplicates of the same logical row
		// collapse correctly.
		return []string{fmt.Sprintf(`
		COPY (
			SELECT DISTINCT * REPLACE (%s) FROM %s
			%s
		) TO '%s' (
			%s,
			KV_METADATA {'%s': 'true'}
		)
	`, timeNormalizeReplace, readSQL, order, escapedOutput, compactionCopyOptions, exactDedupMarker)}
	}

	// The tagged form stages the normalized read in a temp table first, and
	// every aggregate below runs over that table, never over the raw scan.
	//
	// The "time" normalization MUST be fully materialized into a typed relation
	// BEFORE anything keyed on "time" binds. This requires a real temp table,
	// not a CTE or subquery. Three DuckDB facts, all verified empirically
	// against Arc's linked DuckDB on the real 5-column cpu schema
	// (host,time,value,cpu_idle,cpu_user) — NOT a reduced fixture, which hides
	// the bug:
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
	// fully materialized first; the GROUP BY and joins then bind against the
	// table's concrete TIMESTAMPTZ "time" — binds correctly (fact 1) and
	// compares correctly (fact 3). (These facts were found with the
	// one-row-per-key window this code used before #1232; the GROUP BY and
	// joins below rely on the same temp table for the same reason.)
	stage := fmt.Sprintf(
		`CREATE OR REPLACE TEMP TABLE %s AS SELECT * REPLACE (%s) FROM %s`,
		dedupStagingTable, timeNormalizeReplace, readSQL)

	// The key: every tag column plus "time". It only narrows where whole rows
	// are compared; it never decides on its own that two rows are duplicates.
	quotedKeys := make([]string, 0, len(tagColumns)+1)
	for _, tag := range tagColumns {
		// Double-quote escaping: DuckDB treats "" inside a quoted identifier as a literal "
		// (same as PostgreSQL). This prevents identifier breakout even if validation is bypassed.
		escaped := strings.ReplaceAll(tag, `"`, `""`)
		quotedKeys = append(quotedKeys, fmt.Sprintf(`"%s"`, escaped))
	}
	quotedKeys = append(quotedKeys, `"time"`)

	// IS NOT DISTINCT FROM, not =: a tag is NULL on every row of a file that
	// lacks the column (union_by_name), and = would match no key for those
	// rows, dropping them from BOTH branches below.
	match := make([]string, len(quotedKeys))
	for i, k := range quotedKeys {
		match[i] = fmt.Sprintf("k.%s IS NOT DISTINCT FROM s.%s", k, k)
	}
	keyMatch := strings.Join(match, " AND ")

	dupKeys := fmt.Sprintf(
		`CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM %s GROUP BY ALL HAVING count(*) > 1`,
		dedupKeysTable, strings.Join(quotedKeys, ", "), dedupStagingTable)

	copyOut := fmt.Sprintf(`
		COPY (
			(SELECT * FROM %[1]s s WHERE NOT EXISTS (SELECT 1 FROM %[2]s k WHERE %[3]s))
			UNION ALL
			(SELECT DISTINCT * FROM %[1]s s WHERE EXISTS (SELECT 1 FROM %[2]s k WHERE %[3]s))
			%[4]s
		) TO '%[5]s' (
			%[6]s
		)
	`, dedupStagingTable, dedupKeysTable, keyMatch, order, escapedOutput, compactionCopyOptions)

	return []string{stage, dupKeys, copyOut}
}

// timeNormalizeReplace is the DuckDB `REPLACE (...)` expression that coerces a
// possibly-mixed-type "time" column to a single canonical type during compaction.
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

// parquetFilesColumnNames returns the column names of the unified schema of the
// given Parquet files — exactly the schema the compaction COPY will see, since
// both read with union_by_name. DESCRIBE over read_parquet reads footers only
// (no data scan). fileListSQL is a DuckDB array literal or single quoted path,
// as built by Job.compactFiles.
func parquetFilesColumnNames(ctx context.Context, db *sql.DB, fileListSQL string) (map[string]struct{}, error) {
	query := fmt.Sprintf(
		`SELECT column_name FROM (DESCRIBE SELECT * FROM %s)`,
		sqlutil.ReadParquet(fileListSQL, "union_by_name=true"),
	)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to probe parquet schema: %w", err)
	}
	defer rows.Close()
	cols := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to read parquet schema: %w", err)
		}
		cols[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read parquet schema: %w", err)
	}
	return cols, nil
}

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
		`SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM %s) WHERE column_name = 'time'`,
		sqlutil.ReadParquet(fileListSQL, "union_by_name=true"),
	)
	var n int64
	if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return false, fmt.Errorf("failed to probe parquet schema for time column: %w", err)
	}
	return n > 0, nil
}

// countParquetRows counts total rows across Parquet files using metadata (no data scan).
// fileListSQL is a DuckDB array literal or single quoted path, as built by
// Job.compactFiles — both forms have a parquet_file_metadata overload.
func countParquetRows(ctx context.Context, db *sql.DB, fileListSQL string) (int64, error) {
	query := fmt.Sprintf(`SELECT SUM(num_rows) FROM parquet_file_metadata(%s)`, fileListSQL)
	var count int64
	err := db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count parquet rows: %w", err)
	}
	return count, nil
}
