package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// These tests exercise the SQL produced by buildCompactionQuery against
// real DuckDB to verify that compaction does not lose rows and does not
// introduce duplicate rows. They run the COPY directly in-process so the
// subprocess plumbing isn't required for the data-correctness checks.

// openDuckDBForCompactionTest opens an in-memory DuckDB suitable for
// running the compaction COPY query directly.
func openDuckDBForCompactionTest(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// writeFixtureParquet writes a tiny parquet file with a deterministic
// schema (time TIMESTAMP, host VARCHAR, value DOUBLE) to outPath.
// Returns the row count written.
func writeFixtureParquet(t *testing.T, db *sql.DB, outPath string, rows []fixtureRow) {
	t.Helper()
	// Build a VALUES clause inline.
	if len(rows) == 0 {
		t.Fatalf("writeFixtureParquet: empty rows")
	}
	valuesClause := ""
	for i, r := range rows {
		if i > 0 {
			valuesClause += ", "
		}
		valuesClause += fmt.Sprintf("(TIMESTAMP '%s', '%s', %f)",
			r.ts.Format("2006-01-02 15:04:05.000"), r.host, r.value)
	}
	q := fmt.Sprintf(
		`COPY (SELECT * FROM (VALUES %s) AS t(time, host, value))
		 TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`,
		valuesClause, escapeSQLPath(outPath))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write fixture parquet %s: %v", outPath, err)
	}
}

type fixtureRow struct {
	ts    interface{ Format(string) string }
	host  string
	value float64
}

// countParquet returns the row count of one or more parquet files.
func countParquet(t *testing.T, db *sql.DB, fileListSQL string) int64 {
	t.Helper()
	q := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet(%s, union_by_name=false)", fileListSQL)
	var n int64
	if err := db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("count parquet (%s): %v", q, err)
	}
	return n
}

// fileListSQL builds a DuckDB array literal of file paths.
func fileListSQL(paths []string) string {
	out := "["
	for i, p := range paths {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("'%s'", escapeSQLPath(p))
	}
	out += "]"
	return out
}

func TestCompactionQuery_NoDataLoss_StandardCOPY(t *testing.T) {
	// Three input files, 5 rows each = 15 rows total. After compaction
	// (no dedup), output must have exactly 15 rows.
	tmp := t.TempDir()
	db := openDuckDBForCompactionTest(t)

	inputs := []string{
		filepath.Join(tmp, "in1.parquet"),
		filepath.Join(tmp, "in2.parquet"),
		filepath.Join(tmp, "in3.parquet"),
	}
	totalRows := 0
	for i, in := range inputs {
		rows := make([]fixtureRow, 5)
		for j := 0; j < 5; j++ {
			rows[j] = fixtureRow{
				ts:    fakeTime{year: 2026, month: 5, day: 13, hour: i, min: j},
				host:  fmt.Sprintf("h%d", j),
				value: float64(i*10 + j),
			}
		}
		writeFixtureParquet(t, db, in, rows)
		totalRows += len(rows)
	}

	outPath := filepath.Join(tmp, "out.parquet")
	// nil tagColumns -> standard COPY (no dedup overhead)
	q := buildCompactionQuery(fileListSQL(inputs), "", outPath, nil, 0, "")
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("compaction COPY: %v", err)
	}

	got := countParquet(t, db, fileListSQL([]string{outPath}))
	if got != int64(totalRows) {
		t.Errorf("DATA LOSS: input %d rows, output %d rows", totalRows, got)
	}
}

func TestCompactionQuery_DedupRemovesDuplicates(t *testing.T) {
	// Two input files share rows with identical (host, time). The dedup
	// path should keep exactly one row per unique (host, time).
	tmp := t.TempDir()
	db := openDuckDBForCompactionTest(t)

	commonTs := fakeTime{year: 2026, month: 5, day: 13, hour: 10, min: 0}
	in1 := filepath.Join(tmp, "in1.parquet")
	writeFixtureParquet(t, db, in1, []fixtureRow{
		{ts: commonTs, host: "h1", value: 1.0},
		{ts: commonTs, host: "h2", value: 2.0},
		{ts: fakeTime{year: 2026, month: 5, day: 13, hour: 10, min: 1}, host: "h1", value: 3.0},
	})
	in2 := filepath.Join(tmp, "in2.parquet")
	writeFixtureParquet(t, db, in2, []fixtureRow{
		// duplicate (h1, 10:00) — should be deduped
		{ts: commonTs, host: "h1", value: 99.0},
		// duplicate (h2, 10:00) — should be deduped
		{ts: commonTs, host: "h2", value: 88.0},
		// new (h3, 10:00) — should be kept
		{ts: commonTs, host: "h3", value: 4.0},
	})

	outPath := filepath.Join(tmp, "out.parquet")
	q := buildCompactionQuery(fileListSQL([]string{in1, in2}),
		`ORDER BY "host", "time"`, outPath, []string{"host"}, 0, "")
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("dedup COPY: %v", err)
	}

	// Expect exactly 4 distinct (host, time) rows:
	//   (h1, 10:00), (h1, 10:01), (h2, 10:00), (h3, 10:00)
	got := countParquet(t, db, fileListSQL([]string{outPath}))
	if got != 4 {
		t.Errorf("dedup row count: got %d want 4", got)
	}

	// Verify each (host, time) appears exactly once.
	var dupes int64
	dupQuery := fmt.Sprintf(
		`SELECT COUNT(*) FROM (
			SELECT host, time, COUNT(*) AS n
			FROM read_parquet('%s')
			GROUP BY host, time
			HAVING n > 1
		)`,
		escapeSQLPath(outPath))
	if err := db.QueryRowContext(context.Background(), dupQuery).Scan(&dupes); err != nil {
		t.Fatalf("dup-check query: %v", err)
	}
	if dupes != 0 {
		t.Errorf("DATA DUPLICATION: %d (host,time) keys appear more than once", dupes)
	}
}

func TestCompactionQuery_DedupPreservesNonDupRows(t *testing.T) {
	// Even with dedup configured, rows that ARE NOT duplicates should
	// all survive — verifies dedup doesn't over-collapse.
	tmp := t.TempDir()
	db := openDuckDBForCompactionTest(t)

	in := filepath.Join(tmp, "in.parquet")
	rows := make([]fixtureRow, 10)
	for i := 0; i < 10; i++ {
		// Each row has a unique (host, time).
		rows[i] = fixtureRow{
			ts:    fakeTime{year: 2026, month: 5, day: 13, hour: 10, min: i},
			host:  fmt.Sprintf("h%d", i),
			value: float64(i),
		}
	}
	writeFixtureParquet(t, db, in, rows)

	outPath := filepath.Join(tmp, "out.parquet")
	q := buildCompactionQuery(fileListSQL([]string{in}),
		`ORDER BY "host", "time"`, outPath, []string{"host"}, 0, "")
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("dedup COPY: %v", err)
	}

	got := countParquet(t, db, fileListSQL([]string{outPath}))
	if got != 10 {
		t.Errorf("DATA LOSS in dedup path: input 10 unique rows, output %d", got)
	}
}

func TestCompactionQuery_RoundTrip_PreservesAllColumns(t *testing.T) {
	// Verify that compaction preserves all columns, not just the ones
	// referenced in PARTITION BY / ORDER BY.
	tmp := t.TempDir()
	db := openDuckDBForCompactionTest(t)

	in := filepath.Join(tmp, "in.parquet")
	writeFixtureParquet(t, db, in, []fixtureRow{
		{ts: fakeTime{year: 2026, month: 5, day: 13, hour: 10, min: 0}, host: "h1", value: 42.5},
		{ts: fakeTime{year: 2026, month: 5, day: 13, hour: 10, min: 1}, host: "h2", value: 99.0},
	})

	outPath := filepath.Join(tmp, "out.parquet")
	q := buildCompactionQuery(fileListSQL([]string{in}), "", outPath, nil, 0, "")
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("COPY: %v", err)
	}

	// Output schema should be (time, host, value) — same as input.
	rows, err := db.QueryContext(context.Background(),
		fmt.Sprintf("SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('%s'))",
			escapeSQLPath(outPath)))
	if err != nil {
		t.Fatalf("DESCRIBE: %v", err)
	}
	defer rows.Close()
	cols := map[string]bool{}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		cols[c] = true
	}
	for _, want := range []string{"time", "host", "value"} {
		if !cols[want] {
			t.Errorf("compacted output missing column %q (got: %v)", want, cols)
		}
	}
}

// fakeTime is a tiny stand-in for time.Time formatting in fixture builders.
// We only need Format to produce the timestamp DuckDB expects.
type fakeTime struct {
	year, month, day, hour, min int
}

func (f fakeTime) Format(_ string) string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:00.000",
		f.year, f.month, f.day, f.hour, f.min)
}
