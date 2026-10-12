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

// writeFixtureRowsParquet writes a tiny parquet file with a deterministic
// schema (time TIMESTAMP, host VARCHAR, value DOUBLE) to outPath.
// Returns the row count written.
func writeFixtureRowsParquet(t *testing.T, db *sql.DB, outPath string, rows []fixtureRow) {
	t.Helper()
	// Build a VALUES clause inline.
	if len(rows) == 0 {
		t.Fatalf("writeFixtureRowsParquet: empty rows")
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
		writeFixtureRowsParquet(t, db, in, rows)
		totalRows += len(rows)
	}

	outPath := filepath.Join(tmp, "out.parquet")
	// nil tagColumns -> standard COPY (no dedup overhead)
	if err := execCompaction(context.Background(), db, fileListSQL(inputs), "", outPath, nil); err != nil {
		t.Fatalf("compaction COPY: %v", err)
	}

	got := countParquet(t, db, fileListSQL([]string{outPath}))
	if got != int64(totalRows) {
		t.Errorf("DATA LOSS: input %d rows, output %d rows", totalRows, got)
	}
}

func TestCompactionQuery_DedupRemovesDuplicates(t *testing.T) {
	// Two input files share rows. Exact-row dedup (merge policy, upstream
	// #1232): a row is dropped only when another row is identical in EVERY
	// column. Rows that share (host, time) but differ in a field are different
	// data and must both survive — the old one-row-per-(host,time) collapse
	// silently deleted corrections and re-sent points. Checked for a tagged
	// batch (key narrowed to host,time) and a tagless one (whole-row
	// DISTINCT, the production shape): both must give the same rows.
	for name, tags := range map[string][]string{"tagged": {"host"}, "tagless": nil} {
		t.Run(name, func(t *testing.T) {
			tmp := t.TempDir()
			db := openDuckDBForCompactionTest(t)

			commonTs := fakeTime{year: 2026, month: 5, day: 13, hour: 10, min: 0}
			in1 := filepath.Join(tmp, "in1.parquet")
			writeFixtureRowsParquet(t, db, in1, []fixtureRow{
				{ts: commonTs, host: "h1", value: 1.0},
				{ts: commonTs, host: "h2", value: 2.0},
				{ts: fakeTime{year: 2026, month: 5, day: 13, hour: 10, min: 1}, host: "h1", value: 3.0},
			})
			in2 := filepath.Join(tmp, "in2.parquet")
			writeFixtureRowsParquet(t, db, in2, []fixtureRow{
				// exact duplicate of (h1, 10:00, 1.0) — a replayed write: dropped
				{ts: commonTs, host: "h1", value: 1.0},
				// same (h2, 10:00) key, different value — different data: kept
				{ts: commonTs, host: "h2", value: 88.0},
				// new (h3, 10:00) — kept
				{ts: commonTs, host: "h3", value: 4.0},
			})

			outPath := filepath.Join(tmp, "out.parquet")
			if err := execCompaction(context.Background(), db, fileListSQL([]string{in1, in2}),
				`ORDER BY "host", "time"`, outPath, tags); err != nil {
				t.Fatalf("dedup COPY: %v", err)
			}

			// Expect 5 rows: (h1,10:00,1), (h1,10:01,3), (h2,10:00,2),
			// (h2,10:00,88), (h3,10:00,4).
			got := countParquet(t, db, fileListSQL([]string{outPath}))
			if got != 5 {
				t.Errorf("dedup row count: got %d want 5", got)
			}

			// No row may appear twice in every column.
			var dupes int64
			dupQuery := fmt.Sprintf(
				`SELECT COUNT(*) FROM (
					SELECT host, time, value, COUNT(*) AS n
					FROM read_parquet('%s')
					GROUP BY host, time, value
					HAVING n > 1
				)`,
				escapeSQLPath(outPath))
			if err := db.QueryRowContext(context.Background(), dupQuery).Scan(&dupes); err != nil {
				t.Fatalf("dup-check query: %v", err)
			}
			if dupes != 0 {
				t.Errorf("DATA DUPLICATION: %d identical rows appear more than once", dupes)
			}

			// The two (h2, 10:00) rows that differ in value must both survive.
			var h2 int64
			if err := db.QueryRowContext(context.Background(), fmt.Sprintf(
				`SELECT COUNT(*) FROM read_parquet('%s') WHERE host = 'h2'`, escapeSQLPath(outPath))).Scan(&h2); err != nil {
				t.Fatalf("h2 count: %v", err)
			}
			if h2 != 2 {
				t.Errorf("DATA LOSS: %d (h2, 10:00) rows survived, want 2 (they differ in value)", h2)
			}
		})
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
	writeFixtureRowsParquet(t, db, in, rows)

	outPath := filepath.Join(tmp, "out.parquet")
	if err := execCompaction(context.Background(), db, fileListSQL([]string{in}),
		`ORDER BY "host", "time"`, outPath, []string{"host"}); err != nil {
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
	writeFixtureRowsParquet(t, db, in, []fixtureRow{
		{ts: fakeTime{year: 2026, month: 5, day: 13, hour: 10, min: 0}, host: "h1", value: 42.5},
		{ts: fakeTime{year: 2026, month: 5, day: 13, hour: 10, min: 1}, host: "h2", value: 99.0},
	})

	outPath := filepath.Join(tmp, "out.parquet")
	if err := execCompaction(context.Background(), db, fileListSQL([]string{in}), "", outPath, nil); err != nil {
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
