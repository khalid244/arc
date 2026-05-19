package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Heavy tests for compactToDirectory + buildCompactionQuery: edge cases
// and concurrency stress. Cycle 4b/c/d (Job-level wiring) is NOT yet
// implemented, so these target the SQL-builder + compactToDirectory layer.

// ---- Edge cases ----

func TestCompactToDirectory_VeryLargeCap_SingleOutput(t *testing.T) {
	tmp := t.TempDir()
	in := filepath.Join(tmp, "in.parquet")
	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if _, err := d.Exec(fmt.Sprintf(`
		COPY (SELECT i AS time, gen_random_uuid()::VARCHAR AS msg FROM range(10000) t(i))
		TO '%s' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)
	`, in)); err != nil {
		t.Fatal(err)
	}

	outputs, err := compactToDirectory(context.Background(), d,
		[]string{in}, outDir, "", nil,
		1024*1024*1024, // 1 GB cap, far bigger than ~600 KB input
		"big_{uuid}",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(outputs) != 1 {
		t.Errorf("expected exactly 1 output when input ≪ cap, got %d", len(outputs))
	}
}

func TestCompactToDirectory_TinyCap_ManyOutputs(t *testing.T) {
	tmp := t.TempDir()
	in := filepath.Join(tmp, "in.parquet")
	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if _, err := d.Exec(fmt.Sprintf(`
		COPY (SELECT i AS time, gen_random_uuid()::VARCHAR AS msg FROM range(200000) t(i))
		TO '%s' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)
	`, in)); err != nil {
		t.Fatal(err)
	}

	outputs, err := compactToDirectory(context.Background(), d,
		[]string{in}, outDir, "", nil,
		64*1024, // 64 KB cap — tiny, expect many outputs
		"tiny_{uuid}",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(outputs) < 3 {
		t.Errorf("expected several outputs with 64 KB cap on 200K rows, got %d", len(outputs))
	}
	t.Logf("tiny cap → %d outputs", len(outputs))
}

func TestCompactToDirectory_PreservesRowCount_NoDup(t *testing.T) {
	// Stress: large input, verify exact row count and that all row IDs are unique.
	tmp := t.TempDir()
	in := filepath.Join(tmp, "in.parquet")
	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const N = 500000
	if _, err := d.Exec(fmt.Sprintf(`
		COPY (SELECT CAST(i AS BIGINT) AS time, gen_random_uuid()::VARCHAR AS msg FROM range(%d) t(i))
		TO '%s' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)
	`, N, in)); err != nil {
		t.Fatal(err)
	}

	outputs, err := compactToDirectory(context.Background(), d,
		[]string{in}, outDir, "", nil,
		1024*1024, "p_{uuid}", // 1 MB cap
	)
	if err != nil {
		t.Fatal(err)
	}

	// Total rows match
	var total int
	if err := d.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s/*.parquet')`, outDir)).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != N {
		t.Errorf("row count: got %d, expected %d (data loss/dup)", total, N)
	}

	// Distinct row ids match total (no dup)
	var distinct int
	if err := d.QueryRow(fmt.Sprintf(`SELECT COUNT(DISTINCT msg) FROM read_parquet('%s/*.parquet')`, outDir)).Scan(&distinct); err != nil {
		t.Fatal(err)
	}
	if distinct != N {
		t.Errorf("distinct: got %d, expected %d (DUPLICATION)", distinct, N)
	}
	t.Logf("500K rows produced %d output files, all unique", len(outputs))
}

func TestCompactToDirectory_NullTimeRows_PassedThrough(t *testing.T) {
	// compactToDirectory itself doesn't filter NULL time (that's the job's
	// concern). Verify it passes NULL-time rows through. The reorg uses
	// `WHERE time IS NOT NULL` in its own COPY; compactor relies on data
	// being valid before compaction.
	tmp := t.TempDir()
	in := filepath.Join(tmp, "in.parquet")
	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if _, err := d.Exec(fmt.Sprintf(`
		COPY (SELECT * FROM (VALUES
			(1::BIGINT, 'a'),
			(NULL::BIGINT, 'b'),
			(3::BIGINT, 'c')
		) AS t(time, msg)) TO '%s' (FORMAT PARQUET)
	`, in)); err != nil {
		t.Fatal(err)
	}

	outputs, err := compactToDirectory(context.Background(), d,
		[]string{in}, outDir, "", nil,
		10*1024*1024, "nullt_{uuid}",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(outputs) == 0 {
		t.Fatal("expected at least one output file")
	}
	var n int
	if err := d.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s/*.parquet')`, outDir)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("expected 3 rows (NULL time preserved), got %d", n)
	}
}

func TestCompactToDirectory_HeterogeneousSchemas_UnionByName(t *testing.T) {
	// Two inputs with overlapping but different columns. union_by_name=true
	// should produce a union schema; rows that didn't have a column get NULL.
	tmp := t.TempDir()
	in1 := filepath.Join(tmp, "a.parquet")
	in2 := filepath.Join(tmp, "b.parquet")
	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if _, err := d.Exec(fmt.Sprintf(`COPY (SELECT 1::BIGINT AS time, 'x' AS only_a) TO '%s' (FORMAT PARQUET)`, in1)); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Exec(fmt.Sprintf(`COPY (SELECT 2::BIGINT AS time, 'y' AS only_b) TO '%s' (FORMAT PARQUET)`, in2)); err != nil {
		t.Fatal(err)
	}

	outputs, err := compactToDirectory(context.Background(), d,
		[]string{in1, in2}, outDir, "", nil,
		10*1024*1024, "uniobn_{uuid}",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(outputs) == 0 {
		t.Fatal("expected at least one output file")
	}

	// 2 rows, both columns present (some NULL).
	var n int
	if err := d.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s/*.parquet', union_by_name=true)`, outDir)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("expected 2 rows from union schema, got %d", n)
	}

	cols, _ := d.Query(fmt.Sprintf(`DESCRIBE SELECT * FROM read_parquet('%s/*.parquet', union_by_name=true)`, outDir))
	defer cols.Close()
	hasA, hasB := false, false
	for cols.Next() {
		var name, typ, nullable, key, deflt, extra sql.NullString
		_ = cols.Scan(&name, &typ, &nullable, &key, &deflt, &extra)
		if name.String == "only_a" {
			hasA = true
		}
		if name.String == "only_b" {
			hasB = true
		}
	}
	if !hasA || !hasB {
		t.Errorf("union schema must contain both only_a and only_b columns; got only_a=%v only_b=%v", hasA, hasB)
	}
}

// ---- Concurrency stress ----

func TestCompactToDirectory_ConcurrentCalls_NoCollision(t *testing.T) {
	// Run 4 concurrent compactions into separate output dirs sharing the
	// same DuckDB connection. Verify each produces correct row count and
	// no temp-file/path collision corrupts output.
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const workers = 4
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			tmp := t.TempDir()
			in := filepath.Join(tmp, "in.parquet")
			outDir := filepath.Join(tmp, "out")
			if err := os.MkdirAll(outDir, 0755); err != nil {
				errCh <- err
				return
			}
			if _, err := d.Exec(fmt.Sprintf(`
				COPY (SELECT i AS time, gen_random_uuid()::VARCHAR AS msg FROM range(50000) t(i))
				TO '%s' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)
			`, in)); err != nil {
				errCh <- err
				return
			}
			outputs, err := compactToDirectory(context.Background(), d,
				[]string{in}, outDir, "", nil,
				512*1024,
				fmt.Sprintf("w%d_{uuid}", wid),
			)
			if err != nil {
				errCh <- err
				return
			}
			if len(outputs) < 1 {
				errCh <- fmt.Errorf("worker %d: expected >=1 output, got %d", wid, len(outputs))
				return
			}
			var n int
			if err := d.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s/*.parquet')`, outDir)).Scan(&n); err != nil {
				errCh <- err
				return
			}
			if n != 50000 {
				errCh <- fmt.Errorf("worker %d: row count mismatch %d != 50000", wid, n)
				return
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Errorf("concurrent compaction: %v", err)
		}
	}
}

// ---- buildCompactionQuery edge cases ----

func TestBuildCompactionQuery_MaxOutputSize_WithDedupTagColumns(t *testing.T) {
	q := buildCompactionQuery(
		"['a.parquet']",
		`ORDER BY "time"`,
		"/tmp/out",
		[]string{"host", "region"},
		2*1024*1024,
		"dedup_{uuid}",
	)
	if !strings.Contains(q, "FILE_SIZE_BYTES 2097152") {
		t.Errorf("expected FILE_SIZE_BYTES in dedup-mode SQL")
	}
	if !strings.Contains(q, "FILENAME_PATTERN 'dedup_{uuid}'") {
		t.Errorf("expected FILENAME_PATTERN in dedup-mode SQL")
	}
	if !strings.Contains(q, "ROW_NUMBER") {
		t.Errorf("expected ROW_NUMBER dedup logic preserved")
	}
}

func TestCompactToDirectory_DedupTagColumns(t *testing.T) {
	// Real DuckDB run: input has duplicates by (host, time); compactor with
	// tagColumns=["host"] should keep one row per (host, time).
	tmp := t.TempDir()
	in := filepath.Join(tmp, "dup.parquet")
	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if _, err := d.Exec(fmt.Sprintf(`
		COPY (SELECT * FROM (VALUES
			(100::BIGINT, 'h1', 'a'),
			(100::BIGINT, 'h1', 'b'),   -- dup (h1, 100) with b
			(200::BIGINT, 'h1', 'c'),
			(100::BIGINT, 'h2', 'd')
		) AS t(time, host, val)) TO '%s' (FORMAT PARQUET)
	`, in)); err != nil {
		t.Fatal(err)
	}

	_, err = compactToDirectory(context.Background(), d,
		[]string{in}, outDir, `ORDER BY "host", "time"`,
		[]string{"host"},
		10*1024*1024, "dedup_{uuid}",
	)
	if err != nil {
		t.Fatal(err)
	}

	// Expect 3 rows (one (h1,100), one (h1,200), one (h2,100)).
	var n int
	if err := d.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s/*.parquet')`, outDir)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("dedup row count: got %d, expected 3", n)
	}
}
