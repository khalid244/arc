package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Phase A tests for size-bounded multi-output compaction (FILE_SIZE_BYTES).
//
// The contract being pinned:
//   1. buildCompactionQuery, when called with maxOutputBytes > 0 and a
//      non-empty filenamePattern, emits a COPY statement that includes
//      `FILE_SIZE_BYTES <bytes>` and `FILENAME_PATTERN '<pat>'`.
//   2. With maxOutputBytes == 0, the SQL is unchanged (backward compat).
//   3. Running the new SQL against a real DuckDB with input data exceeding
//      the cap produces MULTIPLE output parquet files, each at or below
//      the cap (modulo parquet metadata overhead).

func TestBuildCompactionQuery_WithMaxOutputSize_AddsFileSizeBytes(t *testing.T) {
	q := buildCompactionQuery(
		"['a.parquet']",
		`ORDER BY "time"`,
		"/tmp/out_dir",
		nil,
		1024*1024, // 1 MB cap
		"events_compacted_{uuid}",
	)

	if !strings.Contains(q, "FILE_SIZE_BYTES 1048576") {
		t.Errorf("expected SQL to contain FILE_SIZE_BYTES 1048576, got:\n%s", q)
	}
	if !strings.Contains(q, "FILENAME_PATTERN 'events_compacted_{uuid}'") {
		t.Errorf("expected SQL to contain FILENAME_PATTERN 'events_compacted_{uuid}', got:\n%s", q)
	}
	if !strings.Contains(q, "FORMAT PARQUET") {
		t.Errorf("expected SQL to still contain FORMAT PARQUET, got:\n%s", q)
	}
}

func TestBuildCompactionQuery_NoMaxOutputSize_BackwardCompat(t *testing.T) {
	q := buildCompactionQuery(
		"['a.parquet']",
		`ORDER BY "time"`,
		"/tmp/out.parquet",
		nil,
		0,  // disabled
		"", // no pattern
	)

	if strings.Contains(q, "FILE_SIZE_BYTES") {
		t.Errorf("expected SQL to NOT contain FILE_SIZE_BYTES when maxOutputBytes==0, got:\n%s", q)
	}
	if strings.Contains(q, "FILENAME_PATTERN") {
		t.Errorf("expected SQL to NOT contain FILENAME_PATTERN when maxOutputBytes==0, got:\n%s", q)
	}
	if !strings.Contains(q, "TO '/tmp/out.parquet'") {
		t.Errorf("expected single-file output path preserved, got:\n%s", q)
	}
}

func TestBuildCompactionQuery_WithMaxOutputSize_RealCOPYProducesBoundedFiles(t *testing.T) {
	// Integration: feed DuckDB ~3 MB across two input parquets, request 1 MB cap,
	// expect multiple output files, each at or near 1 MB.
	tmp := t.TempDir()
	in1 := filepath.Join(tmp, "in1.parquet")
	in2 := filepath.Join(tmp, "in2.parquet")
	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}

	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// Build inputs with mostly-incompressible data so FILE_SIZE_BYTES has work
	// to do. UUIDs are 36 random bytes each — ZSTD can't squeeze them.
	make := func(path string, rowOffset int) {
		q := fmt.Sprintf(`
			COPY (
				SELECT
					(CAST(i AS BIGINT) + %d) AS time,
					gen_random_uuid()::VARCHAR AS msg
				FROM range(200000) t(i)
			) TO '%s' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)
		`, rowOffset, path)
		if _, err := d.Exec(q); err != nil {
			t.Fatalf("make input %s: %v", path, err)
		}
	}
	make(in1, 0)
	make(in2, 1_000_000)

	fileListSQL := fmt.Sprintf("['%s', '%s']", in1, in2)

	q := buildCompactionQuery(
		fileListSQL,
		"",
		outDir,
		nil,
		2*1024*1024,            // 2 MB cap (uuid input compresses to ~13 MB total)
		"out_compacted_{uuid}",
	)
	if _, err := d.Exec(q); err != nil {
		t.Fatalf("execute COPY: %v\nquery:\n%s", err, q)
	}

	var outputs []os.FileInfo
	entries, _ := os.ReadDir(outDir)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, info)
	}

	if len(outputs) < 2 {
		t.Errorf("expected >=2 output files (UUID data ~13 MB, cap 2 MB), got %d", len(outputs))
		for _, o := range outputs {
			t.Logf("  - %s (%d B)", o.Name(), o.Size())
		}
	}
	for _, o := range outputs {
		// Parquet writes data in row groups; the cap is approximate. Allow up to
		// 50% overshoot per DuckDB's documented behavior.
		if o.Size() > 2*1024*1024*150/100 {
			t.Errorf("output file %s exceeds cap by >50%%: %d B (cap 2 MB)", o.Name(), o.Size())
		}
	}
	t.Logf("produced %d output files for ~13 MB UUID input with 2 MB cap", len(outputs))
	for _, o := range outputs {
		t.Logf("  %s: %d B", o.Name(), o.Size())
	}

	// Verify total row count is preserved (no data loss).
	rows, err := countRowsAcrossDir(context.Background(), d, outDir)
	if err != nil {
		t.Fatal(err)
	}
	expected := 400000 // 200_000 × 2 inputs
	if rows != expected {
		t.Errorf("row count mismatch: got %d, expected %d", rows, expected)
	}
}

func countRowsAcrossDir(ctx context.Context, d *sql.DB, dir string) (int, error) {
	q := fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s/*.parquet')`, dir)
	var n int
	if err := d.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
