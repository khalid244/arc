package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Cycle 2: pin a Job-level helper that runs the compaction COPY with
// FILE_SIZE_BYTES against an output DIRECTORY (not a single file) and
// returns the local paths of all produced parquet files.
//
// This is the layer that sits BETWEEN buildCompactionQuery (SQL string)
// and the S3 upload step. With it in place, Job can choose between the
// existing single-file path and this multi-file path based on tier config.

func TestCompactToDirectory_MultipleBoundedOutputs(t *testing.T) {
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

	outputs, err := compactToDirectory(
		context.Background(),
		d,
		[]string{in1, in2},
		outDir,
		"",                       // orderBy
		nil,                      // tagColumns
		2*1024*1024,              // 2 MB cap
		"compacted_test_{uuid}",  // pattern
	)
	if err != nil {
		t.Fatalf("compactToDirectory: %v", err)
	}

	if len(outputs) < 2 {
		t.Errorf("expected >=2 output files for ~13 MB input with 2 MB cap, got %d", len(outputs))
		for _, p := range outputs {
			fi, _ := os.Stat(p)
			t.Logf("  - %s (%d B)", p, fi.Size())
		}
	}

	// Every returned path must be a real file under outDir, and must be a parquet.
	// DuckDB's FILE_SIZE_BYTES rolls at row-group boundaries (122,880 rows by
	// default) so individual outputs can overshoot the cap substantially when
	// data is incompressible. The test verifies multi-output happens at all,
	// not that the cap is exact.
	for _, p := range outputs {
		fi, err := os.Stat(p)
		if err != nil {
			t.Errorf("returned path doesn't exist: %s (%v)", p, err)
			continue
		}
		if fi.IsDir() {
			t.Errorf("returned path is a directory, not a file: %s", p)
		}
		if filepath.Ext(p) != ".parquet" {
			t.Errorf("returned path is not a parquet: %s", p)
		}
	}

	// Total row count preserved.
	var n int
	q := fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s/*.parquet')`, outDir)
	if err := d.QueryRow(q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 400000 {
		t.Errorf("row count: got %d, expected 400000 (data loss/dup)", n)
	}
	t.Logf("compactToDirectory produced %d files, %d rows preserved", len(outputs), n)
}

func TestCompactToDirectory_SingleSmallFile(t *testing.T) {
	// Edge case: input is small enough to fit in one output file under the cap.
	// Expectation: function returns exactly 1 output path (not an error).
	tmp := t.TempDir()
	in := filepath.Join(tmp, "tiny.parquet")
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
		COPY (SELECT i AS time, 'small' AS msg FROM range(10) t(i))
		TO '%s' (FORMAT PARQUET)
	`, in)); err != nil {
		t.Fatal(err)
	}

	outputs, err := compactToDirectory(
		context.Background(),
		d,
		[]string{in},
		outDir,
		"",
		nil,
		10*1024*1024, // 10 MB cap (way bigger than data)
		"small_{uuid}",
	)
	if err != nil {
		t.Fatalf("compactToDirectory: %v", err)
	}
	if len(outputs) != 1 {
		t.Errorf("expected exactly 1 output for tiny input, got %d", len(outputs))
	}
}

func TestCompactToDirectory_NoInputs_Error(t *testing.T) {
	// Empty inputs: function should return error rather than running an empty COPY.
	tmp := t.TempDir()
	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}

	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	_, err = compactToDirectory(
		context.Background(),
		d,
		[]string{}, // no inputs
		outDir,
		"",
		nil,
		1024*1024,
		"x_{uuid}",
	)
	if err == nil {
		t.Error("expected error for empty input list, got nil")
	}
}
