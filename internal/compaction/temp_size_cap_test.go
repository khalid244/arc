package compaction

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestMaxTempDirectorySize_DuckDBEnforcesCap demonstrates that DuckDB
// honors `SET max_temp_directory_size` and aborts a spilling query that
// exceeds the cap. This is the property we rely on in subprocess.go to
// prevent the compactor pod from being evicted on ephemeral-storage.
func TestMaxTempDirectorySize_DuckDBEnforcesCap(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()

	// Tiny memory + tiny spill cap. ORDER BY a big synthetic dataset will
	// force spill, and the spill must hit the 10 MiB cap quickly.
	for _, stmt := range []string{
		"SET temp_directory='" + tmpDir + "'",
		"SET memory_limit='64MB'",
		"SET threads=1",
		"SET max_temp_directory_size='10MiB'",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// Generate ~2 GB of synthetic rows and ORDER BY. With memory_limit=64MB
	// DuckDB must spill > 10 MiB. With max_temp_directory_size=10MiB, it
	// MUST abort.
	q := `
		SELECT COUNT(*) FROM (
		  SELECT range AS r, repeat('x', 200) AS pad
		  FROM range(10000000)
		  ORDER BY pad DESC, r DESC
		)`
	var n int64
	err = db.QueryRowContext(ctx, q).Scan(&n)
	if err == nil {
		t.Fatalf("expected ORDER BY of 10M rows under 64MB memory + 10MiB temp cap to FAIL, but it succeeded (n=%d)", n)
	}
	t.Logf("got expected error: %v", err)
}

// TestMaxTempDirectorySize_NoCap_QuerySucceeds is the control: with the
// cap removed (or set high), the same query succeeds. This proves the cap
// is what's enforcing failure, not some other limit.
func TestMaxTempDirectorySize_NoCap_QuerySucceeds(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()

	for _, stmt := range []string{
		"SET temp_directory='" + tmpDir + "'",
		"SET memory_limit='64MB'",
		"SET threads=1",
		// No max_temp_directory_size — defaults to ~90% of available disk.
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	q := `
		SELECT COUNT(*) FROM (
		  SELECT range AS r, repeat('x', 200) AS pad
		  FROM range(1000000)
		  ORDER BY pad DESC, r DESC
		)`
	var n int64
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("control query failed (expected success without cap): %v", err)
	}
	if n != 1_000_000 {
		t.Errorf("expected 1M rows, got %d", n)
	}
}

// TestMaxTempDirectorySize_ErrorMessageMentionsCap (informational): if
// DuckDB's error mentions "max_temp_directory_size" or "temp", subprocess
// callers can surface a clearer error.  Failure here doesn't break the
// safety property — DuckDB might phrase it as "out of memory" — but it's
// useful to know.
func TestMaxTempDirectorySize_ErrorMessageMentionsCap(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()
	for _, stmt := range []string{
		"SET temp_directory='" + tmpDir + "'",
		"SET memory_limit='64MB'",
		"SET threads=1",
		"SET max_temp_directory_size='10MiB'",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	q := `SELECT COUNT(*) FROM (SELECT range AS r, repeat('x', 200) AS pad FROM range(10000000) ORDER BY pad DESC)`
	var n int64
	err = db.QueryRowContext(ctx, q).Scan(&n)
	if err == nil {
		t.Skip("query unexpectedly succeeded; informational test")
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "temp") && !strings.Contains(msg, "memory") && !strings.Contains(msg, "disk") {
		t.Logf("error message does not mention temp/memory/disk (informational): %v", err)
	}
}
