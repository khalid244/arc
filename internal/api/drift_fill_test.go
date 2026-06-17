package api

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver registration for the integration test
)

// TestMissingColumnFromError pins the schema-drift detector: it must extract the
// absent column from DuckDB's binder error and ignore unrelated errors.
func TestMissingColumnFromError(t *testing.T) {
	col, ok := missingColumnFromError(errors.New(
		`arrow query failed: Binder Error: Referenced column "survey_response" not found in FROM clause!` +
			"\nCandidate bindings: \"survey_name\", \"survey_id\""))
	if !ok || col != "survey_response" {
		t.Fatalf("got (%q, %v), want (survey_response, true)", col, ok)
	}

	for _, e := range []error{
		nil,
		errors.New("IO Error: No files found that match the pattern 's3://b/x/**/*.parquet'"),
		errors.New(`Binder Error: table "events" does not exist`),
		errors.New("HTTP 404 Not Found"),
	} {
		if c, ok := missingColumnFromError(e); ok {
			t.Errorf("missingColumnFromError(%v) = (%q, true), want false", e, c)
		}
	}
}

// TestWrapReadParquetWithNullCols verifies that every read_parquet(...) call is
// wrapped so the absent columns resolve to NULL, that balanced parens / quoted
// globs are handled, and that multiple source reads each get a distinct alias.
func TestWrapReadParquetWithNullCols(t *testing.T) {
	// no-op when no columns
	if got := wrapReadParquetWithNullCols("SELECT 1", nil); got != "SELECT 1" {
		t.Fatalf("empty cols should no-op, got %q", got)
	}

	in := "SELECT survey_response FROM read_parquet('s3://b/db/m/2026/04/**/*.parquet', union_by_name=true) WHERE event='survey sent'"
	got := wrapReadParquetWithNullCols(in, []string{"survey_response"})
	want := "SELECT survey_response FROM (SELECT *, CAST(NULL AS VARCHAR) AS \"survey_response\" FROM read_parquet('s3://b/db/m/2026/04/**/*.parquet', union_by_name=true)) _arc_drift_0 WHERE event='survey sent'"
	if got != want {
		t.Fatalf("single read:\n got=%q\nwant=%q", got, want)
	}

	// two source reads (e.g. a UNION) → two distinct aliases, both wrapped
	in2 := "SELECT * FROM read_parquet(['a.parquet']) UNION ALL SELECT * FROM read_parquet(['b.parquet'])"
	got2 := wrapReadParquetWithNullCols(in2, []string{"c1", "c2"})
	if strings.Count(got2, "_arc_drift_0") != 1 || strings.Count(got2, "_arc_drift_1") != 1 {
		t.Fatalf("expected two distinct drift aliases, got %q", got2)
	}
	if strings.Count(got2, `CAST(NULL AS VARCHAR) AS "c1"`) != 2 || strings.Count(got2, `CAST(NULL AS VARCHAR) AS "c2"`) != 2 {
		t.Fatalf("each read should inject both columns, got %q", got2)
	}

	// a "(" inside the quoted glob must not break paren matching
	in3 := "SELECT x FROM read_parquet('s3://b/weird(name)/**/*.parquet', union_by_name=true)"
	got3 := wrapReadParquetWithNullCols(in3, []string{"x"})
	if !strings.Contains(got3, "weird(name)") || !strings.HasSuffix(got3, "_arc_drift_0") {
		t.Fatalf("paren-in-glob not handled: %q", got3)
	}
}

// TestMatchParen covers the string-aware balanced-paren scan.
func TestMatchParen(t *testing.T) {
	s := "f('a(b)c', x)"
	if got := matchParen(s, 1); got != len(s)-1 {
		t.Fatalf("matchParen = %d, want %d (%q)", got, len(s)-1, s)
	}
	if got := matchParen("f(unbalanced", 1); got != -1 {
		t.Fatalf("unbalanced should be -1, got %d", got)
	}
}

// TestDriftFill_DuckDBEndToEnd proves the actual mechanism on real parquet: a column
// present in one file but absent from another (schema drift) makes a raw read over the
// without-file binder-error, while the wrapped form resolves it to NULL — the exact
// transformation the source path applies on a drift window.
func TestDriftFill_DuckDBEndToEnd(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Skipf("duckdb unavailable: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()
	withCol := filepath.Join(dir, "with.parquet")
	noCol := filepath.Join(dir, "no.parquet")
	if _, err := db.Exec("COPY (SELECT 1 AS id, '5' AS survey_response) TO '" + withCol + "' (FORMAT parquet)"); err != nil {
		t.Fatalf("write with.parquet: %v", err)
	}
	if _, err := db.Exec("COPY (SELECT 2 AS id) TO '" + noCol + "' (FORMAT parquet)"); err != nil {
		t.Fatalf("write no.parquet: %v", err)
	}

	// Raw read over the file LACKING the column → the binder error we fix.
	raw := "SELECT survey_response FROM read_parquet('" + noCol + "', union_by_name=true)"
	if _, err := db.Query(raw); err == nil {
		t.Fatalf("expected a binder error on the drifted read, got none")
	} else if _, ok := missingColumnFromError(err); !ok {
		t.Fatalf("expected a 'column not found' error, got: %v", err)
	}

	// Wrapped read → column resolves to NULL, query binds and returns the row.
	wrapped := wrapReadParquetWithNullCols(raw, []string{"survey_response"})
	var got sql.NullString
	if err := db.QueryRow(wrapped).Scan(&got); err != nil {
		t.Fatalf("wrapped query failed: %v\nsql: %s", err, wrapped)
	}
	if got.Valid {
		t.Fatalf("survey_response should be NULL for the drifted file, got %q", got.String)
	}

	// And the user's typical CAST over the synthesized NULL works (no error, NULL out).
	castWrapped := wrapReadParquetWithNullCols(
		"SELECT CAST(survey_response AS INTEGER) r FROM read_parquet('"+noCol+"', union_by_name=true)",
		[]string{"survey_response"})
	var r sql.NullInt64
	if err := db.QueryRow(castWrapped).Scan(&r); err != nil {
		t.Fatalf("CAST over synthesized NULL failed: %v", err)
	}
	if r.Valid {
		t.Fatalf("CAST(NULL AS INTEGER) should be NULL, got %d", r.Int64)
	}
}
