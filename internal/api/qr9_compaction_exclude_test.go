//go:build duckdb_arrow

package api

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

// QR-9 (API half): while a compaction of a table has uploaded its output but
// not yet deleted its inputs, the query path hides the inputs through a helper
// column. It must be named _arc_src_file (a table may have a real "filename"
// column, which read_parquet's filename=true collides with), be removed again
// with SELECT * EXCLUDE so the result schema never changes during a
// compaction, and be built through sqlutil.ReadParquet / ReadParquetList.

func TestReadParquetExprExcludingUsesHelperColumn_QR9(t *testing.T) {
	exclude := compactionSourceFileColumn + " NOT IN ('/d/x.parquet')"
	for name, paths := range map[string][]string{
		"one path":   {"/d/db/m/2026/06/01/00/*.parquet"},
		"path list":  {"/d/db/m/2026/06/01/00/*.parquet", "/d/db/m/2026/06/01/01/*.parquet"},
		"with glob":  {"/d/db/m/**/*.parquet"},
		"two + list": {"/d/a/*.parquet", "/d/b/*.parquet", "/d/c/*.parquet"},
	} {
		got := readParquetExprExcluding("FROM", "", paths, buildReadParquetOptions(), exclude)
		if strings.Contains(got, "filename=true") || strings.Contains(got, "filename = true") {
			t.Fatalf("%s: must not use filename=true: %s", name, got)
		}
		if !strings.Contains(got, "filename='_arc_src_file'") {
			t.Fatalf("%s: helper column must be _arc_src_file: %s", name, got)
		}
		if !strings.HasPrefix(got, "FROM (SELECT * EXCLUDE (_arc_src_file) FROM read_parquet(") {
			t.Fatalf("%s: helper column must be excluded from SELECT *: %s", name, got)
		}
		if !strings.HasSuffix(got, " WHERE "+exclude+")") {
			t.Fatalf("%s: exclusion predicate missing: %s", name, got)
		}
	}
	// No exclusion: exactly the plain read, no wrapper.
	plain := readParquetExprExcluding("FROM", "", []string{"/d/a/*.parquet"}, buildReadParquetOptions(), "")
	if plain != readParquetExpr("FROM", "", []string{"/d/a/*.parquet"}, buildReadParquetOptions()) {
		t.Fatalf("no exclusion must be the plain read, got %s", plain)
	}
}

// End to end on both endpoints: a table with a real "filename" column, read
// over two hour partitions (the read_parquet LIST form), with an uploaded
// compaction's input hidden. SELECT * returns exactly the columns it returns
// with no compaction running, the "filename" values are the table's own, and
// the input rows are not counted twice.
func TestCompactionExcludeRealFilenameColumnBothEndpoints_QR9(t *testing.T) {
	e := newForkQueryEnv(t)
	e.writeParquet("default/logs/2026/06/01/00/input.parquet",
		"SELECT TIMESTAMPTZ '2026-06-01 00:10:00+00' AS time, 'a.txt' AS filename, 1 AS v")
	e.writeParquet("default/logs/2026/06/01/00/output.parquet",
		"SELECT TIMESTAMPTZ '2026-06-01 00:10:00+00' AS time, 'a.txt' AS filename, 1 AS v")
	e.writeParquet("default/logs/2026/06/01/01/other.parquet",
		"SELECT TIMESTAMPTZ '2026-06-01 01:10:00+00' AS time, 'b.txt' AS filename, 2 AS v")
	const where = " WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-01T02:00:00Z'"
	selectAll := "SELECT * FROM logs" + where + " ORDER BY time"
	byFile := "SELECT filename, count(*) AS n, sum(v) AS s FROM logs" + where + " GROUP BY filename ORDER BY filename"

	// Baseline with no compaction active: both inputs and output are visible
	// (that is the duplicate the exclusion exists to hide).
	status, base, _ := e.post("/api/v1/query", selectAll, nil)
	if status != fiber.StatusOK || len(base.Data) != 3 {
		t.Fatalf("baseline: status %d rows %d: %s", status, len(base.Data), base.raw)
	}
	_, baseArrowCols, _, raw := (&qr3Env{forkQueryEnv: e}).postArrow(selectAll)
	if baseArrowCols == nil {
		t.Fatalf("baseline Arrow failed: %s", raw)
	}

	e.h.SetManifestManager(fakeManifests{uploaded: map[string]struct{}{
		"default/logs/2026/06/01/00/input.parquet": {},
	}})
	e.h.InvalidateCaches()

	// The transform really uses the wrapper (list form, helper column).
	converted, _, err := e.h.getTransformedSQL(context.Background(), selectAll, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(converted, "EXCLUDE (_arc_src_file)") || !strings.Contains(converted, "read_parquet([") {
		t.Fatalf("expected the list-form exclusion wrapper, got %s", converted)
	}

	// JSON endpoint.
	status, out, _ := e.post("/api/v1/query", selectAll, nil)
	if status != fiber.StatusOK {
		t.Fatalf("JSON SELECT *: status %d: %s", status, out.raw)
	}
	if strings.Join(out.Columns, ",") != strings.Join(base.Columns, ",") {
		t.Fatalf("JSON SELECT * columns changed during compaction: %v, want %v", out.Columns, base.Columns)
	}
	if len(out.Data) != 2 {
		t.Fatalf("JSON SELECT *: the uploaded output's input must be hidden, got %d rows: %s", len(out.Data), out.raw)
	}
	status, out, _ = e.post("/api/v1/query", byFile, nil)
	if status != fiber.StatusOK {
		t.Fatalf("JSON by filename: status %d: %s", status, out.raw)
	}
	if len(out.Data) != 2 || out.Data[0][0] != "a.txt" || out.Data[0][1] != float64(1) ||
		out.Data[1][0] != "b.txt" || out.Data[1][1] != float64(1) {
		t.Fatalf("JSON by filename: want a.txt=1 b.txt=1 (the table's own filename column), got %s", out.raw)
	}

	// Arrow IPC endpoint.
	a := &qr3Env{forkQueryEnv: e}
	status, cols, _, raw := a.postArrow(selectAll)
	if status != fiber.StatusOK {
		t.Fatalf("Arrow SELECT *: status %d: %s", status, raw)
	}
	if got, want := sortedKeys(cols), sortedKeys(baseArrowCols); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("Arrow SELECT * columns changed during compaction: %v, want %v", got, want)
	}
	if len(cols["filename"]) != 2 {
		t.Fatalf("Arrow SELECT *: want 2 rows, got %v", cols)
	}
	status, cols, _, raw = a.postArrow(byFile)
	if status != fiber.StatusOK {
		t.Fatalf("Arrow by filename: status %d: %s", status, raw)
	}
	if len(cols["filename"]) != 2 || cols["filename"][0] != "a.txt" || cols["n"][0] != float64(1) {
		t.Fatalf("Arrow by filename: want a.txt=1 b.txt=1, got %v", cols)
	}

	// Schema drift on top of the wrapper: a column no file has is NULL-filled
	// inside the exclusion wrapper and the input stays hidden.
	status, out, hdr := e.post("/api/v1/query", "SELECT filename, missing_col FROM logs"+where+" ORDER BY time", nil)
	if status != fiber.StatusOK || len(out.Data) != 2 || hdr["X-Arc-Drift-Fill"] != "missing_col" {
		t.Fatalf("drift fill under the exclusion wrapper: status %d drift=%q: %s", status, hdr["X-Arc-Drift-Fill"], out.raw)
	}
}

func sortedKeys(m map[string][]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
