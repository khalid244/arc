package tiered

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestGroupingSets_Speedup compares two strategies for building all 12
// rollup variants for one daily bucket against real parquet data:
//
//   OLD — 12 separate GROUP BY queries (current production: each variant's
//         build SQL re-scans the source).
//   NEW — ONE GROUPING SETS query that emits all 12 variants' aggregates
//         in a single source scan, with a `variant_id` discriminator.
//
// Set ARC_ROLLUP_FIXTURE=<dir> (default /tmp/local-downloads) to a directory
// of downloads parquet. Run:
//
//   go test -tags=duckdb_arrow -count=1 -timeout 5m -run TestGroupingSets_Speedup -v ./internal/rollup/tiered/
func TestGroupingSets_Speedup(t *testing.T) {
	dir := os.Getenv("ARC_ROLLUP_FIXTURE")
	if dir == "" {
		dir = "/tmp/local-downloads"
	}
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("ARC_ROLLUP_FIXTURE not present at %s — skipping", dir)
	}

	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Load fixture into a real in-memory table so both passes scan local memory
	// (parity: we're measuring the QUERY cost, not the S3 read cost).
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE src AS SELECT * FROM read_parquet('%s/**/*.parquet', union_by_name=true)`,
		dir)); err != nil {
		t.Fatal(err)
	}

	var n int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM src`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	t.Logf("source rows: %d", n)

	// Pick 10 VARCHAR columns from the schema as "per-dim" candidates.
	rows, err := db.Query(`DESCRIBE src`)
	if err != nil {
		t.Fatal(err)
	}
	var dims []string
	for rows.Next() {
		var name, typ string
		var x [4]any
		_ = rows.Scan(&name, &typ, &x[0], &x[1], &x[2], &x[3])
		if typ == "VARCHAR" && len(dims) < 10 {
			dims = append(dims, name)
		}
	}
	rows.Close()
	if len(dims) < 5 {
		t.Skipf("need at least 5 VARCHAR dims, got %d", len(dims))
	}
	t.Logf("dims: %v", dims)

	// OLD path: 12 separate GROUP BY queries. We don't write parquet — we
	// drain to COUNT(*) so the work is dominated by aggregation just like
	// production builds.
	oldStart := time.Now()
	// sketch variant (no per-dim grouping)
	if _, err := db.Exec(`SELECT date_trunc('hour', time) AS bucket, COUNT(*) FROM src GROUP BY bucket`); err != nil {
		t.Fatal(err)
	}
	// per-dim variants (10)
	for _, d := range dims {
		q := fmt.Sprintf(`SELECT date_trunc('hour', time) AS bucket, %s AS dim_v, COUNT(*) FROM src GROUP BY bucket, dim_v`, d)
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("old %s: %v", d, err)
		}
	}
	// dim-rich variant (all dims together)
	dimList := ""
	for i, d := range dims {
		if i > 0 {
			dimList += ", "
		}
		dimList += d
	}
	q := fmt.Sprintf(`SELECT date_trunc('hour', time) AS bucket, %s, COUNT(*) FROM src GROUP BY bucket, %s`, dimList, dimList)
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("old dim-rich: %v", err)
	}
	oldDur := time.Since(oldStart)

	// NEW path: ONE GROUPING SETS query. Each grouping set corresponds to
	// one variant; GROUPING_ID discriminates them in a single result stream.
	newStart := time.Now()
	var gsets string
	gsets = "(date_trunc('hour', time))"             // sketch
	for _, d := range dims {                         // 10 per-dim
		gsets += ", (date_trunc('hour', time), " + d + ")"
	}
	gsets += ", (date_trunc('hour', time), " + dimList + ")" // dim-rich
	q = fmt.Sprintf(`
		SELECT date_trunc('hour', time) AS bucket, %s, COUNT(*) AS cnt
		FROM src
		GROUP BY GROUPING SETS (%s)`, dimList, gsets)
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("new grouping sets: %v", err)
	}
	newDur := time.Since(newStart)

	speedup := float64(oldDur) / float64(newDur)
	t.Logf("")
	t.Logf("OLD (12 separate queries): %v", oldDur.Truncate(time.Millisecond))
	t.Logf("NEW (1 GROUPING SETS):     %v", newDur.Truncate(time.Millisecond))
	t.Logf("speedup: %.1fx", speedup)
	t.Logf("")
	fmt.Println()
}
