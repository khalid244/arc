//go:build realS3

// Opt-in real-data check (not part of the normal suite). Run with:
//
//	set -a; . ~/Downloads/.hammel-s3.env; set +a
//	go test ./internal/rollup/ -tags realS3 -run TestRealSchemaDrift -v
//
// It READS real prod Parquet from Hetzner Object Storage and writes any cube
// output to a LOCAL temp dir — it never writes to the bucket.
package rollup

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func openHetzner(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	for _, s := range []string{
		"INSTALL httpfs", "LOAD httpfs",
		"SET s3_endpoint='" + os.Getenv("S3_ENDPOINT") + "'",
		"SET s3_access_key_id='" + os.Getenv("S3_ACCESS_KEY") + "'",
		"SET s3_secret_access_key='" + os.Getenv("S3_SECRET_KEY") + "'",
		"SET s3_url_style='path'",
		"SET s3_region='fsn1'",
		"SET s3_use_ssl=true",
		"SET TimeZone='UTC'",
	} {
		if _, err := db.Exec(s); err != nil {
			db.Close()
			t.Fatalf("setup %q: %v", s, err)
		}
	}
	return db
}

// eventsDayGlob turns any events object path into a glob over its whole UTC day,
// so the schema probe unions every file that day (matching the month builder).
func eventsDayGlob(bucket, file string) string {
	const marker = "/posthog/events/"
	i := strings.Index(file, marker)
	if i < 0 {
		return "['" + file + "']"
	}
	parts := strings.Split(file[i+len(marker):], "/") // YYYY/MM/DD[/HH]/name.parquet
	if len(parts) < 3 {
		return "['" + file + "']"
	}
	ymd := strings.Join(parts[:3], "/")
	return fmt.Sprintf("['s3://%s/posthog/events/%s/**/*.parquet']", bucket, ymd)
}

// firstFile returns one real object under an s3 glob (or skips if the prefix is empty).
func firstFile(t *testing.T, db *sql.DB, glob string) string {
	var f string
	if err := db.QueryRow("SELECT file FROM glob('" + glob + "') ORDER BY file LIMIT 1").Scan(&f); err != nil {
		t.Skipf("no files under %s (%v)", glob, err)
	}
	return f
}

// TestRealSchemaDrift verifies the NULL-fill drift handling against REAL prod
// data: a cube dimensioned on a column that exists in June but not in April
// (prod's actual drift set) must BUILD over the April day — typed NULL dim, full
// schema, no Binder Error — and build with real values over the June day.
func TestRealSchemaDrift(t *testing.T) {
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		t.Skip("S3_* env not set (source ~/Downloads/.hammel-s3.env)")
	}
	db := openHetzner(t)
	defer db.Close()
	dir := t.TempDir()

	// Sparse event properties live in only SOME files, and the builder globs a whole
	// month (union_by_name across all files). So union over a full DAY (~25-80 files)
	// on each side — single-file sampling misses sparse columns. One real OLD day
	// (prod logs show 2026-04 lacked email/plan/...) and one recent day.
	oldFile := firstFile(t, db, fmt.Sprintf("s3://%s/posthog/events/2026/04/**/*.parquet", bucket))
	newFile := firstFile(t, db, fmt.Sprintf("s3://%s/posthog/events/2026/06/07/**/*.parquet", bucket))
	oldGlob := eventsDayGlob(bucket, oldFile)
	newGlob := eventsDayGlob(bucket, newFile)
	t.Logf("OLD day glob: %s", oldGlob)
	t.Logf("NEW day glob: %s", newGlob)

	oldCols, err := describeColumnSet(db, readParquetFrom(oldGlob))
	if err != nil {
		t.Fatalf("describeColumnSet old: %v", err)
	}
	newCols, err := describeColumnSet(db, readParquetFrom(newGlob))
	if err != nil {
		t.Fatalf("describeColumnSet new: %v", err)
	}
	t.Logf("April cols: %d   June cols: %d", len(oldCols), len(newCols))

	// The exact dimensions prod's rollup log failed to build for 2026-04.
	prodFailing := []string{"email", "plan", "screen", "name", "company", "signup_date",
		"employee_count", "from_source", "group_key", "group_type", "value", "button_name"}
	// Dump the real drift set (present in June, absent in April), sorted.
	var driftSet []string
	for c := range newCols {
		if _, ok := oldCols[c]; !ok {
			driftSet = append(driftSet, c)
		}
	}
	sort.Strings(driftSet)
	t.Logf("REAL drift (June-not-April), %d cols: %s", len(driftSet), strings.Join(driftSet, ", "))

	// Pick a drifted dimension: prefer a prod-failing one, else the first real drift.
	drift := ""
	for _, c := range prodFailing {
		_, inNew := newCols[c]
		_, inOld := oldCols[c]
		if inNew && !inOld {
			drift = c
			break
		}
	}
	if drift == "" && len(driftSet) > 0 {
		drift = driftSet[0]
	}
	t.Logf("drifted dimension chosen: %q (June has it, April doesn't)", drift)
	if drift == "" {
		t.Skip("no drifted dimension found between the two real days")
	}

	spec := CubeSpec{Source: "posthog.events", Grain: "hour", Dims: []string{drift},
		Aggs: []Aggregate{{Kind: AggCount}}, ColTypes: map[string]string{drift: newCols[drift]}}
	lo, hi := "2020-01-01 00:00:00", "2030-01-01 00:00:00"

	// (FIX, real data) the month path BUILDS over real April: typed NULL dim, no
	// Binder Error, no skip — the build that used to retry-storm every tick.
	oldDest := filepath.Join(dir, "apr.parquet")
	oldEntry, err := BuildRange(db, spec, oldGlob, "time", "apr", lo, hi, oldDest)
	if err != nil {
		t.Fatalf("FIX: real April by_%s build must NULL-fill, got: %v", drift, err)
	}
	t.Logf("FIX  April by_%s built OK: rows=%d", drift, oldEntry.Rows)
	if oldEntry.Rows == 0 {
		t.Fatalf("expected NULL-dim group rows in the real April cube")
	}
	var nonNull int64
	if err := db.QueryRow(fmt.Sprintf("SELECT count(%q) FROM read_parquet('%s')", drift, oldDest)).Scan(&nonNull); err != nil {
		t.Fatalf("read April cube: %v", err)
	}
	if nonNull != 0 {
		t.Fatalf("April %q must be all-NULL (column did not exist), got %d non-NULL", drift, nonNull)
	}

	// (FIX) real positive build on the June day, real dimension values.
	junDest := filepath.Join(dir, "jun.parquet")
	entry, err := BuildRange(db, spec, newGlob, "time", "jun", lo, hi, junDest)
	if err != nil {
		t.Fatalf("FIX: real June by_%s build failed: %v", drift, err)
	}
	t.Logf("FIX  June by_%s built OK: rows=%d  -> %s", drift, entry.Rows, junDest)
	if entry.Rows == 0 {
		t.Fatalf("expected rows in the real June cube")
	}
}
