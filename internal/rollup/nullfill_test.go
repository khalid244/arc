package rollup

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// openLocalDuckSketch opens a local DuckDB with the datasketches community
// extension loaded (needed for HLL/KLL build aggregates). Skips when the
// extension cannot be installed (e.g. offline CI without the extension cache).
func openLocalDuckSketch(t *testing.T) *sql.DB {
	t.Helper()
	db := openLocalDuck(t)
	for _, s := range []string{"INSTALL datasketches FROM community", "LOAD datasketches"} {
		if _, err := db.Exec(s); err != nil {
			db.Close()
			t.Skipf("datasketches unavailable: %v", err)
		}
	}
	return db
}

// describeFile returns the lower-cased column->type map of a parquet file.
func describeFile(t *testing.T, db *sql.DB, path string) map[string]string {
	t.Helper()
	rows, err := db.Query("DESCRIBE SELECT * FROM read_parquet('" + path + "')")
	if err != nil {
		t.Fatalf("describe %s: %v", path, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	out := map[string]string{}
	for rows.Next() {
		raw := make([]any, len(cols))
		ptr := make([]any, len(cols))
		for i := range raw {
			ptr[i] = &raw[i]
		}
		if err := rows.Scan(ptr...); err != nil {
			t.Fatalf("scan describe: %v", err)
		}
		out[strings.ToLower(fmt.Sprintf("%v", raw[0]))] = fmt.Sprintf("%v", raw[1])
	}
	return out
}

// TestCubeReadMixedSchemas (F1) pins that the cube read path tolerates cube day
// files with different physical schemas (e.g. a legacy file written before a
// store column existed): read_parquet over the file list must merge by name and
// NULL-fill the missing column, and SUM must simply skip the NULL rows. Without
// union_by_name the read fails with a Binder Error that surfaces to the user
// (the api fallback only catches stale-file 404/403s, not schema mismatches).
func TestCubeReadMixedSchemas(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	dir := t.TempDir()

	// A "legacy" day file missing the _sum_bytes store column, and a full one.
	narrow := filepath.Join(dir, "2026-03-01.parquet")
	full := filepath.Join(dir, "2026-03-02.parquet")
	mustExec(t, db, fmt.Sprintf(
		`COPY (SELECT TIMESTAMPTZ '2026-03-01 10:00:00' AS bucket, 'web' AS site, 3::BIGINT AS _cnt) TO '%s' (FORMAT PARQUET)`, narrow))
	mustExec(t, db, fmt.Sprintf(
		`COPY (SELECT TIMESTAMPTZ '2026-03-02 10:00:00' AS bucket, 'web' AS site, 5::BIGINT AS _cnt, 100::DOUBLE AS _sum_bytes) TO '%s' (FORMAT PARQUET)`, full))
	cubeExpr := fmt.Sprintf("['%s', '%s']", narrow, full)

	q := QueryShape{
		Source: "default.events", TimeCol: "time", Grain: "hour",
		Aggs:   []Aggregate{{Kind: AggCount, Alias: "cnt"}, {Kind: AggSum, Col: "bytes", Alias: "total"}},
		TimeLo: "2026-03-01 00:00:00", TimeHi: "2026-03-03 00:00:00",
	}

	// CubeReadSQL over the mixed files must execute and aggregate.
	readSQL := q.CubeReadSQL(cubeExpr)
	rows, err := db.Query(readSQL)
	if err != nil {
		t.Fatalf("F1 CubeReadSQL over mixed cube schemas must not error, got: %v\nSQL: %s", err, readSQL)
	}
	var nRows int
	var totalSum float64
	var cntSum int64
	for rows.Next() {
		var bucket any
		var cnt int64
		var total sql.NullFloat64
		if err := rows.Scan(&bucket, &cnt, &total); err != nil {
			t.Fatalf("scan: %v", err)
		}
		nRows++
		cntSum += cnt
		if total.Valid {
			totalSum += total.Float64
		}
	}
	rows.Close()
	if nRows != 2 || cntSum != 8 {
		t.Fatalf("cube read rows=%d cntSum=%d, want 2 buckets / 8 count", nRows, cntSum)
	}
	// The missing _sum_bytes in the narrow file reads as NULL; SUM skips it.
	if totalSum != 100 {
		t.Fatalf("sum over mixed schemas = %v, want 100 (missing column treated as NULL)", totalSum)
	}

	// storePassthrough (the merge-read stored branch) must tolerate it too.
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"site"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Col: "bytes"}}}
	passSQL := spec.storePassthrough(cubeExpr, "2026-03-01 00:00:00", "2026-03-03 00:00:00")
	if _, err := db.Exec("CREATE OR REPLACE TEMP TABLE _f1_pass AS " + passSQL); err != nil {
		t.Fatalf("F1 storePassthrough over mixed cube schemas must not error, got: %v\nSQL: %s", err, passSQL)
	}
}

// TestBuildDayNullFillsMissingMetric (F2) pins that BuildDay — the build used by
// the sketch subprocess path and by compaction's rebuildMissingDailies — succeeds
// when the source day lacks a profiled metric column, writing the FULL cube store
// schema with typed-NULL aggregates instead of failing with a Binder Error (which
// left the day unbuilt and re-entered the todo set every tick: the retry storm).
func TestBuildDayNullFillsMissingMetric(t *testing.T) {
	db := openLocalDuckSketch(t)
	defer db.Close()
	dir := t.TempDir()

	// An old day whose schema has time + user_id but NOT the (newer) latency metric.
	src := filepath.Join(dir, "src.parquet")
	mustExec(t, db, fmt.Sprintf(
		`COPY (SELECT TIMESTAMPTZ '2026-04-10 01:00:00' + INTERVAL (i) MINUTE AS "time",
		              'u' || (i %% 3)::VARCHAR AS user_id
		       FROM range(10) t(i)) TO '%s' (FORMAT PARQUET)`, src))

	// A coarse-style, sketch-bearing spec as classify.go emits it (p95 + HLL).
	spec := CubeSpec{Source: "default.events", Grain: "hour", Aggs: []Aggregate{
		{Kind: AggCount},
		{Kind: AggAvg, Col: "latency"},
		{Kind: AggMin, Col: "latency"},
		{Kind: AggMax, Col: "latency"},
		{Kind: AggCountCol, Col: "latency"},
		{Kind: AggPercentile, Col: "latency", P: 0.95},
		{Kind: AggCountDistinct, Col: "user_id"},
	}}

	dest := filepath.Join(dir, "cube.parquet")
	entry, err := BuildDay(db, spec, "['"+src+"']", "time", "2026-04-10", dest)
	if err != nil {
		t.Fatalf("F2 BuildDay over a day missing the 'latency' metric must succeed (NULL-fill), got: %v", err)
	}
	if entry.Rows == 0 {
		t.Fatal("expected cube rows (count(*) groups exist even with NULL metrics)")
	}

	// The output must carry the FULL store schema (no per-day pruned variants).
	got := describeFile(t, db, dest)
	for _, want := range []string{"_cnt", "_cnt_latency", "_sum_latency", "_min_latency", "_max_latency", "_kll_latency", "_theta_user_id"} {
		if _, ok := got[want]; !ok {
			t.Errorf("cube file missing store column %s (got %v)", want, got)
		}
	}
	// And the NULL-filled aggregates read back as NULL while count(*) is real.
	var cnt int64
	var sumLat sql.NullFloat64
	if err := db.QueryRow("SELECT sum(_cnt)::BIGINT, sum(_sum_latency)::DOUBLE FROM read_parquet('"+dest+"')").Scan(&cnt, &sumLat); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if cnt != 10 {
		t.Fatalf("_cnt total = %d, want 10", cnt)
	}
	if sumLat.Valid {
		t.Fatalf("_sum_latency should be NULL for a day without the column, got %v", sumLat.Float64)
	}
}

func mustExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %s: %v", q, err)
	}
}

// TestDescribeColumnSetPropagatesErrors (F6) — the consolidated schema probe must
// return an error when the probed relation is unreadable. The old dayColumns
// returned an empty set with nil-error semantics, which read as "every column is
// absent" and silently skipped every cube for the day.
func TestDescribeColumnSetPropagatesErrors(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	if _, err := describeColumnSet(db, readParquetFrom("['/nonexistent-rollup-probe-dir/*.parquet']")); err == nil {
		t.Fatal("F6: probing an unreadable source must return an error, not an empty column set")
	}
	// And on success the names are lower-cased (DuckDB binds case-insensitively).
	dir := t.TempDir()
	f := filepath.Join(dir, "t.parquet")
	mustExec(t, db, fmt.Sprintf(`COPY (SELECT 1::BIGINT AS "UserId") TO '%s' (FORMAT PARQUET)`, f))
	cols, err := describeColumnSet(db, readParquetFrom("['"+f+"']"))
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if _, ok := cols["userid"]; !ok {
		t.Fatalf("expected case-folded column key 'userid', got %v", cols)
	}
}

// TestCompactionMergesNullFilledDay pins the typed-NULL hard requirement: a
// NULL-filled day's parquet columns must carry the cube's REAL column types (from
// the profiled ColTypes), so compaction's union_by_name COPY of a NULL-filled day
// next to a normal day succeeds with the correct merged schema (a bare untyped
// NULL would write an INT32 column and type-conflict with a VARCHAR/SMALLINT day).
func TestCompactionMergesNullFilledDay(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	dir := t.TempDir()

	spec := CubeSpec{
		Source: "default.events", Grain: "hour", Dims: []string{"status"},
		Aggs:     []Aggregate{{Kind: AggCount}, {Kind: AggSum, Col: "bytes"}},
		ColTypes: map[string]string{"status": "SMALLINT", "bytes": "BIGINT"},
	}

	// Day 1: full schema. Day 2: source predates both status and bytes.
	src1 := filepath.Join(dir, "s1.parquet")
	src2 := filepath.Join(dir, "s2.parquet")
	mustExec(t, db, fmt.Sprintf(
		`COPY (SELECT TIMESTAMPTZ '2026-03-01 10:00:00' AS "time", 200::SMALLINT AS status, 50::BIGINT AS bytes) TO '%s' (FORMAT PARQUET)`, src1))
	mustExec(t, db, fmt.Sprintf(
		`COPY (SELECT TIMESTAMPTZ '2026-03-02 10:00:00' AS "time") TO '%s' (FORMAT PARQUET)`, src2))

	d1 := filepath.Join(dir, "2026-03-01.parquet")
	d2 := filepath.Join(dir, "2026-03-02.parquet")
	if _, err := BuildDay(db, spec, "['"+src1+"']", "time", "2026-03-01", d1); err != nil {
		t.Fatalf("full day build: %v", err)
	}
	if _, err := BuildDay(db, spec, "['"+src2+"']", "time", "2026-03-02", d2); err != nil {
		t.Fatalf("drifted day build must NULL-fill, got: %v", err)
	}

	// The NULL-filled day must carry the SAME column types as the normal day.
	t1, t2 := describeFile(t, db, d1), describeFile(t, db, d2)
	for _, c := range []string{"status", "_sum_bytes", "_cnt"} {
		if t1[c] != t2[c] {
			t.Errorf("column %s: normal day type %q vs NULL-filled day type %q — must match for compaction", c, t1[c], t2[c])
		}
	}

	// Compaction's exact COPY shape (manager.compactMonth) must succeed.
	merged := filepath.Join(dir, "m_2026-03.parquet")
	mergeSQL := fmt.Sprintf("COPY (SELECT * FROM read_parquet(['%s', '%s'], union_by_name=true)) TO '%s' (FORMAT parquet)", d1, d2, merged)
	if _, err := db.Exec(mergeSQL); err != nil {
		t.Fatalf("compaction COPY over NULL-filled + normal day failed: %v", err)
	}
	mt := describeFile(t, db, merged)
	if mt["status"] != t1["status"] {
		t.Fatalf("merged status type = %q, want %q (typed NULL preserved)", mt["status"], t1["status"])
	}
	// And the merged month still aggregates correctly: 2 rows, sum from day 1 only.
	var cnt int64
	var sum sql.NullFloat64
	if err := db.QueryRow("SELECT sum(_cnt)::BIGINT, sum(_sum_bytes)::DOUBLE FROM read_parquet('"+merged+"')").Scan(&cnt, &sum); err != nil {
		t.Fatalf("read merged: %v", err)
	}
	if cnt != 2 || !sum.Valid || sum.Float64 != 50 {
		t.Fatalf("merged cnt=%d sum=%v, want 2 / 50", cnt, sum)
	}
}
