package rollup

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestManifest_PruneAndRoundtrip(t *testing.T) {
	m := &Manifest{CubeID: "c1", Source: "default.downloads", Grain: "hour"}
	m.Upsert(DayEntry{Date: "2025-12-26", URI: "s3://b/26.parquet", BucketLo: "2025-12-26 00:00:00+00", BucketHi: "2025-12-27 00:00:00+00", Rows: 10})
	m.Upsert(DayEntry{Date: "2025-12-28", URI: "s3://b/28.parquet", BucketLo: "2025-12-28 00:00:00+00", BucketHi: "2025-12-29 00:00:00+00", Rows: 30})
	m.Upsert(DayEntry{Date: "2025-12-27", URI: "s3://b/27.parquet", BucketLo: "2025-12-27 00:00:00+00", BucketHi: "2025-12-28 00:00:00+00", Rows: 20})

	// kept sorted
	if m.Days[0].Date != "2025-12-26" || m.Days[2].Date != "2025-12-28" {
		t.Fatalf("days not sorted: %+v", m.Days)
	}
	// idempotent rebuild
	m.Upsert(DayEntry{Date: "2025-12-27", URI: "s3://b/27b.parquet", BucketLo: "2025-12-27 00:00:00+00", BucketHi: "2025-12-28 00:00:00+00", Rows: 21})
	if len(m.Days) != 3 || m.Days[1].URI != "s3://b/27b.parquet" {
		t.Fatalf("upsert not idempotent: %+v", m.Days)
	}

	// range [27, 29) prunes the 26th
	got := m.DaysInRange("2025-12-27 00:00:00+00", "2025-12-29 00:00:00+00")
	if len(got) != 2 || got[0].Date != "2025-12-27" || got[1].Date != "2025-12-28" {
		t.Fatalf("DaysInRange wrong: %+v", got)
	}
	// partial-day overlap still selects the day
	got = m.DaysInRange("2025-12-27 12:00:00+00", "2025-12-27 13:00:00+00")
	if len(got) != 1 || got[0].Date != "2025-12-27" {
		t.Fatalf("partial overlap wrong: %+v", got)
	}
	if ReadExpr(got) != "['s3://b/27b.parquet']" {
		t.Fatalf("ReadExpr=%q", ReadExpr(got))
	}

	// JSON round-trip
	b, err := m.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	m2, err := ParseManifest(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(m2.Days) != 3 || m2.Source != "default.downloads" {
		t.Fatalf("roundtrip lost data: %+v", m2)
	}
}

func dayGlob(date string) string { // date = "2025/12/26"
	return fmt.Sprintf("['s3://arc-test/default/downloads/%s/**/*.parquet']", date)
}

// TestManifest_RangeReadMatchesSource builds real per-day cube files, indexes them
// in a manifest, prunes to a 2-day window, and proves the manifest-pruned read
// equals a source scan over the same window — with ZERO S3 LIST on the read path
// (only the manifest-listed files are opened).
func TestManifest_RangeReadMatchesSource(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)

	spec := CubeSpec{Source: "default.downloads", Grain: "hour",
		Dims: []string{"status"}, Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggAvg, Col: "duration_seconds"}}}

	dir := t.TempDir()
	m := &Manifest{CubeID: "downloads_status", Source: spec.Source, Grain: spec.Grain, Dims: spec.Dims, SchemaHash: spec.SchemaHash()}
	dates := []string{"2025-12-26", "2025-12-27", "2025-12-28"}
	for _, d := range dates {
		dest := filepath.Join(dir, d+".parquet")
		glob := dayGlob(fmt.Sprintf("%s/%s/%s", d[0:4], d[5:7], d[8:10]))
		e, err := BuildDay(db, spec, glob, "time", d, dest)
		if err != nil {
			t.Fatalf("BuildDay %s: %v", d, err)
		}
		if e.Rows == 0 {
			t.Fatalf("day %s built empty", d)
		}
		m.Upsert(e)
	}

	// Query window: 2 days [27, 29). Manifest must prune the 26th.
	q := QueryShape{Source: spec.Source, TimeCol: "time", Grain: "hour", Dims: []string{"status"},
		Aggs:   []Aggregate{{Kind: AggCount, Alias: "n"}, {Kind: AggAvg, Col: "duration_seconds", Alias: "a"}},
		TimeLo: "2025-12-27 00:00:00+00", TimeHi: "2025-12-29 00:00:00+00"}

	selected := m.DaysInRange(q.TimeLo, q.TimeHi)
	if len(selected) != 2 {
		t.Fatalf("manifest selected %d days, want 2: %+v", len(selected), selected)
	}
	cubeExpr := ReadExpr(selected)

	srcGlob := "['s3://arc-test/default/downloads/2025/12/27/**/*.parquet', 's3://arc-test/default/downloads/2025/12/28/**/*.parquet']"
	src := runShape(t, db, q.SourceRefSQL(srcGlob), 2)
	cube := runShape(t, db, q.CubeReadSQL(cubeExpr), 2)

	if len(src.rows) != len(cube.rows) {
		t.Fatalf("group count mismatch: source=%d cube=%d", len(src.rows), len(cube.rows))
	}
	for k, sv := range src.rows {
		cv, ok := cube.rows[k]
		if !ok {
			t.Fatalf("cube missing %q", k)
		}
		for i := range sv {
			if !aggMatch(sv[i], cv[i], q.Aggs[i]) {
				t.Errorf("group %q agg[%s]: source=%v cube=%v", k, q.Aggs[i].Alias, sv[i], cv[i])
			}
		}
	}
	t.Logf("manifest pruned 3->2 days; %d groups matched across the 2-day window", len(src.rows))
}
