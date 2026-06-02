package rollup

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// rawOnSource runs the user's original SQL against source by swapping the bare
// table for a read_parquet over the given glob — the ground truth the router's
// rewrite must reproduce.
func rawOnSource(t *testing.T, db *sql.DB, userSQL, srcGlob string, nKeys int) result {
	t.Helper()
	rp := fmt.Sprintf("read_parquet(%s, union_by_name=true)", srcGlob)
	s := strings.Replace(userSQL, "FROM downloads", "FROM "+rp, 1)
	return runShape(t, db, s, nKeys)
}

// TestRouter_EndToEnd drives the whole read path: raw SQL string -> Parse ->
// coverage match -> manifest prune -> emit -> rewritten SQL, and proves the
// rewritten query equals the original run against source. Covers both a fully
// sealed window and a window with a fresh source tail.
func TestRouter_EndToEnd(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)

	spec := CubeSpec{Source: "default.downloads", Grain: "hour", Dims: []string{"status"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggAvg, Col: "duration_seconds"}}}

	// Build 3 daily cube files + manifest.
	dir := t.TempDir()
	m := &Manifest{CubeID: "downloads_status", Source: spec.Source, Grain: spec.Grain, Dims: spec.Dims, SchemaHash: spec.SchemaHash()}
	for _, d := range []string{"2025-12-26", "2025-12-27", "2025-12-28"} {
		dest := filepath.Join(dir, d+".parquet")
		glob := dayGlob(fmt.Sprintf("%s/%s/%s", d[0:4], d[5:7], d[8:10]))
		e, err := BuildDay(db, spec, glob, "time", d, dest)
		if err != nil {
			t.Fatalf("BuildDay %s: %v", d, err)
		}
		m.Upsert(e)
	}

	srcGlob := "['s3://arc-test/default/downloads/2025/12/26/**/*.parquet', 's3://arc-test/default/downloads/2025/12/27/**/*.parquet', 's3://arc-test/default/downloads/2025/12/28/**/*.parquet']"

	newRouter := func(watermark string) *Router {
		return &Router{
			Cubes:      []CubeSpec{spec},
			Manifests:  map[string]*Manifest{cubeKeyOf(spec): m},
			TimeCol:    "time",
			SourceExpr: func(string) string { return srcGlob },
			Watermark:  func(string) string { return watermark },
		}
	}

	const win = "WHERE time >= TIMESTAMPTZ '2025-12-27' AND time < TIMESTAMPTZ '2025-12-29'"
	cases := []struct {
		name      string
		sql       string
		nKeys     int
		watermark string // "" = fully sealed
		wantServe bool
	}{
		{"sealed count by hour+status", "SELECT date_trunc('hour', time) AS b, status, count(*) AS n FROM downloads " + win + " GROUP BY 1,2", 2, "", true},
		{"sealed avg by day+status", "SELECT date_trunc('day', time) AS b, status, avg(duration_seconds) AS a FROM downloads " + win + " GROUP BY 1,2", 2, "", true},
		{"fresh-tail count", "SELECT date_trunc('hour', time) AS b, status, count(*) AS n FROM downloads " + win + " GROUP BY 1,2", 2, "2025-12-28 00:00:00+00", true},
		{"filtered count", "SELECT date_trunc('hour', time) AS b, count(*) AS n FROM downloads " + win + " AND status = 'ns' GROUP BY 1", 1, "", true},
		// not served -> caller uses source unchanged (still correct, just not accelerated)
		{"uncovered dim city", "SELECT city, count(*) AS n FROM downloads " + win + " GROUP BY 1", 1, "", false},
		{"unsupported join", "SELECT count(*) AS n FROM downloads a JOIN downloads b ON a.ip=b.ip " + win, 0, "", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newRouter(c.watermark)
			d := r.Route(c.sql)
			if d.Served != c.wantServe {
				t.Fatalf("Served=%v want %v (reason=%q)", d.Served, c.wantServe, d.Reason)
			}
			if !d.Served {
				t.Logf("correctly fell through to source: %s", d.Reason)
				return
			}
			want := rawOnSource(t, db, c.sql, srcGlob, c.nKeys)
			got := runShape(t, db, d.SQL, c.nKeys)
			if len(want.rows) != len(got.rows) {
				t.Fatalf("group count: source=%d router=%d\nrewritten: %s", len(want.rows), len(got.rows), d.SQL)
			}
			// Recover the aggregate kinds for tolerance via a parse of the same SQL.
			shape, _, _ := Parse(c.sql, "time")
			for k, sv := range want.rows {
				cv, ok := got.rows[k]
				if !ok {
					t.Fatalf("router missing group %q", k)
				}
				for i := range sv {
					if !aggMatch(sv[i], cv[i], shape.Aggs[i]) {
						t.Errorf("group %q agg[%d]: source=%v router=%v", k, i, sv[i], cv[i])
					}
				}
			}
			t.Logf("served by cube=%s, %d groups matched source", d.Cube, len(want.rows))
		})
	}
}

// dailyEntry builds a manifest DayEntry spanning the whole UTC day `date` (one
// hour-grain cube file), with `rows` rows. Used to construct manifests with and
// without interior gaps for the gap-detection tests.
func dailyEntry(date string, rows int64) DayEntry {
	d, _ := parseTS(date)
	return DayEntry{
		Date:     date,
		URI:      "s3://arc-test/_arc/rollup/default/events/by_region/" + date + ".parquet",
		BucketLo: date + " 00:00:00+00",
		BucketHi: fmtTS(d.Add(24 * time.Hour)), // exclusive end = next-day 00:00 (max bucket + 1h grain)
		Rows:     rows,
	}
}

// TestManifestInteriorGap is the regression lock for the round-1 silent-undercount:
// a manifest with a day PURGED from the middle of its coverage must report a gap for
// any query spanning that day, so the router falls the query to source instead of
// silently re-aggregating the surrounding files and returning fewer rows than source.
func TestManifestInteriorGap(t *testing.T) {
	// Coverage 05-01 .. 05-04 with 05-03 PURGED (the live-repro shape).
	gapped := &Manifest{
		Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Days: []DayEntry{
			dailyEntry("2026-05-01", 144),
			dailyEntry("2026-05-02", 144),
			// 2026-05-03 deliberately absent (purged out from under the manifest)
			dailyEntry("2026-05-04", 144),
		},
	}
	contiguous := &Manifest{
		Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Days: []DayEntry{
			dailyEntry("2026-05-01", 144),
			dailyEntry("2026-05-02", 144),
			dailyEntry("2026-05-03", 144),
			dailyEntry("2026-05-04", 144),
		},
	}
	// A contiguous manifest whose 05-03 produced NO rows legitimately (genuinely-empty
	// source day) but is recorded as known-built via a zero-row covered marker — must
	// NOT be flagged as a gap (else we'd over-fall every spanning query to source).
	emptyKnown := &Manifest{
		Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Days: []DayEntry{
			dailyEntry("2026-05-01", 144),
			dailyEntry("2026-05-02", 144),
			{Date: "2026-05-03-empty", Covers: []string{"2026-05-03"}}, // known-built, zero rows, no file
			dailyEntry("2026-05-04", 144),
		},
	}
	// A compacted month covering 05-01..05-04 (one file) has no interior gap.
	compacted := &Manifest{
		Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Days: []DayEntry{{
			Date: "2026-05", URI: "s3://arc-test/_arc/rollup/default/events/by_region/m_2026-05_1.parquet",
			BucketLo: "2026-05-01 00:00:00+00", BucketHi: "2026-05-05 00:00:00+00", Rows: 576,
			Covers: []string{"2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04"},
		}},
	}
	// THE LIVE-REPRO SHAPE: a compacted month whose bucket span is CONTIGUOUS
	// (05-01..05-04) but whose Covers list is MISSING an interior day (05-03 dropped by
	// the round-1 purge). The span must NOT be trusted — Covers is authoritative — else
	// the gap is hidden and the query silently undercounts. This is the case the live
	// reproduction exercised (May month, 13 days, 05-08 dropped).
	compactedGap := &Manifest{
		Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Days: []DayEntry{{
			Date: "2026-05", URI: "s3://arc-test/_arc/rollup/default/events/by_region/m_2026-05_2.parquet",
			BucketLo: "2026-05-01 00:00:00+00", BucketHi: "2026-05-05 00:00:00+00", Rows: 432,
			Covers: []string{"2026-05-01", "2026-05-02", "2026-05-04"}, // 05-03 dropped
		}},
	}

	const lo, hi = "2026-05-01 00:00:00+00", "2026-05-05 00:00:00+00"
	cases := []struct {
		name string
		m    *Manifest
		want bool
	}{
		{"interior day purged -> gap", gapped, true},
		{"contiguous dailies -> no gap", contiguous, false},
		{"genuinely-empty day known-built -> no gap", emptyKnown, false},
		{"compacted month -> no gap", compacted, false},
		{"compacted month, interior day dropped from Covers -> gap", compactedGap, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.m.HasInteriorGap(lo, hi); got != c.want {
				t.Fatalf("HasInteriorGap=%v want %v", got, c.want)
			}
		})
	}

	// A query that does NOT span the purged day (e.g. only 05-01..05-02) must still be
	// served from the cube — the gap is outside the window.
	if gapped.HasInteriorGap("2026-05-01 00:00:00+00", "2026-05-03 00:00:00+00") {
		t.Fatal("query not spanning the purged day must NOT report a gap")
	}
	// A leading-edge query starting AT the gap is handled by manifestCoversStart, not
	// here; a query starting after the gap (05-04 only) has no interior hole.
	if gapped.HasInteriorGap("2026-05-04 00:00:00+00", "2026-05-05 00:00:00+00") {
		t.Fatal("query entirely after the purged day must NOT report a gap")
	}
}

// TestRouterFallsToSourceOnInteriorGap locks the end-to-end read-path behavior: the
// router must NOT serve a query that spans an interior gap (served=false, reason
// coverage_gap_interior) so the api layer runs it against source — the correct count.
func TestRouterFallsToSourceOnInteriorGap(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Aggs: []Aggregate{{Kind: AggCount}}}
	gapped := &Manifest{
		CubeID: "events_region", Source: spec.Source, Grain: spec.Grain, Dims: spec.Dims,
		Aggs: spec.Aggs, SchemaHash: spec.SchemaHash(),
		Days: []DayEntry{
			dailyEntry("2026-05-01", 144),
			dailyEntry("2026-05-02", 144),
			// 2026-05-03 purged
			dailyEntry("2026-05-04", 144),
		},
	}
	r := &Router{
		Cubes: []CubeSpec{spec}, Manifests: map[string]*Manifest{cubeKeyOf(spec): gapped},
		TimeCol:    "time",
		SourceExpr: func(string) string { return "['s3://arc-test/default/events/**/*.parquet']" },
		Watermark:  func(string) string { return "" }, // fully sealed
	}
	sql := "SELECT region, count(*) AS n FROM events WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-05' GROUP BY 1"
	d := r.Route(sql)
	if d.Served {
		t.Fatalf("router served a gapped range from cube (SILENT UNDERCOUNT); want fall-to-source. SQL=%s", d.SQL)
	}
	if d.Reason != "coverage_gap_interior" {
		t.Fatalf("reason=%q want coverage_gap_interior", d.Reason)
	}
	// And a range that avoids the gap is still served (no over-falling).
	ok := "SELECT region, count(*) AS n FROM events WHERE time >= TIMESTAMPTZ '2026-05-01' AND time < TIMESTAMPTZ '2026-05-03' GROUP BY 1"
	if d := r.Route(ok); !d.Served {
		t.Fatalf("router refused a gap-free range; want served (reason=%q)", d.Reason)
	}
}
