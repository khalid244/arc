package rollup

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// These tests pin the watermark-clamp invariant: MergeReadSQL's contract is "the
// cube is complete below the watermark", but the seal clock (now-grace) keeps
// advancing while cubes are built in whole sealed days — so the router must cap
// the watermark at the manifest's real coverage hi before merging. Without the
// clamp, every bucket in [coverageHi, alignDown(watermark)) is read from cube
// files that do not exist: zero rows, no error (the live repro silently dropped
// 5.2M events — 19% of a 2-day window — while labeled as served-by-cube).

// clampFixture builds hour-grain status cubes for the given days plus a router
// whose seal clock is freely settable. Days NOT in `days` are absent from the
// manifest, so coverage hi is the midnight after the last built day.
func clampFixture(t *testing.T, db *sql.DB, days []string) (func(wm string) *Router, string) {
	t.Helper()
	spec := CubeSpec{Source: "default.downloads", Grain: "hour", Dims: []string{"status"},
		Aggs: []Aggregate{{Kind: AggCount}}}
	dir := t.TempDir()
	m := &Manifest{CubeID: "downloads_status", Source: spec.Source, Grain: spec.Grain,
		Dims: spec.Dims, SchemaHash: spec.SchemaHash()}
	for _, d := range days {
		dest := filepath.Join(dir, d+".parquet")
		glob := dayGlob(fmt.Sprintf("%s/%s/%s", d[0:4], d[5:7], d[8:10]))
		e, err := BuildDay(db, spec, glob, "time", d, dest)
		if err != nil {
			t.Fatalf("BuildDay %s: %v", d, err)
		}
		m.Upsert(e)
	}
	srcGlob := "['s3://arc-test/default/downloads/2025/12/26/**/*.parquet', 's3://arc-test/default/downloads/2025/12/27/**/*.parquet', 's3://arc-test/default/downloads/2025/12/28/**/*.parquet']"
	newRouter := func(wm string) *Router {
		return &Router{
			Cubes:      []CubeSpec{spec},
			Manifests:  map[string]*Manifest{cubeKeyOf(spec): m},
			TimeCol:    "time",
			SourceExpr: func(string) string { return srcGlob },
			Watermark:  func(string) string { return wm },
		}
	}
	return newRouter, srcGlob
}

// mustMatchSource routes sql, requires it to be served, and asserts the rewritten
// query's groups EQUAL the same sql run purely against source — the property the
// unclamped watermark broke.
func mustMatchSource(t *testing.T, db *sql.DB, r *Router, sqlQ, srcGlob string, nKeys int) Decision {
	t.Helper()
	d := r.Route(sqlQ)
	if !d.Served {
		t.Fatalf("not served (reason=%q)", d.Reason)
	}
	want := rawOnSource(t, db, sqlQ, srcGlob, nKeys)
	got := runShape(t, db, d.SQL, nKeys)
	if len(want.rows) != len(got.rows) {
		t.Fatalf("group count: source=%d router=%d\nrewritten: %s", len(want.rows), len(got.rows), d.SQL)
	}
	shape, _, _ := Parse(sqlQ, "time")
	for k, sv := range want.rows {
		cv, ok := got.rows[k]
		if !ok {
			t.Fatalf("router missing group %q\nrewritten: %s", k, d.SQL)
		}
		for i := range sv {
			if !aggMatch(sv[i], cv[i], shape.Aggs[i]) {
				t.Errorf("group %q agg[%d]: source=%v router=%v", k, i, sv[i], cv[i])
			}
		}
	}
	return d
}

func TestRouter_WatermarkClampToCoverage(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)

	// Cube built through Dec 27 only -> coverage hi = Dec 28 00:00. Dec 28 exists
	// in SOURCE but not in any cube: the live-prod shape ("today" never cubed).
	t.Run("straddle: seal clock past coverage", func(t *testing.T) {
		newRouter, srcGlob := clampFixture(t, db, []string{"2025-12-26", "2025-12-27"})
		// Seal clock half-way through the unbuilt day. Unclamped, the cube branch
		// would claim [Dec 27, Dec 28 12:00) and silently lose [Dec 28 00:00, 12:00).
		r := newRouter("2025-12-28 12:00:00+00")
		sqlQ := "SELECT date_trunc('hour', time) AS b, status, count(*) AS n FROM downloads " +
			"WHERE time >= TIMESTAMPTZ '2025-12-27' AND time < TIMESTAMPTZ '2025-12-29' GROUP BY 1,2"
		d := mustMatchSource(t, db, r, sqlQ, srcGlob, 2)
		if !strings.Contains(d.SQL, "'2025-12-28 00:00:00+00'") {
			t.Errorf("source tail must start at coverage hi (Dec 28 00:00), got:\n%s", d.SQL)
		}
	})

	// hi <= seal clock used to take the cube-only "fully sealed" early exit even
	// when the cube's coverage ended a half-day earlier — same hole, no source
	// branch at all. The clamped watermark forces a merge.
	t.Run("early exit: hi past coverage but under seal clock", func(t *testing.T) {
		newRouter, srcGlob := clampFixture(t, db, []string{"2025-12-26", "2025-12-27"})
		r := newRouter("2025-12-28 18:00:00+00")
		sqlQ := "SELECT date_trunc('hour', time) AS b, status, count(*) AS n FROM downloads " +
			"WHERE time >= TIMESTAMPTZ '2025-12-27' AND time < TIMESTAMPTZ '2025-12-28 12:00:00' GROUP BY 1,2"
		mustMatchSource(t, db, r, sqlQ, srcGlob, 2)
	})

	// Regression pin on the OLD behavior where it was correct: when the cube's
	// coverage extends past the seal clock (e.g. shortly after midnight, before
	// grace has elapsed), the boundary must REMAIN the seal clock — the fresh
	// tail past it is re-aggregated from source even though a cube file spans it.
	t.Run("fresh cube: clamp is a no-op", func(t *testing.T) {
		newRouter, srcGlob := clampFixture(t, db, []string{"2025-12-26", "2025-12-27", "2025-12-28"})
		r := newRouter("2025-12-28 18:00:00+00") // coverage hi (Dec 29 00:00) is beyond the clock
		sqlQ := "SELECT date_trunc('hour', time) AS b, status, count(*) AS n FROM downloads " +
			"WHERE time >= TIMESTAMPTZ '2025-12-27' AND time < TIMESTAMPTZ '2025-12-29' GROUP BY 1,2"
		d := mustMatchSource(t, db, r, sqlQ, srcGlob, 2)
		if !strings.Contains(d.SQL, "'2025-12-28 18:00:00+00'") {
			t.Errorf("boundary must stay at the seal clock when the cube is fresher, got:\n%s", d.SQL)
		}
	})
}
