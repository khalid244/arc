package rollup

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Tests for BEST-EFFORT CUBE-ONLY routing (RouteOnly / RouteOnlyHTTP), the
// X-Arc-Rollup-Only redefinition: when the query SHAPE matches a cube, serve
// from the cube UNCONDITIONALLY — return whatever days exist (missing days =
// missing rows, chart gaps), never fall back to source, never error about
// coverage. Declines remain ONLY for shape-level reasons (parse failure,
// no_covering_cube, grain_too_fine, no cube files at all) so the api keeps
// its 422 for "nothing could ever be shown".
//
// AUTO mode (Route) semantics are pinned unchanged alongside every case: the
// coverage-gap guards protect silent-correctness there (auto falls to source
// on gaps so results stay complete).

// writeCubeDayFile writes one hour-grain cube day file for `date` (two buckets,
// 00:00 and 01:00, one 'eu' region row each with _cnt = cnt) and returns its
// manifest entry with bucket bounds matching the file contents.
func writeCubeDayFile(t *testing.T, db *sql.DB, dir, date string, cnt int64) DayEntry {
	t.Helper()
	path := filepath.Join(dir, date+".parquet")
	mustExec(t, db, fmt.Sprintf(
		`COPY (SELECT * FROM (VALUES (TIMESTAMPTZ '%s 00:00:00+00', 'eu', %d::BIGINT), (TIMESTAMPTZ '%s 01:00:00+00', 'eu', %d::BIGINT)) t(bucket, region, _cnt)) TO '%s' (FORMAT PARQUET)`,
		date, cnt, date, cnt, path))
	d, ok := parseTS(date)
	if !ok {
		t.Fatalf("bad date %q", date)
	}
	return DayEntry{
		Date:     date,
		URI:      path,
		BucketLo: date + " 00:00:00+00",
		BucketHi: fmtTS(d.Add(2 * time.Hour)), // max bucket (01:00) + 1h grain
		Rows:     2,
	}
}

// routeOnlySpec is the cube under test: hour grain, region dim, count agg.
func routeOnlySpec() CubeSpec {
	return CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Aggs: []Aggregate{{Kind: AggCount}}}
}

const routeOnlySourceGlob = "['s3://arc-test/default/events/**/*.parquet']"

// newRouteOnlyRouter wires a Router over the given manifest with a fake source
// expr (never resolvable locally — proving best-effort never reads source).
func newRouteOnlyRouter(spec CubeSpec, m *Manifest, watermark string) *Router {
	m.CubeID = "default.events." + cubeKind(spec)
	m.Source = spec.Source
	m.Grain = spec.Grain
	m.Dims = spec.Dims
	m.Aggs = spec.Aggs
	m.SchemaHash = spec.SchemaHash()
	return &Router{
		Cubes:      []CubeSpec{spec},
		Manifests:  map[string]*Manifest{cubeKeyOf(spec): m},
		TimeCol:    "time",
		SourceExpr: func(string) string { return routeOnlySourceGlob },
		Watermark:  func(string) string { return watermark },
	}
}

func dayCountSQL(lo, hi string) string {
	return fmt.Sprintf("SELECT date_trunc('day', time) AS b, region, count(*) AS n FROM events "+
		"WHERE time >= TIMESTAMPTZ '%s' AND time < TIMESTAMPTZ '%s' GROUP BY 1,2", lo, hi)
}

// TestRouteOnly_InteriorGap: cube has D1 and D3, D2 is missing. Best-effort
// must SERVE (rows only from D1 and D3); auto must still decline with
// coverage_gap_interior so the api falls it to source.
func TestRouteOnly_InteriorGap(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	dir := t.TempDir()
	spec := routeOnlySpec()

	m := &Manifest{}
	m.Upsert(writeCubeDayFile(t, db, dir, "2026-06-01", 3)) // D1
	// 2026-06-02 (D2) deliberately missing
	m.Upsert(writeCubeDayFile(t, db, dir, "2026-06-03", 5)) // D3
	r := newRouteOnlyRouter(spec, m, "")

	sqlText := dayCountSQL("2026-06-01", "2026-06-04")

	// AUTO pinned unchanged: declines, reason coverage_gap_interior.
	if d := r.Route(sqlText); d.Served || d.Reason != "coverage_gap_interior" {
		t.Fatalf("auto: Served=%v reason=%q, want decline with coverage_gap_interior", d.Served, d.Reason)
	}
	// Explain (hint path) unchanged too: same decline as auto.
	if d := r.Explain(sqlText); d.Served || d.Reason != "coverage_gap_interior" {
		t.Fatalf("explain: Served=%v reason=%q, want decline with coverage_gap_interior", d.Served, d.Reason)
	}

	// BEST-EFFORT serves the existing days; the missing day is a chart gap.
	d := r.RouteOnly(sqlText)
	if !d.Served {
		t.Fatalf("best-effort: not served (reason=%q), want served", d.Reason)
	}
	if strings.Contains(d.SQL, "s3://arc-test/default/events") {
		t.Fatalf("best-effort SQL references SOURCE (must be cube-only):\n%s", d.SQL)
	}
	got := runShape(t, db, d.SQL, 2)
	wantRows := map[string]float64{
		"2026-06-01 00:00:00 +0000 UTC|eu": 6,  // 2 buckets x _cnt 3
		"2026-06-03 00:00:00 +0000 UTC|eu": 10, // 2 buckets x _cnt 5
	}
	if len(got.rows) != len(wantRows) {
		t.Fatalf("got %d rows want %d (rows=%v)\nSQL: %s", len(got.rows), len(wantRows), got.rows, d.SQL)
	}
	for k, want := range wantRows {
		vals, ok := got.rows[k]
		if !ok {
			t.Fatalf("missing row %q in %v", k, got.rows)
		}
		if vals[0] != want {
			t.Errorf("row %q: n=%v want %v", k, vals[0], want)
		}
	}
}

// TestRouteOnly_LeadingGap: range starts before the first cube day. Best-effort
// serves the days that exist; auto declines with coverage_gap.
func TestRouteOnly_LeadingGap(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	dir := t.TempDir()
	spec := routeOnlySpec()

	m := &Manifest{}
	m.Upsert(writeCubeDayFile(t, db, dir, "2026-06-02", 4))
	m.Upsert(writeCubeDayFile(t, db, dir, "2026-06-03", 7))
	r := newRouteOnlyRouter(spec, m, "")

	sqlText := dayCountSQL("2026-06-01", "2026-06-04")

	// AUTO pinned unchanged: leading gap -> decline (silent-undercount guard).
	if d := r.Route(sqlText); d.Served || d.Reason != "coverage_gap" {
		t.Fatalf("auto: Served=%v reason=%q, want decline with coverage_gap", d.Served, d.Reason)
	}

	d := r.RouteOnly(sqlText)
	if !d.Served {
		t.Fatalf("best-effort: not served (reason=%q), want served", d.Reason)
	}
	got := runShape(t, db, d.SQL, 2)
	if len(got.rows) != 2 {
		t.Fatalf("got %d rows want 2 (rows=%v)", len(got.rows), got.rows)
	}
	if v := got.rows["2026-06-02 00:00:00 +0000 UTC|eu"]; len(v) == 0 || v[0] != 8 {
		t.Errorf("06-02 row = %v, want [8]", v)
	}
	if v := got.rows["2026-06-03 00:00:00 +0000 UTC|eu"]; len(v) == 0 || v[0] != 14 {
		t.Errorf("06-03 row = %v, want [14]", v)
	}
}

// TestRouteOnly_RangeAfterNewestDay is the production Jun-10 case: the cube has
// days through 06-09 and a Grafana chunk asks for 06-10 -> 06-11. Best-effort
// must serve a schema-correct ZERO-ROW result (no read_parquet over an empty
// file list, which DuckDB rejects); auto still declines (no_days_in_range).
func TestRouteOnly_RangeAfterNewestDay(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	dir := t.TempDir()
	spec := routeOnlySpec()

	m := &Manifest{}
	m.Upsert(writeCubeDayFile(t, db, dir, "2026-06-08", 2))
	newest := writeCubeDayFile(t, db, dir, "2026-06-09", 2)
	m.Upsert(newest)
	r := newRouteOnlyRouter(spec, m, "")

	sqlText := dayCountSQL("2026-06-10", "2026-06-11")

	// AUTO pinned unchanged: no day overlaps the range -> decline.
	if d := r.Route(sqlText); d.Served || d.Reason != "no_days_in_range" {
		t.Fatalf("auto: Served=%v reason=%q, want decline with no_days_in_range", d.Served, d.Reason)
	}

	d := r.RouteOnly(sqlText)
	if !d.Served {
		t.Fatalf("best-effort: not served (reason=%q), want served zero-row result", d.Reason)
	}
	// The emitted SQL must read a REAL cube file (schema anchor) with an
	// impossible bucket predicate — never an empty read_parquet list.
	if !strings.Contains(d.SQL, newest.URI) {
		t.Fatalf("zero-day emit should anchor on the newest cube file %q:\n%s", newest.URI, d.SQL)
	}
	rows, err := db.Query(d.SQL)
	if err != nil {
		t.Fatalf("zero-day emitted SQL failed: %v\nSQL: %s", err, d.SQL)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	want := []string{"b", "region", "n"}
	if len(cols) != len(want) {
		t.Fatalf("columns = %v, want %v", cols, want)
	}
	for i := range want {
		if cols[i] != want[i] {
			t.Fatalf("columns = %v, want %v", cols, want)
		}
	}
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if n != 0 {
		t.Fatalf("zero-day result returned %d rows, want 0", n)
	}
}

// TestRouteOnly_NoCubeFilesAtAll: a manifest holding only coverage-only
// '-empty' markers (no file ever materialized) has NOTHING to anchor a
// schema-correct read on — best-effort must decline so the api still 422s.
func TestRouteOnly_NoCubeFilesAtAll(t *testing.T) {
	spec := routeOnlySpec()
	m := &Manifest{}
	m.Upsert(DayEntry{Date: "2026-06-01-empty", Covers: []string{"2026-06-01"}})
	r := newRouteOnlyRouter(spec, m, "")

	d := r.RouteOnly(dayCountSQL("2026-06-10", "2026-06-11"))
	if d.Served {
		t.Fatalf("best-effort served from a file-less manifest: %s", d.SQL)
	}
	if d.Reason != "no_manifest" {
		t.Fatalf("reason=%q want no_manifest", d.Reason)
	}
}

// TestRouteOnly_FreshTail: the range straddles the watermark. Best-effort must
// return CUBE-ONLY rows (no source merge branch); auto still merges via
// UNION ALL BY NAME with the source expression.
func TestRouteOnly_FreshTail(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	dir := t.TempDir()
	spec := routeOnlySpec()

	m := &Manifest{}
	m.Upsert(writeCubeDayFile(t, db, dir, "2026-06-01", 3))
	m.Upsert(writeCubeDayFile(t, db, dir, "2026-06-02", 5))
	r := newRouteOnlyRouter(spec, m, "2026-06-02 12:00:00+00") // watermark mid-range

	sqlText := dayCountSQL("2026-06-01", "2026-06-03")

	// AUTO pinned unchanged: straddling the watermark merges cube + source tail.
	da := r.Route(sqlText)
	if !da.Served {
		t.Fatalf("auto: not served (reason=%q), want merge-on-read", da.Reason)
	}
	if !strings.Contains(da.SQL, "UNION ALL BY NAME") || !strings.Contains(da.SQL, "s3://arc-test/default/events") {
		t.Fatalf("auto SQL should merge cube + source tail:\n%s", da.SQL)
	}

	// BEST-EFFORT: plain cube read, no source, no merge.
	d := r.RouteOnly(sqlText)
	if !d.Served {
		t.Fatalf("best-effort: not served (reason=%q), want served", d.Reason)
	}
	if strings.Contains(d.SQL, "UNION ALL BY NAME") || strings.Contains(d.SQL, "s3://arc-test/default/events") {
		t.Fatalf("best-effort SQL must be cube-only (no merge, no source):\n%s", d.SQL)
	}
	got := runShape(t, db, d.SQL, 2)
	if len(got.rows) != 2 {
		t.Fatalf("got %d rows want 2 (rows=%v)", len(got.rows), got.rows)
	}
	if v := got.rows["2026-06-01 00:00:00 +0000 UTC|eu"]; len(v) == 0 || v[0] != 6 {
		t.Errorf("06-01 row = %v, want [6]", v)
	}
	if v := got.rows["2026-06-02 00:00:00 +0000 UTC|eu"]; len(v) == 0 || v[0] != 10 {
		t.Errorf("06-02 row = %v, want [10]", v)
	}
}

// TestRouteOnly_ShapeMismatchStillDeclines: best-effort relaxes COVERAGE only.
// Shape-level declines (uncovered dim, sub-hour grain, parse failure) keep the
// exact same reasons as auto, so the api's 422 path is unchanged for them.
func TestRouteOnly_ShapeMismatchStillDeclines(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	dir := t.TempDir()
	spec := routeOnlySpec()

	m := &Manifest{}
	m.Upsert(writeCubeDayFile(t, db, dir, "2026-06-01", 3))
	r := newRouteOnlyRouter(spec, m, "")

	cases := []struct {
		name, sql, wantReason string
	}{
		{"dim not in any cube",
			"SELECT city, count(*) AS n FROM events WHERE time >= TIMESTAMPTZ '2026-06-01' AND time < TIMESTAMPTZ '2026-06-02' GROUP BY 1",
			"no_covering_cube"},
		// Grafana's $__timeGroup expansion with a 60s bucket — finer than the
		// hourly cube. (A date_trunc('minute',...) is a parse-level decline
		// instead; both end at the api 422 either way.)
		{"sub-hour grain",
			"SELECT to_timestamp((epoch_ns(time) // 1000000000 // 60) * 60) AS b, count(*) AS n FROM events WHERE time >= TIMESTAMPTZ '2026-06-01' AND time < TIMESTAMPTZ '2026-06-02' GROUP BY 1",
			"grain_too_fine"},
		{"parse failure (join)",
			"SELECT count(*) AS n FROM events a JOIN events b ON a.id=b.id WHERE a.time >= TIMESTAMPTZ '2026-06-01'",
			"parse:"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			auto := r.Route(c.sql)
			be := r.RouteOnly(c.sql)
			if auto.Served || be.Served {
				t.Fatalf("shape mismatch must decline in both modes (auto=%v best-effort=%v)", auto.Served, be.Served)
			}
			if !strings.HasPrefix(be.Reason, c.wantReason) {
				t.Errorf("best-effort reason=%q want prefix %q", be.Reason, c.wantReason)
			}
			if be.Reason != auto.Reason {
				t.Errorf("best-effort reason %q diverged from auto reason %q", be.Reason, auto.Reason)
			}
		})
	}
}

// TestRouteOnly_RecordsWorkload: RouteOnly is a real-query path, so it must
// record the parsed shape to the workload exactly like Route (and RouteOnlyHTTP
// must thread headerDB into the recorded source).
func TestRouteOnly_RecordsWorkload(t *testing.T) {
	var sources []string
	r := &Router{TimeCol: "time", OnQuery: func(q QueryShape) { sources = append(sources, q.Source) }}

	r.RouteOnlyHTTP(dayCountSQL("2026-06-01", "2026-06-02"), "posthog")
	if len(sources) != 1 || sources[0] != "posthog.events" {
		t.Fatalf("recorded sources = %v, want [posthog.events]", sources)
	}
}
