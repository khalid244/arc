package rollup

import (
	"fmt"
	"math"
	"strconv"
	"testing"
)

// approxSourceBody is a source day with 200,000 rows and ~95k distinct uids per
// day (overlapping across days), so a cube read's union of hourly Theta sketches
// (lgK=14, 16,384 entries) runs in ESTIMATION mode: COUNT(DISTINCT) from the cube
// is approximate, unlike the small QR fixture where every sketch is exact.
func approxSourceBody(day string) string {
	return fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 00:00:00+00' + to_seconds(i %% 86400) AS "time",
		['eu','us','ap'][1 + i %% 3] AS region,
		['1.1','1.10','2.0.1','7','007','7.0'][1 + (i // 3) %% 6] AS app_version,
		CASE WHEN i %% 5 = 0 THEN NULL ELSE ['x','y'][1 + i %% 2] END AS tag,
		(hash(i, '%s') %% 150000)::BIGINT AS uid
		FROM range(0, 200000) t(i)`, day, day)
}

// requireCloseToRaw is requireSameAsRaw with the cells of approxCols compared
// within a relative tolerance; every other cell, the column names and types, and
// the row order (when the query orders) must match exactly. It returns the
// largest relative error seen in approxCols.
func (f *qrFixture) requireCloseToRaw(t *testing.T, q string, approxCols map[string]bool, tol float64) (Decision, float64) {
	t.Helper()
	want := f.rawResult(t, q)
	got, d, err := f.served(t, q)
	if err != nil {
		t.Fatalf("served query failed: %v\nSQL: %s", err, d.SQL)
	}
	if fmt.Sprint(got.cols) != fmt.Sprint(want.cols) || !sameTypes(got.types, want.types, intSumColumns(q)) {
		t.Fatalf("columns/types differ\n got  %q %q\n want %q %q\nSQL: %s", got.cols, got.types, want.cols, want.types, d.SQL)
	}
	if !queryOrders(q) {
		t.Fatalf("test query must ORDER BY its keys: %s", q)
	}
	if len(got.cells) != len(want.cells) {
		t.Fatalf("row count %d, want %d\nSQL: %s", len(got.cells), len(want.cells), d.SQL)
	}
	worst := 0.0
	for r := range want.cells {
		for c, col := range want.cols {
			g, w := got.cells[r][c], want.cells[r][c]
			if !approxCols[col] {
				if g != w {
					t.Fatalf("row %d col %s: got %s, want %s (exact)\nSQL: %s", r, col, g, w, d.SQL)
				}
				continue
			}
			gv, err1 := strconv.ParseFloat(g, 64)
			wv, err2 := strconv.ParseFloat(w, 64)
			if err1 != nil || err2 != nil {
				t.Fatalf("row %d col %s: non-numeric %q / %q", r, col, g, w)
			}
			rel := math.Abs(gv-wv) / math.Max(math.Abs(wv), 1)
			worst = math.Max(worst, rel)
			if rel > tol {
				t.Fatalf("row %d col %s: got %v, want %v (rel err %.4f > %.4f)\nSQL: %s", r, col, gv, wv, rel, tol, d.SQL)
			}
		}
	}
	return d, worst
}

func TestApproxSketch_DistinctWithinTolerance(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"app_version", "region", "tag"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggCountDistinct, Col: "uid"}}}
	tf := "time >= '2026-06-01T00:00:00Z' AND time < '2026-06-03T00:00:00Z'"
	cases := map[string]struct {
		sql    string
		approx map[string]bool
	}{
		"distinct + count by dim": {"SELECT region, count(DISTINCT uid) AS u, count(*) AS n FROM events WHERE " + tf + " GROUP BY 1 ORDER BY 1",
			map[string]bool{"u": true}},
		"conditional distinct": {"SELECT region, count(DISTINCT CASE WHEN tag = 'x' AND app_version <> '7' THEN uid END) AS ux, count(DISTINCT uid) AS u FROM events WHERE " + tf + " GROUP BY 1 ORDER BY 1",
			map[string]bool{"ux": true, "u": true}},
		"computed key + expression filter": {"SELECT CASE WHEN app_version ILIKE '1%' THEN 'one' ELSE 'other' END AS line, count(DISTINCT uid) AS u, count(*) AS n FROM events WHERE " + tf +
			" AND (tag IS NULL OR region = 'eu') GROUP BY 1 ORDER BY 1", map[string]bool{"u": true}},
		"hourly distinct": {"SELECT date_trunc('hour', time) AS t, count(DISTINCT uid) AS u FROM events WHERE " + tf + " AND region = 'us' GROUP BY 1 ORDER BY 1",
			map[string]bool{"u": true}},
		"grand total": {"SELECT count(DISTINCT uid) AS u FROM events WHERE " + tf + " ORDER BY 1", map[string]bool{"u": true}},
	}
	for wmName, wm := range map[string]string{"sealed": "", "merge": "2026-06-02 12:00:00+00"} {
		f := newQRFixtureBody(t, spec, wm, approxSourceBody)
		f.r.DimExprs = true
		worst := 0.0
		for name, c := range cases {
			t.Run(wmName+"/"+name, func(t *testing.T) {
				d, w := f.requireCloseToRaw(t, c.sql, c.approx, 0.03)
				if !d.Served {
					t.Fatalf("want served from the cube, declined: %s", d.Reason)
				}
				worst = math.Max(worst, w)
			})
		}
		// The fixture must actually exercise approximate sketches.
		if worst == 0 {
			t.Errorf("%s: every distinct count was exact; the sketches never left exact mode", wmName)
		}
		t.Logf("%s: worst relative error of a distinct count %.4f", wmName, worst)
	}
}
