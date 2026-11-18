package rollup

import "testing"

// A grand-total COUNT over a filter no row matches is 0 at source; the cube read
// summed zero cube rows and returned NULL, so a stat panel showed "No data"
// instead of 0 (e.g. "Android fatal crashes" on a day without one).
func TestGrandTotalCountOverNoRowsIsZero(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region", "tag"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggCountCol, Col: "bytes"}, {Kind: AggCountDistinct, Col: "bytes"}}}
	tf := "time >= '2026-06-01T00:00:00Z' AND time < '2026-06-03T00:00:00Z'"
	cases := map[string]string{
		"count(*)":        "SELECT count(*) AS n FROM events WHERE " + tf + " AND region = 'nowhere'",
		"count(col)":      "SELECT count(bytes) AS n FROM events WHERE " + tf + " AND region = 'nowhere'",
		"count(CASE)":     "SELECT count(CASE WHEN tag = 'x' THEN 1 END) AS n FROM events WHERE " + tf + " AND region = 'nowhere'",
		"count(DISTINCT)": "SELECT count(DISTINCT bytes) AS n FROM events WHERE " + tf + " AND region = 'nowhere'",
	}
	for wmName, wm := range map[string]string{"sealed": "", "merge": "2026-06-02 12:00:00+00"} {
		f := newQRFixture(t, spec, wm)
		for name, q := range cases {
			t.Run(wmName+"/"+name, func(t *testing.T) {
				if d := f.requireSameAsRaw(t, q); !d.Served {
					t.Fatalf("want served from the cube, declined: %s", d.Reason)
				}
			})
		}
	}
}
