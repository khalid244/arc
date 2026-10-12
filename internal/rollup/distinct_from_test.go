package rollup

import "testing"

// IS [NOT] DISTINCT FROM is NULL-safe: `tag IS DISTINCT FROM 'x'` keeps rows
// where tag is NULL, which `tag <> 'x'` drops. The filter must be re-applied
// with the same operator on cube rows.
func TestFilter_DistinctFromKeepsNullSemantics(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region", "tag"}, Aggs: []Aggregate{{Kind: AggCount}}}
	for name, wm := range map[string]string{"sealed": "", "merge": "2026-06-02 12:00:00+00"} {
		f := newQRFixture(t, spec, wm)
		for _, pred := range []string{
			`tag IS DISTINCT FROM 'x'`,
			`tag IS NOT DISTINCT FROM 'y'`,
			`tag IS NOT DISTINCT FROM NULL`,
		} {
			q := "SELECT region, count(*) AS n FROM events WHERE time >= TIMESTAMPTZ '2026-06-01 00:00:00+00' AND time < TIMESTAMPTZ '2026-06-03 00:00:00+00' AND " +
				pred + " GROUP BY 1"
			t.Run(name+"/"+pred, func(t *testing.T) {
				if d := f.requireSameAsRaw(t, q); !d.Served {
					t.Fatalf("want served from the cube, declined: %s", d.Reason)
				}
			})
		}
	}
}
