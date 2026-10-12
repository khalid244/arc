package rollup

import (
	"fmt"
	"testing"
)

// Merge-on-read time-bound fidelity. Cube rows are whole grain buckets, so a
// query bound that is not bucket-aligned must have its partial bucket read from
// source; and a source branch must never read outside [lo, hi).

// A window entirely below the watermark with unaligned bounds (Grafana time
// shift / absolute / zoomed ranges, millisecond bounds): the partial first and
// last buckets come from source, not whole cube buckets.
func TestMergeRead_UnalignedSealedBounds(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Col: "bytes"}}}
	f := newQRFixture(t, spec, "2026-06-03 00:00:00+00") // both days sealed
	for name, w := range map[string][2]string{
		"both unaligned":   {"2026-06-01 00:30:00+00", "2026-06-02 10:15:00+00"},
		"ms hi":            {"2026-06-01 00:00:00+00", "2026-06-02 10:15:00.5+00"},
		"unaligned lo":     {"2026-06-01 03:20:00+00", "2026-06-02 00:00:00+00"},
		"inside one hour":  {"2026-06-01 10:15:00+00", "2026-06-01 10:45:00+00"},
		"aligned (cube)":   {"2026-06-01 02:00:00+00", "2026-06-02 05:00:00+00"},
		"hour-bucketed ms": {"2026-06-01 00:00:00.25+00", "2026-06-01 06:59:59.75+00"},
	} {
		for _, sel := range []string{
			"SELECT region, count(*) AS n, sum(bytes) AS b FROM events WHERE %s GROUP BY 1",
			"SELECT date_trunc('hour', time) AS h, region, count(*) AS n FROM events WHERE %s GROUP BY 1, 2",
		} {
			q := sprintfWhere(sel, w[0], w[1])
			t.Run(name, func(t *testing.T) {
				if d := f.requireSameAsRaw(t, q); !d.Served {
					t.Fatalf("want served, declined: %s", d.Reason)
				}
			})
		}
	}
}

// A window that starts after the watermark's bucket (seal clock behind the
// cube's coverage): the tail must start at lo, and the head must not run past hi.
func TestMergeRead_WindowAfterWatermark(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount}}}
	f := newQRFixture(t, spec, "2026-06-02 06:00:00+00")
	for name, w := range map[string][2]string{
		"inside one hour":   {"2026-06-02 10:15:00+00", "2026-06-02 10:45:00+00"},
		"after watermark":   {"2026-06-02 08:30:00+00", "2026-06-02 20:45:00+00"},
		"aligned, after wm": {"2026-06-02 08:00:00+00", "2026-06-02 20:00:00+00"},
	} {
		q := sprintfWhere("SELECT region, count(*) AS n FROM events WHERE %s GROUP BY 1", w[0], w[1])
		t.Run(name, func(t *testing.T) {
			if d := f.requireSameAsRaw(t, q); !d.Served {
				t.Fatalf("want served, declined: %s", d.Reason)
			}
		})
	}
}

func sprintfWhere(sel, lo, hi string) string {
	return fmt.Sprintf(sel, "time >= TIMESTAMPTZ '"+lo+"' AND time < TIMESTAMPTZ '"+hi+"'")
}
