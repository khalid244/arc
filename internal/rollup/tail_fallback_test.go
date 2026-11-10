package rollup

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// RouteTail plans the cube-only answer of an auto-mode merge (review
// 2026-11-10, suggestion 9): the same query without its tail branch, and the
// window only the tail reads.

const tailSel = "SELECT date_trunc('hour', time) AS h, region, count(*) AS n FROM events WHERE %s GROUP BY 1, 2"

func tailFixture(t *testing.T) *qrFixture {
	t.Helper()
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount}}}
	// The cube covers both days; the watermark puts [2026-06-02 12:00, ...) in the tail.
	return newQRFixture(t, spec, "2026-06-02 12:00:00+00")
}

func sameRows(t *testing.T, f *qrFixture, got, want string) {
	t.Helper()
	g, err := qrQuery(f.exec, got)
	if err != nil {
		t.Fatalf("cube-only SQL failed: %v\nSQL: %s", err, got)
	}
	w := f.rawResult(t, want)
	if strings.Join(g.rows, "\n") != strings.Join(w.rows, "\n") {
		t.Fatalf("cube-only answer\n got  %v\n want %v\nSQL: %s", g.rows, w.rows, got)
	}
}

func TestRouteTailPlansCubeOnlyAnswer(t *testing.T) {
	f := tailFixture(t)
	tailLo := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name, lo, hi string
	}{
		{"aligned", "2026-06-01 00:00:00+00", "2026-06-03 00:00:00+00"},
		{"head patch", "2026-06-01 00:30:00+00", "2026-06-03 00:00:00+00"},
		{"partial last bucket", "2026-06-01 00:00:00+00", "2026-06-02 20:15:00+00"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := sprintfWhere(tailSel, c.lo, c.hi)
			d := f.r.RouteTail(q, "")
			if !d.Served || d.SQL != f.r.Route(q).SQL {
				t.Fatalf("RouteTail must serve exactly Route's SQL: served=%v reason=%q", d.Served, d.Reason)
			}
			if d.Tail == nil {
				t.Fatal("a merge with a tail branch must carry a tail plan")
			}
			hi, _ := parseTS(c.hi)
			if !d.Tail.Lo.Equal(tailLo) || !d.Tail.Hi.Equal(hi) {
				t.Fatalf("tail window = [%v, %v), want [%v, %v)", d.Tail.Lo, d.Tail.Hi, tailLo, hi)
			}
			if strings.Contains(d.Tail.CubeOnlySQL, `"time" >= TIMESTAMPTZ '2026-06-02 12:00:00+00'`) {
				t.Fatalf("the cube-only answer must not read the tail's source:\n%s", d.Tail.CubeOnlySQL)
			}
			sameRows(t, f, d.Tail.CubeOnlySQL, sprintfWhere(tailSel, c.lo, "2026-06-02 12:00:00+00"))
		})
	}

	// A sealed window has no tail: nothing to degrade.
	if d := f.r.RouteTail(sprintfWhere(tailSel, "2026-06-01 00:00:00+00", "2026-06-02 00:00:00+00"), ""); !d.Served || d.Tail != nil {
		t.Fatalf("sealed window: served=%v tail=%+v, want served without a tail plan", d.Served, d.Tail)
	}

	// The CTE-base rewrite splices the cube-only answer the same way.
	base := sprintfWhere(tailSel, "2026-06-01 00:00:00+00", "2026-06-03 00:00:00+00")
	cte := "WITH base AS (" + base + ") SELECT region, sum(n) AS n FROM base GROUP BY 1"
	d := f.r.RouteTail(cte, "")
	if !d.Served || d.Tail == nil || !strings.HasPrefix(d.Tail.CubeOnlySQL, "WITH base AS (") {
		t.Fatalf("CTE: served=%v tail=%+v", d.Served, d.Tail)
	}
	sameRows(t, f, d.Tail.CubeOnlySQL, "WITH base AS ("+sprintfWhere(tailSel, "2026-06-01 00:00:00+00", "2026-06-02 12:00:00+00")+") SELECT region, sum(n) AS n FROM base GROUP BY 1")

	// Route itself is unchanged: no tail plan.
	if d := f.r.Route(base); d.Tail != nil {
		t.Fatal("Route must not plan a tail")
	}
}

// A tail whose existence listing failed is declined as before (the source
// path then answers 503), but a tail-fallback request gets the cube-only
// answer instead. A failed head listing leaves no cube-only answer: it reads
// the head.
func TestRouteTailListingFailure(t *testing.T) {
	f := tailFixture(t)
	failing := ""
	f.r.SourceWindow = func(_, lo, _ string) string {
		if lo == failing {
			return sourceListingFailed
		}
		return f.srcGlob
	}
	q := sprintfWhere(tailSel, "2026-06-01 00:30:00+00", "2026-06-03 00:00:00+00")

	failing = "2026-06-02 12:00:00+00" // the tail
	d := f.r.RouteTail(q, "")
	if d.Served || d.Reason != "source_listing_failed" {
		t.Fatalf("served=%v reason=%q, want declined source_listing_failed", d.Served, d.Reason)
	}
	if d.Tail == nil || d.Cube == "" {
		t.Fatalf("a failed tail listing must leave a cube-only answer and its cube: tail=%+v cube=%q", d.Tail, d.Cube)
	}
	sameRows(t, f, d.Tail.CubeOnlySQL, sprintfWhere(tailSel, "2026-06-01 00:30:00+00", "2026-06-02 12:00:00+00"))
	if d := f.r.Route(q); d.Served || d.Reason != "source_listing_failed" || d.Cube != "" || d.Tail != nil {
		t.Fatalf("Route is unchanged: %+v", d)
	}

	failing = "2026-06-01 00:30:00+00" // the head
	if d := f.r.RouteTail(q, ""); d.Served || d.Tail != nil {
		t.Fatalf("a failed head listing: served=%v tail=%+v, want declined without a cube-only answer", d.Served, d.Tail)
	}
}

// Unreadable compaction manifests: the cube-only answer is offered only when
// it reads no source (no head patch), since a head read could double count.
func TestRouteTailUnreadableManifests(t *testing.T) {
	f := tailFixture(t)
	f.r.SourceExclude = func(string) ([]string, error) { return nil, errors.New("SlowDown") }

	aligned := sprintfWhere(tailSel, "2026-06-01 00:00:00+00", "2026-06-03 00:00:00+00")
	d := f.r.RouteTail(aligned, "")
	if d.Served || d.Reason != "compaction_manifests_unreadable" || d.Tail == nil {
		t.Fatalf("no head: served=%v reason=%q tail=%+v, want declined with a cube-only answer", d.Served, d.Reason, d.Tail)
	}
	sameRows(t, f, d.Tail.CubeOnlySQL, sprintfWhere(tailSel, "2026-06-01 00:00:00+00", "2026-06-02 12:00:00+00"))

	withHead := sprintfWhere(tailSel, "2026-06-01 00:30:00+00", "2026-06-03 00:00:00+00")
	if d := f.r.RouteTail(withHead, ""); d.Served || d.Tail != nil {
		t.Fatalf("with a head patch: served=%v tail=%+v, want declined without a cube-only answer", d.Served, d.Tail)
	}
}
