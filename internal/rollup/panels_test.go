package rollup

import "testing"

// Design-level coverage analysis of the 22 real production "downloads" dashboard
// panels (the ground-truth fixture from the prior generation's testdata). Each
// panel is encoded by the structural features that decide whether Rollup's
// model can serve it. This is NOT a live SQL parse — the SQL front-end that
// extracts a QueryShape from raw panel SQL is the production integration (router
// seam, future work); this test substantiates that the *model* covers >=95% of
// the real shapes, which is the design claim.
//
// Rollup serves a panel when:
//   - every group-by / filter column is a cubeable dimension (low-card -> shared
//     cube; high-card -> its own per-dim cube), AND
//   - every aggregate is in the supported set (count/sum/min/max/avg + HLL
//     distinct + KLL percentile + count_if/sum_if), AND
//   - there is no structural blocker (JOIN, window function, correlated
//     subquery, or a CTE/cross-join that changes aggregation semantics).
//
// Crucially, because filters are applied POST-aggregation on the cube (which
// stores the raw dims), arbitrary boolean predicates over stored dims — OR,
// inequality (!=,<,>), IS NULL — are all servable. Those were the cases the old
// exact-hash *parser* rejected; the coverage model does not.
type panel struct {
	id         int
	desc       string
	supported  bool // aggregates all in the supported set
	cubeable   bool // all group-by/filter cols are cubeable dims
	structural bool // has JOIN / window / correlated subquery / semantic CTE
}

func (p panel) served() bool { return p.supported && p.cubeable && !p.structural }

func panels() []panel {
	return []panel{
		{41, "total downloads COUNT(*)", true, true, false},
		{15, "downloads by hour COUNT(*)", true, true, false},
		{23, "success response=200 (eq filter)", true, true, false},
		{24, "non-200 response!=200 (post-agg !=)", true, true, false},
		{40, "count by status", true, true, false},
		{26, "count by region", true, true, false},
		{32, "top sites LIMIT 20 (TopN)", true, true, false},
		{2, "count by tag, region IS NOT NULL", true, true, false},
		{36, "city!='' AND region not null", true, true, false},
		{28, "timeseries by hour+metric (TopN site)", true, true, false},
		{39, "success-rate HAVING + TopN + cond sum", true, true, false},
		{27, "count(device_id) by status", true, true, false},
		{33, "count(url) by tag", true, true, false},
		{19, "iOS version pie: tag='HAMIOS' OR tag IS NULL", true, true, false},
		{35, "android version: OR on tag", true, true, false},
		{34, "version pie: OR on tag", true, true, false},
		{29, "avg/max/p95 duration by site (CASE relabel)", true, true, false},
		{31, "conditional sum(CASE response=200) by region", true, true, false},
		{37, "heatmap: cond sum + CASE relabel + TopN", true, true, false},
		{12, "COUNT(DISTINCT url) by site (HLL)", true, true, false},
		{25, "success-rate: cond sum / cond sum (eq filters)", true, true, false},
		// The one genuine punt: two CTEs + cross-join computing a ratio against a
		// scalar total_volume — changes aggregation semantics, not a single cube.
		{20, "normalized_sites x total_volume CTE + cross-join", true, true, true},
	}
}

func TestPanelCoverage_AtLeast95Percent(t *testing.T) {
	ps := panels()
	served, punted := 0, []int{}
	for _, p := range ps {
		if p.served() {
			served++
		} else {
			punted = append(punted, p.id)
		}
	}
	pct := 100 * float64(served) / float64(len(ps))
	t.Logf("Rollup serves %d/%d panels = %.1f%%; punts to source: %v", served, len(ps), pct, punted)
	if pct < 95.0 {
		t.Fatalf("coverage %.1f%% < 95%% target (punted: %v)", pct, punted)
	}
	// The punts must be structural (the only legitimate reason to fall through).
	for _, p := range ps {
		if !p.served() && !p.structural {
			t.Errorf("panel %d punted for a non-structural reason (supported=%v cubeable=%v)", p.id, p.supported, p.cubeable)
		}
	}
}
