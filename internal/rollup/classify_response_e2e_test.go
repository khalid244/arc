package rollup

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestRouter_SiteResponseTargetedCube is the full local e2e for the downloads
// success-rate fix, on the real corpus in MinIO:
//   1. response (a DOUBLE) is now a dimension, so the targeted [[rollup.cube]]
//      {site,response} VALIDATES (it was rejected before the classifier fix).
//   2. a by_response_site cube builds from real DOUBLE-response source data.
//   3. the success-rate panel's servable base (per-site 200 vs non-200 counts)
//      ROUTES to that cube and reproduces the source numbers exactly.
func TestRouter_SiteResponseTargetedCube(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)

	readExpr := fmt.Sprintf("read_parquet('%s', union_by_name=true)", testDayGlob)
	cfg := ClassifyConfig{MaxDimCard: 8192, MaxPerDimCard: 50000} // prod values
	p, err := ProfileTable(db, "default.downloads", "time", "hour", readExpr, cfg)
	if err != nil {
		t.Fatalf("profile: %v", err)
	}

	// (1) The targeted cube must now validate — response is a dim post-fix.
	spec, ok := p.targetedSpec([]string{"site", "response"}, nil)
	if !ok {
		t.Fatalf("targetedSpec({site,response}) was REJECTED — response not a dimension; DimCard=%v", p.DimCard)
	}
	t.Logf("targeted cube validated: dims=%v", spec.Dims)

	// (2) Build 3 daily cube files + manifest from the real corpus.
	dir := t.TempDir()
	m := &Manifest{CubeID: "downloads_response_site", Source: spec.Source, Grain: spec.Grain, Dims: spec.Dims, SchemaHash: spec.SchemaHash()}
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
	r := &Router{
		Cubes:      []CubeSpec{spec},
		Manifests:  map[string]*Manifest{cubeKeyOf(spec): m},
		TimeCol:    "time",
		SourceExpr: func(string) string { return srcGlob },
		Watermark:  func(string) string { return "" }, // fully sealed
	}

	// (3) The success-rate panel's servable base: per-site 200 vs non-200 counts.
	const win = "WHERE time >= TIMESTAMPTZ '2025-12-27' AND time < TIMESTAMPTZ '2025-12-29'"
	sql := "SELECT site, " +
		"SUM(CASE WHEN response = 200 THEN 1 ELSE 0 END) AS ok, " +
		"SUM(CASE WHEN response != 200 THEN 1 ELSE 0 END) AS notok " +
		"FROM downloads " + win + " GROUP BY site"

	d := r.Route(sql)
	if !d.Served {
		t.Fatalf("expected the success-rate base to roll up; it fell to source: %s", d.Reason)
	}
	t.Logf("served by cube=%s", d.Cube)

	want := rawOnSource(t, db, sql, srcGlob, 1)
	got := runShape(t, db, d.SQL, 1)
	if len(want.rows) != len(got.rows) {
		t.Fatalf("group count mismatch: source=%d cube=%d\nrewritten: %s", len(want.rows), len(got.rows), d.SQL)
	}
	shape, _, _ := Parse(sql, "time")
	for k, sv := range want.rows {
		cv, ok := got.rows[k]
		if !ok {
			t.Fatalf("cube missing site group %q", k)
		}
		for i := range sv {
			if !aggMatch(sv[i], cv[i], shape.Aggs[i]) {
				t.Errorf("site %q agg[%d]: source=%v cube=%v", k, i, sv[i], cv[i])
			}
		}
	}
	t.Logf("OK: %d site groups, 200/non-200 counts matched source exactly via %s", len(want.rows), d.Cube)
}
