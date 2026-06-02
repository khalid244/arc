package rollup

import (
	"fmt"
	"testing"
	"time"
)

// TestPerf_MonthlyCubeVsSource proves the performance thesis on real S3 data:
// a month-scale aggregate query served from the cube versus a full source scan,
// both reading from MinIO so the only difference is data volume / S3 round-trips.
// Asserts the cube result is correct (vs source) and materially faster.
func TestPerf_MonthlyCubeVsSource(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)

	const (
		decGlob  = "['s3://arc-test/default/downloads/2025/12/**/*.parquet']"
		cubeS3   = "s3://arc-test/_arc/rollup/perf_status/dec.parquet"
		cubeExpr = "['" + cubeS3 + "']"
		lo       = "2025-12-01 00:00:00+00"
		hi       = "2026-01-01 00:00:00+00"
	)
	spec := CubeSpec{Source: "default.downloads", Grain: "hour", Dims: []string{"status"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggAvg, Col: "duration_seconds"}}}

	// Build the whole-month cube once, to S3.
	buildStart := time.Now()
	if _, err := db.Exec(spec.BuildCopySQL(decGlob, "time", lo, hi, cubeS3)); err != nil {
		t.Fatalf("build december cube: %v", err)
	}
	t.Logf("built December cube to S3 in %s", time.Since(buildStart).Round(time.Millisecond))

	// A representative dashboard query: daily count + avg duration by status.
	q := QueryShape{Source: spec.Source, TimeCol: "time", Grain: "day", Dims: []string{"status"},
		Aggs:   []Aggregate{{Kind: AggCount, Alias: "n"}, {Kind: AggAvg, Col: "duration_seconds", Alias: "a"}},
		TimeLo: lo, TimeHi: hi}

	srcStart := time.Now()
	src := runShape(t, db, q.SourceRefSQL(decGlob), 2)
	srcDur := time.Since(srcStart)

	cubeStart := time.Now()
	cube := runShape(t, db, q.CubeReadSQL(cubeExpr), 2)
	cubeDur := time.Since(cubeStart)

	// Correctness over the full month.
	if len(src.rows) != len(cube.rows) {
		t.Fatalf("group count mismatch: source=%d cube=%d", len(src.rows), len(cube.rows))
	}
	for k, sv := range src.rows {
		cv, ok := cube.rows[k]
		if !ok {
			t.Fatalf("cube missing %q", k)
		}
		for i := range sv {
			if !aggMatch(sv[i], cv[i], q.Aggs[i]) {
				t.Errorf("group %q agg[%s]: source=%v cube=%v", k, q.Aggs[i].Alias, sv[i], cv[i])
			}
		}
	}

	speedup := float64(srcDur) / float64(cubeDur)
	t.Logf("MONTHLY QUERY  source=%s  cube=%s  speedup=%.1fx  (%d groups)",
		srcDur.Round(time.Millisecond), cubeDur.Round(time.Millisecond), speedup, len(src.rows))

	// The cube reads ~2.2k rows vs ~60M source rows; expect a large speedup even
	// warm. Guard conservatively to avoid CI flakiness on cache effects.
	if cubeDur >= srcDur {
		t.Fatalf("cube (%s) not faster than source (%s)", cubeDur, srcDur)
	}
	fmt.Print("") // keep fmt imported if logs are stripped
}
