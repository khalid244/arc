package rollup

import "testing"

// TestTheta_SketchSQL pins the Theta sketch expressions for COUNT(DISTINCT): a
// theta store column built with datasketch_theta, merged via the same build
// aggregate (the KLL pattern), and estimated with datasketch_theta_estimate.
// Theta replaces HLL to keep mergeability while unlocking set algebra
// (intersect / a_not_b) across cubes.
func TestTheta_SketchSQL(t *testing.T) {
	a := Aggregate{Kind: AggCountDistinct, Col: "device_id", Alias: "uniq"}

	sc := a.storeCols()
	if len(sc) != 1 || sc[0][0] != "_theta_device_id" {
		t.Fatalf("storeCols = %v, want [_theta_device_id ...]", sc)
	}
	if sc[0][1] != `datasketch_theta(14, "device_id")` {
		t.Errorf("build expr = %q", sc[0][1])
	}
	if got := mergeExpr("_theta_device_id"); got != "datasketch_theta(14, _theta_device_id::sketch_theta)" {
		t.Errorf("mergeExpr = %q", got)
	}
	if got := a.finalExpr(); got != "datasketch_theta_estimate(datasketch_theta(14, _theta_device_id::sketch_theta))" {
		t.Errorf("finalExpr = %q", got)
	}
	// Still flagged approximate so the compare harness applies a tolerance.
	if !a.Approximate() {
		t.Error("AggCountDistinct must report Approximate()")
	}
	// The cube must store the theta column for the distinct agg to be derivable.
	cube := CubeSpec{Source: "default.downloads", Grain: "hour", Aggs: []Aggregate{a}}
	if !cube.aggDerivable(a) {
		t.Error("cube storing _theta_device_id should derive COUNT(DISTINCT device_id)")
	}
}
