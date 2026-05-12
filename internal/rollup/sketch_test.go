package rollup

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"testing"
)

func TestSketchHLL_MergeAcrossRows(t *testing.T) {
	db := openDuckDBWithDataSketches(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE rollup_2rows (sk BLOB)`)
	mustExec(t, db, `INSERT INTO rollup_2rows
		SELECT datasketch_hll(12, i)::BLOB FROM range(1, 1001) tbl(i)`)
	mustExec(t, db, `INSERT INTO rollup_2rows
		SELECT datasketch_hll(12, i)::BLOB FROM range(500, 1501) tbl(i)`)

	var estimate float64
	row := db.QueryRowContext(context.Background(), `
		SELECT datasketch_hll_estimate(datasketch_hll_union(12, sk::sketch_hll))
		FROM rollup_2rows`)
	if err := row.Scan(&estimate); err != nil {
		t.Fatalf("query: %v", err)
	}

	if math.Abs(estimate-1500)/1500 > 0.05 {
		t.Errorf("HLL estimate %f outside 5%% of 1500", estimate)
	}
}

func TestSketchTDigest_MergeAcrossRows(t *testing.T) {
	db := openDuckDBWithDataSketches(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE rollup_2rows (sk BLOB)`)
	mustExec(t, db, `INSERT INTO rollup_2rows
		SELECT datasketch_tdigest(200, i::DOUBLE)::BLOB FROM range(1, 1001) tbl(i)`)
	mustExec(t, db, `INSERT INTO rollup_2rows
		SELECT datasketch_tdigest(200, i::DOUBLE)::BLOB FROM range(1001, 2001) tbl(i)`)

	var p99 float64
	row := db.QueryRowContext(context.Background(), `
		SELECT datasketch_tdigest_quantile(datasketch_tdigest(200, sk::sketch_tdigest_double), 0.99)
		FROM rollup_2rows`)
	if err := row.Scan(&p99); err != nil {
		t.Fatalf("query: %v", err)
	}

	if math.Abs(p99-1980)/1980 > 0.02 {
		t.Errorf("t-digest P99 %f outside 2%% of 1980", p99)
	}
}

func TestMergeSketchExpr_HLL(t *testing.T) {
	got := MergeSketchExpr("user_id__hll", AggHLL, &SketchConfig{HLLLgK: 12})
	want := "datasketch_hll_union(12, user_id__hll::sketch_hll)"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestMergeSketchExpr_TDigest(t *testing.T) {
	got := MergeSketchExpr("latency_ms__tdigest", AggTDigest, &SketchConfig{TDigestK: 200})
	want := "datasketch_tdigest(200, latency_ms__tdigest::sketch_tdigest_double)"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestEstimateSketchExpr_HLL(t *testing.T) {
	inner := MergeSketchExpr("user_id__hll", AggHLL, &SketchConfig{HLLLgK: 12})
	got := EstimateSketchExpr(inner, AggHLL, 0)
	want := "datasketch_hll_estimate(datasketch_hll_union(12, user_id__hll::sketch_hll))"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestEstimateSketchExpr_TDigestQuantile(t *testing.T) {
	inner := MergeSketchExpr("latency_ms__tdigest", AggTDigest, &SketchConfig{TDigestK: 200})
	got := EstimateSketchExpr(inner, AggTDigest, 0.99)
	want := "datasketch_tdigest_quantile(datasketch_tdigest(200, latency_ms__tdigest::sketch_tdigest_double), 0.99)"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func mustExec(t *testing.T, db *sql.DB, sqlstr string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), sqlstr); err != nil {
		t.Fatalf("exec %q: %v", sqlstr, err)
	}
}

var _ = fmt.Sprintf
