package tiered

import (
	"context"
	"fmt"
	"testing"
)

func TestClassify_SingleScanGroupingSets(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMPTZ, dim_a VARCHAR, dim_b VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO evt SELECT '2026-05-10 00:00:00+00'::TIMESTAMPTZ, 'a' || (i % 3), 'b' || (i % 5) FROM range(100) t(i)`); err != nil {
		t.Fatal(err)
	}

	spec, err := Classify(ctx, db, ClassifyOpts{
		Source:            "SELECT * FROM evt",
		TimeColumn:        "time",
		DimColumns:        []string{"dim_a", "dim_b"},
		CoverageThreshold: 0.99,
		DimRichCap:        100,
		Table:             "test",
		TZ:                "UTC",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Dims) != 2 {
		t.Errorf("expected 2 dims, got %d", len(spec.Dims))
	}
	if got := len(spec.Dims["dim_a"].KeptValues); got != 3 {
		t.Errorf("dim_a kept = %d, want 3", got)
	}
	if got := len(spec.Dims["dim_b"].KeptValues); got != 5 {
		t.Errorf("dim_b kept = %d, want 5", got)
	}
}

func TestClassify_FrequencyClassifier(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Synthetic table: 100 rows, site dim where top 3 cover 99 rows, 1 tail value.
	if _, err := db.Exec(`CREATE TABLE evt (time TIMESTAMP, site VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	stmts := []string{
		"INSERT INTO evt SELECT '2026-05-01 00:00:00'::TIMESTAMP, 'a' FROM range(60)",
		"INSERT INTO evt SELECT '2026-05-01 00:00:00'::TIMESTAMP, 'b' FROM range(30)",
		"INSERT INTO evt SELECT '2026-05-01 00:00:00'::TIMESTAMP, 'c' FROM range(9)",
		"INSERT INTO evt SELECT '2026-05-01 00:00:00'::TIMESTAMP, 'tail'",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}

	source := "SELECT * FROM evt"
	spec, err := Classify(ctx, db, ClassifyOpts{
		Source:            source,
		TimeColumn:        "time",
		DimColumns:        []string{"site"},
		CoverageThreshold: 0.99,
		DimRichCap:        100,
		Table:             "main.evt",
		TZ:                "UTC",
	})
	if err != nil {
		t.Fatal(err)
	}
	dim := spec.Dims["site"]
	if dim.Role != "Dim" {
		t.Errorf("site.Role = %q, want Dim", dim.Role)
	}
	// With low-cardinality keep-all (≤ keepAllUnderDistinct), all 4 values
	// are retained — the coverage-threshold cut applies only when distinct
	// count exceeds the keep-all cap.
	if len(dim.KeptValues) != 4 {
		t.Errorf("kept values = %d, want 4 (a, b, c, tail); got %v", len(dim.KeptValues), dim.KeptValues)
	}
}

func TestComputeKeptValues_KeepsAllWhenLowCardinality(t *testing.T) {
	// 3 distinct values, top one covers 99.6% — without the keep-all rule
	// the 0.995 threshold would drop the other two. With the rule, all kept.
	freqs := []dimFreq{
		{Val: "true", N: 996},
		{Val: "false", N: 3},
		{Val: "off", N: 1},
	}
	got := computeKeptValues(freqs, 0.995)
	if len(got) != 3 {
		t.Errorf("got %d kept values, want 3 (all of them): %v", len(got), got)
	}
}

func TestComputeKeptValues_AppliesCoverageWhenHighCardinality(t *testing.T) {
	// 30 distinct values (> keepAllUnderDistinct=20). Coverage threshold applies.
	freqs := make([]dimFreq, 30)
	for i := range freqs {
		freqs[i] = dimFreq{Val: fmt.Sprintf("v%d", i), N: 1}
	}
	freqs[0].N = 990 // top value covers 99%, rest tiny
	got := computeKeptValues(freqs, 0.99)
	if len(got) >= 30 {
		t.Errorf("expected coverage to cut tail, got %d", len(got))
	}
}
