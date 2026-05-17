package tiered

import (
	"context"
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
	if len(dim.KeptValues) != 3 {
		t.Errorf("kept values = %d, want 3 (a, b, c); got %v", len(dim.KeptValues), dim.KeptValues)
	}
}
