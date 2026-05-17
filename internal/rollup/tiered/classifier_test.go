package tiered

import (
	"context"
	"testing"
)

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
