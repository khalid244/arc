package precalc

import (
	"context"
	"os"
	"testing"
)

const testDataGlob = "/tmp/arc-bench-downloads/2026/05/**/*.parquet"

func skipIfNoTestData(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/tmp/arc-bench-downloads"); os.IsNotExist(err) {
		t.Skip("/tmp/arc-bench-downloads not present; skipping integration test")
	}
}

func TestClassify_RealDownloads(t *testing.T) {
	skipIfNoTestData(t)
	ctx := context.Background()
	db, err := OpenWithDataSketches("Asia/Riyadh")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	source := "SELECT * FROM read_parquet('" + testDataGlob + "')"
	spec, err := Classify(ctx, db, ClassifyOpts{
		Source:            source,
		TimeColumn:        "time",
		DimColumns:        []string{"site", "country", "os", "vpn", "status", "tag", "app_version", "os_version"},
		CoverageThreshold: 0.99,
		DimRichCap:        100,
		Table:             "default.downloads",
		TZ:                "Asia/Riyadh",
	})
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]struct {
		wantRole   string
		minKept    int
		maxKept    int
	}{
		"site":        {wantRole: "Dim", minKept: 30, maxKept: 80},   // ~50
		"country":     {wantRole: "Dim", minKept: 20, maxKept: 80},   // ~39
		"vpn":         {wantRole: "Dim", minKept: 1, maxKept: 5},     // ~2
		"status":      {wantRole: "Dim", minKept: 1, maxKept: 10},    // ~2
		"tag":         {wantRole: "Dim", minKept: 2, maxKept: 10},    // ~4
		"app_version": {wantRole: "Dim", minKept: 5, maxKept: 20},    // ~11
	}
	for dim, want := range cases {
		got, ok := spec.Dims[dim]
		if !ok {
			t.Errorf("missing dim %q in spec", dim)
			continue
		}
		if got.Role != want.wantRole {
			t.Errorf("dim %s role = %q, want %q", dim, got.Role, want.wantRole)
		}
		if n := len(got.KeptValues); n < want.minKept || n > want.maxKept {
			t.Errorf("dim %s kept = %d, want in [%d, %d]", dim, n, want.minKept, want.maxKept)
		}
	}
}
