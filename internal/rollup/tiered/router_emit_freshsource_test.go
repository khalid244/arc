package tiered

import (
	"strings"
	"testing"
)

// TestEmit_BySite_OpenTail_FinestTier reproduces the prod error
//   Binder Error: Referenced column "cnt" not found
// The rewrite path: query at 1h bucket, by_site variant, time range
// extending into the grace window. The router picks tier=1h (finest), so
// the open-tail fresh CTE reads from the SOURCE table instead of a finer
// rollup. The fresh CTE must use source-mode aggregate fragments
// (COUNT(*) instead of SUM(cnt)) and dim classification via CASE WHEN
// (raw source has `site`, not `site_class`).
func TestEmit_BySite_OpenTail_FinestTier(t *testing.T) {
	timeLo := mustTime("2026-05-12")
	tailLo := mustTime("2026-05-18") // last day of rollup
	timeHi := mustTime("2026-05-19") // present; open-tail = [5/18, 5/19)

	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/db/events/1h/2026/05/12/by_site/a.parquet",
			"_arc/rollup/db/events/1h/2026/05/13/by_site/b.parquet",
			"_arc/rollup/db/events/1h/2026/05/14/by_site/c.parquet",
			"_arc/rollup/db/events/1h/2026/05/15/by_site/d.parquet",
			"_arc/rollup/db/events/1h/2026/05/16/by_site/e.parquet",
			"_arc/rollup/db/events/1h/2026/05/17/by_site/f.parquet",
		},
	}
	spec := makeSpec("Asia/Riyadh", map[string]DimSpec{
		"site": {Role: "PerDim", KeptValues: []string{"youtu.be", "www.instagram.com", "youtube.com"}},
	})
	shape := &QueryShape{
		Table:      "events", // bare; FROM <table> after Arc transforms
		TimeColumn: "time",
		TimeLo:     timeLo,
		TimeHi:     timeHi,
		BucketArg:  "hour",
		Aggregates: []Aggregate{{Kind: AggCountStar, OutputAlias: "value"}},
		GroupDims:  []string{"site"},
		Filters: map[string]FilterPredicate{
			"site": {Op: "IN", Values: []string{"youtu.be", "www.instagram.com", "youtube.com"}},
		},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:         shape,
		Tier:          Tier1h, // finest tier; no finer → fresh from source
		TailLo:        tailLo,
		Variant:       "by_site",
		Files:         idx,
		Spec:          spec,
		StoragePrefix: "s3://hammel-arc/",
	})

	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	if !strings.Contains(sql, "fresh AS") {
		t.Fatalf("expected fresh CTE: %s", sql)
	}
	if !strings.Contains(sql, "FROM events") {
		t.Errorf("expected `FROM events` in fresh CTE (Arc transforms it later): %s", sql)
	}

	// Locate the fresh CTE block.
	freshIdx := strings.Index(sql, ", fresh AS")
	if freshIdx == -1 {
		t.Fatalf("could not find fresh CTE start")
	}
	// Inspect from that point onward.
	freshBlock := sql[freshIdx:]
	// End of fresh CTE: the next "\n)" closes it.
	if end := strings.Index(freshBlock, "\n)"); end != -1 {
		freshBlock = freshBlock[:end]
	}

	// Bug repro: rollup-mode `SUM(cnt)` must NOT appear in fresh CTE
	// (source files don't have `cnt`). Pre-fix this assertion fails
	// because the fresh CTE was using the same innerSelects as the
	// rollup CTE.
	if strings.Contains(freshBlock, "SUM(cnt)") {
		t.Errorf("fresh CTE must use COUNT(*), not SUM(cnt): %s", freshBlock)
	}
	// Source-mode aggregate must be present.
	if !strings.Contains(freshBlock, "COUNT(*)") {
		t.Errorf("fresh CTE should compute COUNT(*) over source rows: %s", freshBlock)
	}
	// Dim filter in fresh CTE must use the SOURCE column name (`site`),
	// not the rollup column (`site_class`) — source files only have
	// `site`. Pre-fix this assertion fails.
	if strings.Contains(freshBlock, "site_class IN (") {
		t.Errorf("fresh CTE WHERE must filter on `site`, not `site_class`: %s", freshBlock)
	}
	// dim classification (CASE WHEN) for UNION schema compatibility
	if !strings.Contains(freshBlock, "CASE WHEN site") || !strings.Contains(freshBlock, "AS site_class") {
		t.Errorf("fresh CTE should classify site → site_class via CASE WHEN: %s", freshBlock)
	}
}
