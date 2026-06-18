package rollup

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestCompactedDays pins which dates CompactedDays reports: only days folded into a
// month file (Covers), never a loose daily entry's own date. A loose daily can be
// superseded in place by a rebuild (same "YYYY-MM-DD" key); a day inside a month
// cannot (the month is keyed "YYYY-MM"), so only the latter must be protected from
// re-materialization.
func TestCompactedDays(t *testing.T) {
	m := &Manifest{Days: []DayEntry{
		{Date: "2026-05", Covers: []string{"2026-05-01", "2026-05-02", "2026-05-03"}},
		{Date: "2026-05-20"}, // loose daily — NOT compacted
		{Date: "2026-04", Covers: []string{"2026-04-30"}},
	}}
	c := m.CompactedDays()
	for _, d := range []string{"2026-05-01", "2026-05-02", "2026-05-03", "2026-04-30"} {
		if !c[d] {
			t.Errorf("CompactedDays missing folded day %s", d)
		}
	}
	if c["2026-05-20"] {
		t.Error("a loose daily entry's date must NOT be reported as compacted")
	}
	if c["2026-05"] || c["2026-04"] {
		t.Error("the month label itself is not a compacted day")
	}
	if len(c) != 4 {
		t.Errorf("CompactedDays size = %d, want 4", len(c))
	}
}

// cubeCnt returns the SERVED _cnt total over [lo,hi): the sum across EVERY cube file
// DaysInRange selects, exactly as the read path reads them. If two files overlap the
// same day (the duplication bug), this sum doubles — which is what we assert against.
func cubeCnt(t *testing.T, db *sql.DB, m *Manifest, lo, hi string) int64 {
	t.Helper()
	days := m.DaysInRange(lo, hi)
	if len(days) == 0 {
		return 0
	}
	var n int64
	q := "SELECT coalesce(sum(_cnt),0)::BIGINT FROM read_parquet(" + ReadExpr(days) +
		", union_by_name=true) WHERE bucket >= TIMESTAMPTZ '" + lo + "' AND bucket < TIMESTAMPTZ '" + hi + "'"
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("cubeCnt(%s,%s): %v", lo, hi, err)
	}
	return n
}

// TestRebuildFloorWidening_NoCompactedDayOverlap is the regression for the May-2026
// cube duplication. A month is compacted (its days folded into one month file), then
// a WIDENED rebuild_days floor pulls those days back into the rebuild window. The
// day-build loop must NOT re-materialize a compacted day as a loose daily: that
// appends a "YYYY-MM-DD" entry beside the "YYYY-MM" month that supersedeUpsert cannot
// replace, so DaysInRange selects BOTH files and the read path sums them — the day
// double-counts. With CompactedDays() guarding buildExactDays, no overlap is created.
//
// Before the fix this test fails: 2026-05-01 selects 2 cube files and the served
// total doubles.
func TestRebuildFloorWidening_NoCompactedDayOverlap(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	root := t.TempDir()
	stg := newFakeStorage()
	m := driftManager(t, db, root, stg)
	ctx := context.Background()

	writeSourceDay(t, db, root, "2026/05/01",
		`SELECT TIMESTAMPTZ '2026-05-01 01:00:00' AS "time", 'web' AS site
		 UNION ALL SELECT TIMESTAMPTZ '2026-05-01 02:00:00', 'ios'`)
	writeSourceDay(t, db, root, "2026/05/02",
		`SELECT TIMESTAMPTZ '2026-05-02 03:00:00' AS "time", 'web' AS site`)

	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}}}
	mkCubeDir(t, root, spec)
	sealed := []time.Time{
		time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC), // newest first
		time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
	}

	// (1) Build May as a clean COMPACTED month (floor after May → fully sealed).
	floorAfter := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	man, ok := m.loadManifest(ctx, spec)
	if !ok {
		t.Fatal("manifest load failed")
	}
	cb := &cubeBuild{spec: spec, man: man, built: man.BuiltDays(), compacted: man.CompactedDays(),
		monthBuild: cleanFullySealedMonths(man.BuiltDays(), sealed, floorAfter)}
	if !cb.monthBuild["2026-05"] {
		t.Fatal("setup: 2026-05 should build as a clean month")
	}
	m.buildExactMonths(ctx, "default.events", []*cubeBuild{cb}, sealed)

	man2, _ := m.loadManifest(ctx, spec)
	if !man2.CompactedDays()["2026-05-01"] || !man2.CompactedDays()["2026-05-02"] {
		t.Fatalf("setup: May days not folded into a compacted month; days=%v", uris(man2.Days))
	}
	totalBefore := cubeCnt(t, db, man2, "2026-05-01 00:00:00", "2026-05-03 00:00:00")
	if totalBefore != 3 {
		t.Fatalf("setup: compacted May total = %d, want 3", totalBefore)
	}

	// (2) WIDEN the floor to BEFORE May (the rebuild_days 30->42 move) so the compacted
	// days now satisfy day >= rebuildFloor. Reconstruct the build state from the
	// PERSISTED manifest — exactly what the next tick does — and run the day build.
	floorWide := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	cb2 := &cubeBuild{spec: spec, man: man2, built: man2.BuiltDays(), compacted: man2.CompactedDays(),
		monthBuild: cleanFullySealedMonths(man2.BuiltDays(), sealed, floorWide)}
	m.buildExactDays(ctx, "default.events", []*cubeBuild{cb2}, sealed, floorWide)

	// (3) No overlapping loose daily entry may have been appended.
	man3, _ := m.loadManifest(ctx, spec)
	for _, d := range []struct{ lo, hi string }{
		{"2026-05-01 00:00:00", "2026-05-02 00:00:00"},
		{"2026-05-02 00:00:00", "2026-05-03 00:00:00"},
	} {
		if got := man3.DaysInRange(d.lo, d.hi); len(got) != 1 {
			t.Fatalf("OVERLAP at %s: %d cube files selected %v — a compacted day was re-materialized as a loose daily (the May-2026 double-count)", d.lo, len(got), uris(got))
		}
	}

	// (4) The served total is unchanged — no doubling.
	if totalAfter := cubeCnt(t, db, man3, "2026-05-01 00:00:00", "2026-05-03 00:00:00"); totalAfter != totalBefore {
		t.Fatalf("served cube total changed after widened-floor day build: before=%d after=%d (duplication)", totalBefore, totalAfter)
	}
}
