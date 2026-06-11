package rollup

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// supersededURIs returns the URIs currently parked on a manifest's deferred-delete
// list, for test assertions.
func supersededURIs(m *Manifest) []string {
	out := make([]string, 0, len(m.Superseded))
	for _, s := range m.Superseded {
		out = append(out, s.URI)
	}
	return out
}

// daySuffixedURI reports whether uri is a "<date>_<nonce>.parquet" cube file.
func daySuffixedURI(uri, date string) bool {
	base := uri[strings.LastIndex(uri, "/")+1:]
	return strings.HasPrefix(base, date+"_") && strings.HasSuffix(base, ".parquet")
}

// pathHasFixedDayName reports whether uri is the old overwrite-in-place
// "<date>.parquet" (no nonce) file name.
func pathHasFixedDayName(uri, date string) bool {
	base := uri[strings.LastIndex(uri, "/")+1:]
	return base == date+".parquet"
}

// TestDayBuildUniqueURI proves a (re)build of a day writes a NEW unique URI: the
// cube day path carries a nonce, so two builds of the same date never collide on
// one S3 object. This is the whole fix for the ETag-changed-mid-read 500: the read
// path is handed an immutable file that the next rebuild never overwrites.
func TestDayBuildUniqueURI(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount}}}
	m := testManager(newFakeStorage())
	a := m.cubeDayURI(spec, "2026-05-28")
	b := m.cubeDayURI(spec, "2026-05-28")
	if a == b {
		t.Fatalf("cubeDayURI returned the SAME uri twice (%q): a rebuild would overwrite the in-flight file in place — the exact ETag-change 500", a)
	}
	// Each is a nonce-suffixed name under the cube dir, NOT the fixed <date>.parquet.
	if !daySuffixedURI(a, "2026-05-28") || !daySuffixedURI(b, "2026-05-28") {
		t.Fatalf("uris not of form <date>_<nonce>.parquet: %q / %q", a, b)
	}
	if pathHasFixedDayName(a, "2026-05-28") {
		t.Fatalf("cubeDayURI still emits the fixed overwrite-in-place name: %q", a)
	}
}

// TestSupersedeRebuildParksOldURI proves that replacing an existing day entry with
// a freshly-built one (different URI) parks the OLD uri on the manifest's
// superseded list rather than deleting it immediately — in-flight queries (up to
// 300s) and route-only pods (5m manifest cache) must keep reading the old file.
func TestSupersedeRebuildParksOldURI(t *testing.T) {
	man := &Manifest{Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Days: []DayEntry{dailyEntry2("2026-05-28")}}
	oldURI := man.Days[0].URI

	// A fresh build of the same date with a NEW unique uri.
	newURI := uri("2026-05-28") + "#rebuilt" // distinct from the legacy one
	fresh := DayEntry{Date: "2026-05-28", URI: newURI,
		BucketLo: "2026-05-28 00:00:00+00", BucketHi: "2026-05-29 00:00:00+00", Rows: 150}

	man.supersedeUpsert(fresh, time.Unix(1000, 0))

	// Manifest now points at the new file.
	got := man.DaysInRange("2026-05-28 00:00:00+00", "2026-05-29 00:00:00+00")
	if len(got) != 1 || got[0].URI != newURI {
		t.Fatalf("after rebuild Days point at %v, want [%s]", uris(got), newURI)
	}
	// The OLD uri is parked for deferred deletion, NOT gone.
	if su := supersededURIs(man); len(su) != 1 || su[0] != oldURI {
		t.Fatalf("superseded = %v, want [%s] (old file must not be deleted immediately)", su, oldURI)
	}
}

// TestSupersedeUpsertSameURINoOp proves that re-upserting an entry whose URI is
// UNCHANGED (idempotent re-run that produced the same file name — should not happen
// with nonces, but be defensive) does not park the live uri for deletion.
func TestSupersedeUpsertSameURINoOp(t *testing.T) {
	e := dailyEntry2("2026-05-28")
	man := &Manifest{Days: []DayEntry{e}}
	man.supersedeUpsert(e, time.Unix(1000, 0)) // identical uri
	if su := supersededURIs(man); len(su) != 0 {
		t.Fatalf("re-upsert of the SAME uri parked it for deletion: %v (would delete the live file)", su)
	}
}

// TestSweepSupersededDeletesPriorPassOnly proves the deferred-deletion grace:
// uris parked in a PREVIOUS pass (older than one ForwardTick) are deleted and
// cleared, while uris parked THIS pass survive until the next pass.
func TestSweepSupersededDeletesPriorPassOnly(t *testing.T) {
	fs := newFakeStorage()
	m := testManager(fs)
	m.cfg.ForwardTick = 6 * time.Hour

	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	oldURI := uri("2026-05-28")   // parked last pass (7h ago) -> eligible
	freshURI := uri("2026-05-27") // parked this pass (1m ago) -> must survive
	man := &Manifest{Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Days: []DayEntry{dailyEntry2("2026-05-27")},
		Superseded: []SupersededFile{
			{URI: oldURI, At: now.Add(-7 * time.Hour).UnixNano()},
			{URI: freshURI, At: now.Add(-1 * time.Minute).UnixNano()},
		}}
	spec := man.Spec()

	m.sweepSuperseded(context.Background(), spec, man, now)

	// The prior-pass uri was deleted (exactly once) and dropped from the list.
	if len(fs.deleted) != 1 {
		t.Fatalf("DeleteBatch calls = %d, want 1", len(fs.deleted))
	}
	wantKey := m.keyFromURI(oldURI)
	if len(fs.deleted[0]) != 1 || fs.deleted[0][0] != wantKey {
		t.Fatalf("deleted keys = %v, want [%s]", fs.deleted[0], wantKey)
	}
	// The this-pass uri must remain parked (still inside the grace window).
	if su := supersededURIs(man); len(su) != 1 || su[0] != freshURI {
		t.Fatalf("after sweep superseded = %v, want only the this-pass uri [%s]", su, freshURI)
	}
	// A manifest write persisted the cleaned list.
	if len(fs.objs) == 0 {
		t.Error("expected manifest write after sweep cleaned the list")
	}
}

// TestSweepSupersededNoEligibleNoWrite proves the sweep is a cheap no-op when
// nothing is eligible: no delete, no manifest write (avoid churning the object
// every tick when there is nothing to clean).
func TestSweepSupersededNoEligibleNoWrite(t *testing.T) {
	fs := newFakeStorage()
	m := testManager(fs)
	m.cfg.ForwardTick = 6 * time.Hour
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	man := &Manifest{Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Superseded: []SupersededFile{{URI: uri("2026-05-27"), At: now.Add(-1 * time.Minute).UnixNano()}}}
	m.sweepSuperseded(context.Background(), man.Spec(), man, now)
	if len(fs.deleted) != 0 {
		t.Fatalf("DeleteBatch called with nothing eligible: %v", fs.deleted)
	}
	if len(fs.objs) != 0 {
		t.Fatal("manifest written despite nothing to clean (needless object churn every tick)")
	}
	if len(man.Superseded) != 1 {
		t.Fatalf("this-pass entry wrongly dropped: %v", supersededURIs(man))
	}
}

// TestSupersedeLegacyFixedNameEntry proves backward compatibility: a manifest with
// a legacy fixed-name uri (".../2026-05-28.parquet") reads fine, and the first
// rebuild supersedes that legacy file via the SAME deferred path (parked, then
// swept on a later pass) — so legacy objects are not orphaned forever.
func TestSupersedeLegacyFixedNameEntry(t *testing.T) {
	legacy := uri("2026-05-28") // ".../2026-05-28.parquet" (fixed legacy name)
	man := &Manifest{Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Days: []DayEntry{{Date: "2026-05-28", URI: legacy,
			BucketLo: "2026-05-28 00:00:00+00", BucketHi: "2026-05-29 00:00:00+00", Rows: 144}}}

	// Legacy entry reads fine before any rebuild.
	if got := man.DaysInRange("2026-05-28 00:00:00+00", "2026-05-29 00:00:00+00"); len(got) != 1 || got[0].URI != legacy {
		t.Fatalf("legacy entry not readable: %v", uris(got))
	}

	// First rebuild: new nonce uri replaces the legacy one; legacy uri parked.
	m := testManager(newFakeStorage())
	spec := man.Spec()
	newURI := m.cubeDayURI(spec, "2026-05-28")
	fresh := DayEntry{Date: "2026-05-28", URI: newURI,
		BucketLo: "2026-05-28 00:00:00+00", BucketHi: "2026-05-29 00:00:00+00", Rows: 150}
	man.supersedeUpsert(fresh, time.Unix(2000, 0))

	if got := man.DaysInRange("2026-05-28 00:00:00+00", "2026-05-29 00:00:00+00"); len(got) != 1 || got[0].URI != newURI {
		t.Fatalf("after rebuild not pointing at new uri: %v", uris(got))
	}
	if su := supersededURIs(man); len(su) != 1 || su[0] != legacy {
		t.Fatalf("legacy uri not parked for deferred deletion: %v", su)
	}
}

// TestSupersedeEmptyMarkerNoURI proves a real rebuild that supersedes a prior
// '-empty' coverage marker does NOT park a delete (the marker has no file), and a
// rebuild that produces a zero-row '-empty' marker likewise parks nothing.
func TestSupersedeEmptyMarkerNoURI(t *testing.T) {
	// Real rebuild replacing an -empty marker: persist() removes the marker, then
	// upserts the real entry. The marker carried no URI, so nothing to delete.
	man := &Manifest{Days: []DayEntry{{Date: "2026-05-28-empty", Covers: []string{"2026-05-28"}}}}
	man.Remove("2026-05-28-empty")
	fresh := DayEntry{Date: "2026-05-28", URI: uri("2026-05-28"),
		BucketLo: "2026-05-28 00:00:00+00", BucketHi: "2026-05-29 00:00:00+00", Rows: 10}
	man.supersedeUpsert(fresh, time.Unix(3000, 0))
	if su := supersededURIs(man); len(su) != 0 {
		t.Fatalf("superseding an -empty marker parked a delete: %v (marker has no file)", su)
	}
	// And the entry landed.
	if len(man.Days) != 1 || man.Days[0].URI != uri("2026-05-28") {
		t.Fatalf("real entry not upserted over empty marker: %v", datesOf(man.Days))
	}
}

// TestSupersededCloneIsolated proves clone() deep-copies the Superseded slice so a
// router-published snapshot can never be mutated by a concurrent sweep clearing the
// original's list.
func TestSupersededCloneIsolated(t *testing.T) {
	man := &Manifest{Superseded: []SupersededFile{{URI: "a", At: 1}, {URI: "b", At: 2}}}
	c := man.clone()
	man.Superseded = man.Superseded[:0] // sweep clears the original
	if len(c.Superseded) != 2 {
		t.Fatalf("clone shares the Superseded backing array: len=%d after original cleared", len(c.Superseded))
	}
}

// TestSupersededRoundTrips proves the Superseded list survives manifest
// serialization (so a deferred delete recorded one pass is honored the next, even
// across a builder restart that reloads from the persisted object).
func TestSupersededRoundTrips(t *testing.T) {
	man := &Manifest{Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Days:       []DayEntry{dailyEntry2("2026-05-28")},
		Superseded: []SupersededFile{{URI: uri("2026-05-27"), At: 12345}}}
	b, err := man.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseManifest(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Superseded) != 1 || got.Superseded[0].URI != uri("2026-05-27") || got.Superseded[0].At != 12345 {
		t.Fatalf("Superseded did not round-trip: %+v", got.Superseded)
	}
}

// TestDayRebuildEndToEndUniqueFile is the full-pipeline proof: building a day, then
// REBUILDING it (the RebuildDays re-roll) writes a SECOND physical file under a new
// name, the manifest points at it, the FIRST file is parked superseded (still on
// disk), and a later sweep deletes it. This is the live ETag-500 scenario end to
// end against real Parquet.
func TestDayRebuildEndToEndUniqueFile(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	root := t.TempDir()
	stg := newFakeStorage()
	m := driftManager(t, db, root, stg)
	m.cfg.ForwardTick = 6 * time.Hour
	ctx := context.Background()

	writeSourceDay(t, db, root, "2026/06/01",
		`SELECT TIMESTAMPTZ '2026-06-01 10:00:00' AS "time", 'web' AS site
		 UNION ALL SELECT TIMESTAMPTZ '2026-06-01 11:00:00', 'ios'`)
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}}}
	mkCubeDir(t, root, spec)
	cubeDirPath := filepath.Join(root, "_arc/rollup", cubeDir(spec))

	day := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	sealed := []time.Time{day}
	// rebuildFloor BEFORE the day so the day stays inside the rebuild window: the
	// skip guard is `built[date] && day.Before(rebuildFloor)`, so a day at/after the
	// floor is always re-rolled (the RebuildDays behavior that drives the bug).
	rebuildFloor := day.AddDate(0, 0, -2)

	build := func() *Manifest {
		man, _ := m.loadManifest(ctx, spec)
		cb := &cubeBuild{spec: spec, man: man, built: man.BuiltDays(), monthBuild: map[string]bool{}}
		m.buildExactDays(ctx, "default.events", []*cubeBuild{cb}, sealed, rebuildFloor)
		out, _ := m.loadManifest(ctx, spec)
		return out
	}

	man1 := build()
	first := man1.DaysInRange("2026-06-01 00:00:00+00", "2026-06-02 00:00:00+00")
	if len(first) != 1 || first[0].URI == "" {
		t.Fatalf("first build: want 1 file-backed entry, got %v", uris(first))
	}
	uri1 := first[0].URI

	// Ensure a distinct nonce (build names use UnixNano; a fast machine could collide).
	time.Sleep(2 * time.Millisecond)

	man2 := build()
	second := man2.DaysInRange("2026-06-01 00:00:00+00", "2026-06-02 00:00:00+00")
	if len(second) != 1 {
		t.Fatalf("rebuild: want 1 file-backed entry, got %v", uris(second))
	}
	uri2 := second[0].URI
	if uri2 == uri1 {
		t.Fatalf("rebuild reused the SAME uri %q — this is the overwrite-in-place ETag-500 bug", uri1)
	}
	// The old uri is parked superseded, and BOTH physical files still exist on disk
	// (the read path may be mid-flight on the old one).
	if su := supersededURIs(man2); len(su) != 1 || su[0] != uri1 {
		t.Fatalf("old uri not parked superseded: %v", su)
	}
	if _, err := os.Stat(localPath(t, root, uri1)); err != nil {
		t.Fatalf("old cube file deleted immediately (no grace): %v", err)
	}
	if _, err := os.Stat(localPath(t, root, uri2)); err != nil {
		t.Fatalf("new cube file missing: %v", err)
	}
	if n := countParquet(t, cubeDirPath); n != 2 {
		t.Fatalf("want 2 physical cube files after rebuild (old parked + new), got %d", n)
	}

	// A sweep one tick later issues the delete (through the storage backend) for the
	// parked file ONLY and clears the list. The fake storage records the delete keys
	// rather than touching the local FS, so assert on the issued keys.
	m.sweepSuperseded(ctx, spec, man2, time.Now().Add(7*time.Hour))
	if len(man2.Superseded) != 0 {
		t.Fatalf("sweep did not clear the superseded list: %v", supersededURIs(man2))
	}
	if len(stg.deleted) != 1 || len(stg.deleted[0]) != 1 || stg.deleted[0][0] != m.keyFromURI(uri1) {
		t.Fatalf("sweep deleted wrong keys: %v, want only [%s]", stg.deleted, m.keyFromURI(uri1))
	}
}

// TestCompactionUsesManifestURIs proves month compaction sources its merge file
// list from manifest DayEntry.URI (nonce-named day files), not a reconstructed
// fixed "<date>.parquet" name — so the nonce naming does not break compaction. The
// merged month must aggregate the real rows and the daily files must be parked
// superseded (deferred deletion), not lost.
func TestCompactionUsesManifestURIs(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	root := t.TempDir()
	stg := newFakeStorage()
	m := driftManager(t, db, root, stg)
	m.cfg.ForwardTick = 6 * time.Hour
	m.cfg.CompactMinDays = 2
	ctx := context.Background()

	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}}}
	mkCubeDir(t, root, spec)

	// Build two nonce-named daily cube files directly (the build path) so the manifest
	// carries real unique URIs — exactly what compaction must read from.
	man := &Manifest{Source: spec.Source, Grain: spec.Grain, Dims: spec.Dims, Aggs: spec.Aggs}
	for _, d := range []string{"2026-03-01", "2026-03-02"} {
		writeSourceDay(t, db, root, strings.ReplaceAll(d, "-", "/"),
			`SELECT TIMESTAMPTZ '`+d+` 10:00:00' AS "time", 'web' AS site
			 UNION ALL SELECT TIMESTAMPTZ '`+d+` 11:00:00', 'ios'`)
		dest := m.cubeDayURI(spec, d)
		entry, err := BuildDay(db, spec, m.sourceDayGlob(spec.Source, mustDay(t, d)), "time", d, dest)
		if err != nil {
			t.Fatalf("build %s: %v", d, err)
		}
		if !daySuffixedURI(entry.URI, d) {
			t.Fatalf("day cube file is not nonce-named: %q", entry.URI)
		}
		man.Upsert(entry)
		// Register the file as present so compaction's stat-guard
		// (partitionExistingDailies) sees it and merges it, instead of treating it as a
		// missing pointer and rebuilding from source with a fresh nonce.
		stg.existing[m.keyFromURI(entry.URI)] = true
		time.Sleep(time.Millisecond)
	}
	dailyURIs := []string{man.Days[0].URI, man.Days[1].URI}

	// Compact the month. rebuildFloor AFTER the days so they are sealed/compactable.
	rebuildFloor := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	m.compactCube(ctx, spec, man, rebuildFloor)

	// One merged monthly entry now; it must aggregate all 4 rows.
	monthly := man.DaysInRange("2026-03-01 00:00:00+00", "2026-04-01 00:00:00+00")
	if len(monthly) != 1 || len(monthly[0].Covers) == 0 {
		t.Fatalf("want 1 compacted monthly entry covering the dailies, got %v", uris(monthly))
	}
	var rows int64
	if err := db.QueryRow("SELECT sum(_cnt)::BIGINT FROM read_parquet('" + monthly[0].URI + "')").Scan(&rows); err != nil {
		t.Fatalf("read merged month: %v", err)
	}
	if rows != 4 {
		t.Fatalf("merged month _cnt = %d, want 4 (2 days x 2 rows) — compaction missed the manifest-uri files", rows)
	}
	// The daily files are parked superseded (deferred), not deleted in place.
	parked := map[string]bool{}
	for _, s := range man.Superseded {
		parked[s.URI] = true
	}
	for _, u := range dailyURIs {
		if !parked[u] {
			t.Fatalf("daily file %q not parked superseded after compaction; superseded=%v", u, supersededURIs(man))
		}
		if _, err := os.Stat(localPath(t, root, u)); err != nil {
			t.Fatalf("daily file deleted immediately by compaction (no grace): %v", err)
		}
	}
}

// localPath maps a local-root cube URI back to its filesystem path.
func localPath(t *testing.T, root, uri string) string {
	t.Helper()
	return filepath.Join(root, strings.TrimPrefix(uri, root+"/"))
}

// countParquet counts .parquet files directly in dir (non-recursive).
func countParquet(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ents {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".parquet") {
			n++
		}
	}
	return n
}

func mustDay(t *testing.T, date string) time.Time {
	t.Helper()
	d, err := time.Parse("2006-01-02", date)
	if err != nil {
		t.Fatal(err)
	}
	return d.UTC()
}
