package rollup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Source fingerprints (fingerprint.go): a pass rebuilds a sealed day inside the
// rebuild window only when the day's source files changed.

// recentDates returns n dates ending 2 days ago (oldest first), so they sit inside
// buildSource's real-clock rebuild window and are sealed at any time of day.
func recentDates(n int) []string {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = today.AddDate(0, 0, -(n + 1 - i)).Format("2006-01-02")
	}
	return out
}

func daysOf(dates []string) []time.Time {
	out := make([]time.Time, len(dates))
	for i, d := range dates {
		out[i] = day(d)
	}
	return out
}

// passFixture is freshFixture over recent days with the fresh step off and the
// fixture clock on real time: buildSource's own seal/window math uses time.Now.
func passFixture(t *testing.T, n int) (*Manager, *localListStorage, []CubeSpec, []string) {
	t.Helper()
	dates := recentDates(n)
	m, stg, clock, specs := freshFixture(t, dates...)
	*clock = time.Now()
	m.cfg.FreshDays = -1
	return m, stg, specs, dates
}

// pass runs one real buildSource pass as a new tick and returns its manifest
// writes.
func pass(t *testing.T, m *Manager, stg *localListStorage, dates []string) int {
	t.Helper()
	writes := 0
	stg.onWrite = func(p string) {
		if filepath.Base(p) == "manifest.json" {
			writes++
		}
	}
	defer func() { stg.onWrite = nil }()
	m.tickBuilt = map[string]bool{} // a new tick, as tick() does
	if err := m.buildSource(context.Background(), freshSrc, daysOf(dates)); err != nil {
		t.Fatalf("buildSource: %v", err)
	}
	return writes
}

// uriSnapshot maps cube@date -> file URI for every cube and date.
func uriSnapshot(t *testing.T, m *Manager, specs []CubeSpec, dates []string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, s := range specs {
		for _, d := range dates {
			out[CubeID(s)+"@"+d] = entryURI(t, m, s, d)
		}
	}
	return out
}

// addSourceFile writes another Parquet file into a day's partition (a late-event
// batch landing, or a compaction output).
func addSourceFile(t *testing.T, m *Manager, root, date, name, selectBody string) {
	t.Helper()
	mustExec(t, m.db, fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", selectBody, filepath.Join(dayDir(root, date), name)))
}

// cubeCount sums _cnt over a cube's daily file for date.
func cubeCount(t *testing.T, m *Manager, spec CubeSpec, date string) int64 {
	t.Helper()
	uri := entryURI(t, m, spec, date)
	if uri == "" {
		t.Fatalf("%s: no entry for %s", CubeID(spec), date)
	}
	var n int64
	if err := m.db.QueryRow("SELECT coalesce(sum(_cnt), 0)::BIGINT FROM read_parquet('" + uri + "')").Scan(&n); err != nil {
		t.Fatalf("read cube %s: %v", uri, err)
	}
	return n
}

func sourceCount(t *testing.T, m *Manager, root, date string) int64 {
	t.Helper()
	var n int64
	if err := m.db.QueryRow("SELECT count(*)::BIGINT FROM read_parquet('" + filepath.Join(dayDir(root, date), "*.parquet") + "')").Scan(&n); err != nil {
		t.Fatalf("read source %s: %v", date, err)
	}
	return n
}

// --- Idea 1: the fingerprint and recording it -------------------------------

func TestDaySourceSig(t *testing.T) {
	m, stg, _, dates := passFixture(t, 1)
	ctx := context.Background()
	d, dir := day(dates[0]), dayDir(stg.root, dates[0])

	ds1 := m.listDaySource(ctx, freshSrc, d)
	sig1, ok := ds1.sig, ds1.ok
	if !ok || sig1 == "" {
		t.Fatalf("fingerprint unavailable: %q %v", sig1, ok)
	}
	if again := m.listDaySource(ctx, freshSrc, d).sig; again != sig1 {
		t.Fatal("fingerprint not stable for unchanged files")
	}
	if err := os.WriteFile(filepath.Join(dir, "_SUCCESS.tmp"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if s := m.listDaySource(ctx, freshSrc, d).sig; s != sig1 {
		t.Error("a non-Parquet file must not change the fingerprint (the build never reads it)")
	}
	sub := filepath.Join(dir, "13")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	mustExec(t, m.db, fmt.Sprintf(`COPY (SELECT TIMESTAMPTZ '%s 13:00:00' AS "time", 'web' AS site) TO '%s' (FORMAT PARQUET)`, dates[0], filepath.Join(sub, "late.parquet")))
	sig2 := m.listDaySource(ctx, freshSrc, d).sig
	if sig2 == sig1 {
		t.Fatal("a new Parquet file in an hour sub-partition must change the fingerprint")
	}
	if err := os.Remove(filepath.Join(sub, "late.parquet")); err != nil {
		t.Fatal(err)
	}
	if s := m.listDaySource(ctx, freshSrc, d).sig; s != sig1 {
		t.Error("removing the file must restore the original fingerprint")
	}

	stg.failList = true
	if ok := m.listDaySource(ctx, freshSrc, d).ok; ok {
		t.Error("a listing error must make the fingerprint unavailable (never skip on doubt)")
	}
	stg.failList = false
	if ds := m.listDaySource(ctx, freshSrc, day("2001-01-01")); !ds.ok || !ds.empty || ds.sig == "" {
		t.Errorf("a cleanly listed day without Parquet files is EMPTY with a fingerprint, got %+v", ds)
	}
	plain := driftManager(t, m.db, stg.root, newFakeStorage()) // backend without ListObjects
	if plain.listDaySource(ctx, freshSrc, d).ok {
		t.Error("a backend without object listing has no fingerprint")
	}
}

func TestBuildRecordsSourceSig(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 2)
	pass(t, m, stg, dates)
	for _, s := range specs {
		man, _ := m.loadManifest(context.Background(), s)
		for _, d := range dates {
			want := m.listDaySource(context.Background(), freshSrc, day(d)).sig
			if got := man.DailySourceSig(d); got != want {
				t.Errorf("%s %s: recorded fingerprint %q, want %q", CubeID(s), d, got, want)
			}
		}
	}
}

// --- Idea 2: rebuild only changed days --------------------------------------

func TestPassSkipsUnchangedDays(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 3)
	if w := pass(t, m, stg, dates); w == 0 {
		t.Fatal("setup: first pass built nothing")
	}
	before := uriSnapshot(t, m, specs, dates)
	if w := pass(t, m, stg, dates); w != 0 {
		t.Errorf("second pass over unchanged sources wrote %d manifests, want 0", w)
	}
	if after := uriSnapshot(t, m, specs, dates); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Error("second pass rebuilt cube files although no source file changed")
	}
}

func TestPassRebuildsOnlyChangedDay(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 3)
	pass(t, m, stg, dates)
	before := uriSnapshot(t, m, specs, dates)

	late := dates[1]
	addSourceFile(t, m, stg.root, late, "late.parquet", fmt.Sprintf(
		`SELECT TIMESTAMPTZ '%s 23:30:00' AS "time", 'web' AS site UNION ALL SELECT TIMESTAMPTZ '%s 23:40:00', 'tv'`, late, late))
	pass(t, m, stg, dates)

	after := uriSnapshot(t, m, specs, dates)
	for _, s := range specs {
		for _, d := range dates {
			k := CubeID(s) + "@" + d
			changed := after[k] != before[k]
			if d == late && !changed {
				t.Errorf("%s: day with a late file was not rebuilt", k)
			}
			if d != late && changed {
				t.Errorf("%s: unchanged day was rebuilt", k)
			}
		}
		if got, want := cubeCount(t, m, s, late), sourceCount(t, m, stg.root, late); got != want {
			t.Errorf("%s %s: cube count %d, source rows %d — late rows missing", CubeID(s), late, got, want)
		}
	}
}

// A compaction rewrites a day's files without changing its rows: one rebuild,
// identical counts.
func TestPassRebuildsAfterCompactionRewrite(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 1)
	d := dates[0]
	addSourceFile(t, m, stg.root, d, "b.parquet", fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 15:00:00' AS "time", 'ios' AS site`, d))
	pass(t, m, stg, dates)
	before := uriSnapshot(t, m, specs, dates)
	wantRows := sourceCount(t, m, stg.root, d)

	merged := filepath.Join(stg.root, "merged.parquet")
	mustExec(t, m.db, fmt.Sprintf("COPY (SELECT * FROM read_parquet('%s')) TO '%s' (FORMAT PARQUET)", filepath.Join(dayDir(stg.root, d), "*.parquet"), merged))
	for _, f := range []string{"part.parquet", "b.parquet"} {
		if err := os.Remove(filepath.Join(dayDir(stg.root, d), f)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Rename(merged, filepath.Join(dayDir(stg.root, d), "compacted.parquet")); err != nil {
		t.Fatal(err)
	}
	pass(t, m, stg, dates)
	after := uriSnapshot(t, m, specs, dates)
	for _, s := range specs {
		if after[CubeID(s)+"@"+d] == before[CubeID(s)+"@"+d] {
			t.Errorf("%s: rewritten day not rebuilt", CubeID(s))
		}
		if got := cubeCount(t, m, s, d); got != wantRows {
			t.Errorf("%s: count %d after compaction rebuild, want %d", CubeID(s), got, wantRows)
		}
	}
	if w := pass(t, m, stg, dates); w != 0 {
		t.Errorf("pass after the compaction rebuild wrote %d manifests, want 0", w)
	}
}

// Entries written before fingerprints existed carry none: rebuilt once, then
// skipped.
func TestLegacyEntryRebuiltOnceThenSkipped(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 2)
	pass(t, m, stg, dates)
	ctx := context.Background()
	for _, s := range specs { // strip the fingerprints, as an old build would have left them
		man, _ := m.loadManifest(ctx, s)
		for i := range man.Days {
			man.Days[i].SourceSig = ""
		}
		b, _ := json.Marshal(man)
		if err := stg.fakeStorage.Write(ctx, m.manifestKey(s), b); err != nil {
			t.Fatal(err)
		}
	}
	if w := pass(t, m, stg, dates); w == 0 {
		t.Fatal("entries without a fingerprint must be rebuilt")
	}
	if w := pass(t, m, stg, dates); w != 0 {
		t.Errorf("after the one-time rebuild a pass wrote %d manifests, want 0", w)
	}
}

// No fingerprint (listing error, or a backend without object listing) is never a
// reason to skip: the day is rebuilt as before.
func TestNoFingerprintRebuildsAsBefore(t *testing.T) {
	m, stg, _, dates := passFixture(t, 1)
	pass(t, m, stg, dates)
	stg.failList = true
	if w := pass(t, m, stg, dates); w == 0 {
		t.Error("with the listing failing, the pass must rebuild (old behaviour), not skip")
	}
}

// --- Idea 3: late events after a fresh build (no timed settle rebuild) -------

func TestLateEventsAfterFreshBuildCaughtNextPass(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 2)
	m.cfg.FreshDays = 2
	ctx := context.Background()
	newest := dates[len(dates)-1]

	m.tickBuilt = map[string]bool{}
	m.runFresh(ctx, map[string][]time.Time{freshSrc: daysOf(dates)}, true)
	for _, s := range specs {
		if entryURI(t, m, s, newest) == "" {
			t.Fatalf("setup: fresh step did not build %s", newest)
		}
	}
	addSourceFile(t, m, stg.root, newest, "late.parquet", fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 23:59:00' AS "time", 'web' AS site`, newest))

	// Same tick: the fingerprint changed since the fresh build, so the pass
	// rebuilds the day even though it was already built this tick.
	if err := m.buildSource(ctx, freshSrc, daysOf(dates)); err != nil {
		t.Fatal(err)
	}
	for _, s := range specs {
		if got, want := cubeCount(t, m, s, newest), sourceCount(t, m, stg.root, newest); got != want {
			t.Errorf("%s: count %d, want %d after the same tick's pass", CubeID(s), got, want)
		}
	}
	// And a day built this tick whose fingerprint did NOT change is not rebuilt.
	before := uriSnapshot(t, m, specs, dates)
	if err := m.buildSource(ctx, freshSrc, daysOf(dates)); err != nil {
		t.Fatal(err)
	}
	if after := uriSnapshot(t, m, specs, dates); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Error("unchanged days rebuilt within the same tick")
	}
}

// --- Idea 4: an unchanged pass is cheap enough for a short tick -------------

func TestUnchangedPassIsListingOnly(t *testing.T) {
	const n = 30
	m, stg, _, dates := passFixture(t, n)
	pass(t, m, stg, dates)
	stg.lists = 0
	start := time.Now()
	if w := pass(t, m, stg, dates); w != 0 {
		t.Fatalf("unchanged pass wrote %d manifests", w)
	}
	took := time.Since(start)
	// One listing per day inside the rebuild window — no source read, no cube
	// write. Built days older than the window are never re-checked at all.
	floor := time.Now().UTC().Add(-m.cfg.Grace).AddDate(0, 0, -m.cfg.RebuildDays)
	inWindow := 0
	for _, d := range daysOf(dates) {
		if !d.Before(floor) {
			inWindow++
		}
	}
	// Plus one listing per cube directory (the missing-cube-file check).
	if want := inWindow + 2; stg.lists != want {
		t.Errorf("unchanged pass listed %d times, want %d (one per day in the window + one per cube)", stg.lists, want)
	}
	t.Logf("unchanged %d-day pass: %d listings, %s", n, stg.lists, took)
}

// --- Review round 2 -----------------------------------------------------------

// The build reads EXACTLY the fingerprinted list: a file landing between the
// listing and the read is not in the cube, and the next pass (whose listing now
// includes it) rebuilds the day with it.
func TestBuildReadsExactlyTheFingerprintedFiles(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 1)
	d := dates[0]
	before := sourceCount(t, m, stg.root, d)
	landed := false
	stg.afterList = func(prefix string) {
		if !landed && strings.HasPrefix(prefix, "default/events/") { // the day listing, not a cube directory
			landed = true
			addSourceFile(t, m, stg.root, d, "landed.parquet", fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 12:00:00' AS "time", 'tv' AS site`, d))
		}
	}
	pass(t, m, stg, dates)
	stg.afterList = nil
	for _, s := range specs {
		if got := cubeCount(t, m, s, d); got != before {
			t.Fatalf("%s: cube has %d rows, want %d (only the fingerprinted files)", CubeID(s), got, before)
		}
	}
	pass(t, m, stg, dates)
	for _, s := range specs {
		if got, want := cubeCount(t, m, s, d), sourceCount(t, m, stg.root, d); got != want {
			t.Errorf("%s: after the next pass cube has %d rows, want %d", CubeID(s), got, want)
		}
	}
}

// Changing how cubes are built (build version, time column) must change the
// fingerprint, so a deployed build fix reaches every day in the window.
func TestFingerprintCoversBuildSettings(t *testing.T) {
	m, _, _, dates := passFixture(t, 1)
	ctx := context.Background()
	base := m.listDaySource(ctx, freshSrc, day(dates[0])).sig
	m.cfg.TimeCol = "ts"
	if m.listDaySource(ctx, freshSrc, day(dates[0])).sig == base {
		t.Error("time column not part of the fingerprint")
	}
}

// A day rebuilt with zero rows must drop its old data entry (else the stale file
// keeps being served) and must be rebuilt again once data arrives.
func TestEmptyRebuildDropsStaleEntryThenRecovers(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 1)
	d := dates[0]
	pass(t, m, stg, dates)

	// The day's only file is replaced by one whose rows belong to another day.
	other := day(d).AddDate(0, 0, -400).Format("2006-01-02")
	addSourceFile(t, m, stg.root, d, "moved.parquet", fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 10:00:00' AS "time", 'web' AS site`, other))
	if err := os.Remove(filepath.Join(dayDir(stg.root, d), "part.parquet")); err != nil {
		t.Fatal(err)
	}
	pass(t, m, stg, dates)
	lo, hi := d+" 00:00:00", day(d).AddDate(0, 0, 1).Format("2006-01-02")+" 00:00:00"
	for _, s := range specs {
		man, _ := m.loadManifest(context.Background(), s)
		if got := man.DaysInRange(lo, hi); len(got) != 0 {
			t.Fatalf("%s: empty day still serves %v", CubeID(s), uris(got))
		}
	}

	// Late data arrives: the empty day must be rebuilt (not frozen as "compacted").
	addSourceFile(t, m, stg.root, d, "late.parquet", fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 09:00:00' AS "time", 'ios' AS site`, d))
	pass(t, m, stg, dates)
	for _, s := range specs {
		if got := cubeCount(t, m, s, d); got != 1 {
			t.Errorf("%s: count %d after late data on an empty day, want 1", CubeID(s), got)
		}
	}
}

// The tick-start fresh step gets the tick's scan completeness: a failed listing
// must not count the newest day as done.
func TestFreshTickStartIncompleteScan(t *testing.T) {
	m, _, clock, _ := freshFixture(t, "2026-06-03")
	*clock = time.Date(2026, 6, 4, 2, 0, 0, 0, time.UTC)
	m.runFresh(context.Background(), map[string][]time.Time{}, false)
	if !m.freshThrough.IsZero() {
		t.Fatalf("incomplete tick scan advanced freshThrough to %s", m.freshThrough)
	}
}

// A replaced cube file must outlive a route-only pod's full reload (one
// ForwardTick) plus a query reading it.
func TestSupersedeGraceOutlivesReload(t *testing.T) {
	c := Config{ForwardTick: 30 * time.Minute}.withDefaults()
	if g := c.supersedeGrace(); g < c.ForwardTick+c.routerRefresh()+5*time.Minute {
		t.Fatalf("grace %s leaves no margin over the %s full reload + refresh + 5m query timeout", g, c.ForwardTick)
	}
	m := testManager(newFakeStorage())
	m.cfg = c
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}}}
	man := m.freshManifest(spec)
	now := time.Now()
	man.Superseded = []SupersededFile{{URI: "s3://arc-test/_arc/rollup/x/old.parquet", At: now.Add(-c.ForwardTick - time.Minute).UnixNano()}}
	m.sweepSuperseded(context.Background(), spec, man, now)
	if len(man.Superseded) != 1 {
		t.Fatal("file deleted one ForwardTick after replacement: a pod may still route to it")
	}
}

// --- Multi-agent review fixes ---------------------------------------------------

// corruptDay replaces a day's only source file with garbage (every read fails).
func corruptDay(t *testing.T, root, date string) (restore func()) {
	t.Helper()
	part := filepath.Join(dayDir(root, date), "part.parquet")
	good, err := os.ReadFile(part)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(part, []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	return func() { _ = os.WriteFile(part, good, 0o644) }
}

// A failing day must not hold back the NEXT day that seals: a newly sealed day is
// attempted at once, whatever the backoff.
func TestFreshNewSealedDayBypassesBackoff(t *testing.T) {
	m, stg, clock, specs := freshFixture(t, "2026-06-02", "2026-06-03")
	ctx := context.Background()
	m.cfg.FreshDays = 1
	corruptDay(t, stg.root, "2026-06-02")
	*clock = time.Date(2026, 6, 3, 1, 0, 0, 0, time.UTC) // 06-02 seals
	m.maybeFresh(ctx)
	if m.freshRetryAt.IsZero() {
		t.Fatal("setup: the failing day should be backing off")
	}
	*clock = time.Date(2026, 6, 4, 1, 0, 0, 0, time.UTC) // 06-03 seals, still inside the backoff? no: but even if it were
	m.freshRetryAt = clock.Add(time.Hour)                // force: backoff still running
	if !m.freshDue() {
		t.Fatal("a newly sealed day must not wait for an older day's backoff")
	}
	m.maybeFresh(ctx)
	for _, s := range specs {
		if entryURI(t, m, s, "2026-06-03") == "" {
			t.Errorf("%s: newly sealed 06-03 not built", CubeID(s))
		}
	}
}

// A persistently failing day is retried with backoff for freshGiveUpAfter, then
// left to the regular pass — not retried every hour while it stays in the window.
func TestFreshGivesUpAfterTwoHours(t *testing.T) {
	m, stg, clock, _ := freshFixture(t, "2026-06-02")
	ctx := context.Background()
	corruptDay(t, stg.root, "2026-06-02")
	start := time.Date(2026, 6, 3, 1, 0, 0, 0, time.UTC)
	*clock = start
	attempts := 0
	for i := 0; i < 20 && m.freshDue(); i++ {
		m.maybeFresh(ctx)
		attempts++
		if !m.freshRetryAt.IsZero() {
			*clock = m.freshRetryAt
		}
	}
	if m.freshDue() || !m.freshThrough.Equal(day("2026-06-02")) {
		t.Fatalf("did not give up: due=%v freshThrough=%s", m.freshDue(), m.freshThrough)
	}
	if elapsed := clock.Sub(start); elapsed < freshGiveUpAfter || attempts > 8 {
		t.Errorf("gave up after %s and %d attempts, want >= %s with few attempts", elapsed, attempts, freshGiveUpAfter)
	}
}

// A storage brownout of a quarter hour around the seal must not cancel the day's
// priority: the next retry after storage recovers builds it.
func TestFreshSurvivesShortBrownout(t *testing.T) {
	m, stg, clock, specs := freshFixture(t, "2026-06-02")
	ctx := context.Background()
	start := time.Date(2026, 6, 3, 1, 0, 0, 0, time.UTC)
	*clock = start
	stg.failList = true
	for i := 0; i < 20 && m.freshDue(); i++ {
		if clock.Sub(start) > 16*time.Minute {
			stg.failList = false
		}
		m.maybeFresh(ctx)
		if !m.freshRetryAt.IsZero() {
			*clock = m.freshRetryAt
		}
	}
	for _, s := range specs {
		if entryURI(t, m, s, "2026-06-02") == "" {
			t.Errorf("%s: day not built after a 16-minute brownout", CubeID(s))
		}
	}
}

// A cube file deleted from storage is rebuilt by the next pass even though the
// source fingerprint is unchanged.
func TestPassRebuildsMissingCubeFile(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 2)
	pass(t, m, stg, dates)
	lost := entryURI(t, m, specs[1], dates[0])
	if err := os.Remove(lost); err != nil {
		t.Fatal(err)
	}
	pass(t, m, stg, dates)
	got := entryURI(t, m, specs[1], dates[0])
	if got == lost {
		t.Fatal("cube file missing from storage was not rebuilt")
	}
	if _, err := os.Stat(got); err != nil {
		t.Fatalf("rebuilt entry points at a missing file: %v", err)
	}
	if entryURI(t, m, specs[0], dates[0]) == "" || cubeCount(t, m, specs[1], dates[0]) != sourceCount(t, m, stg.root, dates[0]) {
		t.Error("rebuilt cube day has wrong data")
	}
}

// Every source file of a built day deleted (retention, a whole-day delete): the
// cubes keep the day — rollups outlive raw retention. A cube that never built the
// day gets an empty marker instead of retrying it forever.
func TestPassKeepsCubesWhenWholeDayDeleted(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 2)
	pass(t, m, stg, dates)
	d := dates[0]
	want := cubeCount(t, m, specs[0], d)
	if err := os.Remove(filepath.Join(dayDir(stg.root, d), "part.parquet")); err != nil {
		t.Fatal(err)
	}
	if w := pass(t, m, stg, dates); w != 0 {
		t.Errorf("pass over a day whose files were all deleted wrote %d manifests, want 0", w)
	}
	for _, s := range specs {
		if got := cubeCount(t, m, s, d); got != want {
			t.Errorf("%s: cube count %d after the raw day was deleted, want the kept %d", CubeID(s), got, want)
		}
	}
	// A cube that never built the (now empty) day records it as empty.
	extra := CubeSpec{Source: freshSrc, Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Col: "value"}}}
	mkCubeDir(t, stg.root, extra)
	man, _ := m.loadManifest(context.Background(), extra)
	cb := m.newCubeBuild(extra, man, nil, time.Time{})
	m.tickBuilt = nil
	m.buildExactDays(context.Background(), freshSrc, []*cubeBuild{cb}, []time.Time{day(d)}, time.Time{}, needPass)
	if got := cb.man.DailySourceSig(d); got == "" || !cb.built[d] {
		t.Errorf("unbuilt empty day: want an empty marker with a fingerprint, got sig %q built=%v", got, cb.built[d])
	}
}

// A month cube reads exactly the days it covers: a day partition that appears
// after the scan is not folded in unlisted (it would double-count with the loose
// daily built for it next tick).
func TestMonthBuildReadsOnlyCoveredDays(t *testing.T) {
	m, stg, _, _ := freshFixture(t, "2026-07-03", "2026-07-04", "2026-07-05")
	ctx := context.Background()
	spec := CubeSpec{Source: freshSrc, Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}}}
	mkCubeDir(t, stg.root, spec)
	sealed := []time.Time{day("2026-07-04"), day("2026-07-03")} // the scan did not see 07-05 yet
	cols := map[string]string{"time": "TIMESTAMP WITH TIME ZONE", "site": "VARCHAR"}
	entry, err := m.buildMonth(spec, "2026-07", m.monthSourceOf(ctx, freshSrc, "2026-07", sealed), cols)
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := m.db.QueryRow("SELECT sum(_cnt)::BIGINT FROM read_parquet('" + entry.URI + "')").Scan(&n); err != nil {
		t.Fatal(err)
	}
	want := sourceCount(t, m, stg.root, "2026-07-03") + sourceCount(t, m, stg.root, "2026-07-04")
	if n != want || fmt.Sprint(entry.Covers) != "[2026-07-03 2026-07-04]" {
		t.Errorf("month cube rows=%d covers=%v, want rows=%d covering only 07-03/07-04", n, entry.Covers, want)
	}
}

// A covered day whose folder holds no Parquet file must not fail the month (a
// glob over it errors "No files found" every tick, leaving the month uncovered).
func TestMonthBuildSkipsEmptyDayFolder(t *testing.T) {
	m, stg, _, _ := freshFixture(t, "2026-07-03", "2026-07-04")
	ctx := context.Background()
	spec := CubeSpec{Source: freshSrc, Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}}}
	mkCubeDir(t, stg.root, spec)
	if err := os.MkdirAll(dayDir(stg.root, "2026-07-05"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dayDir(stg.root, "2026-07-05"), "_SUCCESS"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sealed := []time.Time{day("2026-07-05"), day("2026-07-04"), day("2026-07-03")}
	cols := map[string]string{"time": "TIMESTAMP WITH TIME ZONE", "site": "VARCHAR"}
	entry, err := m.buildMonth(spec, "2026-07", m.monthSourceOf(ctx, freshSrc, "2026-07", sealed), cols)
	if err != nil {
		t.Fatalf("month with an empty day folder failed: %v", err)
	}
	if fmt.Sprint(entry.Covers) != "[2026-07-03 2026-07-04 2026-07-05]" || entry.Rows == 0 {
		t.Errorf("covers=%v rows=%d, want all three days covered with the two days' rows", entry.Covers, entry.Rows)
	}
	// Every day empty: a zero-row entry (persisted as an empty marker), not an error.
	empty := m.monthSourceOf(ctx, freshSrc, "2026-07", []time.Time{day("2026-07-05")})
	if e, err := m.buildMonth(spec, "2026-07", empty, cols); err != nil || e.Rows != 0 || len(e.Covers) != 1 {
		t.Errorf("all-empty month: entry=%+v err=%v, want zero rows covering 07-05", e, err)
	}
}

// With tiered storage, an empty hot listing may mean the files moved to the cold
// tier: never treat it as an empty day.
func TestTieredStorageEmptyListingIsUnknown(t *testing.T) {
	m, stg, _, _ := passFixture(t, 1)
	m.cfg.TieredStorage = true
	if err := os.MkdirAll(dayDir(stg.root, "2001-01-01"), 0o755); err != nil {
		t.Fatal(err)
	}
	if ds := m.listDaySource(context.Background(), freshSrc, day("2001-01-01")); ds.ok || ds.empty {
		t.Errorf("tiering on: empty hot listing must be unknown, got %+v", ds)
	}
}

// File names DuckDB would expand as globs are never put in the exact list.
func TestFingerprintFallsBackOnGlobLikeNames(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 1)
	d := dates[0]
	addSourceFile(t, m, stg.root, d, "part*.parquet", fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 11:00:00' AS "time", 'tv' AS site`, d))
	if ds := m.listDaySource(context.Background(), freshSrc, day(d)); ds.ok {
		t.Fatalf("a '*' in a file name must fall back to the day glob, got %+v", ds)
	}
	pass(t, m, stg, dates)
	for _, s := range specs {
		if got, want := cubeCount(t, m, s, d), sourceCount(t, m, stg.root, d); got != want {
			t.Errorf("%s: count %d, want %d (a file read twice?)", CubeID(s), got, want)
		}
	}
}

// Objects outside the day prefix are never read into the day's cube.
type outsideListStorage struct{ *localListStorage }

func (s outsideListStorage) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objs, err := s.localListStorage.ListObjects(ctx, prefix)
	return append(objs, storage.ObjectInfo{Path: "default/events/2001/01/01/stray.parquet", Size: 1}), err
}

func TestFingerprintIgnoresObjectsOutsidePrefix(t *testing.T) {
	m, stg, _, dates := passFixture(t, 1)
	m.stg = outsideListStorage{stg}
	ds := m.listDaySource(context.Background(), freshSrc, day(dates[0]))
	if !ds.ok || strings.Contains(ds.expr, "stray") || !strings.HasPrefix(ds.sig, "1-") {
		t.Errorf("an object outside the day prefix leaked into the day source: %+v", ds)
	}
}

// MaxDaysPerTick caps days BUILT: unchanged in-window days must not use up the
// cap and starve unbuilt days.
func TestMaxDaysPerTickCountsBuiltDaysOnly(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 5)
	m.cfg.MaxDaysPerTick = 3
	pass(t, m, stg, dates)
	pass(t, m, stg, dates)
	for _, s := range specs {
		for _, d := range dates {
			if entryURI(t, m, s, d) == "" {
				t.Errorf("%s %s: still unbuilt after two passes with MaxDaysPerTick=3 over 5 days", CubeID(s), d)
			}
		}
	}
}

// A replaced cube file's deletion grace starts when a manifest write that drops
// it actually lands, not at an earlier failed attempt.
func TestSupersedeGraceStartsAtLandedWrite(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 1)
	d := dates[0]
	pass(t, m, stg, dates)
	key := m.manifestKey(specs[1])
	stg.failWrite = func(p string) error {
		if p == key {
			return fmt.Errorf("503 SlowDown")
		}
		return nil
	}
	addSourceFile(t, m, stg.root, d, "late.parquet", fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 22:00:00' AS "time", 'web' AS site`, d))
	man, _ := m.loadManifest(context.Background(), specs[1])
	cb := m.newCubeBuild(specs[1], man, nil, time.Time{})
	m.tickBuilt = nil
	m.buildExactDays(context.Background(), freshSrc, []*cubeBuild{cb}, []time.Time{day(d)}, time.Time{}, needPass)
	failedAt := time.Now()
	time.Sleep(20 * time.Millisecond)
	stg.failWrite = nil
	landedBefore := time.Now()
	if err := m.writeManifest(context.Background(), specs[1], cb.man); err != nil {
		t.Fatal(err)
	}
	reread, _ := m.loadManifest(context.Background(), specs[1])
	if len(reread.Superseded) != 1 {
		t.Fatalf("want 1 parked file, got %d", len(reread.Superseded))
	}
	if at := time.Unix(0, reread.Superseded[0].At); at.Before(landedBefore) {
		t.Errorf("grace starts at %s (failed write ~%s), want >= the landed write %s", at.Format(time.StampMilli), failedAt.Format(time.StampMilli), landedBefore.Format(time.StampMilli))
	}
}

// A route-only reload that is not clean forgets the generation, so the next
// refresh retries instead of keeping stale manifests for a whole ForwardTick.
func TestRouteOnlyUncleanReloadRetries(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	stg := newFakeStorage()
	builder := testManager(stg)
	reader := driftManager(t, db, t.TempDir(), stg) // its glob of s3://unused fails: every reload is unclean
	builder.genDirty = true
	builder.maybeBumpGeneration(context.Background(), true)
	now := time.Now()
	reader.lastCleanReload = now
	reader.routeOnlyRefresh(context.Background(), now)
	if reader.lastGeneration != "" {
		t.Fatalf("unclean reload recorded generation %q", reader.lastGeneration)
	}
	if !reader.lastCleanReload.Equal(now) {
		t.Error("unclean reload counted as clean")
	}
	if _, changed := reader.generationChanged(context.Background()); !changed {
		t.Error("after an unclean reload the next refresh must retry")
	}
}

// Source gone AND cube file gone: nothing is left to keep, so the entry is
// dropped (an empty marker) instead of routing queries to a missing file.
func TestEmptySourceAndMissingCubeFileDropsEntry(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 1)
	pass(t, m, stg, dates)
	d := dates[0]
	if err := os.Remove(entryURI(t, m, specs[1], d)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dayDir(stg.root, d), "part.parquet")); err != nil {
		t.Fatal(err)
	}
	pass(t, m, stg, dates)
	man, _ := m.loadManifest(context.Background(), specs[1])
	lo, hi := d+" 00:00:00", day(d).AddDate(0, 0, 1).Format("2006-01-02")+" 00:00:00"
	if got := man.DaysInRange(lo, hi); len(got) != 0 {
		t.Errorf("manifest still routes %s to %v although source and cube file are gone", d, uris(got))
	}
	if got := entryURI(t, m, specs[0], d); got == "" {
		t.Error("the other cube (file intact) must keep its day")
	}
}
