package rollup

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// driftManager wires a Manager to a local DuckDB and a local uriRoot directory,
// so the real build pipeline (month/day builds, manifest persistence) runs
// against local Parquet — the same pattern as month_drift_test.go but at the
// Manager level.
func driftManager(t *testing.T, db *sql.DB, root string, stg Storage) *Manager {
	t.Helper()
	return &Manager{
		db: db, stg: stg, log: zerolog.New(io.Discard), cfg: Config{}.withDefaults(),
		s3: S3Params{Bucket: "unused"}, uriRoot: root,
		manifests: map[string]*Manifest{}, profiles: map[string]TableProfile{},
		dimRichBailed: map[string]bool{}, workload: NewWorkload(),
	}
}

// writeSourceDay writes one source parquet under <root>/default/events/YYYY/MM/DD
// with the given SELECT body (must yield a "time" column).
func writeSourceDay(t *testing.T, db *sql.DB, root, day, selectBody string) {
	t.Helper()
	dir := filepath.Join(root, "default", "events", day)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", selectBody, filepath.Join(dir, "part.parquet")))
}

// mkCubeDir pre-creates the cube output folder (local COPY does not mkdir).
func mkCubeDir(t *testing.T, root string, spec CubeSpec) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "_arc/rollup", cubeDir(spec)), 0o755); err != nil {
		t.Fatal(err)
	}
}

// TestMonthDriftBuildsDurably (F4) — a month whose source lacks a cube dimension
// must still BUILD (NULL dim) and persist normal manifest coverage, so the month
// never re-enters the pending set after a restart and leaves no coverage hole.
// The old markMonthResolved was in-memory only: every reconstruction of the build
// state (each tick rebuilds cubeBuild from the manifest) saw the month pending
// again — a per-tick full-month probe storm and a permanent router coverage hole.
func TestMonthDriftBuildsDurably(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	root := t.TempDir()
	stg := newFakeStorage()
	m := driftManager(t, db, root, stg)
	ctx := context.Background()

	// April source has time+site only — the "email" dim did not exist yet.
	writeSourceDay(t, db, root, "2026/04/10",
		`SELECT TIMESTAMPTZ '2026-04-10 01:30:00' AS "time", 'web' AS site
		 UNION ALL SELECT TIMESTAMPTZ '2026-04-10 02:15:00', 'ios'`)
	writeSourceDay(t, db, root, "2026/04/11",
		`SELECT TIMESTAMPTZ '2026-04-11 09:00:00' AS "time", 'web' AS site
		 UNION ALL SELECT TIMESTAMPTZ '2026-04-11 09:30:00', 'web'`)

	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"email"}, Aggs: []Aggregate{{Kind: AggCount}}}
	mkCubeDir(t, root, spec)
	sealed := []time.Time{
		time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC), // newest first, like buildSource
		time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC),
	}
	rebuildFloor := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	man, ok := m.loadManifest(ctx, spec)
	if !ok {
		t.Fatal("fresh manifest load failed")
	}
	cb := &cubeBuild{spec: spec, man: man, built: man.BuiltDays(),
		monthBuild: cleanFullySealedMonths(man.BuiltDays(), sealed, rebuildFloor)}
	if !cb.monthBuild["2026-04"] {
		t.Fatal("setup: 2026-04 should be a pending month build")
	}

	m.buildExactMonths(ctx, "default.events", []*cubeBuild{cb}, sealed)

	// Durability: reconstruct the build state from the PERSISTED manifest, exactly
	// as the next tick (or a restart) does in buildSource.
	man2, ok := m.loadManifest(ctx, spec)
	if !ok {
		t.Fatal("manifest reload failed")
	}
	built := man2.BuiltDays()
	if !built["2026-04-10"] || !built["2026-04-11"] {
		t.Fatalf("F4: drifted month must persist durable coverage; BuiltDays=%v (month would be re-probed EVERY tick forever)", built)
	}
	if pending := cleanFullySealedMonths(built, sealed, rebuildFloor); pending["2026-04"] {
		t.Fatal("F4: 2026-04 re-entered the pending month-build set after manifest reload")
	}
	// No interior coverage hole over the span (router-level guarantee).
	if man2.HasInteriorGap("2026-04-10 00:00:00", "2026-04-12 00:00:00") {
		t.Fatal("F4: drifted month left an interior coverage gap")
	}
	// The cube file itself must carry the full schema: a NULL email dim, real counts.
	days := man2.DaysInRange("2026-04-10 00:00:00", "2026-04-12 00:00:00")
	if len(days) != 1 {
		t.Fatalf("want 1 monthly cube file, got %v", uris(days))
	}
	var cnt int64
	var emailNulls int64
	if err := db.QueryRow("SELECT sum(_cnt)::BIGINT, count(*) FILTER (email IS NULL) FROM read_parquet('"+days[0].URI+"')").Scan(&cnt, &emailNulls); err != nil {
		t.Fatalf("read cube: %v", err)
	}
	if cnt != 4 {
		t.Fatalf("cube _cnt total = %d, want 4", cnt)
	}
	if emailNulls == 0 {
		t.Fatal("expected NULL email dim groups in the drifted month's cube")
	}
}

// TestDayDriftBuildsAndPersists (F5) — a sealed day whose source lacks a cube's
// dimension must build (NULL dim) and be marked built in the manifest, instead of
// being skipped after the full-day temp-table scan EVERY tick forever.
func TestDayDriftBuildsAndPersists(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	root := t.TempDir()
	stg := newFakeStorage()
	m := driftManager(t, db, root, stg)
	ctx := context.Background()

	writeSourceDay(t, db, root, "2026/06/01",
		`SELECT TIMESTAMPTZ '2026-06-01 10:00:00' AS "time", 'web' AS site
		 UNION ALL SELECT TIMESTAMPTZ '2026-06-01 11:00:00', 'ios'`)

	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"email"}, Aggs: []Aggregate{{Kind: AggCount}}}
	mkCubeDir(t, root, spec)
	day := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	sealed := []time.Time{day}
	rebuildFloor := day.AddDate(0, 0, -2)

	man, _ := m.loadManifest(ctx, spec)
	cb := &cubeBuild{spec: spec, man: man, built: man.BuiltDays(), monthBuild: map[string]bool{}}
	m.buildExactDays(ctx, "default.events", []*cubeBuild{cb}, sealed, rebuildFloor)

	man2, _ := m.loadManifest(ctx, spec)
	if !man2.BuiltDays()["2026-06-01"] {
		t.Fatalf("F5: drift day must be built+persisted (NULL dim), got BuiltDays=%v — the day would be rescanned every tick forever", man2.BuiltDays())
	}
}

// TestZeroRowDayPersistsCoverage (F5) — a day whose cube build yields zero rows
// must still persist a coverage marker (like purgeMissingDailies' '-empty'
// markers); dropping it leaves the day permanently "unbuilt", so the full-day
// source scan repeats every tick.
func TestZeroRowDayPersistsCoverage(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	root := t.TempDir()
	stg := newFakeStorage()
	m := driftManager(t, db, root, stg)
	ctx := context.Background()

	// The day's files exist but hold rows OUTSIDE the UTC day window (e.g. a
	// misplaced partition), so the day build aggregates zero rows.
	writeSourceDay(t, db, root, "2026/06/02",
		`SELECT TIMESTAMPTZ '2026-07-15 10:00:00' AS "time", 'web' AS site`)

	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}}}
	mkCubeDir(t, root, spec)
	day := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
	man, _ := m.loadManifest(ctx, spec)
	cb := &cubeBuild{spec: spec, man: man, built: man.BuiltDays(), monthBuild: map[string]bool{}}
	m.buildExactDays(ctx, "default.events", []*cubeBuild{cb}, []time.Time{day}, day.AddDate(0, 0, -2))

	man2, _ := m.loadManifest(ctx, spec)
	if !man2.BuiltDays()["2026-06-02"] {
		t.Fatalf("F5: zero-row day must persist a coverage marker, got BuiltDays=%v", man2.BuiltDays())
	}
	// The marker is pure coverage metadata — it must not leak into file selection.
	if got := man2.DaysInRange("2026-06-02 00:00:00", "2026-06-03 00:00:00"); len(got) != 0 {
		t.Fatalf("zero-row marker leaked into DaysInRange: %v", uris(got))
	}
}

// TestMonthProbeFailureBounded (F7) — when the month schema probe fails (e.g. an
// S3 outage), the failure must be cached for the tick (one attempt per distinct
// month, not per cube x month) and failed attempts must consume the per-tick
// budget (CompactMaxPerTick) — otherwise N cubes x M months hammer the failing
// backend every tick with unbounded probe requests.
func TestMonthProbeFailureBounded(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	root := t.TempDir() // NO source files: every month probe fails
	stg := newFakeStorage()

	months := map[string]bool{"2026-01": true, "2026-02": true, "2026-03": true}
	newCubes := func() []*cubeBuild {
		var cubes []*cubeBuild
		for _, dim := range []string{"a", "b"} {
			spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{dim}, Aggs: []Aggregate{{Kind: AggCount}}}
			mb := map[string]bool{}
			for ym := range months {
				mb[ym] = true
			}
			cubes = append(cubes, &cubeBuild{spec: spec, man: &Manifest{}, built: map[string]bool{}, monthBuild: mb})
		}
		return cubes
	}

	// (a) failures are cached per tick: attempts <= distinct months, not cubes x months.
	m := driftManager(t, db, root, stg)
	probes := 0
	m.monthProbeHook = func(string, string) { probes++ }
	m.buildExactMonths(context.Background(), "default.events", newCubes(), nil)
	if probes > len(months) {
		t.Fatalf("F7: %d probe attempts in one tick for %d distinct months x 2 cubes — failed probes must be cached per tick", probes, len(months))
	}

	// (b) failed attempts count toward the per-tick budget.
	m2 := driftManager(t, db, root, stg)
	m2.cfg.CompactMaxPerTick = 2
	probes2 := 0
	m2.monthProbeHook = func(string, string) { probes2++ }
	m2.buildExactMonths(context.Background(), "default.events", newCubes(), nil)
	if probes2 > 2 {
		t.Fatalf("F7: %d probe attempts with CompactMaxPerTick=2 — failed attempts must consume the per-tick budget", probes2)
	}
}
