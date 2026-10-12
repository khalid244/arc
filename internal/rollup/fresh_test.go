package rollup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Fresh-day priority (fresh.go): the newest sealed days are built ahead of the
// full pass, as soon as they seal — even mid-pass — so auto-mode queries stop
// raw-scanning a whole extra day while a long re-roll or month backfill runs.

const freshSrc = "default.events"

func day(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

// localListStorage is fakeStorage plus real directory listing under root, so
// Manager.scan discovers the local source partitions the way it walks S3. The
// hooks observe writes and inject failures.
type localListStorage struct {
	*fakeStorage
	root      string
	onWrite   func(path string)
	failWrite func(path string) error
	failList  bool
	lists     int                 // ListObjects calls
	afterList func(prefix string) // runs right after a listing (a file landing mid-build)
}

func (s *localListStorage) ListDirectories(_ context.Context, prefix string) ([]string, error) {
	if s.failList {
		return nil, errors.New("SlowDown: reduce your request rate")
	}
	ents, err := os.ReadDir(filepath.Join(s.root, prefix))
	if err != nil {
		return nil, nil
	}
	var out []string
	for _, e := range ents {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// ListObjects walks root/prefix recursively, like an S3 listing without a
// delimiter (paths relative to root, as the S3 backend returns them).
func (s *localListStorage) ListObjects(_ context.Context, prefix string) ([]storage.ObjectInfo, error) {
	s.lists++
	if s.failList {
		return nil, errors.New("SlowDown: reduce your request rate")
	}
	var out []storage.ObjectInfo
	base := filepath.Join(s.root, prefix)
	_ = filepath.WalkDir(base, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		fi, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		rel, _ := filepath.Rel(s.root, p)
		out = append(out, storage.ObjectInfo{Path: filepath.ToSlash(rel), Size: fi.Size(), LastModified: fi.ModTime()})
		return nil
	})
	if s.afterList != nil {
		s.afterList(prefix)
	}
	return out, nil
}

func (s *localListStorage) Write(ctx context.Context, p string, d []byte) error {
	if s.failWrite != nil {
		if err := s.failWrite(p); err != nil {
			return err
		}
	}
	if err := s.fakeStorage.Write(ctx, p, d); err != nil {
		return err
	}
	if s.onWrite != nil {
		s.onWrite(p)
	}
	return nil
}

func dayDir(root, d string) string {
	return filepath.Join(root, "default", "events", d[:4], d[5:7], d[8:10])
}

// freshFixture writes one small source day per date and returns a Manager with
// a 1h grace, a 30-day rebuild window, the fresh step on (FreshDays=2), a
// settable clock, and every planned cube's output dir created.
func freshFixture(t *testing.T, dates ...string) (*Manager, *localListStorage, *time.Time, []CubeSpec) {
	t.Helper()
	db := openLocalDuck(t)
	t.Cleanup(func() { db.Close() })
	root := t.TempDir()
	for _, d := range dates {
		ts := d + " 10:00:00"
		writeSourceDay(t, db, root, d[:4]+"/"+d[5:7]+"/"+d[8:10],
			`SELECT TIMESTAMPTZ '`+ts+`' AS "time", 'web' AS site
			 UNION ALL SELECT TIMESTAMPTZ '`+ts+`' + INTERVAL 1 HOUR, 'ios'`)
	}
	stg := &localListStorage{fakeStorage: newFakeStorage(), root: root}
	m := driftManager(t, db, root, stg)
	m.cfg.Grace = time.Hour
	m.cfg.RebuildDays = 30
	m.cfg.FreshDays = 2
	clock := new(time.Time)
	m.nowFn = func() time.Time { return *clock }
	specs, err := m.planSpecs(freshSrc)
	if err != nil {
		t.Fatalf("planSpecs: %v", err)
	}
	if len(specs) < 2 {
		t.Fatalf("setup: want >=2 cubes (coarse + by_site), got %d", len(specs))
	}
	for _, s := range specs {
		if specHasSketch(s) {
			t.Fatalf("setup: sketch cube %s would need the subprocess builder", CubeID(s))
		}
		mkCubeDir(t, root, s)
	}
	return m, stg, clock, specs
}

// entryURI returns the persisted manifest's file URI for date ("" if absent).
func entryURI(t *testing.T, m *Manager, spec CubeSpec, date string) string {
	t.Helper()
	man, ok := m.loadManifest(context.Background(), spec)
	if !ok {
		t.Fatalf("load manifest %s", CubeID(spec))
	}
	for _, d := range man.Days {
		if d.Date == date {
			return d.URI
		}
	}
	return ""
}

// heldBuilds loads every cube as buildSource does and registers it as held.
func heldBuilds(t *testing.T, m *Manager, specs []CubeSpec) []*cubeBuild {
	t.Helper()
	m.held = map[string]*cubeBuild{}
	var cbs []*cubeBuild
	for _, s := range specs {
		man, ok := m.loadManifest(context.Background(), s)
		if !ok {
			t.Fatalf("load manifest %s", CubeID(s))
		}
		cb := m.newCubeBuild(s, man, nil, time.Time{})
		m.held[cubeKeyOf(s)] = cb
		cbs = append(cbs, cb)
	}
	return cbs
}

func TestNewestSealedDay(t *testing.T) {
	cases := []struct {
		now   string
		grace time.Duration
		want  string
	}{
		{"2026-10-12T00:59:59Z", time.Hour, "2026-10-10"}, // Oct 11 + 1h grace not reached yet
		{"2026-10-12T01:00:00Z", time.Hour, "2026-10-11"}, // exactly sealed
		{"2026-10-12T15:40:00Z", time.Hour, "2026-10-11"},
		{"2026-10-12T05:59:00Z", 6 * time.Hour, "2026-10-10"},
		{"2026-10-12T06:00:00Z", 6 * time.Hour, "2026-10-11"},
	}
	for _, c := range cases {
		now, _ := time.Parse(time.RFC3339, c.now)
		if got := newestSealedDay(now, c.grace).Format("2006-01-02"); got != c.want {
			t.Errorf("newestSealedDay(%s, %s) = %s, want %s", c.now, c.grace, got, c.want)
		}
	}
}

// TestFreshBuildsOnlyMissingNewestDays — the fresh step builds the FreshDays
// newest sealed days, only for cubes that lack them: an already-built day is not
// re-rolled (that is the full pass's job), and older days are left alone.
func TestFreshBuildsOnlyMissingNewestDays(t *testing.T) {
	m, _, clock, specs := freshFixture(t, "2026-06-01", "2026-06-02", "2026-06-03")
	ctx := context.Background()
	coarse, bySite := specs[0], specs[1]

	// Coarse already has 06-02 (built earlier); by_site has nothing.
	*clock = time.Date(2026, 6, 3, 2, 0, 0, 0, time.UTC)
	m.freshThrough, m.freshAttempted = day("2026-06-02"), day("2026-06-02") // that day's fresh step already ran; keep the setup build plain
	man, _ := m.loadManifest(ctx, coarse)
	cb := m.newCubeBuild(coarse, man, nil, time.Time{})
	m.buildExactDays(ctx, freshSrc, []*cubeBuild{cb}, []time.Time{day("2026-06-02")}, time.Time{}, needMissing)
	pre := entryURI(t, m, coarse, "2026-06-02")
	if pre == "" {
		t.Fatal("setup: coarse 06-02 not built")
	}

	*clock = time.Date(2026, 6, 4, 2, 0, 0, 0, time.UTC) // 06-03 sealed at 06-04 01:00
	m.runFresh(ctx, map[string][]time.Time{freshSrc: {day("2026-06-01"), day("2026-06-02"), day("2026-06-03")}}, true)

	if got := entryURI(t, m, coarse, "2026-06-02"); got != pre {
		t.Errorf("coarse 06-02 was rebuilt by the fresh step (%s -> %s); it only fills missing days", pre, got)
	}
	for _, s := range []CubeSpec{coarse, bySite} {
		if entryURI(t, m, s, "2026-06-03") == "" {
			t.Errorf("%s: newest sealed day 06-03 not built", CubeID(s))
		}
		if entryURI(t, m, s, "2026-06-01") != "" {
			t.Errorf("%s: 06-01 is outside the fresh window (FreshDays=2) but was built", CubeID(s))
		}
	}
	if entryURI(t, m, bySite, "2026-06-02") == "" {
		t.Error("by_site: missing 06-02 is inside the fresh window but was not built")
	}
	if !m.freshThrough.Equal(day("2026-06-03")) {
		t.Errorf("freshThrough = %s, want 2026-06-03", m.freshThrough)
	}
	if _, err := m.stg.Read(ctx, m.generationKey()); err != nil {
		t.Error("a fresh step that built cubes must bump the generation for route-only pods")
	}
}

// TestFreshDueOncePerSealedDay — the trigger is a pure clock check against the
// seal boundary: once per newly sealed day, never re-fired for the same day.
func TestFreshDueOncePerSealedDay(t *testing.T) {
	m, _, clock, _ := freshFixture(t, "2026-06-03")
	ctx := context.Background()

	*clock = time.Date(2026, 6, 4, 2, 0, 0, 0, time.UTC)
	if !m.freshDue() {
		t.Fatal("first check after start must be due")
	}
	m.runFresh(ctx, map[string][]time.Time{freshSrc: {day("2026-06-03")}}, true)
	if m.freshDue() {
		t.Fatal("due again for the same sealed day")
	}
	*clock = time.Date(2026, 6, 5, 0, 59, 0, 0, time.UTC) // 06-04 not sealed until 01:00
	if m.freshDue() {
		t.Fatal("due before 06-04 sealed")
	}
	*clock = time.Date(2026, 6, 5, 1, 0, 0, 0, time.UTC)
	if !m.freshDue() {
		t.Fatal("not due after 06-04 sealed")
	}
	m.cfg.FreshDays = -1
	if m.freshDue() {
		t.Fatal("FreshDays<0 must disable the fresh step")
	}
}

// TestFreshSharesHeldManifests — when the fresh step fires mid-pass it must write
// through the pass's in-memory manifests. A separately loaded copy would be
// overwritten by the pass's next persist, silently dropping the fresh day. Days
// the fresh step built are then skipped by the rest of the pass.
func TestFreshSharesHeldManifests(t *testing.T) {
	m, _, clock, specs := freshFixture(t, "2026-06-01", "2026-06-03")
	ctx := context.Background()
	*clock = time.Date(2026, 6, 4, 2, 0, 0, 0, time.UTC)
	m.tickBuilt = map[string]bool{}
	cbs := heldBuilds(t, m, specs)

	m.runFresh(ctx, map[string][]time.Time{freshSrc: {day("2026-06-01"), day("2026-06-03")}}, true)
	fresh := map[string]string{}
	for _, s := range specs {
		fresh[CubeID(s)] = entryURI(t, m, s, "2026-06-03")
	}

	// The pass carries on with its own (pre-computed) work: 06-03 and 06-01.
	m.buildExactDays(ctx, freshSrc, cbs, []time.Time{day("2026-06-03"), day("2026-06-01")}, time.Time{}, needPass)

	for _, s := range specs {
		got := entryURI(t, m, s, "2026-06-03")
		if got == "" {
			t.Fatalf("%s: fresh 06-03 entry lost after the pass persisted its manifest", CubeID(s))
		}
		if got != fresh[CubeID(s)] {
			t.Errorf("%s: 06-03 rebuilt by the same pass after the fresh step built it", CubeID(s))
		}
		if entryURI(t, m, s, "2026-06-01") == "" {
			t.Errorf("%s: pass did not build 06-01", CubeID(s))
		}
	}
}

// TestFreshFiresBetweenPassDays — a day that seals while the full pass is busy
// re-rolling older days is built at the next day boundary, not after the pass.
func TestFreshFiresBetweenPassDays(t *testing.T) {
	m, stg, clock, specs := freshFixture(t, "2026-05-30", "2026-05-31", "2026-06-01")
	ctx := context.Background()
	m.tickBuilt = map[string]bool{}
	*clock = time.Date(2026, 6, 1, 2, 0, 0, 0, time.UTC)                    // newest sealed = 05-31
	m.freshThrough, m.freshAttempted = day("2026-05-31"), day("2026-05-31") // its fresh step already ran

	coarse := specs[0]
	man, _ := m.loadManifest(ctx, coarse)
	cb := m.newCubeBuild(coarse, man, nil, time.Time{})
	m.held = map[string]*cubeBuild{cubeKeyOf(coarse): cb}

	// 06-01 seals (clock passes 06-02 01:00) right after the pass's first day lands.
	writes := 0
	stg.onWrite = func(p string) {
		if filepath.Base(p) == "manifest.json" {
			if writes++; writes == 1 {
				*clock = time.Date(2026, 6, 2, 1, 30, 0, 0, time.UTC)
			}
		}
	}
	m.buildExactDays(ctx, freshSrc, []*cubeBuild{cb}, []time.Time{day("2026-05-31"), day("2026-05-30")}, time.Time{}, needPass)

	if entryURI(t, m, coarse, "2026-06-01") == "" {
		t.Fatal("06-01 sealed mid-pass but the fresh step did not build it")
	}
	if entryURI(t, m, coarse, "2026-05-30") == "" {
		t.Fatal("pass did not finish its own days after the fresh step")
	}
	if !m.freshThrough.Equal(day("2026-06-01")) {
		t.Errorf("freshThrough = %s, want 2026-06-01", m.freshThrough)
	}
}

// TestFreshThroughRealBuildSource — the real pass path: buildSource holds every
// cube it loads and (May being older than the rebuild floor on the real clock)
// takes the one-file month path; the fresh step fires between those month builds
// and every cube ends with each day covered by exactly one entry (no lost update,
// no loose daily overlapping the month).
func TestFreshThroughRealBuildSource(t *testing.T) {
	m, stg, clock, specs := freshFixture(t, "2026-05-29", "2026-05-30", "2026-05-31", "2026-06-01")
	ctx := context.Background()
	m.tickBuilt = map[string]bool{}
	*clock = time.Date(2026, 6, 1, 2, 0, 0, 0, time.UTC)
	m.freshThrough, m.freshAttempted = day("2026-05-31"), day("2026-05-31") // its fresh step already ran

	writes := 0
	stg.onWrite = func(p string) {
		if filepath.Base(p) == "manifest.json" {
			if writes++; writes == 1 {
				*clock = time.Date(2026, 6, 2, 1, 30, 0, 0, time.UTC) // 06-01 seals mid-pass
			}
		}
	}
	// buildSource's own seal check uses the real clock; give it the days the
	// pass saw at its start (06-01 was not sealed yet).
	if err := m.buildSource(ctx, freshSrc, []time.Time{day("2026-05-29"), day("2026-05-30"), day("2026-05-31")}); err != nil {
		t.Fatalf("buildSource: %v", err)
	}
	if m.held != nil {
		t.Error("buildSource must release held cubes when it returns")
	}
	for _, s := range specs {
		man, _ := m.loadManifest(ctx, s)
		count := map[string]int{}
		for _, d := range man.Days {
			if len(d.Covers) == 0 {
				count[d.Date]++
			}
			for _, c := range d.Covers {
				count[c]++
			}
		}
		for _, d := range []string{"2026-05-29", "2026-05-30", "2026-05-31", "2026-06-01"} {
			if count[d] != 1 {
				t.Errorf("%s: %s has %d entries, want exactly 1 (days=%v)", CubeID(s), d, count[d], count)
			}
		}
	}
}

// TestFreshRetriesAfterFailure — a run that leaves a day unbuilt does not count:
// the day is retried with backoff (not only at the next sealed day), and the
// backoff doubles so a persistent failure does not retry at every build unit.
func TestFreshRetriesAfterFailure(t *testing.T) {
	m, stg, clock, specs := freshFixture(t, "2026-06-03")
	ctx := context.Background()
	*clock = time.Date(2026, 6, 4, 2, 0, 0, 0, time.UTC)

	// 1) Listing fails (S3 SlowDown): nothing discovered, so nothing may count as done.
	stg.failList = true
	m.maybeFresh(ctx)
	if !m.freshThrough.IsZero() {
		t.Fatalf("listing failure advanced freshThrough to %s", m.freshThrough)
	}
	if m.freshDue() {
		t.Fatal("retry must back off, not fire at the very next build unit")
	}
	stg.failList = false

	// 2) After the backoff the day is retried; now the source read fails.
	*clock = clock.Add(freshRetryMin)
	if !m.freshDue() {
		t.Fatal("not due again after the backoff")
	}
	part := filepath.Join(dayDir(stg.root, "2026-06-03"), "part.parquet")
	good, _ := os.ReadFile(part)
	if err := os.WriteFile(part, []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	m.maybeFresh(ctx)
	if !m.freshThrough.IsZero() || entryURI(t, m, specs[0], "2026-06-03") != "" {
		t.Fatal("a failed source scan must leave the day pending")
	}
	if m.freshBackoff != 2*freshRetryMin {
		t.Errorf("backoff = %s, want doubled to %s", m.freshBackoff, 2*freshRetryMin)
	}

	// 3) Source healthy again: the retry builds it and clears the backoff.
	if err := os.WriteFile(part, good, 0o644); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(2 * freshRetryMin)
	m.maybeFresh(ctx)
	for _, s := range specs {
		if entryURI(t, m, s, "2026-06-03") == "" {
			t.Errorf("%s: 06-03 not built on retry", CubeID(s))
		}
	}
	if !m.freshThrough.Equal(day("2026-06-03")) || m.freshBackoff != 0 {
		t.Errorf("after success: freshThrough=%s backoff=%s, want 2026-06-03 and 0", m.freshThrough, m.freshBackoff)
	}
}

// TestPersistWriteFailureNotMarkedBuilt — a fresh build whose manifest write
// fails must not be recorded as built this tick, or the pass would skip the day
// and it would be missing from storage for the whole tick.
func TestPersistWriteFailureNotMarkedBuilt(t *testing.T) {
	m, stg, clock, specs := freshFixture(t, "2026-06-03")
	ctx := context.Background()
	*clock = time.Date(2026, 6, 4, 2, 0, 0, 0, time.UTC)
	m.tickBuilt = map[string]bool{}
	m.cfg.FreshDays = 1
	bySite := specs[1]
	siteManifest := m.manifestKey(bySite)
	stg.failWrite = func(p string) error {
		if p == siteManifest {
			return errors.New("503 SlowDown")
		}
		return nil
	}
	m.runFresh(ctx, map[string][]time.Time{freshSrc: {day("2026-06-03")}}, true)
	if m.tickBuilt[tickBuiltKey(bySite, "2026-06-03")] {
		t.Fatal("a cube-day whose manifest write failed was recorded as built this tick")
	}
	if !m.freshThrough.IsZero() {
		t.Error("a run with a failed manifest write must not count as done")
	}

	// The pass then loads the cube from storage (no 06-03) and must build it.
	stg.failWrite = nil
	man, _ := m.loadManifest(ctx, bySite)
	cb := m.newCubeBuild(bySite, man, nil, time.Time{})
	m.buildExactDays(ctx, freshSrc, []*cubeBuild{cb}, []time.Time{day("2026-06-03")}, time.Time{}, needPass)
	if entryURI(t, m, bySite, "2026-06-03") == "" {
		t.Fatal("pass skipped the day whose fresh manifest write failed")
	}
}

// TestGenerationPolling — route-only pods reload manifests only when the
// builder bumped the generation object.
func TestGenerationPolling(t *testing.T) {
	stg := newFakeStorage()
	builder, reader := testManager(stg), testManager(stg)
	ctx := context.Background()
	if _, changed := reader.generationChanged(ctx); changed {
		t.Fatal("no generation object yet: must not report a change")
	}
	builder.maybeBumpGeneration(ctx, true)
	if _, err := stg.Read(ctx, builder.generationKey()); err == nil {
		t.Fatal("bumped although no manifest was written")
	}
	builder.genDirty = true // a manifest write
	builder.maybeBumpGeneration(ctx, true)
	gen, changed := reader.generationChanged(ctx)
	if !changed {
		t.Fatal("bump not seen")
	}
	if _, again := reader.generationChanged(ctx); !again {
		t.Fatal("a generation must be reported until a clean reload records it")
	}
	reader.lastGeneration = gen // what runRouteOnly does after a clean reload
	if _, changed := reader.generationChanged(ctx); changed {
		t.Fatal("same generation reported after it was recorded")
	}
	// Non-forced bumps are rate-limited to one per RouterRefresh.
	builder.genDirty = true
	builder.maybeBumpGeneration(ctx, false)
	if _, changed := reader.generationChanged(ctx); changed {
		t.Fatal("non-forced bump within RouterRefresh of the last one must wait")
	}
	builder.lastBump = time.Now().Add(-builder.cfg.routerRefresh())
	builder.maybeBumpGeneration(ctx, false)
	if _, changed := reader.generationChanged(ctx); !changed {
		t.Fatal("non-forced bump after RouterRefresh not seen")
	}
}

func TestRouterRefreshClampedToForwardTick(t *testing.T) {
	c := Config{ForwardTick: 6 * time.Hour}.withDefaults()
	if got := c.routerRefresh(); got != 5*time.Minute {
		t.Errorf("default refresh = %s, want 5m", got)
	}
	c = Config{ForwardTick: 2 * time.Minute, RouterRefresh: 10 * time.Minute}.withDefaults()
	if got := c.routerRefresh(); got != 2*time.Minute {
		t.Errorf("refresh = %s, want clamp to ForwardTick 2m (superseded files are deleted one ForwardTick later)", got)
	}
}
