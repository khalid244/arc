package rollup

import (
	"context"
	"sort"
	"strconv"
	"time"
)

// Fresh-day priority.
//
// A builder pass walks every cube over the last RebuildDays days. A pass that has
// real work — a restart that backfills new cubes' months, a schema change, the
// first pass after source fingerprints were introduced — can run for hours
// (posthog.events in prod: ~30 min per day across ~180 cubes). A day that sealed
// mid-pass used to wait for the NEXT pass (which starts with month backfills),
// so yesterday's cube landed up to a day late and every auto-mode query
// raw-scanned that whole extra day.
//
// The fresh step builds just the FreshDays newest sealed days, only for the cubes
// that lack them, ahead of everything else. It is checked at the start of every
// tick, between sources, between month builds, between day builds and between
// sketch batches of a running pass, and on a short idle timer between ticks.
//
//   - It fires once per newly sealed day (a clock comparison against the seal
//     boundary), so its steady-state cost is nil.
//   - A run that leaves anything unbuilt (listing error, transient manifest read,
//     source scan or build failure, manifest write failure) does not count: it
//     retries with backoff (freshRetryMin doubling to freshRetryMax) for up to
//     freshGiveUpAfter per sealed day — then the day is left to the regular pass,
//     so one persistently failing cube cannot retry for days, while a short
//     storage brownout does not cancel the day's priority. A day that newly seals
//     is attempted at once, with a fresh backoff.
//   - Late events that land after the fresh build change the day's source
//     fingerprint (fingerprint.go), so the next pass rebuilds the day.
//   - After a run that built anything it bumps the generation object, so
//     route-only pods reload manifests within RouterRefresh.

const (
	// freshPollInterval is how often an idle builder (between ticks) checks
	// whether a day has sealed.
	freshPollInterval = 5 * time.Minute
	freshRetryMin     = 5 * time.Minute
	freshRetryMax     = time.Hour
	// freshGiveUpAfter: prod's compactor keeps rewriting the just-sealed day's
	// last hours until ~2h after midnight (late window 2h, hourly compaction
	// every 10 min), so early reads can 404 on files compacted away — retry
	// well past that before handing the day to the regular pass.
	freshGiveUpAfter = 4 * time.Hour
)

// needMode selects which days a cube builds.
type needMode int

const (
	needPass    needMode = iota // regular pass: unbuilt days, plus every day inside the rebuild window whose source changed
	needMissing                 // fresh step: unbuilt days only
)

func (m *Manager) now() time.Time {
	if m.nowFn != nil {
		return m.nowFn()
	}
	return time.Now()
}

// newestSealedDay is the newest UTC day D whose whole span is older than the
// grace window — buildSource's seal rule: D+24h <= now-grace.
func newestSealedDay(now time.Time, grace time.Duration) time.Time {
	return now.UTC().Add(-grace).Truncate(24*time.Hour).AddDate(0, 0, -1)
}

// freshDue reports whether the fresh step has work: a day that sealed and was
// never attempted (always due), or a failed attempt whose backoff has elapsed.
func (m *Manager) freshDue() bool {
	if m.inFresh || m.cfg.FreshDays <= 0 {
		return false
	}
	now := m.now()
	newest := newestSealedDay(now, m.cfg.Grace)
	if newest.After(m.freshAttempted) {
		return true
	}
	return newest.After(m.freshThrough) && !now.Before(m.freshRetryAt)
}

// maybeFresh runs the fresh step when it is due. Call it only between build
// units: never while a day's _rollup_day temp table is live.
func (m *Manager) maybeFresh(ctx context.Context) {
	if m.freshDue() {
		m.runFresh(ctx, nil, false)
	}
}

// runFresh builds, for every source, the FreshDays newest sealed days for the
// cubes missing them. parts is the source->days discovery to use, and complete
// whether every listing behind it succeeded; nil parts scans now. Safe to call
// from anywhere the builder is between build units; it never panics out.
func (m *Manager) runFresh(ctx context.Context, parts map[string][]time.Time, complete bool) {
	if m.inFresh || m.cfg.FreshDays <= 0 {
		return
	}
	m.inFresh = true
	m.runBuilt = map[string]bool{}
	through := newestSealedDay(m.now(), m.cfg.Grace)
	if through.After(m.freshAttempted) { // a newly sealed day: fresh retry state
		m.freshAttempted, m.freshAttempts = through, 0
		m.freshBackoff, m.freshRetryAt, m.freshFirstFail = 0, time.Time{}, time.Time{}
	}
	ok, unbuilt := false, 0
	defer func() {
		if r := recover(); r != nil {
			m.log.Error().Interface("panic", r).Msg("Rollup fresh-day build panicked")
			m.db.Exec("DROP TABLE IF EXISTS _rollup_day") // a panic mid-day leaves it behind
			ok = false
		}
		m.inFresh = false
		m.runBuilt = nil
		m.finishFresh(ok, through, unbuilt)
	}()

	if parts == nil {
		parts, complete = m.scanChecked(ctx)
	}
	from := through.AddDate(0, 0, -(m.cfg.FreshDays - 1))

	sources := make([]string, 0, len(parts))
	for s := range parts {
		sources = append(sources, s)
	}
	sort.Strings(sources)
	allBuilt := complete
	builtAny := false
	for _, source := range sources {
		if ctx.Err() != nil {
			return
		}
		var days []time.Time
		for _, d := range parts[source] {
			if !d.Before(from) && !d.After(through) {
				days = append(days, d)
			}
		}
		if len(days) == 0 {
			continue
		}
		start := time.Now()
		built, remaining, err := m.buildFreshSource(ctx, source, days, parts[source])
		if err != nil {
			m.log.Warn().Str("source", source).Err(err).Msg("Rollup fresh-day build failed")
			allBuilt = false
			unbuilt++
			continue
		}
		if remaining > 0 {
			allBuilt = false
			unbuilt += remaining
		}
		if built > 0 {
			builtAny = true
			m.log.Info().Str("source", source).Str("through", through.Format("2006-01-02")).
				Int("cube_days", built).Int("unbuilt", remaining).Dur("took", time.Since(start)).
				Msg("Rollup built newly sealed days ahead of the pass")
		}
	}
	if builtAny {
		m.maybeBumpGeneration(ctx, true)
	}
	if !complete {
		unbuilt++ // a listing failed: some source's days may be missing from parts
	}
	if ctx.Err() == nil && allBuilt {
		m.freshThrough = through
		ok = true
	}
}

// finishFresh records a run's outcome. Success clears the retry state. Failure
// schedules a retry with a doubling backoff; once a sealed day has been failing
// for freshGiveUpAfter it gives up on that day (the regular pass still builds
// every unbuilt day) — a persistently failing cube must not cost a retry, and a
// full re-read of its day, every hour for FreshDays days.
func (m *Manager) finishFresh(ok bool, through time.Time, unbuilt int) {
	if ok {
		m.freshBackoff, m.freshRetryAt, m.freshAttempts, m.freshFirstFail = 0, time.Time{}, 0, time.Time{}
		return
	}
	m.freshAttempts++
	now := m.now()
	if m.freshFirstFail.IsZero() {
		m.freshFirstFail = now
	}
	if now.Sub(m.freshFirstFail) >= freshGiveUpAfter {
		m.log.Warn().Str("through", through.Format("2006-01-02")).Int("attempts", m.freshAttempts).Int("unbuilt", unbuilt).
			Msg("Rollup fresh-day build giving up; the regular pass will build what is left")
		m.freshThrough = through
		m.freshBackoff, m.freshRetryAt, m.freshAttempts, m.freshFirstFail = 0, time.Time{}, 0, time.Time{}
		return
	}
	if m.freshBackoff == 0 {
		m.freshBackoff = freshRetryMin
	} else if m.freshBackoff *= 2; m.freshBackoff > freshRetryMax {
		m.freshBackoff = freshRetryMax
	}
	m.freshRetryAt = now.Add(m.freshBackoff)
	m.log.Warn().Str("through", through.Format("2006-01-02")).Int("attempt", m.freshAttempts).Int("unbuilt", unbuilt).
		Time("retry_at", m.freshRetryAt).Msg("Rollup fresh-day build incomplete; retrying with backoff")
}

// buildFreshSource builds one source's missing fresh days and returns how many
// cube-days it persisted and how many it wanted but could not (including cubes
// whose manifest could not be read). Cubes the running pass already holds are
// built through the pass's own in-memory state: a separately loaded manifest copy
// would be overwritten by the pass's next persist, silently dropping the day.
func (m *Manager) buildFreshSource(ctx context.Context, source string, days, all []time.Time) (built, remaining int, err error) {
	// For the source the running pass is on, use the pass's own full plan:
	// re-planning mid-pass could differ (and re-record the plan). Cubes the pass
	// has not loaded yet are not in m.held and get loaded below.
	specs := m.heldSpecs
	if source != m.heldSource || specs == nil {
		if specs, err = m.planSpecs(source); err != nil {
			return 0, 0, err
		}
	}
	sealedUntil := m.now().UTC().Add(-m.cfg.Grace)
	rebuildFloor := sealedUntil.AddDate(0, 0, -m.cfg.RebuildDays)
	var sealed []time.Time
	for _, d := range all {
		if !d.AddDate(0, 0, 1).After(sealedUntil) {
			sealed = append(sealed, d)
		}
	}
	type want struct {
		cb   *cubeBuild
		date string
	}
	var wants []want
	var exact, sketch []*cubeBuild
	for _, spec := range specs {
		cb := m.held[cubeKeyOf(spec)]
		if cb == nil {
			man, ok := m.loadManifest(ctx, spec)
			if !ok {
				remaining++ // transient read failure — counts as unbuilt, so the run retries
				continue
			}
			cb = m.newCubeBuild(spec, man, sealed, rebuildFloor)
		}
		n := 0
		for _, d := range days {
			if m.dayNeeds(cb, d, rebuildFloor, needMissing) {
				wants = append(wants, want{cb, d.Format("2006-01-02")})
				n++
			}
		}
		if n == 0 {
			continue
		}
		if specHasSketch(spec) {
			sketch = append(sketch, cb)
		} else {
			exact = append(exact, cb)
		}
	}
	if len(wants) == 0 {
		return 0, remaining, nil
	}
	sort.Slice(days, func(i, j int) bool { return days[i].After(days[j]) })
	m.buildExactDays(ctx, source, exact, days, rebuildFloor, needMissing)
	for _, cb := range sketch {
		m.buildSketchDays(ctx, source, cb, days, rebuildFloor, needMissing)
	}
	for _, w := range wants {
		if m.runBuilt[tickBuiltKey(w.cb.spec, w.date)] {
			built++
		} else {
			remaining++
		}
	}
	return built, remaining, nil
}

// newCubeBuild is a cube's per-pass build state, derived from its manifest. Only
// exact cubes take the one-file month path (#3); a sketch cube builds every day
// on its own, so it gets no pending month builds to defer days to.
func (m *Manager) newCubeBuild(spec CubeSpec, man *Manifest, sealed []time.Time, rebuildFloor time.Time) *cubeBuild {
	monthBuild := map[string]bool{}
	if !specHasSketch(spec) {
		monthBuild = cleanFullySealedMonths(man.BuiltDays(), sealed, rebuildFloor)
	}
	return &cubeBuild{spec: spec, man: man, built: man.BuiltDays(),
		compacted: man.CompactedDays(), monthBuild: monthBuild}
}

// dayNeeds reports whether cube cb is a CANDIDATE to build day in the given mode.
// A day pending a month build or folded into a compacted month is never built as
// a loose daily. The fresh step only wants days not built yet (nor this tick).
// For a built day inside the rebuild window the pass makes the final call at
// build time against the day's current listing (skipDay in fingerprint.go).
func (m *Manager) dayNeeds(cb *cubeBuild, day time.Time, rebuildFloor time.Time, mode needMode) bool {
	date := day.Format("2006-01-02")
	if cb.monthBuild[day.Format("2006-01")] { // belongs to a month-build (this/next tick) — not a day build
		return false
	}
	// A day already folded into a compacted month is final — never re-materialize
	// it as a loose daily. A widened rebuild_days floor can pull such a day back
	// into the rebuild window (day >= rebuildFloor); rebuilding it then appends a
	// "YYYY-MM-DD" entry that cannot supersede the "YYYY-MM" month, so DaysInRange
	// selects both and the day double-counts. The month owns its days. (Root cause
	// of the May-2026 duplication: rebuild_days 30->42->30.)
	if cb.compacted[date] {
		return false
	}
	if mode == needMissing {
		return !cb.built[date] && !m.tickBuilt[tickBuiltKey(cb.spec, date)]
	}
	return !(cb.built[date] && day.Before(rebuildFloor))
}

func tickBuiltKey(spec CubeSpec, date string) string { return cubeKeyOf(spec) + "@" + date }

// markBuilt records a successfully persisted cube-day for this tick (and for the
// running fresh step, if any).
func (m *Manager) markBuilt(spec CubeSpec, date string) {
	if m.tickBuilt == nil {
		m.tickBuilt = map[string]bool{}
	}
	k := tickBuiltKey(spec, date)
	m.tickBuilt[k] = true
	if m.runBuilt != nil {
		m.runBuilt[k] = true
	}
}

// generationKey is a tiny object the builder rewrites after it changes cubes;
// route-only pods poll it and reload manifests only when it changes.
func (m *Manager) generationKey() string { return m.cfg.StoragePrefix + "/_generation" }

// maybeBumpGeneration rewrites the generation object if any manifest was written
// since the last bump: always when force (end of a fresh step or tick), else at
// most once per RouterRefresh (after each persist), so route-only pods see
// rebuilt and replaced cubes promptly without a bump per cube-day.
func (m *Manager) maybeBumpGeneration(ctx context.Context, force bool) {
	if !m.genDirty || (!force && time.Since(m.lastBump) < m.cfg.routerRefresh()) {
		return
	}
	m.genSeq++ // unique even for two bumps within one clock tick
	gen := strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatUint(m.genSeq, 10)
	m.lastBump = time.Now() // also after a failure: retries stay rate-limited during a storage brownout
	if err := m.stg.Write(ctx, m.generationKey(), []byte(gen)); err != nil {
		// Route-only pods still do a full reload every ForwardTick.
		m.log.Warn().Err(err).Msg("Rollup generation bump failed; route-only pods pick up new cubes on their next full reload")
		return
	}
	m.genDirty = false
}

// readGeneration reads the builder's current generation.
func (m *Manager) readGeneration(ctx context.Context) (string, bool) {
	b, err := m.stg.Read(ctx, m.generationKey())
	if err != nil || len(b) == 0 {
		return "", false
	}
	return string(b), true
}

// generationChanged reports whether the builder bumped the generation since this
// route-only pod last fully loaded one, and the new value. A missing or
// unreadable object reports no change; the ForwardTick full reload covers that.
// The caller records the value only after a clean reload.
func (m *Manager) generationChanged(ctx context.Context) (string, bool) {
	gen, ok := m.readGeneration(ctx)
	if !ok || gen == m.lastGeneration {
		return "", false
	}
	return gen, true
}
