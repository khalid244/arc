package rollup

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Manager is the production orchestrator. It discovers tables across databases,
// auto-classifies each into a cube set, and incrementally materializes them
// ONE DAY AT A TIME (bounding memory) on a background tick — then keeps the
// read-path Router up to date. No table or column is hardcoded; everything is
// derived from the data and config.
type Manager struct {
	cfg Config
	s3  S3Params
	stg Storage
	log zerolog.Logger

	db       *sql.DB // connection for classification + glob discovery (no datasketches)
	execPath string  // path to this binary, for spawning build subprocesses

	workload *Workload // observed query dimensions, drives per-dim cube selection

	mu            sync.RWMutex
	router        *Router                 // immutable once built; swapped atomically on change
	manifests     map[string]*Manifest    // source of truth, keyed by cubeKeyOf
	profiles      map[string]TableProfile // source -> classified schema profile (cached)
	dimRichBailed map[string]bool         // sources already warned for skipped dim-rich cube (log once)
}

// Storage is the subset of arc's storage backend the Manager needs (manifest
// read/write + batch delete for compaction). Table/day discovery is done via
// DuckDB glob, not the backend, so no directory-listing method is required.
type Storage interface {
	Read(ctx context.Context, path string) ([]byte, error)
	Write(ctx context.Context, path string, data []byte) error
	DeleteBatch(ctx context.Context, paths []string) error
	// StatFile returns the byte size of the object at path, or -1 (nil error) if it
	// does not exist. Used by compaction to skip manifest daily entries whose files
	// have been deleted out from under it (the build-side stale-pointer guard), so a
	// single missing daily no longer fails the whole month's COPY forever.
	StatFile(ctx context.Context, path string) (int64, error)
}

// S3Params configures the Manager's build connection (where it reads source
// Parquet and writes cube Parquet via DuckDB httpfs).
type S3Params struct {
	Endpoint, AccessKey, SecretKey, Bucket string
	PathStyle, UseSSL                      bool
}

// NewManager opens the build connection, loads any existing manifests, and wires
// an initial Router so queries are served immediately from whatever is already
// materialized (a restart resumes, it does not rebuild).
func NewManager(cfg Config, s3 S3Params, stg Storage, log zerolog.Logger) (*Manager, error) {
	cfg = cfg.withDefaults()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	db.SetMaxOpenConns(1)
	// The Manager's own connection only classifies tables and globs partitions —
	// no datasketches, so the crash-prone sketch path never runs in the server
	// process (builds are subprocessed; see subprocess.go).
	if err := configureBuildConn(db, s3, cfg.MemLimit, cfg.BuildThreads, false); err != nil {
		db.Close()
		return nil, fmt.Errorf("configure duckdb: %w", err)
	}
	execPath, err := os.Executable()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("resolve executable: %w", err)
	}
	m := &Manager{cfg: cfg, s3: s3, stg: stg, log: log, db: db, execPath: execPath,
		workload: NewWorkload(), manifests: map[string]*Manifest{}, profiles: map[string]TableProfile{},
		dimRichBailed: map[string]bool{}}
	// Resume the learned workload so cube selection survives restarts.
	if b, err := stg.Read(context.Background(), m.workloadKey()); err == nil && len(b) > 0 {
		_ = m.workload.LoadBytes(b)
	}
	m.reloadRouter(context.Background())
	return m, nil
}

func (m *Manager) workloadKey() string { return m.cfg.StoragePrefix + "/_workload.json" }

// RouteHTTP makes the Manager itself the stable RollupRouter handed to the query
// handler. It forwards to the current (immutable) Router, which the Manager swaps
// atomically as builds land — so a single SetRollupRouter at startup always sees
// the latest cubes without races.
func (m *Manager) RouteHTTP(sql, headerDB string) (rewritten string, served bool, cube string) {
	m.mu.RLock()
	r := m.router
	m.mu.RUnlock()
	if r == nil {
		return "", false, "no_router"
	}
	d := r.Route(sql)
	return d.SQL, d.Served, d.Cube
}

// ExplainHTTP forwards a non-executing rollup-support check to the live router.
func (m *Manager) ExplainHTTP(sql, headerDB string) (served bool, cube, reason string) {
	m.mu.RLock()
	r := m.router
	m.mu.RUnlock()
	if r == nil {
		return false, "", "rollup router not ready yet"
	}
	return r.ExplainHTTP(sql, headerDB)
}

func esc(s string) string { return strings.ReplaceAll(s, "'", "''") }
func urlStyle(pathStyle bool) string {
	if pathStyle {
		return "path"
	}
	return "vhost"
}

// Router returns the live read-path router (its cube/manifest set grows as builds
// complete). Safe to hand to the query handler once.
func (m *Manager) Router() *Router {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.router
}

// Close releases the build connection.
func (m *Manager) Close() error { return m.db.Close() }

// Start runs the forward-build loop until ctx is cancelled. It builds immediately
// on startup, then every ForwardTick. When Builder is false this process never
// materializes cubes — it only refreshes the read-path Router (see runRouteOnly),
// so a single builder pod can own writes while query replicas serve cubes.
func (m *Manager) Start(ctx context.Context) {
	if !m.cfg.Builder {
		m.runRouteOnly(ctx)
		return
	}
	m.log.Info().Dur("tick", m.cfg.ForwardTick).Str("grain", m.cfg.Grain).Msg("Rollup manager started")
	m.tick(ctx)
	t := time.NewTicker(m.cfg.ForwardTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.log.Info().Msg("Rollup manager stopping")
			return
		case <-t.C:
			m.tick(ctx)
		}
	}
}

// runRouteOnly keeps the read-path Router fresh on pods that route but never build
// (rollup.builder = false). It re-reads cube manifests from object storage every
// ForwardTick, so cubes materialized by the sole builder pod become servable here
// without this process running the (single-writer) build path. The initial Router
// was already wired in NewManager, so queries are served from the first request.
func (m *Manager) runRouteOnly(ctx context.Context) {
	m.log.Info().Dur("refresh", m.cfg.ForwardTick).Msg("Rollup router started (builder disabled on this pod)")
	m.reloadRouter(ctx)
	t := time.NewTicker(m.cfg.ForwardTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.log.Info().Msg("Rollup router stopping")
			return
		case <-t.C:
			m.reloadRouter(ctx)
		}
	}
}

// tick performs one full pass: discover sources+days via glob, classify, build.
func (m *Manager) tick(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			m.log.Error().Interface("panic", r).Msg("Rollup tick panicked")
		}
	}()
	parts := m.scan(ctx)
	for source, days := range parts {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := m.buildSource(ctx, source, days); err != nil {
			m.log.Warn().Str("source", source).Err(err).Msg("Rollup build failed; will retry next tick")
		}
	}
	m.reloadRouter(ctx)
	m.persistWorkload(ctx)
}

// persistWorkload saves the learned dim counts so cube selection survives restarts.
func (m *Manager) persistWorkload(ctx context.Context) {
	if b, err := m.workload.Bytes(); err == nil {
		_ = m.stg.Write(ctx, m.workloadKey(), b)
	}
}

// scan discovers every "db.measurement" source and its available UTC days by
// globbing the source Parquet partition tree (one S3 LIST per database), parsing
// db/measurement/YYYY/MM/DD from each path. No full data scan, no backend
// directory-listing dependency.
func (m *Manager) scan(ctx context.Context) map[string][]time.Time {
	dbs := m.cfg.Databases
	var patterns []string
	if len(dbs) == 0 {
		patterns = []string{fmt.Sprintf("s3://%s/*/*/**/*.parquet", m.s3.Bucket)}
	} else {
		for _, db := range dbs {
			patterns = append(patterns, fmt.Sprintf("s3://%s/%s/*/**/*.parquet", m.s3.Bucket, db))
		}
	}
	bucketPrefix := fmt.Sprintf("s3://%s/", m.s3.Bucket)
	set := map[string]map[time.Time]bool{}
	for _, pat := range patterns {
		for _, file := range m.globFiles(pat) {
			rel := strings.TrimPrefix(file, bucketPrefix)
			segs := strings.Split(rel, "/")
			if len(segs) < 6 { // db/m/YYYY/MM/DD/file
				continue
			}
			db, meas := segs[0], segs[1]
			if m.skipMeasurement(db, meas) {
				continue
			}
			day, err := time.Parse("2006/01/02", segs[2]+"/"+segs[3]+"/"+segs[4])
			if err != nil {
				continue
			}
			src := db + "." + meas
			if set[src] == nil {
				set[src] = map[time.Time]bool{}
			}
			set[src][day.UTC()] = true
		}
	}
	out := map[string][]time.Time{}
	for src, days := range set {
		ds := make([]time.Time, 0, len(days))
		for d := range days {
			ds = append(ds, d)
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i].Before(ds[j]) })
		out[src] = ds
	}
	return out
}

// globFiles runs DuckDB's glob() to list S3 object paths matching pattern.
func (m *Manager) globFiles(pattern string) []string {
	r, err := m.db.Query(fmt.Sprintf("SELECT file FROM glob('%s')", esc(pattern)))
	if err != nil {
		m.log.Warn().Str("pattern", pattern).Err(err).Msg("Rollup glob failed")
		return nil
	}
	defer r.Close()
	var out []string
	for r.Next() {
		var f string
		if err := r.Scan(&f); err == nil {
			out = append(out, f)
		}
	}
	return out
}

// dayColumns returns the column-name set of the current _rollup_day temp table,
// read from the result-set metadata of a zero-row probe (reliable for temp tables).
func (m *Manager) dayColumns() map[string]bool {
	cols := map[string]bool{}
	rows, err := m.db.Query("SELECT * FROM _rollup_day LIMIT 0")
	if err != nil {
		return cols
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return cols
	}
	for _, n := range names {
		cols[n] = true
	}
	return cols
}

// skipMeasurement reports sources the rollup never builds or routes: internal
// (_-prefixed db/measurement), late-arrival variants (*_late — the late-event
// reorg merges these back into the base table, so rolling them up would
// double-count), and any operator-excluded measurement names.
func (m *Manager) skipMeasurement(db, meas string) bool {
	if db == "" || meas == "" {
		return true
	}
	if strings.HasPrefix(db, "_") || strings.HasPrefix(meas, "_") {
		return true
	}
	if strings.HasSuffix(meas, "_late") {
		return true
	}
	return m.excluded(meas)
}

func (m *Manager) excluded(ms string) bool {
	for _, e := range m.cfg.ExcludeMeasurements {
		if e == ms {
			return true
		}
	}
	return false
}

// ensureProfile classifies a source's schema once and caches it.
func (m *Manager) ensureProfile(source string) (TableProfile, error) {
	m.mu.RLock()
	p, ok := m.profiles[source]
	m.mu.RUnlock()
	if ok {
		return p, nil
	}
	p, err := ProfileTable(m.db, source, m.cfg.TimeCol, m.cfg.Grain, m.recentSampleGlob(source), m.cfg.classifyConfig())
	if err != nil {
		return TableProfile{}, err
	}
	m.mu.Lock()
	m.profiles[source] = p
	m.mu.Unlock()
	m.log.Info().Str("source", source).Int("dims", len(p.DimCard)).Int("metrics", len(p.Metrics)).
		Int("sketch_cols", len(p.SketchCols)).Msg("Rollup profiled table")
	return p, nil
}

// planSpecs selects the cube set for a source (recomputed each tick as the
// workload grows): the coarse cube always; a per-dim cube for every low-card
// dimension (cheap, commonly grouped — covers the bulk out-of-the-box); and a
// per-dim cube for higher-cardinality dimensions only once they are actually
// queried (workload-driven, so storage scales with use). Capped at MaxDims.
func (m *Manager) planSpecs(source string) ([]CubeSpec, error) {
	p, err := m.ensureProfile(source)
	if err != nil {
		return nil, err
	}
	cubes := []CubeSpec{p.CoarseSpec()}
	queried := m.workload.DimCounts(source)
	type cand struct {
		dim     string
		card, q int
	}
	var cands []cand
	for d, card := range p.DimCard {
		if card <= m.cfg.MaxDimCard || queried[d] > 0 {
			cands = append(cands, cand{d, card, queried[d]})
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		li, lj := cands[i].card <= m.cfg.MaxDimCard, cands[j].card <= m.cfg.MaxDimCard
		if li != lj {
			return li // low-card dims first (always built)
		}
		if cands[i].q != cands[j].q {
			return cands[i].q > cands[j].q // then most-queried
		}
		if cands[i].card != cands[j].card {
			return cands[i].card < cands[j].card
		}
		return cands[i].dim < cands[j].dim
	})
	for i, c := range cands {
		if i >= m.cfg.MaxDims {
			break
		}
		cubes = append(cubes, p.PerDimSpec(c.dim))
	}
	// Optional dim-rich cube: one exact cube over all eligible dims, covering
	// multi-dimension queries no single-dim cube can serve.
	if m.cfg.DimRich {
		if drs, ok := p.DimRichSpec(m.cfg.DimRichMaxDims); ok {
			cubes = append(cubes, drs)
		} else if n := len(p.DimCard); n > m.cfg.DimRichMaxDims {
			// The dim-rich cube was SKIPPED because the table is too high-dimensional.
			// Make that observable: multi-dimension queries on this source will fall
			// through to a full source scan instead of rolling up. Logged once per
			// source so it surfaces without spamming every tick.
			m.warnDimRichSkipped(source, n)
		}
	}
	return cubes, nil
}

// warnDimRichSkipped emits a loud, once-per-source warning when the dim-rich cube
// is skipped for high-dimensionality — turning a silent multi-dim coverage gap
// into an operator-visible signal (raise rollup.dim_rich_max_dims to close it).
func (m *Manager) warnDimRichSkipped(source string, dims int) {
	m.mu.Lock()
	first := !m.dimRichBailed[source]
	m.dimRichBailed[source] = true
	m.mu.Unlock()
	if !first {
		return
	}
	m.log.Warn().
		Str("source", source).
		Int("eligible_dims", dims).
		Int("dim_rich_max_dims", m.cfg.DimRichMaxDims).
		Msg("Rollup dim-rich cube SKIPPED (too high-dimensional); multi-dimension queries on this source will fall through to source — raise rollup.dim_rich_max_dims to cover them")
}

// cubeBuild is the per-cube build state for one tick.
type cubeBuild struct {
	spec       CubeSpec
	man        *Manifest
	built      map[string]bool // already-materialized days (incl. days inside monthly files)
	monthBuild map[string]bool // YYYY-MM months to write as ONE file (clean + fully sealed)
	changed    bool
}

// buildSource materializes every cube for a source up to the seal boundary.
// Build-cost optimizations preserve the proven aggregation (identical SQL, so
// results match source) and stay memory-bounded:
//   - #3: a *fully-sealed, not-yet-built* month is written as ONE monthly file in a
//     single streaming+spilling COPY, skipping the day-by-day + compaction churn.
//   - #1: the remaining recent days are built day-outer, scanning each day's source
//     ONCE into a temp table shared by every exact cube (the Manager's DuckDB is a
//     single connection, so the temp table is visible to all the COPYs).
//   - #2: a cube whose manifest read fails transiently is skipped, never rebuilt
//     from scratch (which would destroy good cube data).
//
// Sketch cubes keep the isolated per-day subprocess path. Manifests persist after
// each unit so a crash resumes cleanly.
func (m *Manager) buildSource(ctx context.Context, source string, days []time.Time) error {
	if len(days) == 0 {
		return nil
	}
	specs, err := m.planSpecs(source)
	if err != nil {
		return err
	}
	sealedUntil := time.Now().UTC().Add(-m.cfg.Grace)
	rebuildFloor := sealedUntil.AddDate(0, 0, -m.cfg.RebuildDays)

	// Sealed days only, newest first — recent ranges (the common dashboard view)
	// get covered first; older history backfills over subsequent ticks.
	var sealed []time.Time
	for _, day := range days {
		if !day.AddDate(0, 0, 1).After(sealedUntil) {
			sealed = append(sealed, day)
		}
	}
	sort.Slice(sealed, func(i, j int) bool { return sealed[i].After(sealed[j]) })

	// Load manifests (skip transient read failures — #2), compact, classify.
	var exact, sketch []*cubeBuild
	for _, spec := range specs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		man, ok := m.loadManifest(ctx, spec)
		if !ok {
			m.log.Warn().Str("cube", CubeID(spec)).Msg("Rollup manifest unreadable (transient); skipping this tick — NOT rebuilding from scratch")
			continue
		}
		m.compactCube(ctx, spec, man, rebuildFloor)
		cb := &cubeBuild{spec: spec, man: man, built: man.BuiltDays(),
			monthBuild: cleanFullySealedMonths(man.BuiltDays(), sealed, rebuildFloor)}
		if specHasSketch(spec) {
			sketch = append(sketch, cb)
		} else {
			exact = append(exact, cb)
		}
	}

	m.buildExactMonths(ctx, source, exact, sealed)             // #3
	m.buildExactDays(ctx, source, exact, sealed, rebuildFloor) // #1
	for _, cb := range sketch {
		m.buildSketchDays(ctx, source, cb, sealed, rebuildFloor)
	}
	return nil
}

// persist upserts a non-empty cube file into the manifest, writes it back, and
// republishes the router — so a cube becomes queryable as soon as any of its data
// lands, mid-backfill, rather than only when the whole (long) tick finishes.
func (m *Manager) persist(ctx context.Context, cb *cubeBuild, e DayEntry) {
	if e.Rows == 0 {
		return
	}
	cb.man.Upsert(e)
	cb.changed = true
	if err := m.writeManifest(ctx, cb.spec, cb.man); err != nil {
		m.log.Warn().Str("cube", CubeID(cb.spec)).Err(err).Msg("Rollup manifest write failed")
	}
	m.updateRouter(cb.man)
}

// buildExactMonths writes clean fully-sealed months as one monthly file per cube
// (#3), bounded by CompactMaxPerTick months/cube/tick.
func (m *Manager) buildExactMonths(ctx context.Context, source string, cubes []*cubeBuild, sealed []time.Time) {
	for _, cb := range cubes {
		months := sortedKeysDesc(cb.monthBuild)
		n := 0
		for _, ym := range months {
			if n >= m.cfg.CompactMaxPerTick {
				break
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
			entry, err := m.buildMonth(cb.spec, source, ym, sealed)
			if err != nil {
				m.log.Warn().Str("cube", CubeID(cb.spec)).Str("month", ym).Err(err).Msg("Rollup month build failed; retries next tick")
				continue
			}
			m.persist(ctx, cb, entry)
			m.log.Info().Str("cube", CubeID(cb.spec)).Str("month", ym).Int64("rows", entry.Rows).Int("days", len(entry.Covers)).Msg("Rollup built sealed month in one COPY (opt #3)")
			for _, d := range entry.Covers { // so the day phase skips these
				cb.built[d] = true
			}
			delete(cb.monthBuild, ym)
			n++
		}
	}
}

// buildMonth materializes a whole month of a cube into one monthly file.
func (m *Manager) buildMonth(spec CubeSpec, source, ym string, sealed []time.Time) (DayEntry, error) {
	first, err := time.Parse("2006-01", ym)
	if err != nil {
		return DayEntry{}, err
	}
	lo := fmtTS(first.UTC())
	hi := fmtTS(first.AddDate(0, 1, 0).UTC())
	dest := m.cubeFileURI(spec, fmt.Sprintf("m_%s_%d", ym, time.Now().UTC().UnixNano()))
	entry, err := BuildRange(m.db, spec, m.sourceMonthGlob(source, ym), m.cfg.TimeCol, ym, lo, hi, dest)
	if err != nil {
		return DayEntry{}, err
	}
	entry.Date = ym
	for _, d := range sealed {
		if d.Format("2006-01") == ym {
			entry.Covers = append(entry.Covers, d.Format("2006-01-02"))
		}
	}
	sort.Strings(entry.Covers)
	return entry, nil
}

// buildExactDays builds the remaining recent days day-outer: each day's source is
// scanned ONCE into a temp table shared by every exact cube that needs it (#1).
func (m *Manager) buildExactDays(ctx context.Context, source string, cubes []*cubeBuild, sealed []time.Time, rebuildFloor time.Time) {
	if len(cubes) == 0 {
		return
	}
	type dayNeed struct {
		day     time.Time
		needers []*cubeBuild
	}
	var order []dayNeed
	for _, day := range sealed {
		if len(order) >= m.cfg.MaxDaysPerTick {
			break
		}
		date := day.Format("2006-01-02")
		ym := day.Format("2006-01")
		var needers []*cubeBuild
		for _, cb := range cubes {
			if cb.monthBuild[ym] { // belongs to a month-build (this/next tick) — not a day build
				continue
			}
			if cb.built[date] && day.Before(rebuildFloor) {
				continue
			}
			needers = append(needers, cb)
		}
		if len(needers) > 0 {
			order = append(order, dayNeed{day, needers})
		}
	}
	for _, dn := range order {
		select {
		case <-ctx.Done():
			return
		default:
		}
		date := dn.day.Format("2006-01-02")
		lo := fmtTS(dn.day.UTC())
		hi := fmtTS(dn.day.UTC().Add(24 * time.Hour))
		create := fmt.Sprintf("CREATE OR REPLACE TEMP TABLE _rollup_day AS SELECT * FROM read_parquet(%s, union_by_name=true)",
			m.sourceDayGlob(source, dn.day))
		if _, err := m.db.Exec(create); err != nil {
			m.log.Warn().Str("source", source).Str("day", date).Err(err).Msg("Rollup day source scan failed; retries next tick")
			continue
		}
		// Source schemas drift across days (sparse event properties come and go),
		// so a cube column from the recent-sample profile may be absent from an
		// older day. Adapt each cube to the day's actual columns before building.
		present := m.dayColumns()
		for _, cb := range dn.needers {
			spec, ok := cb.spec.prunedToColumns(present)
			if !ok {
				continue // a dimension column does not exist this day — nothing to roll up by it
			}
			entry, berr := BuildFromTable(m.db, spec, "_rollup_day", m.cfg.TimeCol, date, lo, hi, m.cubeDayURI(cb.spec, date))
			if berr != nil {
				m.log.Warn().Str("cube", CubeID(cb.spec)).Str("day", date).Err(berr).Msg("Rollup day build failed")
				continue
			}
			m.persist(ctx, cb, entry)
		}
		m.db.Exec("DROP TABLE IF EXISTS _rollup_day")
	}
}

// buildSketchDays is the unchanged isolated per-day subprocess build for cubes
// carrying HLL/KLL sketches (only the coarse cube), where a datasketches native
// crash must not take down arc.
func (m *Manager) buildSketchDays(ctx context.Context, source string, cb *cubeBuild, sealed []time.Time, rebuildFloor time.Time) {
	var todo []time.Time
	for _, day := range sealed {
		if len(todo) >= m.cfg.MaxDaysPerTick {
			break
		}
		date := day.Format("2006-01-02")
		if cb.built[date] && day.Before(rebuildFloor) {
			continue
		}
		todo = append(todo, day)
	}
	if len(todo) == 0 {
		return
	}
	works := make([]DayWork, len(todo))
	for i, day := range todo {
		date := day.Format("2006-01-02")
		works[i] = DayWork{Date: date, Glob: m.sourceDayGlob(source, day), Dest: m.cubeDayURI(cb.spec, date)}
	}
	if err := m.spawnBuildBatch(ctx, cb.spec, works, func(e DayEntry) { m.persist(ctx, cb, e) }); err != nil {
		m.log.Warn().Str("cube", CubeID(cb.spec)).Err(err).Msg("Rollup sketch batch ended early (partial kept; retries next tick)")
	}
}

// cleanFullySealedMonths returns the YYYY-MM months whose whole span is older than
// the rebuild floor AND that have no day already built — safe to write as one file.
func cleanFullySealedMonths(built map[string]bool, sealed []time.Time, rebuildFloor time.Time) map[string]bool {
	byMonth := map[string][]time.Time{}
	for _, d := range sealed {
		ym := d.Format("2006-01")
		byMonth[ym] = append(byMonth[ym], d)
	}
	out := map[string]bool{}
	for ym, dd := range byMonth {
		first, err := time.Parse("2006-01", ym)
		if err != nil || first.AddDate(0, 1, 0).After(rebuildFloor) {
			continue // not fully sealed
		}
		clean := true
		for _, d := range dd {
			if built[d.Format("2006-01-02")] {
				clean = false
				break
			}
		}
		if clean {
			out[ym] = true
		}
	}
	return out
}

// sourceMonthGlob returns the read_parquet list arg for a whole month's source.
func (m *Manager) sourceMonthGlob(source, ym string) string {
	db2, meas := splitSource(source)
	return fmt.Sprintf("['s3://%s/%s/%s/%s/%s/**/*.parquet']", m.s3.Bucket, db2, meas, ym[:4], ym[5:7])
}

// sortedKeysDesc returns map keys sorted descending (newest month first).
func sortedKeysDesc(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(out)))
	return out
}

// compactCube merges a cube's sealed daily files into one file per month. It
// groups the manifest's loose daily entries (date strictly before the rebuild
// floor, so they will never be rebuilt) by calendar month and compacts any month
// that has reached CompactMinDays, newest months first, up to CompactMaxPerTick.
func (m *Manager) compactCube(ctx context.Context, spec CubeSpec, man *Manifest, rebuildFloor time.Time) {
	if m.cfg.CompactMinDays <= 0 {
		return
	}
	loose := map[string][]DayEntry{} // YYYY-MM -> sealed single-day entries
	monthlyURI := map[string]DayEntry{}
	for _, d := range man.Days {
		if len(d.Covers) > 0 {
			monthlyURI[d.Date] = d // existing compacted month (Date == "YYYY-MM")
			continue
		}
		day, err := time.Parse("2006-01-02", d.Date)
		if err != nil || !day.Before(rebuildFloor) {
			continue // not a daily file, or still in the rebuild window
		}
		ym := d.Date[:7]
		loose[ym] = append(loose[ym], d)
	}
	months := make([]string, 0, len(loose))
	for ym, ds := range loose {
		if len(ds) >= m.cfg.CompactMinDays {
			months = append(months, ym)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(months))) // newest first
	for i, ym := range months {
		if i >= m.cfg.CompactMaxPerTick {
			break
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		old, hasOld := monthlyURI[ym]
		if err := m.compactMonth(ctx, spec, man, ym, loose[ym], old, hasOld); err != nil {
			m.log.Warn().Str("cube", CubeID(spec)).Str("month", ym).Err(err).Msg("Rollup compaction failed; retries next tick")
		}
	}
}

// compactMonth merges a month's loose daily files (and any existing monthly file)
// into a single new monthly Parquet, rewrites the manifest, then deletes the
// superseded files. Order matters for crash-safety: write the new file, then the
// manifest (the read path's source of truth), then delete — a crash anywhere
// leaves the read path correct, at worst an orphan object.
func (m *Manager) compactMonth(ctx context.Context, spec CubeSpec, man *Manifest, ym string, dailies []DayEntry, old DayEntry, hasOld bool) error {
	// Pre-filter to daily files that still physically exist. A daily file can be
	// deleted out from under the manifest (a DELETE rewrite, a retention sweep, a
	// never-completed write), leaving a stale pointer. Without this guard the
	// month's COPY 404s on the first missing file and the whole compaction fails
	// EVERY tick, never converging. We compact the files that exist and still purge
	// the missing entries from the manifest below (drop[]), so the stale pointers
	// self-heal instead of poisoning compaction forever. This mirrors the read
	// path's stale-pointer fallback.
	present, missing := m.partitionExistingDailies(ctx, dailies)
	// CORRECTNESS: a missing interior daily must NOT be silently dropped — that
	// publishes a gapped compacted month (surrounding days present, the missing day
	// absent) and the read path would re-aggregate it as if those rows never existed,
	// a silent undercount. Instead REBUILD each missing daily from source FIRST. If
	// source still has the day's rows we materialize a fresh daily file (it joins
	// `present`); if source is genuinely empty the day legitimately has no data and is
	// recorded as known-built (emptyDates) so the merged month's Covers marks it
	// processed — never a gap. Only when source is also unreadable do we leave it as a
	// true stale pointer to purge. This makes the manifest go old-state -> complete,
	// never gapped (approach B); the read-path interior-gap net (approach A) is the
	// belt-and-suspenders catch.
	var emptyDates []string
	if len(missing) > 0 {
		recovered, stillEmpty, unresolved := m.rebuildMissingDailies(ctx, spec, ym, missing)
		present = append(present, recovered...)
		emptyDates = stillEmpty
		missing = unresolved
		if len(recovered) > 0 || len(stillEmpty) > 0 {
			m.log.Info().Str("cube", CubeID(spec)).Str("month", ym).
				Int("rebuilt", len(recovered)).Int("empty", len(stillEmpty)).
				Msg("Rollup compaction: rebuilt missing daily cube files from source before compacting (gap-free)")
		}
	}
	if len(missing) > 0 {
		dates := make([]string, len(missing))
		for i, d := range missing {
			dates[i] = d.Date
		}
		m.log.Warn().Str("cube", CubeID(spec)).Str("month", ym).Int("skipped", len(missing)).
			Strs("missing_dates", dates).Msg("Rollup compaction: daily cube files missing from storage and unrebuildable — purging stale manifest entries")
	}
	// Stat-guard the existing monthly file too. If it has gone missing, appending its
	// URI to the COPY would 404 every tick and the month would never compact — the
	// exact never-converging loop this guard exists to close. Drop the stale monthly
	// pointer; the present (and just-rebuilt) dailies still carry the month's data, so
	// we compact from them. (A monthly file holds already-aggregated days that have no
	// loose daily anymore; if it is gone AND there are no dailies, there is nothing to
	// rebuild from — handled by the empty-sources guard below.)
	if hasOld {
		if size, err := m.stg.StatFile(ctx, m.keyFromURI(old.URI)); err == nil && size < 0 {
			m.log.Warn().Str("cube", CubeID(spec)).Str("month", ym).Str("uri", old.URI).
				Msg("Rollup compaction: existing monthly cube file missing from storage — dropping stale pointer and compacting from dailies")
			hasOld = false
		}
	}
	if len(present) == 0 && !hasOld {
		// Nothing real to compact. Purge the stale daily pointers so this month stops
		// being retried; fold any genuinely-empty rebuilt days into the manifest as a
		// zero-row covered entry so the read path knows they were processed (not gaps).
		return m.purgeMissingDailies(ctx, spec, man, missing, emptyDates, ym)
	}

	srcs := make([]string, 0, len(present)+1)
	for _, d := range present {
		srcs = append(srcs, d.URI)
	}
	if hasOld {
		srcs = append(srcs, old.URI)
	}
	// Unique name so we never read and overwrite the same object in one COPY.
	newName := fmt.Sprintf("m_%s_%d", ym, time.Now().UTC().UnixNano())
	newURI := m.cubeFileURI(spec, newName)
	quoted := make([]string, len(srcs))
	for i, s := range srcs {
		quoted[i] = "'" + s + "'"
	}
	// A plain concatenation is correct: the read path GROUPs by bucket+dims and
	// re-aggregates store columns, so any cross-partition duplicate buckets merge
	// at read time. Copying sketch BLOBs verbatim needs no datasketches extension.
	copySQL := fmt.Sprintf("COPY (SELECT * FROM read_parquet([%s], union_by_name=true)) TO '%s' (FORMAT parquet)",
		strings.Join(quoted, ", "), newURI)
	if _, err := m.db.Exec(copySQL); err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	// Build the merged entry: span and rows from the files that actually exist
	// (missing dailies contribute nothing — their data is gone); Covers = every
	// daily date now represented by this one file.
	schemaHash := old.SchemaHash
	if len(present) > 0 {
		schemaHash = present[0].SchemaHash
	}
	merged := DayEntry{Date: ym, URI: newURI, SchemaHash: schemaHash}
	covered := map[string]bool{}
	add := func(e DayEntry) {
		merged.Rows += e.Rows
		if merged.BucketLo == "" || e.BucketLo < merged.BucketLo {
			merged.BucketLo = e.BucketLo
		}
		if e.BucketHi > merged.BucketHi {
			merged.BucketHi = e.BucketHi
		}
	}
	for _, d := range present {
		add(d)
		covered[d.Date] = true
	}
	if hasOld {
		add(old)
		for _, c := range old.Covers {
			covered[c] = true
		}
	}
	// Genuinely-empty rebuilt days contribute no rows but ARE marked covered, so the
	// read-path interior-gap net treats them as known-built (a legitimately-empty day,
	// not a hole) rather than falling every spanning query to source.
	for _, d := range emptyDates {
		covered[d] = true
	}
	for c := range covered {
		merged.Covers = append(merged.Covers, c)
	}
	sort.Strings(merged.Covers)

	// Swap the daily (and old monthly) entries for the single merged entry. Both the
	// present and the missing dailies are dropped from the manifest: present ones are
	// folded into the merged file; missing ones are stale pointers we purge so they
	// stop being retried. Only present files are scheduled for deletion (the missing
	// ones are already gone).
	supersededURIs := make([]string, 0, len(srcs))
	drop := map[string]bool{}
	for _, d := range present {
		drop[d.Date] = true
		supersededURIs = append(supersededURIs, d.URI)
	}
	for _, d := range missing {
		drop[d.Date] = true
	}
	for _, d := range emptyDates {
		drop[d] = true // folded into the merged month's Covers as known-empty
	}
	if hasOld {
		drop[old.Date] = true
		supersededURIs = append(supersededURIs, old.URI)
	}
	kept := man.Days[:0]
	for _, d := range man.Days {
		if !drop[d.Date] {
			kept = append(kept, d)
		}
	}
	man.Days = kept
	man.Upsert(merged)
	if err := m.writeManifest(ctx, spec, man); err != nil {
		return fmt.Errorf("manifest: %w", err) // new file orphaned; read path still on old files
	}
	m.updateRouter(man)

	// Safe to delete now: nothing references the superseded files.
	keys := make([]string, len(supersededURIs))
	for i, u := range supersededURIs {
		keys[i] = m.keyFromURI(u)
	}
	if err := m.stg.DeleteBatch(ctx, keys); err != nil {
		m.log.Warn().Str("cube", CubeID(spec)).Err(err).Msg("Rollup compaction: superseded files not deleted (orphaned)")
	}
	m.log.Info().Str("cube", CubeID(spec)).Str("month", ym).Int("merged_files", len(srcs)).Int64("rows", merged.Rows).Msg("Rollup compacted month")
	return nil
}

// partitionExistingDailies splits daily entries into those whose backing object
// still exists and those that have gone missing (a stale manifest pointer). It
// stat's each file via the storage backend (one HEAD per file, no data read). A
// stat error other than "absent" is treated as PRESENT — a transient backend
// blip must never cause us to drop a good file (only a definitive -1 size means
// gone), so we never lose real cube data to a flaky check.
func (m *Manager) partitionExistingDailies(ctx context.Context, dailies []DayEntry) (present, missing []DayEntry) {
	for _, d := range dailies {
		size, err := m.stg.StatFile(ctx, m.keyFromURI(d.URI))
		if err == nil && size < 0 {
			missing = append(missing, d) // definitively absent
			continue
		}
		present = append(present, d) // exists, or stat errored transiently — keep it
	}
	return present, missing
}

// rebuildMissingDailies re-materializes each missing daily cube file directly from
// source so a purged/expired/never-landed file does not become a permanent hole in
// the compacted month. For each missing date it runs the same day build the forward
// loop uses (identical aggregation, so the file is bit-identical to the original):
//   - recovered: source still has the day's rows; a fresh daily file was written and
//     its entry is returned to be folded into the compaction (so the month is gap-free).
//   - empty: source returned zero rows — the day legitimately has no data; no file is
//     written but the date is reported so the caller can mark it known-built (covered),
//     keeping it out of the read-path gap net.
//   - unresolved: the day's source could not be read at all (transient) — left as a
//     stale pointer for the caller to handle (purge / retry), never silently compacted.
//
// This is build-side approach B: never publish a gapped manifest.
func (m *Manager) rebuildMissingDailies(ctx context.Context, spec CubeSpec, ym string, missing []DayEntry) (recovered []DayEntry, empty []string, unresolved []DayEntry) {
	for _, d := range missing {
		select {
		case <-ctx.Done():
			unresolved = append(unresolved, d)
			continue
		default:
		}
		day, err := time.Parse("2006-01-02", d.Date)
		if err != nil {
			unresolved = append(unresolved, d) // not a daily date — cannot rebuild
			continue
		}
		dest := m.cubeFileURI(spec, fmt.Sprintf("%s_%d", d.Date, time.Now().UTC().UnixNano()))
		entry, berr := BuildDay(m.db, spec, m.sourceDayGlob(spec.Source, day), m.cfg.TimeCol, d.Date, dest)
		if berr != nil {
			m.log.Warn().Str("cube", CubeID(spec)).Str("day", d.Date).Err(berr).
				Msg("Rollup compaction: rebuild of missing daily from source failed (transient); leaving for retry")
			unresolved = append(unresolved, d)
			continue
		}
		if entry.Rows == 0 {
			empty = append(empty, d.Date) // source genuinely empty for this day
			continue
		}
		recovered = append(recovered, entry)
	}
	return recovered, empty, unresolved
}

// purgeMissingDailies removes stale daily entries (whose files are gone and could
// not be rebuilt from source) from the manifest. When the month had genuinely-empty
// rebuilt days it also writes a zero-row covered entry recording them as known-built,
// so the read-path interior-gap net does not mistake a legitimately-empty day for a
// purged hole. Used when a month has nothing real left to compact, so it stops being
// retried every tick.
func (m *Manager) purgeMissingDailies(ctx context.Context, spec CubeSpec, man *Manifest, missing []DayEntry, emptyDates []string, ym string) error {
	if len(missing) == 0 && len(emptyDates) == 0 {
		return nil
	}
	drop := map[string]bool{}
	for _, d := range missing {
		drop[d.Date] = true
	}
	for _, d := range emptyDates {
		drop[d] = true
	}
	kept := man.Days[:0]
	for _, d := range man.Days {
		if !drop[d.Date] {
			kept = append(kept, d)
		}
	}
	man.Days = kept
	// Record genuinely-empty days as a known-built, zero-row covered marker so a query
	// spanning them is served from the cube (correct: they contribute nothing) rather
	// than tripping the interior-gap net. The marker has NO file URI and NO bucket span:
	// it is pure coverage metadata, so DaysInRange/ReadExpr skip it (empty bucket bounds
	// never overlap a window) while coveredDays() still registers its Covers dates as
	// known-built. _ = ym (kept for signature symmetry / future per-month markers).
	_ = ym
	if len(emptyDates) > 0 {
		sort.Strings(emptyDates)
		// One marker per empty date keeps Upsert idempotent (keyed by Date) and avoids
		// clobbering an unrelated month marker.
		for _, d := range emptyDates {
			man.Upsert(DayEntry{
				Date:       d + "-empty",
				SchemaHash: spec.SchemaHash(),
				Covers:     []string{d},
			})
		}
	}
	if err := m.writeManifest(ctx, spec, man); err != nil {
		return fmt.Errorf("manifest (purge): %w", err)
	}
	m.updateRouter(man)
	m.log.Info().Str("cube", CubeID(spec)).Int("purged", len(missing)).Int("empty", len(emptyDates)).
		Msg("Rollup compaction: purged stale daily manifest entries (no real files left to compact)")
	return nil
}

// recentSampleGlob points the classifier at the whole table (it row-caps its own
// sample), keeping classification independent of which day is latest.
func (m *Manager) recentSampleGlob(source string) string {
	db2, meas := splitSource(source)
	return fmt.Sprintf("read_parquet('s3://%s/%s/%s/**/*.parquet', union_by_name=true)", m.s3.Bucket, db2, meas)
}

func (m *Manager) sourceDayGlob(source string, day time.Time) string {
	db2, meas := splitSource(source)
	return fmt.Sprintf("['s3://%s/%s/%s/%s/**/*.parquet']", m.s3.Bucket, db2, meas, day.Format("2006/01/02"))
}

func (m *Manager) cubeDayURI(spec CubeSpec, date string) string {
	return m.cubeFileURI(spec, date)
}

// cubeFileURI is the full s3:// URI of a cube file named <name>.parquet (a daily
// date, or a compacted "m_<month>_<nanos>" file).
func (m *Manager) cubeFileURI(spec CubeSpec, name string) string {
	return fmt.Sprintf("s3://%s/%s/%s/%s.parquet", m.s3.Bucket, m.cfg.StoragePrefix, cubeDir(spec), name)
}

// keyFromURI strips the s3://<bucket>/ prefix to the bucket-relative object key
// the storage backend's DeleteBatch expects.
func (m *Manager) keyFromURI(uri string) string {
	return strings.TrimPrefix(uri, fmt.Sprintf("s3://%s/", m.s3.Bucket))
}

func (m *Manager) manifestKey(spec CubeSpec) string {
	return fmt.Sprintf("%s/%s/manifest.json", m.cfg.StoragePrefix, cubeDir(spec))
}

// loadManifest reads a cube's manifest. ok=false means the read FAILED
// transiently (e.g. an S3 timeout) — the caller must NOT treat that as an empty
// cube and rebuild from scratch (which would destroy good cube data and trigger a
// slow re-backfill). ok=true returns either the stored manifest or, for a
// genuinely-absent / schema-changed cube, a fresh empty one to (re)build into.
func (m *Manager) loadManifest(ctx context.Context, spec CubeSpec) (*Manifest, bool) {
	b, err := m.stg.Read(ctx, m.manifestKey(spec))
	if err != nil {
		if isStorageNotFound(err) {
			return m.freshManifest(spec), true // genuinely new cube — build it
		}
		return nil, false // transient read failure — skip this cube this tick
	}
	if len(b) > 0 {
		if man, perr := ParseManifest(b); perr == nil && man.SchemaHash == spec.SchemaHash() {
			return man, true
		}
		// empty/corrupt body or a schema change (new dims/aggs) → rebuild fresh.
	}
	return m.freshManifest(spec), true
}

func (m *Manager) freshManifest(spec CubeSpec) *Manifest {
	return &Manifest{CubeID: CubeID(spec), Source: spec.Source, Grain: spec.Grain, Dims: spec.Dims, Aggs: spec.Aggs, SchemaHash: spec.SchemaHash()}
}

// isStorageNotFound reports whether a storage Read error means "object absent"
// (safe to build a fresh cube) versus a transient failure (must not rebuild).
// String-matched to stay backend-agnostic (S3 / local / Azure / resilient).
func isStorageNotFound(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	for _, mark := range []string{"nosuchkey", "not found", "notfound", "404", "no such file", "does not exist", "not exist", "objectnotfound"} {
		if strings.Contains(s, mark) {
			return true
		}
	}
	return false
}

func (m *Manager) writeManifest(ctx context.Context, spec CubeSpec, man *Manifest) error {
	b, err := man.Bytes()
	if err != nil {
		return err
	}
	return m.stg.Write(ctx, m.manifestKey(spec), b)
}

// updateRouter records the latest manifest for one cube and swaps in a freshly
// built (immutable) Router. Copy-on-write: Route never sees a half-mutated router.
func (m *Manager) updateRouter(man *Manifest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.manifests[cubeKeyOf(man.Spec())] = man
	m.rebuildRouterLocked()
}

// rebuildRouterLocked constructs a new Router from the current manifest set.
// Caller must hold m.mu.
func (m *Manager) rebuildRouterLocked() {
	manifests := make([]*Manifest, 0, len(m.manifests))
	for _, man := range m.manifests {
		if len(man.Days) > 0 {
			manifests = append(manifests, man)
		}
	}
	r := NewRouter(manifests, m.cfg.TimeCol, m.sourceExpr, m.watermark)
	r.OnQuery = m.workload.Record
	m.router = r
}

// reloadRouter reloads all manifests from storage (startup + end of each tick),
// so a restart serves from already-materialized cubes. Manifests are found by
// globbing the cube prefix.
func (m *Manager) reloadRouter(ctx context.Context) {
	pat := fmt.Sprintf("s3://%s/%s/*/*/*/manifest.json", m.s3.Bucket, m.cfg.StoragePrefix)
	loaded := map[string]*Manifest{}
	for _, file := range m.globFiles(pat) {
		key := strings.TrimPrefix(file, fmt.Sprintf("s3://%s/", m.s3.Bucket))
		b, err := m.stg.Read(ctx, key)
		if err != nil || len(b) == 0 {
			continue
		}
		if man, err := ParseManifest(b); err == nil {
			loaded[cubeKeyOf(man.Spec())] = man
		}
	}
	m.mu.Lock()
	for k, v := range loaded {
		m.manifests[k] = v
	}
	m.rebuildRouterLocked() // sets OnQuery on the new router
	m.mu.Unlock()
}

// sourceExpr / watermark are the Router callbacks. Watermark is the seal boundary
// (now-grace); everything before it is served from cubes, the fresh tail from
// source via merge-on-read.
func (m *Manager) sourceExpr(source string) string {
	db2, meas := splitSource(source)
	return fmt.Sprintf("['s3://%s/%s/%s/**/*.parquet']", m.s3.Bucket, db2, meas)
}

func (m *Manager) watermark(string) string {
	return fmtTS(time.Now().UTC().Add(-m.cfg.Grace))
}

// CubeID is the stable storage/manifest identifier for a cube: source +
// dimensions. The classifier produces one cube per dimension-set per source, so
// this is unique.
func CubeID(s CubeSpec) string {
	db, table := splitSource(s.Source)
	return db + "." + table + "." + cubeKind(s)
}

// cubeKind is the cube's role within its table: "coarse" (no dims) or "by_<dims>".
func cubeKind(s CubeSpec) string {
	if len(s.Dims) == 0 {
		return "coarse"
	}
	return "by_" + strings.Join(s.Dims, "_")
}

// cubeDir is the object-storage folder for a cube: <database>/<table>/<kind>, so
// each database and table gets its own folder under the rollup prefix.
func cubeDir(s CubeSpec) string {
	db, table := splitSource(s.Source)
	return db + "/" + table + "/" + cubeKind(s)
}
