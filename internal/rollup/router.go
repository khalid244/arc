package rollup

import "strings"

// Router is the read-path entry point: it turns a raw incoming aggregate SQL
// string into a rewritten query against the appropriate cube, or reports that
// the query is not served (the caller then runs it against source unchanged).
//
// It composes the proven pieces: Parse (SQL -> QueryShape), PickNarrowest
// (coverage match), the per-cube Manifest (S3-latency range pruning), and the
// merge-on-read emitter. Every uncertain case falls through to source — a
// served=false result is always safe.
type Router struct {
	Cubes     []CubeSpec
	Manifests map[string]*Manifest // keyed by cubeKeyOf(spec)
	TimeCol   string               // time column for the parser (e.g. "time")

	// SourceExpr maps a logical source ("default.downloads") to the WHOLE-TABLE
	// read_parquet argument for raw source. Used as the fallback fresh-tail/head
	// glob when SourceWindow is unset (correct but unpruned — footer-reads the
	// whole table).
	SourceExpr func(source string) string
	// SourceWindow, when set, resolves an EXISTENCE-PRUNED source glob for a
	// (source, lo, hi) window — only the day partitions that hold files, "" when
	// the window is confidently empty, the whole-table glob on uncertainty (see
	// prunedSourceGlob). It supersedes SourceExpr for the merge source branches so
	// the fresh-tail read lists only the days it needs instead of the whole table.
	SourceWindow func(source, lo, hi string) string
	// Watermark returns the seal boundary for a source as a timestamp literal
	// body; everything before it is served from the cube, the tail from source.
	// Return "" to mean "fully sealed" (no fresh tail needed).
	Watermark func(source string) string

	// OnQuery, when set, is called with every successfully-parsed aggregate shape
	// (served or not) so the Manager can learn which dimensions are queried and
	// build per-dim cubes for them. Must be cheap and non-blocking.
	OnQuery func(QueryShape)
}

// NewRouter rebuilds a Router from a set of loaded manifests (e.g. read from
// object storage at startup). sourceExpr maps a logical source to its raw
// read_parquet argument (for the fresh tail); watermark returns the seal
// boundary per source ("" = fully sealed, cube-only — correct for static data).
//
// Each manifest is DEEP-COPIED (m.clone()) before being stored: the Router serves
// only frozen snapshots, fully decoupled from the build's working manifests. The
// build pipeline keeps mutating its own *Manifest in place (Upsert append+sort,
// Days = kept, purge) — those mutations can never reach a Router that has already
// published, and a query iterating m.Days lock-free is reading a private copy that
// nothing else writes. This is the entire read-path race fix: the snapshot cost
// lives here, on the infrequent publish, not on the hot per-query path.
func NewRouter(manifests []*Manifest, timeCol string, sourceExpr, watermark func(string) string) *Router {
	r := &Router{TimeCol: timeCol, Manifests: map[string]*Manifest{}, SourceExpr: sourceExpr, Watermark: watermark}
	for _, m := range manifests {
		spec := m.Spec()
		r.Cubes = append(r.Cubes, spec)
		r.Manifests[cubeKeyOf(spec)] = m.clone()
	}
	return r
}

// Decision is the outcome of routing one query.
type Decision struct {
	Served bool   // true if SQL was rewritten onto a cube
	SQL    string // the rewritten query when Served; original concern of caller otherwise
	Cube   string // which cube served it (cubeKeyOf), for observability
	Reason string // why not served (parse/coverage/manifest), for /explain headers
}

// Route decides whether and how to serve sql from a cube, recording the parsed
// shape to the workload so it can drive cube selection.
func (r *Router) Route(sql string) Decision { return r.route(sql, "", true, false) }

// RouteOnly is the BEST-EFFORT CUBE-ONLY entry point behind X-Arc-Rollup-Only:
// when the query SHAPE matches a cube that has materialized at least one file,
// it serves from the cube UNCONDITIONALLY — whatever days exist in the range
// (missing days are simply missing rows / chart gaps), a zero-day range becomes
// a schema-correct zero-row read, and source is NEVER touched (no watermark
// merge, no fallback). Declines only for shape-level reasons (parse failure,
// no_covering_cube, grain_too_fine, cube never materialized), which the api
// maps to 422. The coverage guards (leading/interior gap, fresh tail) are
// deliberately skipped here: they protect AUTO mode's silent-correctness, where
// falling to source keeps results complete — in rollup-only mode the operator
// asked for "cube or nothing", so partial coverage IS the contract.
func (r *Router) RouteOnly(sql string) Decision { return r.route(sql, "", true, true) }

// Explain returns the same decision as Route WITHOUT recording the query to the
// workload — for a non-executing "will this roll up?" check from the query editor.
// (Recording editor keystrokes would nudge cube selection toward shapes nobody
// has actually run.) Explain always reports the AUTO-mode decision (coverage
// guards included), so the editor hint matches what an auto run would do.
func (r *Router) Explain(sql string) Decision { return r.route(sql, "", false, false) }

func (r *Router) route(sql, headerDB string, record, bestEffort bool) Decision {
	shape, ok, reason := ParseWithDB(sql, r.TimeCol, headerDB)
	if ok {
		if record && r.OnQuery != nil {
			r.OnQuery(shape) // record the workload (served or not) to drive cube selection
		}
		out, cube, served, why := r.serveShape(shape, bestEffort)
		if served {
			return Decision{Served: true, SQL: out, Cube: cube}
		}
		return Decision{Reason: why}
	}
	// Not a top-level simple aggregate. Try CTE-base rewriting: when the query is
	// `WITH base AS (<rollup-servable aggregation>) <CASE / TopN / …>`, serve just
	// the base CTE from the cube and let DuckDB run the lightweight outer SQL.
	if d, handled := r.tryRewriteCTEBase(sql, headerDB, record, bestEffort); handled {
		return d
	}
	return Decision{Reason: "parse:" + reason}
}

// serveShape produces the cube-read SQL for a parsed shape (cube-only when the
// window is fully sealed, merge-on-read otherwise). served=false carries a reason.
// bestEffort selects the rollup-only contract (see RouteOnly): coverage guards
// and the source merge are skipped, the cube serves whatever it has.
func (r *Router) serveShape(shape QueryShape, bestEffort bool) (sql, cube string, served bool, reason string) {
	spec := PickNarrowest(r.Cubes, shape)
	if spec == nil {
		return "", "", false, r.whyNoCover(shape)
	}
	key := cubeKeyOf(*spec)
	m := r.Manifests[key]
	if m == nil || len(m.Days) == 0 {
		return "", "", false, "no_manifest"
	}
	label := m.CubeID // friendly cube name for the X-Arc-Rollup-Cube header
	if label == "" {
		label = key
	}
	days := m.DaysInRange(shape.TimeLo, shape.TimeHi)
	cubeExpr := ReadExpr(days)
	if bestEffort {
		return r.serveShapeBestEffort(shape, m, label, cubeExpr)
	}
	if cubeExpr == "" {
		return "", "", false, "no_days_in_range"
	}
	// A gap at the start means silent undercount — fall through to source instead.
	if !manifestCoversStart(m, shape.TimeLo) {
		return "", "", false, "coverage_gap"
	}
	// An INTERIOR gap (a day missing from the middle of the cube's coverage, e.g. a
	// purged/expired/never-landed file) is the silent-undercount trap: DaysInRange
	// returns the surrounding files, the start-check passes, and the cube read omits
	// the missing day's rows with no error — so the read-path source fallback (which
	// only fires on an execution error) never engages. Detect it here and fall the
	// whole query to source, which is always correct. Generalizes the leading
	// coverage_gap guard to holes anywhere in the range, from ANY cause.
	if m.HasInteriorGap(shape.TimeLo, shape.TimeHi) {
		return "", "", false, "coverage_gap_interior"
	}
	wm := ""
	if r.Watermark != nil {
		wm = r.Watermark(shape.Source)
	}
	// Clamp the seal-clock watermark (now-grace) to the cube's REAL coverage hi.
	// MergeReadSQL's contract is "the cube is complete below the watermark", but
	// cubes hold whole sealed DAYS, so the newest cube data ends at a midnight
	// (coverage hi) while the seal clock has already crossed into today. Passing the
	// raw watermark makes MergeReadSQL read buckets in [coverageHi, alignDown(wm))
	// from cube files that do not exist — zero rows, no error: a silent undercount.
	// Capping the watermark at coverage hi keeps the contract truthful: the source
	// tail then starts at coverage hi, so every bucket comes from exactly one branch.
	// When coverage hi >= wm (a fully-fresh cube, e.g. before grace has elapsed past
	// the last sealed day) the cap is a no-op and behavior is unchanged.
	if wm != "" {
		if _, covHi, ok := m.Coverage(); ok {
			if w, ok := parseTS(wm); ok && covHi.Before(w) {
				wm = fmtTS(covHi)
			}
		}
	}
	if wm == "" {
		return shape.CubeReadSQL(cubeExpr), label, true, ""
	}
	out, ok := shape.MergeReadSQL(*spec, cubeExpr, r.sourceGlobFn(shape.Source), wm)
	if !ok {
		return "", "", false, "merge_emit_failed"
	}
	return out, label, true, ""
}

// sourceGlobFn binds a source to its merge-branch glob resolver: the
// existence-pruned SourceWindow when wired (production), else the whole-table
// SourceExpr (correct but unpruned), else a no-op that yields no source branch.
func (r *Router) sourceGlobFn(source string) SourceGlobFn {
	if r.SourceWindow != nil {
		return func(lo, hi string) string { return r.SourceWindow(source, lo, hi) }
	}
	if r.SourceExpr != nil {
		whole := r.SourceExpr(source)
		return func(string, string) string { return whole }
	}
	return func(string, string) string { return "" }
}

// serveShapeBestEffort emits the rollup-only (X-Arc-Rollup-Only) cube read:
// always a plain cube read clipped to the requested range — existing days only,
// no coverage guards, no source merge. A range overlapping ZERO cube days still
// returns a schema-correct result: DuckDB's read_parquet rejects an empty file
// list, so the read is anchored on ONE real cube file (the newest) under an
// impossible bucket predicate (hi = lo), preserving column names and types
// while guaranteeing zero rows. Only a manifest with no file-backed entry at
// all (cube never materialized — nothing to anchor a schema on) declines,
// keeping the api's 422 for that case.
func (r *Router) serveShapeBestEffort(shape QueryShape, m *Manifest, label, cubeExpr string) (sql, cube string, served bool, reason string) {
	if cubeExpr != "" {
		return shape.CubeReadSQL(cubeExpr), label, true, ""
	}
	probe, ok := newestFileEntry(m)
	if !ok {
		return "", "", false, "no_manifest"
	}
	return shape.CubeReadEmptySQL(ReadExpr([]DayEntry{probe})), label, true, ""
}

// newestFileEntry returns the most recent manifest entry backed by a real cube
// file, skipping coverage-only '-empty' markers (they carry no URI and no
// bucket span). ok=false means the manifest holds no files at all.
func newestFileEntry(m *Manifest) (DayEntry, bool) {
	for i := len(m.Days) - 1; i >= 0; i-- {
		if m.Days[i].URI != "" {
			return m.Days[i], true
		}
	}
	return DayEntry{}, false
}

// RouteHTTP adapts Route to the api.RollupRouter interface signature (matched
// structurally, so neither package imports the other). headerDB is the request's
// x-arc-database header: an unqualified FROM table resolves to it, so the shape
// is recorded — and coverage-matched — under the database the query actually
// runs against (see ParseWithDB).
func (r *Router) RouteHTTP(sql, headerDB string) (rewritten string, served bool, cube string) {
	d := r.route(sql, headerDB, true, false)
	return d.SQL, d.Served, d.Cube
}

// RouteOnlyHTTP adapts RouteOnly (best-effort cube-only, X-Arc-Rollup-Only) to
// the api.RollupRouter interface. It is a real-query path, so the shape is
// recorded to the workload exactly like RouteHTTP.
func (r *Router) RouteOnlyHTTP(sql, headerDB string) (rewritten string, served bool, cube string) {
	d := r.route(sql, headerDB, true, true)
	return d.SQL, d.Served, d.Cube
}

// ExplainHTTP adapts Explain to the api.RollupRouter interface: a non-executing
// "will this roll up?" check returning whether a cube covers the query, which
// one, and (when not) a human-readable reason. headerDB resolves unqualified
// tables exactly as RouteHTTP does, so the explanation matches what a real run
// would decide.
func (r *Router) ExplainHTTP(sql, headerDB string) (served bool, cube, reason string) {
	d := r.route(sql, headerDB, false, false)
	return d.Served, d.Cube, humanizeReason(d.Reason)
}

// humanizeReason maps the router's terse decision codes to a one-line explanation
// the query editor can show before the query runs.
func humanizeReason(code string) string {
	switch code {
	case "":
		return ""
	case "no_covering_cube":
		return "no cube stores this query's group-by dimensions"
	case "grain_too_fine":
		return "time bucket is finer than the hourly cube; widen the range so the bucket is ≥ 1h"
	case "no_manifest", "no_days_in_range":
		return "no cube data materialized for this time range yet"
	case "coverage_gap":
		return "cube has a gap at the start of the range; runs against source to avoid undercount"
	case "coverage_gap_interior":
		return "cube is missing a day inside the range; runs against source to avoid undercount"
	case "merge_emit_failed":
		return "could not build the merge-on-read query for this shape"
	}
	if strings.HasPrefix(code, "parse:") {
		return "query shape not rollup-able: " + strings.TrimPrefix(code, "parse:")
	}
	if strings.HasPrefix(code, "cte base: ") {
		return "the base CTE is not rollup-able: " + strings.TrimPrefix(code, "cte base: ")
	}
	return code
}

// whyNoCover distinguishes the two reasons PickNarrowest finds nothing: a time
// bucket finer than every cube's grain (actionable — widen the range), versus a
// genuine dimension/aggregate gap.
func (r *Router) whyNoCover(q QueryShape) string {
	for i := range r.Cubes {
		if r.Cubes[i].coversExceptGrain(q) && !grainDivides(r.Cubes[i].Grain, q.Grain) {
			return "grain_too_fine"
		}
	}
	return "no_covering_cube"
}

// manifestCoversStart reports whether the manifest's earliest stored bucket is at
// or before the query's lower bound (no leading gap). The fresh tail beyond the
// watermark is patched from source, so only the start needs cube coverage.
func manifestCoversStart(m *Manifest, lo string) bool {
	covLo, _, ok := m.Coverage()
	if !ok {
		return false
	}
	loT, ok := parseTS(lo)
	if !ok {
		return false
	}
	return !covLo.After(loT)
}
