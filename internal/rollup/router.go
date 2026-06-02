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

	// SourceExpr maps a logical source ("default.downloads") to the read_parquet
	// argument for raw source (used for the fresh tail / head patch).
	SourceExpr func(source string) string
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
func (r *Router) Route(sql string) Decision { return r.route(sql, true) }

// Explain returns the same decision as Route WITHOUT recording the query to the
// workload — for a non-executing "will this roll up?" check from the query editor.
// (Recording editor keystrokes would nudge cube selection toward shapes nobody
// has actually run.)
func (r *Router) Explain(sql string) Decision { return r.route(sql, false) }

func (r *Router) route(sql string, record bool) Decision {
	shape, ok, reason := Parse(sql, r.TimeCol)
	if ok {
		if record && r.OnQuery != nil {
			r.OnQuery(shape) // record the workload (served or not) to drive cube selection
		}
		out, cube, served, why := r.serveShape(shape)
		if served {
			return Decision{Served: true, SQL: out, Cube: cube}
		}
		return Decision{Reason: why}
	}
	// Not a top-level simple aggregate. Try CTE-base rewriting: when the query is
	// `WITH base AS (<rollup-servable aggregation>) <CASE / TopN / …>`, serve just
	// the base CTE from the cube and let DuckDB run the lightweight outer SQL.
	if d, handled := r.tryRewriteCTEBase(sql, record); handled {
		return d
	}
	return Decision{Reason: "parse:" + reason}
}

// serveShape produces the cube-read SQL for a parsed shape (cube-only when the
// window is fully sealed, merge-on-read otherwise). served=false carries a reason.
func (r *Router) serveShape(shape QueryShape) (sql, cube string, served bool, reason string) {
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
	if wm == "" {
		return shape.CubeReadSQL(cubeExpr), label, true, ""
	}
	srcExpr := ""
	if r.SourceExpr != nil {
		srcExpr = r.SourceExpr(shape.Source)
	}
	out, ok := shape.MergeReadSQL(*spec, cubeExpr, srcExpr, wm)
	if !ok {
		return "", "", false, "merge_emit_failed"
	}
	return out, label, true, ""
}

// RouteHTTP adapts Route to the api.RollupRouter interface signature (matched
// structurally, so neither package imports the other). headerDB is reserved for
// threading non-default databases into the parser; the corpus is "default".
func (r *Router) RouteHTTP(sql, headerDB string) (rewritten string, served bool, cube string) {
	d := r.Route(sql)
	return d.SQL, d.Served, d.Cube
}

// ExplainHTTP adapts Explain to the api.RollupRouter interface: a non-executing
// "will this roll up?" check returning whether a cube covers the query, which
// one, and (when not) a human-readable reason.
func (r *Router) ExplainHTTP(sql, headerDB string) (served bool, cube, reason string) {
	d := r.Explain(sql)
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
