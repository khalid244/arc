package rollup

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// quoteIdent renders s as a DuckDB quoted identifier: wrapped in double quotes
// with every embedded double quote doubled. It is the ONLY way the rollup
// renders an identifier taken from a query, a schema or a spec. Go's %q is not
// equivalent — it escapes a quote as \" and a backslash as \\, which DuckDB
// does not unescape, so a name holding a double quote broke out of the
// identifier and rewrote the emitted statement.
func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// storeIdent renders a cube store-column name (_cnt, _sum_<col>, ...). The name
// embeds a sanitized source column name, so it is emitted bare only when it is a
// plain identifier (every built-in store name, and every name from an ordinary
// column, starts with '_' and so is never a keyword) — keeping the SQL of
// existing cubes byte-identical — and quoted otherwise.
func storeIdent(name string) string {
	if plainIdent.MatchString(name) {
		return name
	}
	return quoteIdent(name)
}

var plainIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// origExpr renders the aggregate exactly as written against raw source rows —
// used by the compare harness to produce the ground-truth reference result.
func (a Aggregate) origExpr() string {
	switch a.Kind {
	case AggCount:
		return "count(*)"
	case AggCountCol:
		return fmt.Sprintf("count(%s)", quoteIdent(a.Col))
	case AggSum:
		return fmt.Sprintf("sum(%s)", quoteIdent(a.Col))
	case AggMin:
		return fmt.Sprintf("min(%s)", quoteIdent(a.Col))
	case AggMax:
		return fmt.Sprintf("max(%s)", quoteIdent(a.Col))
	case AggAvg:
		return fmt.Sprintf("avg(%s)", quoteIdent(a.Col))
	case AggCountDistinct:
		return fmt.Sprintf("count(DISTINCT %s)", quoteIdent(a.Col))
	case AggPercentile:
		return fmt.Sprintf("quantile_cont(%s, %g)", quoteIdent(a.Col), a.P)
	case AggCondSum:
		then := a.ThenK
		if a.ThenCol != "" {
			then = quoteIdent(a.ThenCol)
		}
		return fmt.Sprintf("sum(CASE WHEN %s THEN %s ELSE %s END)", a.Cond, then, a.ElseK)
	}
	return "NULL"
}

// sql renders the constant: a string single-quoted (embedded quotes doubled), a
// number or NULL as its SQL text. Non-string text that is not a plain number or
// NULL (a Lit not built by constLit) is quoted, never emitted raw.
func (l Lit) sql() string {
	if !l.Str && (l.V == "NULL" || numericText.MatchString(l.V)) {
		return l.V
	}
	return "'" + strings.ReplaceAll(l.V, "'", "''") + "'"
}

var numericText = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$`)

func (f Filter) render() string {
	switch f.Op {
	case OpEq:
		return fmt.Sprintf("%s = %s", quoteIdent(f.Col), f.Values[0].sql())
	case OpNe:
		return fmt.Sprintf("%s <> %s", quoteIdent(f.Col), f.Values[0].sql())
	case OpDistinct:
		return fmt.Sprintf("%s IS DISTINCT FROM %s", quoteIdent(f.Col), f.Values[0].sql())
	case OpNotDistinct:
		return fmt.Sprintf("%s IS NOT DISTINCT FROM %s", quoteIdent(f.Col), f.Values[0].sql())
	case OpIn, OpNotIn:
		vs := make([]string, len(f.Values))
		for i, v := range f.Values {
			vs[i] = v.sql()
		}
		op := "IN"
		if f.Op == OpNotIn {
			op = "NOT IN"
		}
		return fmt.Sprintf("%s %s (%s)", quoteIdent(f.Col), op, strings.Join(vs, ", "))
	case OpIsNull:
		return fmt.Sprintf("%s IS NULL", quoteIdent(f.Col))
	case OpIsNotNull:
		return fmt.Sprintf("%s IS NOT NULL", quoteIdent(f.Col))
	}
	return "TRUE"
}

func renderFilters(fs []Filter) string {
	if len(fs) == 0 {
		return ""
	}
	parts := make([]string, len(fs))
	for i, f := range fs {
		parts[i] = f.render()
	}
	return " AND " + strings.Join(parts, " AND ")
}

// selectList renders "bucket?, dims..., aggExpr AS alias..." given a function that
// maps each Aggregate to its SQL expression (origExpr for source, finalExpr for cube).
func (q QueryShape) bucketAlias() string {
	if q.BucketAlias != "" {
		return q.BucketAlias
	}
	return "bucket"
}

func (q QueryShape) selectList(bucket string, aggExpr func(Aggregate) string) string {
	var sel []string
	if bucket != "" {
		sel = append(sel, fmt.Sprintf("%s AS %s", bucket, quoteIdent(q.bucketAlias())))
	}
	for _, d := range q.Dims {
		sel = append(sel, quoteIdent(d))
	}
	for _, a := range q.Aggs {
		sel = append(sel, fmt.Sprintf("%s AS %s", aggExpr(a), quoteIdent(a.Alias)))
	}
	return strings.Join(sel, ", ")
}

// groupOrder renders GROUP BY then ORDER BY over bucket (if any) + dims, by
// position. The ORDER BY reproduces the user's clause when present (so TopN keeps
// the right rows); otherwise it falls back to the grouping order for stable output.
func (q QueryShape) groupOrder(hasBucket bool) string {
	n := len(q.Dims)
	if hasBucket {
		n++
	}
	if n == 0 {
		return q.orderClause(nil) // no grouping (grand total); still honor a user ORDER BY
	}
	pos := make([]string, n)
	for i := range pos {
		pos[i] = strconv.Itoa(i + 1)
	}
	g := strings.Join(pos, ", ")
	return fmt.Sprintf(" GROUP BY %s%s", g, q.orderClause(pos))
}

// orderClause renders " ORDER BY …": the user's resolved positions when set, else
// the supplied group positions (groupPos may be nil to emit no ORDER BY).
func (q QueryShape) orderClause(groupPos []string) string {
	if len(q.OrderBy) > 0 {
		parts := make([]string, len(q.OrderBy))
		for i, k := range q.OrderBy {
			dir := ""
			if k.Desc {
				dir = " DESC"
			}
			parts[i] = strconv.Itoa(k.Pos) + dir
		}
		return " ORDER BY " + strings.Join(parts, ", ")
	}
	if len(groupPos) == 0 {
		return ""
	}
	return " ORDER BY " + strings.Join(groupPos, ", ")
}

// limitClause renders the user's LIMIT (applied after the bucket-ordered output,
// matching a Grafana `ORDER BY time LIMIT n`); "" when no limit.
func (q QueryShape) limitClause() string {
	if q.Limit > 0 {
		return fmt.Sprintf(" LIMIT %d", q.Limit)
	}
	return ""
}

// SourceRefSQL is the ground-truth query against raw source rows for [lo,hi).
// This is what every cube result is compared against in the test harness.
func (q QueryShape) SourceRefSQL(sourceExpr string) string {
	tc := quoteIdent(q.TimeCol)
	bucket := bucketExpr(q.Grain, tc)
	sel := q.selectList(bucket, Aggregate.origExpr)
	where := fmt.Sprintf("%s >= TIMESTAMPTZ '%s' AND %s < TIMESTAMPTZ '%s'", tc, q.TimeLo, tc, q.TimeHi)
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s%s%s",
		sel, readParquetFrom(sourceExpr), where, renderFilters(q.Filters), q.groupOrder(bucket != "")+q.limitClause())
}

// CubeReadSQL serves the query purely from cube rows in [lo,hi). Valid when the
// whole query window is sealed (timeHi <= watermark); freshness is handled by
// MergeReadSQL. cubeExpr is the read_parquet(...) argument for the cube files.
func (q QueryShape) CubeReadSQL(cubeExpr string) string {
	bucket := bucketExpr(q.Grain, "bucket")
	sel := q.selectList(bucket, Aggregate.finalExpr)
	where := fmt.Sprintf("bucket >= TIMESTAMPTZ '%s' AND bucket < TIMESTAMPTZ '%s'", q.TimeLo, q.TimeHi)
	// union_by_name: cube files are written with the full store schema, but a
	// legacy/foreign file with a narrower schema must merge-by-name (absent
	// columns read as NULL, which the merge aggregates ignore) instead of
	// surfacing a Binder Error to the user — cheap defense in depth.
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s%s%s",
		sel, readParquetFrom(cubeExpr), where, renderFilters(q.Filters), q.groupOrder(bucket != "")+q.limitClause())
}

// CubeReadEmptySQL emits a schema-correct ZERO-ROW cube read for a range that
// overlaps no materialized day (rollup-only best-effort mode). cubeExpr must
// reference at least one REAL cube file — read_parquet rejects an empty list —
// which anchors the column names and types, while the impossible bucket
// predicate (lo <= bucket < lo) guarantees no rows survive; DuckDB prunes the
// scan at the parquet footer. "No data yet for this chunk" thus renders as an
// empty chart frame instead of a coverage error.
func (q QueryShape) CubeReadEmptySQL(cubeExpr string) string {
	empty := q
	empty.TimeHi = empty.TimeLo
	return empty.CubeReadSQL(cubeExpr)
}

// storePassthrough renders the stored cube branch of a merge: bucket + dims +
// every store column passed through, with sketch BLOBs cast back to their
// concrete sketch type so the column types line up with the freshly-built
// source branches under UNION ALL BY NAME.
func (s CubeSpec) storePassthrough(cubeExpr, lo, hi string) string {
	var sel []string
	sel = append(sel, "bucket")
	for _, d := range s.Dims {
		sel = append(sel, quoteIdent(d))
	}
	for _, sc := range s.orderedStoreCols() {
		name := storeIdent(sc[0])
		switch {
		case strings.HasPrefix(sc[0], "_theta_"):
			sel = append(sel, fmt.Sprintf("%s::%s AS %s", name, thetaType, name))
		case strings.HasPrefix(sc[0], "_kll_"):
			sel = append(sel, fmt.Sprintf("%s::%s AS %s", name, kllType, name))
		default:
			sel = append(sel, name)
		}
	}
	// union_by_name for the same mixed-schema tolerance as CubeReadSQL.
	return fmt.Sprintf("SELECT %s FROM %s WHERE bucket >= TIMESTAMPTZ '%s' AND bucket < TIMESTAMPTZ '%s'",
		strings.Join(sel, ", "), readParquetFrom(cubeExpr), lo, hi)
}

// SourceGlobFn resolves the read_parquet glob arg for raw source rows in [lo,hi)
// (TIMESTAMPTZ literal bodies, UTC), pruned to the day partitions that actually
// exist so the read never errors on an empty day (DuckDB raises "No files found"
// on a zero-match glob). Returns "" when no source files exist in the window —
// MergeReadSQL then omits that source branch (the cube already covers everything
// present). See prunedSourceGlob for the never-lossy contract.
type SourceGlobFn func(lo, hi string) string

// StaticGlob adapts a fixed glob string to a SourceGlobFn (no pruning) — used by
// tests and as the whole-table fallback when no existence resolver is wired.
func StaticGlob(glob string) SourceGlobFn { return func(string, string) string { return glob } }

// MergeReadSQL serves q across the watermark: the whole cube buckets inside
// [lo, min(hi, watermark)) from the cube, and everything else re-aggregated from
// source — a head patch [lo, first whole bucket) when lo is not cube-grain
// aligned, and a tail [last whole bucket end, hi) covering both the fresh data
// past the watermark and a partial trailing bucket when hi is not aligned. A
// cube bucket is never read unless it lies entirely inside the window, and no
// source branch reads outside [lo, hi). All branches share the cube's
// store-column schema and are merged with UNION ALL BY NAME, then the outer
// SELECT applies the final aggregate expressions. A window that is aligned and
// fully sealed is a plain cube read (CubeReadSQL).
//
// source resolves each source branch's glob, pruned to existing day partitions;
// when it returns "" for a branch (no files in that window) the branch is omitted
// rather than emitting a zero-match glob that DuckDB would error on.
//
// CONTRACT: the cube must be COMPLETE below watermark — every bucket in
// [storedLo, storedHi) is read from cube files only. The seal clock
// (now-grace) does not satisfy this on its own because cubes hold whole sealed
// days that end at a midnight earlier than the clock; the router caps the passed
// watermark at the cube's real coverage hi (serveShape) so the assumption holds.
func (q QueryShape) MergeReadSQL(s CubeSpec, cubeExpr string, source SourceGlobFn, watermark string) (string, bool) {
	return q.mergeReadSQL(s, cubeExpr, source, nil, watermark)
}

// mergeReadSQL is MergeReadSQL with the source branches hiding the files that
// exclude returns (compaction inputs whose output is already in place; see
// sourceRelation). exclude is called at most once, and only when a source
// branch is emitted; nil hides nothing.
func (q QueryShape) mergeReadSQL(s CubeSpec, cubeExpr string, source SourceGlobFn, exclude func() []string, watermark string) (string, bool) {
	lo, ok1 := parseTS(q.TimeLo)
	hi, ok2 := parseTS(q.TimeHi)
	w, ok3 := parseTS(watermark)
	if !ok1 || !ok2 || !ok3 {
		return "", false
	}
	cg := s.Grain
	// Whole cube buckets inside the window and below the watermark.
	storedLo, storedHi := alignUp(lo, cg), hi
	if w.Before(storedHi) {
		storedHi = w
	}
	storedHi = alignDown(storedHi, cg)
	if !storedHi.After(storedLo) {
		storedLo, storedHi = hi, hi // no whole bucket fits: the window is read from source
	}
	if storedLo.Equal(lo) && storedHi.Equal(hi) {
		return q.CubeReadSQL(cubeExpr), true // aligned and fully sealed
	}

	var excluded []string
	excludedOnce := false
	from := func(glob string) string {
		if !excludedOnce && exclude != nil {
			excluded, excludedOnce = exclude(), true
		}
		return sourceRelation(glob, excluded)
	}
	var branches []string
	// Stored cube portion [storedLo, storedHi).
	if storedHi.After(storedLo) {
		branches = append(branches, s.storePassthrough(cubeExpr, fmtTS(storedLo), fmtTS(storedHi)))
	}
	// Head patch [lo, storedLo) — partial leading bucket (or the whole window when
	// no bucket fits) from source; skipped when the window holds no source files.
	if storedLo.After(lo) {
		hlo, hhi := canonTS(lo), canonTS(storedLo)
		if g := source(hlo, hhi); g != "" {
			branches = append(branches, s.buildSelectFrom(from(g), q.TimeCol, hlo, hhi, nil))
		}
	}
	// Tail [storedHi, hi) from source — the fresh data past the watermark and any
	// partial trailing bucket; skipped when the window holds no source files.
	if hi.After(storedHi) {
		tlo, thi := canonTS(storedHi), canonTS(hi)
		if g := source(tlo, thi); g != "" {
			branches = append(branches, s.buildSelectFrom(from(g), q.TimeCol, tlo, thi, nil))
		}
	}
	if len(branches) == 0 {
		return "", false
	}

	bucket := bucketExpr(q.Grain, "bucket")
	sel := q.selectList(bucket, Aggregate.finalExpr)
	filters := strings.TrimPrefix(renderFilters(q.Filters), " AND ")
	where := ""
	if filters != "" {
		where = " WHERE " + filters
	}
	return fmt.Sprintf("WITH parts AS (%s) SELECT %s FROM parts%s%s",
		strings.Join(branches, " UNION ALL BY NAME "), sel, where, q.groupOrder(bucket != "")+q.limitClause()), true
}
