package rollup

import (
	"fmt"
	"strconv"
	"strings"
)

// origExpr renders the aggregate exactly as written against raw source rows —
// used by the compare harness to produce the ground-truth reference result.
func (a Aggregate) origExpr() string {
	switch a.Kind {
	case AggCount:
		return "count(*)"
	case AggCountCol:
		return fmt.Sprintf("count(%q)", a.Col)
	case AggSum:
		return fmt.Sprintf("sum(%q)", a.Col)
	case AggMin:
		return fmt.Sprintf("min(%q)", a.Col)
	case AggMax:
		return fmt.Sprintf("max(%q)", a.Col)
	case AggAvg:
		return fmt.Sprintf("avg(%q)", a.Col)
	case AggCountDistinct:
		return fmt.Sprintf("count(DISTINCT %q)", a.Col)
	case AggPercentile:
		return fmt.Sprintf("quantile_cont(%q, %g)", a.Col, a.P)
	case AggCondSum:
		then := a.ThenK
		if a.ThenCol != "" {
			then = fmt.Sprintf("%q", a.ThenCol)
		}
		return fmt.Sprintf("sum(CASE WHEN %s THEN %s ELSE %s END)", a.Cond, then, a.ElseK)
	}
	return "NULL"
}

func litValue(v string) string {
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return v // numeric literal, unquoted
	}
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func (f Filter) render() string {
	switch f.Op {
	case OpEq:
		return fmt.Sprintf("%q = %s", f.Col, litValue(f.Values[0]))
	case OpNe:
		return fmt.Sprintf("%q <> %s", f.Col, litValue(f.Values[0]))
	case OpIn, OpNotIn:
		vs := make([]string, len(f.Values))
		for i, v := range f.Values {
			vs[i] = litValue(v)
		}
		op := "IN"
		if f.Op == OpNotIn {
			op = "NOT IN"
		}
		return fmt.Sprintf("%q %s (%s)", f.Col, op, strings.Join(vs, ", "))
	case OpIsNull:
		return fmt.Sprintf("%q IS NULL", f.Col)
	case OpIsNotNull:
		return fmt.Sprintf("%q IS NOT NULL", f.Col)
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
		sel = append(sel, fmt.Sprintf("%s AS %q", bucket, q.bucketAlias()))
	}
	for _, d := range q.Dims {
		sel = append(sel, fmt.Sprintf("%q", d))
	}
	for _, a := range q.Aggs {
		sel = append(sel, fmt.Sprintf("%s AS %q", aggExpr(a), a.Alias))
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
	bucket := bucketExpr(q.Grain, fmt.Sprintf("%q", q.TimeCol))
	sel := q.selectList(bucket, Aggregate.origExpr)
	where := fmt.Sprintf("%q >= TIMESTAMPTZ '%s' AND %q < TIMESTAMPTZ '%s'",
		q.TimeCol, q.TimeLo, q.TimeCol, q.TimeHi)
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

// storePassthrough renders the stored cube branch of a merge: bucket + dims +
// every store column passed through, with sketch BLOBs cast back to their
// concrete sketch type so the column types line up with the freshly-built
// source branches under UNION ALL BY NAME.
func (s CubeSpec) storePassthrough(cubeExpr, lo, hi string) string {
	var sel []string
	sel = append(sel, "bucket")
	for _, d := range s.Dims {
		sel = append(sel, fmt.Sprintf("%q", d))
	}
	for _, sc := range s.orderedStoreCols() {
		switch {
		case strings.HasPrefix(sc[0], "_theta_"):
			sel = append(sel, fmt.Sprintf("%s::%s AS %s", sc[0], thetaType, sc[0]))
		case strings.HasPrefix(sc[0], "_kll_"):
			sel = append(sel, fmt.Sprintf("%s::%s AS %s", sc[0], kllType, sc[0]))
		default:
			sel = append(sel, sc[0])
		}
	}
	// union_by_name for the same mixed-schema tolerance as CubeReadSQL.
	return fmt.Sprintf("SELECT %s FROM %s WHERE bucket >= TIMESTAMPTZ '%s' AND bucket < TIMESTAMPTZ '%s'",
		strings.Join(sel, ", "), readParquetFrom(cubeExpr), lo, hi)
}

// MergeReadSQL serves q across the watermark: sealed buckets from the cube, the
// fresh tail [watermark,hi) re-aggregated from source, and a head patch for a
// partial leading bucket when lo is not cube-grain aligned. All branches share
// the cube's store-column schema and are merged with UNION ALL BY NAME, then the
// outer SELECT applies the final aggregate expressions. cg is the cube grain.
func (q QueryShape) MergeReadSQL(s CubeSpec, cubeExpr, sourceExpr, watermark string) (string, bool) {
	lo, ok1 := parseTS(q.TimeLo)
	hi, ok2 := parseTS(q.TimeHi)
	w, ok3 := parseTS(watermark)
	if !ok1 || !ok2 || !ok3 {
		return "", false
	}
	cg := s.Grain
	if hi.Before(w) || hi.Equal(w) {
		return q.CubeReadSQL(cubeExpr), true // fully sealed
	}
	storedLo := alignUp(lo, cg)
	mergeBoundary := alignDown(w, cg)
	if mergeBoundary.After(hi) {
		mergeBoundary = hi
	}

	var branches []string
	// Stored cube portion [storedLo, mergeBoundary).
	if mergeBoundary.After(storedLo) {
		branches = append(branches, s.storePassthrough(cubeExpr, fmtTS(storedLo), fmtTS(mergeBoundary)))
	}
	// Head patch [lo, storedLo) — partial leading bucket from source.
	if storedLo.After(lo) {
		branches = append(branches, s.BuildSelect(sourceExpr, q.TimeCol, fmtTS(lo), fmtTS(storedLo)))
	}
	// Fresh tail [mergeBoundary, hi) from source.
	if hi.After(mergeBoundary) {
		branches = append(branches, s.BuildSelect(sourceExpr, q.TimeCol, fmtTS(mergeBoundary), fmtTS(hi)))
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
