package tiered

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// EmitArgs bundles everything needed to emit the merge-on-read SQL.
type EmitArgs struct {
	Ctx     context.Context
	Shape   *QueryShape
	Tier    Tier      // picked by PickTier
	TailLo  time.Time // open-tail boundary; equal to Shape.TimeHi means no open tail
	Variant string    // "sketch" | "by_<dim>" | "all"
	Files   FileIndex
	Spec    *Spec
	// StoragePrefix is prepended to every path in the emitted read_parquet
	// calls. Empty means use paths as-is (test/local mode). Production sets
	// it to "s3://<bucket>/" so DuckDB sees full S3 URLs.
	StoragePrefix string
	// SchemaHashLookup returns the parquet KV-metadata schema_hash for
	// the file at `path` (empty string when the file has no stamp).
	// When set and Spec.SchemaHash() returns a non-empty value, files
	// whose stamped hash differs are excluded from the read set. nil
	// disables schema-hash filtering (test/local mode).
	SchemaHashLookup func(path string) (string, error)
	// SkipCoverageCheck disables the gap-detection refusal. Tests with
	// synthetic file fixtures (manual TailLo unrelated to actual file
	// coverage) set this; production code never does so the coverage
	// safety net stays armed against bug B (silent-undercount on
	// skipped builder windows).
	SkipCoverageCheck bool
}

// EmitMergeOnRead generates the rewritten SQL that reads precalc tier files
// (and falls back to raw source for the open tail when TailLo < Shape.TimeHi).
// Returns (sql, true) when the rewrite is well-defined; (originalSQL, false)
// otherwise (caller falls back).
func EmitMergeOnRead(a EmitArgs) (string, bool) {
	ctx := a.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	// When the query has time bounds, use the window-scoped list so we only
	// include rollup files overlapping [TimeLo, TailLo). Without this the
	// emitted read_parquet([...]) embeds the entire tier's history (months
	// of files), forcing DuckDB to LIST + probe every one of them.
	var mainFiles []string
	var err error
	if !a.Shape.TimeLo.IsZero() || !a.Shape.TimeHi.IsZero() {
		hi := a.TailLo
		if hi.IsZero() {
			hi = a.Shape.TimeHi
		}
		mainFiles, err = a.Files.FilesForTierVariantWindow(ctx, string(a.Tier), a.Variant, a.Shape.TimeLo, hi)
	} else {
		mainFiles, err = a.Files.FilesForTierVariant(ctx, string(a.Tier), a.Variant)
	}
	if err != nil || len(mainFiles) == 0 {
		return a.Shape.OriginalSQL, false
	}

	// Schema-hash safety net: files whose stamped schema_hash differs
	// from the current Spec's hash are excluded from the read set so
	// a stale-spec read can't silently miscount. No-op when either
	// the lookup function or the spec hash is missing.
	if a.SchemaHashLookup != nil && a.Spec != nil {
		if specHash, err := a.Spec.SchemaHash(); err == nil && specHash != "" {
			mainFiles, _ = filterPathsBySchemaHash(mainFiles, specHash, a.SchemaHashLookup)
			if len(mainFiles) == 0 {
				return a.Shape.OriginalSQL, false
			}
		}
	}

	// Coverage safety net: refuse the rewrite if the surviving file
	// set has gaps inside the closed-rollup window [TimeLo, TailLo).
	// A skipped build day (SIGSEGV, watermark mis-advance) would
	// otherwise silently undercount. Loud fallback to source is the
	// correct behaviour. Skip when time bounds are absent OR when the
	// caller opts out (synthetic shape tests with manual TailLo).
	if !a.SkipCoverageCheck && a.Shape.BucketArg != "" && (!a.Shape.TimeLo.IsZero() || !a.TailLo.IsZero()) {
		coverHi := a.TailLo
		if coverHi.IsZero() {
			coverHi = a.Shape.TimeHi
		}
		if !rollupCoversWindow(mainFiles, a.Shape.TimeLo, coverHi) {
			return a.Shape.OriginalSQL, false
		}
	}

	// Scalar aggregate / dim-rollup: no date_trunc in the user query (BucketArg=="").
	// Refuse if there is an open tail (not yet implemented for the no-bucket paths).
	// Use scalar fragments: the inner CTE already reduces to one row so the outer
	// must project, not re-aggregate (avoids double-merging sketch blobs).
	if a.Shape.BucketArg == "" {
		if !a.TailLo.Equal(a.Shape.TimeHi) {
			return a.Shape.OriginalSQL, false
		}
		scalarInner, scalarOuter, ok := buildAggFragmentsScalar(a.Shape.Aggregates, a.Variant)
		if !ok {
			return a.Shape.OriginalSQL, false
		}
		if len(a.Shape.GroupDims) > 0 {
			return emitDimRollup(a, mainFiles, scalarInner, scalarOuter), true
		}
		return emitScalarAggregate(a, mainFiles, scalarInner, scalarOuter), true
	}

	innerSelects, outerExprs, ok := buildAggFragments(a.Shape.Aggregates, a.Variant)
	if !ok {
		return a.Shape.OriginalSQL, false
	}

	involved := involvedDims(a.Shape)
	hasDims := len(involved) > 0
	hasTimeBounds := !a.Shape.TimeLo.IsZero() || !a.Shape.TimeHi.IsZero()
	hasOpenTail := !a.TailLo.Equal(a.Shape.TimeHi)

	// rollup CTE
	rollupSel := NewSelect(RollupMode).
		Project(FuncExpr("date_trunc", Raw("'"+a.Shape.BucketArg+"'"), Col("bucket")), "_bkt")
	if hasDims {
		for _, dim := range involved {
			rollupSel.Project(Col(dim+"_class"), "")
		}
	}
	for _, inner := range innerSelects {
		rollupSel.Project(Raw(inner), "")
	}
	rollupSel.From(ReadParquet(applyPrefix(a.StoragePrefix, mainFiles)))
	if hasTimeBounds {
		rollupSel.Where(BinOp(">=", Col("bucket"), TimestampLit(a.Shape.TimeLo)))
		rollupSel.Where(BinOp("<", Col("bucket"), TimestampLit(a.TailLo)))
	}
	for _, dim := range involved {
		if fp, ok := a.Shape.Filters[dim]; ok {
			rollupSel.Where(filterPredToExpr(dim+"_class", fp))
		}
	}
	rollupSel.GroupBy(Col("_bkt"))
	if hasDims {
		for _, dim := range involved {
			rollupSel.GroupBy(Col(dim + "_class"))
		}
	}

	// optional fresh CTE
	var freshSel *SelectStmt
	if hasOpenTail {
		finer := finerTier(a.Tier)
		if finer == Tier("") {
			sourceInnerSelects := make([]string, 0, len(a.Shape.Aggregates))
			for i, agg := range a.Shape.Aggregates {
				inner, ok := aggInnerFragment(SourceMode, agg, i)
				if !ok {
					return a.Shape.OriginalSQL, false
				}
				sourceInnerSelects = append(sourceInnerSelects, inner)
			}
			freshSel = NewSelect(SourceMode).
				Project(FuncExpr("date_trunc", Raw("'"+a.Shape.BucketArg+"'"), Col(a.Shape.TimeColumn)), "_bkt")
			if hasDims {
				for _, dim := range involved {
					var kept []string
					if ds, ok := a.Spec.Dims[dim]; ok {
						kept = ds.KeptValues
					}
					freshSel.Project(Raw(dimClassExpr(SourceMode, dim, kept)), dim+"_class")
				}
			}
			for _, inner := range sourceInnerSelects {
				freshSel.Project(Raw(inner), "")
			}
			freshSel.From(Table(a.Shape.Table)).
				Where(BinOp(">=", Col(a.Shape.TimeColumn), TimestampLit(a.TailLo))).
				Where(BinOp("<", Col(a.Shape.TimeColumn), TimestampLit(a.Shape.TimeHi)))
			for _, dim := range involved {
				if fp, ok := a.Shape.Filters[dim]; ok {
					freshSel.Where(filterPredToExpr(dimFilterCol(SourceMode, dim), fp))
				}
			}
			freshSel.GroupBy(Col("_bkt"))
			if hasDims {
				for _, dim := range involved {
					freshSel.GroupBy(Raw(dim + "_class"))
				}
			}
		} else {
			finerFiles, ferr := a.Files.FilesForTierVariantWindow(ctx, string(finer), a.Variant, a.TailLo, a.Shape.TimeHi)
			if ferr != nil || len(finerFiles) == 0 {
				return a.Shape.OriginalSQL, false
			}
			freshSel = NewSelect(RollupMode).
				Project(FuncExpr("date_trunc", Raw("'"+a.Shape.BucketArg+"'"), Col("bucket")), "_bkt")
			if hasDims {
				for _, dim := range involved {
					freshSel.Project(Col(dim+"_class"), "")
				}
			}
			for _, inner := range innerSelects {
				freshSel.Project(Raw(inner), "")
			}
			freshSel.From(ReadParquet(applyPrefix(a.StoragePrefix, finerFiles))).
				Where(BinOp(">=", Col("bucket"), TimestampLit(a.TailLo))).
				Where(BinOp("<", Col("bucket"), TimestampLit(a.Shape.TimeHi)))
			for _, dim := range involved {
				if fp, ok := a.Shape.Filters[dim]; ok {
					freshSel.Where(filterPredToExpr(dim+"_class", fp))
				}
			}
			freshSel.GroupBy(Col("_bkt"))
			if hasDims {
				for _, dim := range involved {
					freshSel.GroupBy(Col(dim + "_class"))
				}
			}
		}
	}

	// outer SELECT
	groupDimSet := make(map[string]bool, len(a.Shape.GroupDims))
	for _, d := range a.Shape.GroupDims {
		groupDimSet[d] = true
	}
	bktExpr := outerBucketExpr(a.Shape.UserBucketSecs)
	bktAlias := a.Shape.BucketAlias
	if bktAlias == "" {
		bktAlias = a.Shape.BucketArg
	}
	main := NewSelect(RollupMode).
		Project(bktExpr, bktAlias)
	for _, dim := range involved {
		if groupDimSet[dim] {
			main.Project(Col(dim+"_class"), dim)
		}
	}
	for _, outer := range outerExprs {
		main.Project(Raw(outer), "")
	}
	if hasOpenTail {
		main.From(SubQueryUnionAll(
			NewSelect(RollupMode).Project(Star(), "").From(FromCTE("rollup")),
			NewSelect(RollupMode).Project(Star(), "").From(FromCTE("fresh")),
		))
	} else {
		main.From(FromCTE("rollup"))
	}
	main.GroupBy(bktExpr)
	for _, dim := range a.Shape.GroupDims {
		main.GroupBy(Col(dim + "_class"))
	}
	for _, h := range a.Shape.Having {
		expr := stripAlias(outerExprs[h.AggIndex])
		main.Having(Raw(fmt.Sprintf("%s %s %v", expr, h.Op, h.Value)))
	}
	if a.Shape.OrderLimit != nil {
		ol := a.Shape.OrderLimit
		expr := stripAlias(outerExprs[ol.AggIndex])
		main.OrderByExpr(Raw(expr), ol.Desc).Limit(ol.Limit)
	} else {
		main.OrderByExpr(bktExpr, false)
		for _, dim := range a.Shape.GroupDims {
			main.OrderByExpr(Col(dim+"_class"), false)
		}
	}

	stmt := NewStatement().
		Setup(fmt.Sprintf("SET TimeZone = '%s'", a.Spec.TZ)).
		WithCTE("rollup", rollupSel)
	if freshSel != nil {
		stmt.WithCTE("fresh", freshSel)
	}
	stmt.Body(main)
	sql, err := stmt.Build()
	if err != nil {
		return a.Shape.OriginalSQL, false
	}
	return sql, true
}

// buildAggFragments iterates aggregates and returns slices of inner SELECT
// fragments and outer SELECT expressions. Returns ok=false if any aggregate
// can't be translated.
func buildAggFragments(aggs []Aggregate, variant string) (innerSelects, outerExprs []string, ok bool) {
	innerSelects = make([]string, 0, len(aggs))
	outerExprs = make([]string, 0, len(aggs))
	for i, agg := range aggs {
		inner, outer, translated := TranslateAggregate(agg, i, variant)
		if !translated {
			return nil, nil, false
		}
		innerSelects = append(innerSelects, inner)
		outerExprs = append(outerExprs, outer)
	}
	return innerSelects, outerExprs, true
}

// buildAggFragmentsScalar is like buildAggFragments but for the scalar
// (no-bucket, no-GROUP-BY) path where the inner CTE reduces to one row.
func buildAggFragmentsScalar(aggs []Aggregate, variant string) (innerSelects, outerExprs []string, ok bool) {
	innerSelects = make([]string, 0, len(aggs))
	outerExprs = make([]string, 0, len(aggs))
	for i, agg := range aggs {
		inner, outer, translated := TranslateAggregateScalar(agg, i, variant)
		if !translated {
			return nil, nil, false
		}
		innerSelects = append(innerSelects, inner)
		outerExprs = append(outerExprs, outer)
	}
	return innerSelects, outerExprs, true
}

// buildFilterExpr builds a WHERE predicate for a storage class column.
func buildFilterExpr(col string, fp FilterPredicate) string {
	switch fp.Op {
	case "=":
		return fmt.Sprintf("%s = '%s'", col, strings.ReplaceAll(fp.Values[0], "'", "''"))
	case "IN":
		quoted := make([]string, len(fp.Values))
		for i, v := range fp.Values {
			quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		}
		return fmt.Sprintf("%s IN (%s)", col, strings.Join(quoted, ", "))
	case "NOT IN":
		quoted := make([]string, len(fp.Values))
		for i, v := range fp.Values {
			quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		}
		return fmt.Sprintf("%s NOT IN (%s)", col, strings.Join(quoted, ", "))
	case "IS NULL":
		// Class columns are never SQL-NULL (CASE WHEN COALESCE(dim, '_null_') ...).
		// "dim IS NULL" in the user query maps to "dim_class = '_null_'" since
		// the builder coalesced NULL source values to that sentinel.
		return fmt.Sprintf("%s = '_null_'", col)
	case "IS NOT NULL":
		return fmt.Sprintf("%s <> '_null_'", col)
	}
	return ""
}

// stripAlias removes trailing " AS <alias>" from an outer expression so it can
// be used in HAVING / ORDER BY.
func stripAlias(expr string) string {
	upper := strings.ToUpper(expr)
	idx := strings.LastIndex(upper, " AS ")
	if idx < 0 {
		return expr
	}
	return strings.TrimSpace(expr[:idx])
}

// fmtTimestamp formats a time as a DuckDB TIMESTAMP literal with UTC offset.
func fmtTimestamp(t time.Time) string {
	return fmt.Sprintf("TIMESTAMP '%s'", t.UTC().Format("2006-01-02 15:04:05+00"))
}

// finerTier always returns the empty Tier — 1h is the only tier in the
// system, so any open tail must fall to source-scan.
func finerTier(Tier) Tier { return Tier("") }

// pathList formats parquet paths for read_parquet([...]). prefix (e.g.
// "s3://hammel-arc/") is prepended to each entry; pass "" to use paths
// unchanged.
func pathList(prefix string, paths []string) string {
	q := make([]string, len(paths))
	for i, p := range paths {
		full := p
		if prefix != "" && !strings.Contains(p, "://") {
			full = prefix + p
		}
		q[i] = "'" + strings.ReplaceAll(full, "'", "''") + "'"
	}
	return "[" + strings.Join(q, ", ") + "]"
}

// emitDimRollup emits SQL for queries that GROUP BY dimension columns with no
// date_trunc bucket (BucketArg==""). Caller guarantees no open tail.
func emitDimRollup(a EmitArgs, mainFiles, innerSelects, outerExprs []string) string {
	involved := involvedDims(a.Shape)
	hasTimeBounds := !a.Shape.TimeLo.IsZero() || !a.Shape.TimeHi.IsZero()

	rollupSel := NewSelect(RollupMode)
	for _, dim := range involved {
		rollupSel.Project(Col(dim+"_class"), "")
	}
	for _, inner := range innerSelects {
		rollupSel.Project(Raw(inner), "")
	}
	rollupSel.From(ReadParquet(applyPrefix(a.StoragePrefix, mainFiles)))
	if hasTimeBounds {
		rollupSel.Where(BinOp(">=", Col("bucket"), TimestampLit(a.Shape.TimeLo)))
		rollupSel.Where(BinOp("<", Col("bucket"), TimestampLit(a.TailLo)))
	}
	for _, dim := range involved {
		if fp, ok := a.Shape.Filters[dim]; ok {
			rollupSel.Where(filterPredToExpr(dim+"_class", fp))
		}
	}
	for _, dim := range involved {
		rollupSel.GroupBy(Col(dim + "_class"))
	}

	main := NewSelect(RollupMode)
	for _, dim := range involved {
		main.Project(Col(dim+"_class"), dim)
	}
	for _, outer := range outerExprs {
		main.Project(Raw(outer), "")
	}
	main.From(FromCTE("rollup"))

	for _, h := range a.Shape.Having {
		expr := stripAlias(outerExprs[h.AggIndex])
		main.Having(Raw(fmt.Sprintf("%s %s %v", expr, h.Op, h.Value)))
	}

	if a.Shape.OrderLimit != nil {
		ol := a.Shape.OrderLimit
		expr := stripAlias(outerExprs[ol.AggIndex])
		main.OrderByExpr(Raw(expr), ol.Desc).Limit(ol.Limit)
	}

	stmt := NewStatement().
		Setup(fmt.Sprintf("SET TimeZone = '%s'", a.Spec.TZ)).
		WithCTE("rollup", rollupSel).
		Body(main)
	sql, err := stmt.Build()
	if err != nil {
		return a.Shape.OriginalSQL
	}
	return sql
}

// emitScalarAggregate emits SQL for queries that have no date_trunc (BucketArg=="").
// The output is a single-row scalar aggregate over all precalc buckets in range.
// Caller guarantees no open tail (TailLo == TimeHi).
func emitScalarAggregate(a EmitArgs, mainFiles, innerSelects, outerExprs []string) string {
	involved := involvedDims(a.Shape)
	hasTimeBounds := !a.Shape.TimeLo.IsZero() || !a.Shape.TimeHi.IsZero()

	rollupSel := NewSelect(RollupMode)
	for _, inner := range innerSelects {
		rollupSel.Project(Raw(inner), "")
	}
	rollupSel.From(ReadParquet(applyPrefix(a.StoragePrefix, mainFiles)))
	if hasTimeBounds {
		rollupSel.Where(BinOp(">=", Col("bucket"), TimestampLit(a.Shape.TimeLo)))
		rollupSel.Where(BinOp("<", Col("bucket"), TimestampLit(a.TailLo)))
	}
	for _, dim := range involved {
		if fp, ok := a.Shape.Filters[dim]; ok {
			rollupSel.Where(filterPredToExpr(dim+"_class", fp))
		}
	}

	main := NewSelect(RollupMode)
	for _, outer := range outerExprs {
		main.Project(Raw(outer), "")
	}
	main.From(FromCTE("rollup"))

	stmt := NewStatement().
		Setup(fmt.Sprintf("SET TimeZone = '%s'", a.Spec.TZ)).
		WithCTE("rollup", rollupSel).
		Body(main)
	sql, err := stmt.Build()
	if err != nil {
		// IR construction should not fail for emitScalarAggregate inputs
		// (no source-mode involvement). If it does, the bug is in the
		// fragments we received from buildAggFragmentsScalar — fall back
		// to the original SQL by returning a sentinel that EmitMergeOnRead
		// turns into a refuse.
		return a.Shape.OriginalSQL
	}
	return sql
}

// outerBucketExpr returns the expression used for the outer SELECT bucket
// column. When the user's SQL used the Grafana plugin's
// to_timestamp((epoch_ns(t)//1e9//N)*N) idiom with N != hourly seconds,
// the inner CTE groups at the finest aligned bucket (hour or day) and the
// outer wraps _bkt in the same to_timestamp expression so the final result
// honors the user's requested bucket size (e.g., 3h, 6h, 2d).
func outerBucketExpr(userBucketSecs int64) Expr {
	if userBucketSecs <= 0 {
		return Col("_bkt")
	}
	return Raw(fmt.Sprintf("to_timestamp((epoch_ns(_bkt)//1000000000//%d)*%d)",
		userBucketSecs, userBucketSecs))
}

// applyPrefix prepends StoragePrefix to each path that isn't already a full URL.
func applyPrefix(prefix string, paths []string) []string {
	if prefix == "" {
		return paths
	}
	out := make([]string, len(paths))
	for i, p := range paths {
		if strings.Contains(p, "://") {
			out[i] = p
		} else {
			out[i] = prefix + p
		}
	}
	return out
}

// filterPredToExpr converts a FilterPredicate against a class column (rollup
// mode) into a typed IR expression. IS NULL / IS NOT NULL map to the
// "_null_" sentinel that the rollup builder writes for missing values.
func filterPredToExpr(col string, fp FilterPredicate) Expr {
	switch fp.Op {
	case "=":
		quoted := "'" + strings.ReplaceAll(fp.Values[0], "'", "''") + "'"
		return BinOp("=", Col(col), Raw(quoted))
	case "IN":
		return In(Col(col), fp.Values, false)
	case "NOT IN":
		return In(Col(col), fp.Values, true)
	case "IS NULL":
		return BinOp("=", Col(col), Raw("'_null_'"))
	case "IS NOT NULL":
		return BinOp("<>", Col(col), Raw("'_null_'"))
	}
	return Raw("")
}
