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

	var b strings.Builder

	fmt.Fprintf(&b, "SET TimeZone = '%s';\n", a.Spec.TZ)

	b.WriteString("WITH rollup AS (\n  SELECT\n")
	b.WriteString("    date_trunc('")
	b.WriteString(a.Shape.BucketArg)
	b.WriteString("', bucket) AS _bkt")

	if hasDims {
		for _, dim := range involved {
			fmt.Fprintf(&b, ",\n    %s_class", dim)
		}
	}

	for _, inner := range innerSelects {
		fmt.Fprintf(&b, ",\n    %s", inner)
	}

	fmt.Fprintf(&b, "\n  FROM read_parquet(%s)", pathList(a.StoragePrefix, mainFiles))
	if hasTimeBounds {
		b.WriteString("\n  WHERE bucket >= ")
		b.WriteString(fmtTimestamp(a.Shape.TimeLo))
		b.WriteString(" AND bucket < ")
		b.WriteString(fmtTimestamp(a.TailLo))
	}

	dimFiltersAdded := 0
	for _, dim := range involved {
		fp, ok := a.Shape.Filters[dim]
		if !ok {
			continue
		}
		if !hasTimeBounds && dimFiltersAdded == 0 {
			b.WriteString("\n  WHERE ")
		} else {
			b.WriteString("\n    AND ")
		}
		b.WriteString(buildFilterExpr(dim+"_class", fp))
		dimFiltersAdded++
	}

	if hasDims {
		b.WriteString("\n  GROUP BY _bkt")
		for _, dim := range involved {
			fmt.Fprintf(&b, ", %s_class", dim)
		}
	} else {
		b.WriteString("\n  GROUP BY _bkt")
	}

	b.WriteString("\n)")

	hasOpenTail := !a.TailLo.Equal(a.Shape.TimeHi)
	if hasOpenTail {
		finer := finerTier(a.Tier)
		if finer == Tier("") {
			// Source-scan branch: read raw rows. Every dim/agg reference
			// routes through the col_mode helpers so column names and
			// aggregate fragments stay in lock-step with the rollup CTE.
			// Sketch aggregates have no source-mode equivalent, so the
			// helper refuses and we fall back to source for the whole
			// query.
			sourceInnerSelects := make([]string, 0, len(a.Shape.Aggregates))
			for i, agg := range a.Shape.Aggregates {
				inner, ok := aggInnerFragment(SourceMode, agg, i)
				if !ok {
					return a.Shape.OriginalSQL, false
				}
				sourceInnerSelects = append(sourceInnerSelects, inner)
			}

			b.WriteString("\n, fresh AS (\n  SELECT\n")
			b.WriteString("    date_trunc('")
			b.WriteString(a.Shape.BucketArg)
			b.WriteString("', ")
			b.WriteString(a.Shape.TimeColumn)
			b.WriteString(") AS _bkt")

			if hasDims {
				for _, dim := range involved {
					var kept []string
					if ds, ok := a.Spec.Dims[dim]; ok {
						kept = ds.KeptValues
					}
					fmt.Fprintf(&b, ",\n    %s AS %s_class",
						dimClassExpr(SourceMode, dim, kept), dim)
				}
			}

			for _, inner := range sourceInnerSelects {
				fmt.Fprintf(&b, ",\n    %s", inner)
			}

			fmt.Fprintf(&b, "\n  FROM %s", a.Shape.Table)
			b.WriteString("\n  WHERE ")
			b.WriteString(a.Shape.TimeColumn)
			b.WriteString(" >= ")
			b.WriteString(fmtTimestamp(a.TailLo))
			b.WriteString(" AND ")
			b.WriteString(a.Shape.TimeColumn)
			b.WriteString(" < ")
			b.WriteString(fmtTimestamp(a.Shape.TimeHi))

			for _, dim := range involved {
				fp, ok := a.Shape.Filters[dim]
				if !ok {
					continue
				}
				b.WriteString("\n    AND ")
				b.WriteString(buildFilterExpr(dimFilterCol(SourceMode, dim), fp))
			}

			if hasDims {
				b.WriteString("\n  GROUP BY _bkt")
				for _, dim := range involved {
					fmt.Fprintf(&b, ", %s_class", dim)
				}
			} else {
				b.WriteString("\n  GROUP BY _bkt")
			}

			b.WriteString("\n)")
		} else {
			finerFiles, ferr := a.Files.FilesForTierVariantWindow(ctx, string(finer), a.Variant, a.TailLo, a.Shape.TimeHi)
			if ferr != nil || len(finerFiles) == 0 {
				return a.Shape.OriginalSQL, false
			}

			b.WriteString("\n, fresh AS (\n  SELECT\n")
			b.WriteString("    date_trunc('")
			b.WriteString(a.Shape.BucketArg)
			b.WriteString("', bucket) AS _bkt")

			if hasDims {
				for _, dim := range involved {
					fmt.Fprintf(&b, ",\n    %s_class", dim)
				}
			}

			for _, inner := range innerSelects {
				fmt.Fprintf(&b, ",\n    %s", inner)
			}

			fmt.Fprintf(&b, "\n  FROM read_parquet(%s)", pathList(a.StoragePrefix, finerFiles))
			b.WriteString("\n  WHERE bucket >= ")
			b.WriteString(fmtTimestamp(a.TailLo))
			b.WriteString(" AND bucket < ")
			b.WriteString(fmtTimestamp(a.Shape.TimeHi))

			for _, dim := range involved {
				fp, ok := a.Shape.Filters[dim]
				if !ok {
					continue
				}
				b.WriteString("\n    AND ")
				b.WriteString(buildFilterExpr(dim+"_class", fp))
			}

			if hasDims {
				b.WriteString("\n  GROUP BY _bkt")
				for _, dim := range involved {
					fmt.Fprintf(&b, ", %s_class", dim)
				}
			} else {
				b.WriteString("\n  GROUP BY _bkt")
			}

			b.WriteString("\n)")
		}
	}

	// Only emit dims that were in the original GROUP BY (GroupDims), not
	// filter-only dims. Filter-only dims are still used in the inner CTE's
	// GROUP BY for correct aggregation, but must not appear in the output.
	groupDimSet := make(map[string]bool, len(a.Shape.GroupDims))
	for _, d := range a.Shape.GroupDims {
		groupDimSet[d] = true
	}

	b.WriteString("\nSELECT\n  _bkt AS ")
	b.WriteString(a.Shape.BucketArg)

	for _, dim := range involved {
		if !groupDimSet[dim] {
			continue
		}
		fmt.Fprintf(&b, ",\n  %s_class AS %s", dim, dim)
	}

	for _, outer := range outerExprs {
		fmt.Fprintf(&b, ",\n  %s", outer)
	}

	if hasOpenTail {
		b.WriteString("\nFROM (SELECT * FROM rollup UNION ALL SELECT * FROM fresh)")
	} else {
		b.WriteString("\nFROM rollup")
	}

	b.WriteString("\nGROUP BY _bkt")
	for _, dim := range a.Shape.GroupDims {
		fmt.Fprintf(&b, ", %s_class", dim)
	}

	if len(a.Shape.Having) > 0 {
		b.WriteString("\nHAVING ")
		havingParts := make([]string, len(a.Shape.Having))
		for i, h := range a.Shape.Having {
			rawOuter := outerExprs[h.AggIndex]
			expr := stripAlias(rawOuter)
			havingParts[i] = fmt.Sprintf("%s %s %v", expr, h.Op, h.Value)
		}
		b.WriteString(strings.Join(havingParts, " AND "))
	}

	if a.Shape.OrderLimit != nil {
		ol := a.Shape.OrderLimit
		rawOuter := outerExprs[ol.AggIndex]
		expr := stripAlias(rawOuter)
		b.WriteString("\nORDER BY ")
		b.WriteString(expr)
		if ol.Desc {
			b.WriteString(" DESC")
		}
		fmt.Fprintf(&b, "\nLIMIT %d", ol.Limit)
	} else {
		b.WriteString("\nORDER BY _bkt")
		for _, dim := range a.Shape.GroupDims {
			fmt.Fprintf(&b, ", %s_class", dim)
		}
	}

	return b.String(), true
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

// finerTier returns the next-finer tier or the empty Tier if t is the finest (1h).
func finerTier(t Tier) Tier {
	switch t {
	case Tier1mo:
		return Tier1w
	case Tier1w:
		return Tier1d
	case Tier1d:
		return Tier1h
	}
	return Tier("")
}

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

	var b strings.Builder

	fmt.Fprintf(&b, "SET TimeZone = '%s';\n", a.Spec.TZ)

	b.WriteString("WITH rollup AS (\n  SELECT")
	for _, dim := range involved {
		fmt.Fprintf(&b, "\n    %s_class,", dim)
	}
	for i, inner := range innerSelects {
		if i == 0 {
			fmt.Fprintf(&b, "\n    %s", inner)
		} else {
			fmt.Fprintf(&b, ",\n    %s", inner)
		}
	}

	fmt.Fprintf(&b, "\n  FROM read_parquet(%s)", pathList(a.StoragePrefix, mainFiles))

	whereAdded := false
	if hasTimeBounds {
		b.WriteString("\n  WHERE bucket >= ")
		b.WriteString(fmtTimestamp(a.Shape.TimeLo))
		b.WriteString(" AND bucket < ")
		b.WriteString(fmtTimestamp(a.TailLo))
		whereAdded = true
	}

	for _, dim := range involved {
		fp, ok := a.Shape.Filters[dim]
		if !ok {
			continue
		}
		if !whereAdded {
			b.WriteString("\n  WHERE ")
			whereAdded = true
		} else {
			b.WriteString("\n    AND ")
		}
		b.WriteString(buildFilterExpr(dim+"_class", fp))
	}

	b.WriteString("\n  GROUP BY")
	for i, dim := range involved {
		if i == 0 {
			fmt.Fprintf(&b, " %s_class", dim)
		} else {
			fmt.Fprintf(&b, ", %s_class", dim)
		}
	}
	b.WriteString("\n)")

	b.WriteString("\nSELECT")
	for _, dim := range involved {
		fmt.Fprintf(&b, "\n  %s_class AS %s,", dim, dim)
	}
	for i, outer := range outerExprs {
		if i == 0 {
			fmt.Fprintf(&b, "\n  %s", outer)
		} else {
			fmt.Fprintf(&b, ",\n  %s", outer)
		}
	}
	b.WriteString("\nFROM rollup")

	if len(a.Shape.Having) > 0 {
		b.WriteString("\nHAVING ")
		havingParts := make([]string, len(a.Shape.Having))
		for i, h := range a.Shape.Having {
			rawOuter := outerExprs[h.AggIndex]
			expr := stripAlias(rawOuter)
			havingParts[i] = fmt.Sprintf("%s %s %v", expr, h.Op, h.Value)
		}
		b.WriteString(strings.Join(havingParts, " AND "))
	}

	if a.Shape.OrderLimit != nil {
		ol := a.Shape.OrderLimit
		rawOuter := outerExprs[ol.AggIndex]
		expr := stripAlias(rawOuter)
		b.WriteString("\nORDER BY ")
		b.WriteString(expr)
		if ol.Desc {
			b.WriteString(" DESC")
		}
		fmt.Fprintf(&b, "\nLIMIT %d", ol.Limit)
	}

	return b.String()
}

// emitScalarAggregate emits SQL for queries that have no date_trunc (BucketArg=="").
// The output is a single-row scalar aggregate over all precalc buckets in range.
// Caller guarantees no open tail (TailLo == TimeHi).
func emitScalarAggregate(a EmitArgs, mainFiles, innerSelects, outerExprs []string) string {
	involved := involvedDims(a.Shape)
	hasTimeBounds := !a.Shape.TimeLo.IsZero() || !a.Shape.TimeHi.IsZero()

	var b strings.Builder

	fmt.Fprintf(&b, "SET TimeZone = '%s';\n", a.Spec.TZ)

	b.WriteString("WITH rollup AS (\n  SELECT")
	for i, inner := range innerSelects {
		if i == 0 {
			fmt.Fprintf(&b, "\n    %s", inner)
		} else {
			fmt.Fprintf(&b, ",\n    %s", inner)
		}
	}

	fmt.Fprintf(&b, "\n  FROM read_parquet(%s)", pathList(a.StoragePrefix, mainFiles))

	whereAdded := false
	if hasTimeBounds {
		b.WriteString("\n  WHERE bucket >= ")
		b.WriteString(fmtTimestamp(a.Shape.TimeLo))
		b.WriteString(" AND bucket < ")
		b.WriteString(fmtTimestamp(a.TailLo))
		whereAdded = true
	}

	for _, dim := range involved {
		fp, ok := a.Shape.Filters[dim]
		if !ok {
			continue
		}
		if !whereAdded {
			b.WriteString("\n  WHERE ")
			whereAdded = true
		} else {
			b.WriteString("\n    AND ")
		}
		b.WriteString(buildFilterExpr(dim+"_class", fp))
	}

	b.WriteString("\n)")

	b.WriteString("\nSELECT")
	for i, outer := range outerExprs {
		if i == 0 {
			fmt.Fprintf(&b, "\n  %s", outer)
		} else {
			fmt.Fprintf(&b, ",\n  %s", outer)
		}
	}
	b.WriteString("\nFROM rollup")

	return b.String()
}
