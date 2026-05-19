package tiered

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Builder executes precalc build SQL against a DuckDB connection and writes
// the result to a Parquet file. Stateless across calls; safe to reuse for
// many variants. Caller is responsible for tier directory structure and
// final atomic rename (see manifest.go).
type Builder struct {
	DB     *sql.DB
	HLLLgK int
	KLLk   int

	// Fields below are stamped into Parquet KV-metadata for every file
	// the builder writes. Setting them is optional in tests; production
	// callers (Publisher) populate them from the current spec + tier.
	SchemaHash     string
	TierTZ         string
	BuilderVersion string
	BucketLo       time.Time
	BucketHi       time.Time
}

// execWithTZ pins the session TimeZone to b.TierTZ before running `stmt` so
// the build SQL's date_trunc() aligns buckets to the spec's timezone — must
// match what the router's emitter uses at query time. Uses a dedicated
// connection so the SET TimeZone and the COPY land on the same session
// (sql.DB is a pool; consecutive ExecContext calls may otherwise span
// different connections).
func (b *Builder) execWithTZ(ctx context.Context, stmt string) error {
	if b.TierTZ == "" {
		_, err := b.DB.ExecContext(ctx, stmt)
		return err
	}
	conn, err := b.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET TimeZone = '%s'", escapeSQLString(b.TierTZ))); err != nil {
		return fmt.Errorf("set TimeZone: %w", err)
	}
	_, err = conn.ExecContext(ctx, stmt)
	return err
}

// BuildSketchVariant builds the no-dim sketch variant for one tier and
// writes it to `outPath`. The output is a self-contained Parquet file the
// caller is expected to rename into its final tier directory.
func (b *Builder) BuildSketchVariant(ctx context.Context, args BuildArgs, outPath string) error {
	if b.HLLLgK == 0 {
		b.HLLLgK = 14
	}
	if b.KLLk == 0 {
		b.KLLk = 200
	}
	args.HLLLgK = b.HLLLgK
	args.KLLk = b.KLLk

	inner := BuildSketchVariantSQL(args)
	stmt := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD%s)`, inner, escapePath(outPath), b.kvMetadataClause())
	if err := b.execWithTZ(ctx, stmt); err != nil {
		return fmt.Errorf("build sketch variant: %w", err)
	}
	return nil
}

// BuildPerDimVariant builds a per-dim variant for the given dim and writes
// it to outPath. Values outside the dim's kept_set bucket as _OTHER_.
//
// Returns an error if the dim has no spec entry or an empty kept-values
// list — without kept values the variant would either crash on `IN ()`
// or collapse into a single _OTHER_ bucket, which is indistinguishable
// from the sketch variant and not useful.
func (b *Builder) BuildPerDimVariant(ctx context.Context, args BuildArgs, spec *Spec, dim, outPath string) error {
	dimSpec, ok := spec.Dims[dim]
	if !ok {
		return fmt.Errorf("build per-dim variant %s: spec has no entry for this dim", dim)
	}
	if len(dimSpec.KeptValues) == 0 {
		return fmt.Errorf("build per-dim variant %s: kept_values is empty; use the sketch variant instead", dim)
	}
	if b.HLLLgK == 0 {
		b.HLLLgK = 14
	}
	if b.KLLk == 0 {
		b.KLLk = 200
	}
	args.HLLLgK = b.HLLLgK
	args.KLLk = b.KLLk

	inner := BuildPerDimVariantSQL(args, spec, dim)
	stmt := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD%s)`, inner, escapePath(outPath), b.kvMetadataClause())
	if err := b.execWithTZ(ctx, stmt); err != nil {
		return fmt.Errorf("build per-dim variant %s: %w", dim, err)
	}
	return nil
}

// BuildDimRichVariant builds the dim-rich variant for all dims under
// dim_rich_cap and writes it to outPath.
func (b *Builder) BuildDimRichVariant(ctx context.Context, args BuildArgs, spec *Spec, dimRichCap int, outPath string) error {
	inner := BuildDimRichVariantSQL(args, spec, dimRichCap)
	stmt := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD%s)`, inner, escapePath(outPath), b.kvMetadataClause())
	if err := b.execWithTZ(ctx, stmt); err != nil {
		return fmt.Errorf("build dim-rich variant: %w", err)
	}
	return nil
}

// BuildAllVariants builds all variants for one bucket in a single GROUPING SETS
// SQL pass. It materialises the aggregate result into a DuckDB table, then
// COPYs filtered subsets to one parquet file per variant under outDir.
//
// Returns a map from variant name to local file path for each file written.
// On error some files may already have been written; the caller should clean up
// outDir regardless.
func (b *Builder) BuildAllVariants(ctx context.Context, args BuildArgs, spec *Spec, dimRichCap int, outDir string) (map[string]string, error) {
	if b.HLLLgK == 0 {
		b.HLLLgK = 14
	}
	if b.KLLk == 0 {
		b.KLLk = 200
	}
	args.HLLLgK = b.HLLLgK
	args.KLLk = b.KLLk

	plans := variantsForSpec(spec, dimRichCap)
	if len(plans) == 0 {
		return nil, nil
	}

	allDims := allVariantDims(spec, dimRichCap)

	// Acquire a single connection so SET TimeZone, CREATE TABLE, COPYs, and DROP
	// all share the same session (sql.DB is a pool).
	conn, err := b.DB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Close()

	if b.TierTZ != "" {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET TimeZone = '%s'", escapeSQLString(b.TierTZ))); err != nil {
			return nil, fmt.Errorf("set TimeZone: %w", err)
		}
	}

	const aggTable = "_arc_bucket_agg"

	// Materialise aggregate results (~10K–100K rows; small by design).
	aggSQL := BuildAllVariantsSQL(args, spec, dimRichCap)
	createStmt := fmt.Sprintf("CREATE OR REPLACE TABLE %s AS %s", aggTable, aggSQL)
	if _, err := conn.ExecContext(ctx, createStmt); err != nil {
		return nil, fmt.Errorf("materialize aggregate table: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", aggTable))
	}()

	kvClause := b.kvMetadataClause()
	result := make(map[string]string, len(plans))

	for _, plan := range plans {
		variantID := VariantGroupingID(plan.Variant, allDims, spec, dimRichCap)
		if variantID < 0 {
			return nil, fmt.Errorf("no grouping ID for variant %q", plan.Variant)
		}

		outPath := fmt.Sprintf("%s/%s.parquet", outDir, plan.Variant)

		var selectCols string
		switch {
		case plan.Variant == "sketch":
			selectCols = b.sketchSelectCols(args)
		case plan.Dim != "":
			selectCols = b.perDimSelectCols(args, plan.Dim)
		case plan.Variant == "all":
			selectCols = b.dimRichSelectCols(args, spec, dimRichCap)
		default:
			return nil, fmt.Errorf("unknown variant %q", plan.Variant)
		}

		copyStmt := fmt.Sprintf(
			"COPY (SELECT %s FROM %s WHERE variant_id = %d) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD%s)",
			selectCols, aggTable, variantID, escapePath(outPath), kvClause,
		)
		if _, err := conn.ExecContext(ctx, copyStmt); err != nil {
			return nil, fmt.Errorf("copy variant %s: %w", plan.Variant, err)
		}
		result[plan.Variant] = outPath
	}

	return result, nil
}

// sketchSelectCols returns the SELECT column list for the sketch variant from
// the materialised aggregate table.
func (b *Builder) sketchSelectCols(args BuildArgs) string {
	var parts []string
	parts = append(parts, "bucket", "cnt")
	for _, m := range args.MetricCols {
		if !m.Numeric {
			continue
		}
		parts = append(parts,
			"cnt_"+m.Name, "sum_"+m.Name, "sum_sq_"+m.Name,
			"min_"+m.Name, "max_"+m.Name,
		)
	}
	for _, c := range args.HLLCols {
		parts = append(parts, "hll_"+c)
	}
	for _, c := range args.KLLCols {
		parts = append(parts, "kll_"+c)
	}
	return strings.Join(parts, ", ")
}

// perDimSelectCols returns the SELECT column list for a per-dim variant from
// the materialised aggregate table.
func (b *Builder) perDimSelectCols(args BuildArgs, dim string) string {
	var parts []string
	parts = append(parts, "bucket", dim+"_class", "cnt")
	for _, m := range args.MetricCols {
		if !m.Numeric {
			continue
		}
		parts = append(parts,
			"cnt_"+m.Name, "sum_"+m.Name,
			"min_"+m.Name, "max_"+m.Name,
		)
	}
	for _, c := range args.HLLCols {
		parts = append(parts, "hll_"+c)
	}
	return strings.Join(parts, ", ")
}

// dimRichSelectCols returns the SELECT column list for the dim-rich (all)
// variant from the materialised aggregate table.
func (b *Builder) dimRichSelectCols(args BuildArgs, spec *Spec, dimRichCap int) string {
	var dims []string
	for name, d := range spec.Dims {
		if d.Role == "Dim" && d.EffectiveCard <= dimRichCap {
			dims = append(dims, name)
		}
	}
	sort.Strings(dims)

	var parts []string
	parts = append(parts, "bucket")
	for _, name := range dims {
		parts = append(parts, name+"_class")
	}
	parts = append(parts, "cnt")
	for _, m := range args.MetricCols {
		if !m.Numeric {
			continue
		}
		parts = append(parts,
			"cnt_"+m.Name, "sum_"+m.Name, "sum_sq_"+m.Name,
			"min_"+m.Name, "max_"+m.Name,
		)
	}
	return strings.Join(parts, ", ")
}

// escapePath escapes a path for safe embedding inside a single-quoted SQL
// string. Paths containing single quotes break the COPY statement.
func escapePath(p string) string {
	return strings.ReplaceAll(p, "'", "''")
}

// kvMetadataClause returns the KV_METADATA fragment to append inside a
// COPY options list. Returns "" when no metadata fields are set.
func (b *Builder) kvMetadataClause() string {
	if b.SchemaHash == "" && b.TierTZ == "" && b.BuilderVersion == "" && b.BucketLo.IsZero() && b.BucketHi.IsZero() {
		return ""
	}
	var parts []string
	if b.SchemaHash != "" {
		parts = append(parts, fmt.Sprintf("'schema_hash': '%s'", escapeSQLString(b.SchemaHash)))
	}
	if b.TierTZ != "" {
		parts = append(parts, fmt.Sprintf("'tier_tz': '%s'", escapeSQLString(b.TierTZ)))
	}
	if b.BuilderVersion != "" {
		parts = append(parts, fmt.Sprintf("'builder_version': '%s'", escapeSQLString(b.BuilderVersion)))
	}
	if !b.BucketLo.IsZero() {
		parts = append(parts, fmt.Sprintf("'bucket_lo': '%s'", b.BucketLo.UTC().Format(time.RFC3339)))
	}
	if !b.BucketHi.IsZero() {
		parts = append(parts, fmt.Sprintf("'bucket_hi': '%s'", b.BucketHi.UTC().Format(time.RFC3339)))
	}
	if len(parts) == 0 {
		return ""
	}
	return fmt.Sprintf(", KV_METADATA {%s}", strings.Join(parts, ", "))
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
