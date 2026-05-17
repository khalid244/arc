package tiered

import (
	"context"
	"database/sql"
	"fmt"
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
	if _, err := b.DB.ExecContext(ctx, stmt); err != nil {
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
	if _, err := b.DB.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("build per-dim variant %s: %w", dim, err)
	}
	return nil
}

// BuildDimRichVariant builds the dim-rich variant for all dims under
// dim_rich_cap and writes it to outPath.
func (b *Builder) BuildDimRichVariant(ctx context.Context, args BuildArgs, spec *Spec, dimRichCap int, outPath string) error {
	inner := BuildDimRichVariantSQL(args, spec, dimRichCap)
	stmt := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD%s)`, inner, escapePath(outPath), b.kvMetadataClause())
	if _, err := b.DB.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("build dim-rich variant: %w", err)
	}
	return nil
}

// RollupSketchVariant reads a lower-tier sketch parquet and writes the next
// coarser tier's sketch variant. Used hierarchically: 1h → 1d → 1w → 1mo.
func (b *Builder) RollupSketchVariant(ctx context.Context, args RollupArgs, outPath string) error {
	if b.HLLLgK == 0 {
		b.HLLLgK = 14
	}
	if b.KLLk == 0 {
		b.KLLk = 200
	}
	args.HLLLgK = b.HLLLgK
	args.KLLk = b.KLLk

	inner := BuildRollupSketchSQL(args)
	stmt := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD%s)`, inner, escapePath(outPath), b.kvMetadataClause())
	if _, err := b.DB.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("rollup sketch %s: %w", args.TargetTier, err)
	}
	return nil
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
