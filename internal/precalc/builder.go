package precalc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Builder executes precalc build SQL against a DuckDB connection and writes
// the result to a Parquet file. Stateless across calls; safe to reuse for
// many variants. Caller is responsible for tier directory structure and
// final atomic rename (see manifest.go).
type Builder struct {
	DB     *sql.DB
	HLLLgK int
	KLLk   int
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
	stmt := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`, inner, escapePath(outPath))
	if _, err := b.DB.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("build sketch variant: %w", err)
	}
	return nil
}

// escapePath escapes a path for safe embedding inside a single-quoted SQL
// string. Paths containing single quotes break the COPY statement.
func escapePath(p string) string {
	return strings.ReplaceAll(p, "'", "''")
}
