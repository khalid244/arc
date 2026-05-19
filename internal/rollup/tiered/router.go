package tiered

import (
	"context"
	"database/sql"
	"time"

	"github.com/rs/zerolog"
)

// MetricsSink is the hook used by the tiered subsystem to emit counters.
// Defined as an interface here to avoid an import cycle between tiered and
// internal/metrics. The caller passes whatever concrete impl they want.
// nil is valid — all call sites check for nil before invoking.
type MetricsSink interface {
	// Router metrics (T54)
	IncRewriteAttempts()
	IncRewriteAccepted()
	IncRewriteRefusedParser()
	IncRewriteRefusedVariant()
	IncRewriteRefusedTier()
	IncRewriteRefusedEmit()
	AddRewriteNanos(int64)

	// Builder / scheduler metrics (T55)
	IncBuildSuccess()
	IncBuildErrors()
	AddBuildNanos(int64)
	SetMaxWatermarkLagSeconds(int64)
}

// RewriteDeps bundles the dependencies Rewrite needs. Constructed once by the
// caller (Arc's query handler) and reused across many Rewrite() calls.
type RewriteDeps struct {
	DB          *sql.DB  // for EXPLAIN
	Files       FileIndex
	Spec        *Spec
	DimRichCap  int            // default 100 if zero
	GraceWindow time.Duration  // default 6h if zero
	Metrics     MetricsSink    // optional; nil = no metrics
	Logger      zerolog.Logger // optional; zero value is fine — emits to disabled logger
	// StoragePrefix is prepended to every parquet path in the emitted
	// read_parquet calls (e.g. "s3://hammel-arc/"). Empty for local mode.
	StoragePrefix string
	// SchemaHashLookup returns the parquet KV-metadata schema_hash for
	// a given file path. Passed through to the emitter so mismatched
	// files are excluded from the read set. nil disables filtering.
	SchemaHashLookup func(path string) (string, error)
}

// Rewrite is the top-level router entrypoint. It parses the user SQL,
// picks a variant + tier, generates the merge-on-read SQL, and returns
// (rewritten, true). On any guard failure it returns (originalSQL, false)
// so the caller runs the original against source.
func Rewrite(ctx context.Context, userSQL string, d RewriteDeps) (string, bool) {
	start := time.Now()
	if d.Metrics != nil {
		d.Metrics.IncRewriteAttempts()
		defer func() { d.Metrics.AddRewriteNanos(time.Since(start).Nanoseconds()) }()
	}

	if d.DimRichCap == 0 {
		d.DimRichCap = 100
	}
	if d.GraceWindow == 0 {
		d.GraceWindow = 6 * time.Hour
	}

	shape, err := ExtractQueryShape(ctx, d.DB, userSQL)
	if err != nil || !shape.Supported {
		if d.Metrics != nil {
			d.Metrics.IncRewriteRefusedParser()
		}
		reason := "parser failed"
		if err != nil {
			reason = err.Error()
		} else if shape != nil && shape.Reason != "" {
			reason = shape.Reason
		}
		d.Logger.Info().Str("stage", "parser").Str("reason", reason).Msg("tiered router refused")
		return userSQL, false
	}
	variant := PickVariant(shape, d.Spec, d.DimRichCap)
	if variant == "" {
		if d.Metrics != nil {
			d.Metrics.IncRewriteRefusedVariant()
		}
		d.Logger.Info().Str("stage", "variant").Interface("dims", shape.GroupDims).Str("time_col", shape.TimeColumn).Msg("tiered router refused: no matching variant")
		return userSQL, false
	}
	tier, tailLo, ok := PickTier(ctx, shape, d.Files, variant, d.GraceWindow)
	if !ok {
		if d.Metrics != nil {
			d.Metrics.IncRewriteRefusedTier()
		}
		d.Logger.Info().Str("stage", "tier").Str("variant", variant).Time("time_lo", shape.TimeLo).Time("time_hi", shape.TimeHi).Msg("tiered router refused: no tier covers the time range")
		return userSQL, false
	}
	out, ok := EmitMergeOnRead(EmitArgs{
		Ctx:              ctx,
		Shape:            shape,
		Tier:             tier,
		TailLo:           tailLo,
		Variant:          variant,
		Files:            d.Files,
		Spec:             d.Spec,
		StoragePrefix:    d.StoragePrefix,
		SchemaHashLookup: d.SchemaHashLookup,
	})
	if !ok {
		if d.Metrics != nil {
			d.Metrics.IncRewriteRefusedEmit()
		}
		d.Logger.Info().Str("stage", "emit").Str("variant", variant).Str("tier", string(tier)).Msg("tiered router refused: emit failed")
		return userSQL, false
	}
	if d.Metrics != nil {
		d.Metrics.IncRewriteAccepted()
	}
	return out, true
}
