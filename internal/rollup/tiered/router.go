package tiered

import (
	"context"
	"database/sql"
	"time"
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
	DB          *sql.DB       // for EXPLAIN
	Files       FileIndex
	Spec        *Spec
	DimRichCap  int           // default 100 if zero
	GraceWindow time.Duration // default 6h if zero
	Metrics     MetricsSink   // optional; nil = no metrics
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
		return userSQL, false
	}
	variant := PickVariant(shape, d.Spec, d.DimRichCap)
	if variant == "" {
		if d.Metrics != nil {
			d.Metrics.IncRewriteRefusedVariant()
		}
		return userSQL, false
	}
	tier, tailLo, ok := PickTier(ctx, shape, d.Files, variant, d.GraceWindow)
	if !ok {
		if d.Metrics != nil {
			d.Metrics.IncRewriteRefusedTier()
		}
		return userSQL, false
	}
	out, ok := EmitMergeOnRead(EmitArgs{
		Ctx:     ctx,
		Shape:   shape,
		Tier:    tier,
		TailLo:  tailLo,
		Variant: variant,
		Files:   d.Files,
		Spec:    d.Spec,
	})
	if !ok {
		if d.Metrics != nil {
			d.Metrics.IncRewriteRefusedEmit()
		}
		return userSQL, false
	}
	if d.Metrics != nil {
		d.Metrics.IncRewriteAccepted()
	}
	return out, true
}
