package tiered

import (
	"context"
	"database/sql"
	"time"
)

// RewriteDeps bundles the dependencies Rewrite needs. Constructed once by the
// caller (Arc's query handler) and reused across many Rewrite() calls.
type RewriteDeps struct {
	DB          *sql.DB    // for EXPLAIN
	Manifest    *Manifest
	Spec        *Spec
	DimRichCap  int           // default 100 if zero
	GraceWindow time.Duration // default 6h if zero
}

// Rewrite is the top-level router entrypoint. It parses the user SQL,
// picks a variant + tier, generates the merge-on-read SQL, and returns
// (rewritten, true). On any guard failure it returns (originalSQL, false)
// so the caller runs the original against source.
func Rewrite(ctx context.Context, userSQL string, d RewriteDeps) (string, bool) {
	if d.DimRichCap == 0 {
		d.DimRichCap = 100
	}
	if d.GraceWindow == 0 {
		d.GraceWindow = 6 * time.Hour
	}

	shape, err := ExtractQueryShape(ctx, d.DB, userSQL)
	if err != nil || !shape.Supported {
		return userSQL, false
	}
	variant := PickVariant(shape, d.Spec, d.DimRichCap)
	if variant == "" {
		return userSQL, false
	}
	tier, tailLo, ok := PickTier(shape, d.Manifest, variant, d.GraceWindow)
	if !ok {
		return userSQL, false
	}
	out, ok := EmitMergeOnRead(EmitArgs{
		Shape:    shape,
		Tier:     tier,
		TailLo:   tailLo,
		Variant:  variant,
		Manifest: d.Manifest,
		Spec:     d.Spec,
	})
	if !ok {
		return userSQL, false
	}
	return out, true
}
