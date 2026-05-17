// Package tiered is the tiered-rollup implementation that lives under
// internal/rollup/. From an operator's perspective this is part of the
// rollup feature; the sub-package only exists to isolate the new
// multi-tier code path from the original single-tier implementation in
// the parent package.
//
// Layers: classifier produces a Spec (per-table classifier output, kept
// values per dim, pinned timezone); builder materializes Parquet variants
// per (table, tier, variant) following the spec; router (forthcoming)
// rewrites compatible SQL to use the tiered variants.
//
// Operator-facing reference: docs/rollups.md (the existing rollup doc
// will be updated to describe the new tiered behavior).
package tiered
