// Package precalc implements Arc's tiered precalculation system.
//
// Layers: classifier produces a Spec (per-table classifier output, kept values
// per dim, pinned timezone); builder materializes Parquet variants per
// (table, tier, variant) following the spec; router intercepts SQL pre-DuckDB
// and rewrites to precalc when safe.
//
// See docs/precalc.md for operator-facing reference (TODO: write after Phase 6).
package precalc
