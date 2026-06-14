package rollup

import (
	"strings"
	"time"
)

// SourcePruner narrows a whole-table source glob to the partition paths that exist
// within a window. It is satisfied by *pruning.PartitionPruner.ExistingPartitionPaths
// — the SAME battle-tested partition pruning + existence filtering (+ cache) the
// normal query read path uses — so the rollup merge's fresh-tail / head source reads
// list only real partitions instead of footer-reading the whole table, with no
// duplicated partition-layout or existence logic to drift.
type SourcePruner interface {
	// ExistingPartitionPaths returns the existing hour/day partition read_parquet
	// paths overlapping [from,to). (paths, true) with an empty slice means "no data
	// in range"; (nil, false) means "not prunable — use the whole-table glob".
	ExistingPartitionPaths(wholeTablePath string, from, to time.Time) (paths []string, optimizable bool)
}

// SetSourcePruner wires the shared partition pruner so the router's merge source
// branches prune to existing partitions. Optional: when unset the merge falls back
// to the whole-table source glob (correct, but unpruned). Called once at startup
// (before serving), so no lock is needed; the already-wired Router.SourceWindow
// closure reads m.srcPruner per call and sees the value.
func (m *Manager) SetSourcePruner(p SourcePruner) { m.srcPruner = p }

// sourceWindowGlob is the Router's source resolver (Router.SourceWindow): it prunes
// the whole-table source glob for [lo,hi) via the shared PartitionPruner.
func (m *Manager) sourceWindowGlob(source, lo, hi string) string {
	return prunedSourceGlob(m.sourceExpr(source), lo, hi, m.srcPruner)
}

// prunedSourceGlob asks the pruner for the existing partition paths in [lo,hi) and
// renders them as a read_parquet list arg. NEVER lossy: returns the whole-table
// glob on any uncertainty (no pruner, non-canonical glob, unparseable bounds, or
// "not prunable"); returns "" only when the window is confidently empty (caller
// then omits the source branch, since a zero-match per-day glob would error
// "No files found"). Split from sourceWindowGlob so it is unit-testable with a
// fake SourcePruner (no DuckDB/Manager needed).
func prunedSourceGlob(wholeTable, lo, hi string, pruner SourcePruner) string {
	if pruner == nil {
		return wholeTable
	}
	base, ok := wholeTableGlobBase(wholeTable)
	if !ok {
		return wholeTable
	}
	loT, ok1 := parseTS(lo)
	hiT, ok2 := parseTS(hi)
	if !ok1 || !ok2 || !hiT.After(loT) {
		return wholeTable
	}
	paths, optimizable := pruner.ExistingPartitionPaths(base+"/**/*.parquet", loT, hiT)
	if !optimizable {
		return wholeTable
	}
	if len(paths) == 0 {
		return "" // confidently empty window — caller skips this source branch
	}
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = "'" + p + "'"
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}
