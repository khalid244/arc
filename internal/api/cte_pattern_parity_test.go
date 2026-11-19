package api

import (
	"testing"

	"github.com/basekick-labs/arc/internal/pruning"
)

// The pruner only trusts a CTE name the storage-path rewrite also leaves
// alone; both must read CTE names with the same pattern.
func TestCTENamePatternMatchesPruner(t *testing.T) {
	if patternCTENames.String() != pruning.CTENamePatternText {
		t.Fatalf("patternCTENames %q differs from pruning.CTENamePatternText %q", patternCTENames.String(), pruning.CTENamePatternText)
	}
}
