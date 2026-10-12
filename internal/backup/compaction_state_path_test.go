package backup

import (
	"testing"

	"github.com/basekick-labs/arc/internal/compaction"
)

// TestCompactionStateDirMatchesManifestBasePath pins the literal this package
// keeps so it does not import internal/compaction on the production path. The
// fork moved compaction manifests to "_arc/compaction"; a backup that still
// looked under upstream's "_compaction_state" would copy none of them, and a
// mid-job restore would bring back an output next to the inputs it replaced
// (#930).
func TestCompactionStateDirMatchesManifestBasePath(t *testing.T) {
	if compactionStateDir != compaction.ManifestBasePath {
		t.Fatalf("compactionStateDir = %q, compaction.ManifestBasePath = %q", compactionStateDir, compaction.ManifestBasePath)
	}
	key := compaction.ManifestBasePath + "/hourly/prod/job-1.json"
	if !isCompactionState(key) {
		t.Fatalf("isCompactionState(%q) = false", key)
	}
	if got := routeKey(key); got != "prod" {
		t.Fatalf("routeKey(%q) = %q, want prod", key, got)
	}
	sc, err := newScope([]string{"prod"})
	if err != nil {
		t.Fatal(err)
	}
	if !sc.ownsPath(key) {
		t.Fatalf("scope [prod] does not own %q", key)
	}
	if sc.ownsPath(compaction.ManifestBasePath + "/hourly/other/job-1.json") {
		t.Fatal("scope [prod] owns another database's manifest")
	}
	// Reorg manifests share the "_arc" root but are not compaction state.
	if isCompactionState("_arc/reorg/events_late_2026101200_j.json") {
		t.Fatal("a reorg manifest classified as compaction state")
	}
}
