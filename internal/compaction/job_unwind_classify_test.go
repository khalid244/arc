//go:build duckdb_arrow

package compaction

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// CS-5 / CS-8: the upload unwind must treat only a CLASSIFIED not-found as
// "the possibly-committed output is gone". A Ceph 503 whose hex RequestID
// contains "404" used to satisfy storage.IsNotFound's string fallback, so the
// unwind deleted the manifest while the committed output and every input were
// still live: every row twice, with no manifest left to resolve it.
func TestJobRun_UnwindKeepsManifestWhenOutputDelete5xxMentions404(t *testing.T) {
	rig := newManifestFirstRig(t)
	rig.backend.commitThenFail = true
	rig.backend.failOutputDelete = true
	rig.backend.outputDeleteErr = errors.New("operation error S3: DeleteObject, https response error " +
		"StatusCode: 503, RequestID: tx00000a3f2e0404b1c2d3e-0068f0a1b2-1a2b3c-fsn1, HostID: 1a2b3c-fsn1-prod, " +
		"api error SlowDown: Please reduce your request rate.")

	job, err := rig.run(t, "manifest-first-503-mentions-404")
	if err == nil {
		t.Fatal("Job.Run succeeded despite the upload error")
	}
	if !strings.Contains(err.Error(), outputUploadUnresolvedMarker) {
		t.Fatalf("a 503 on the output delete is unresolved, not 'already gone': %v", err)
	}
	ctx := context.Background()
	left, _ := rig.mm.ListManifests(ctx)
	if len(left) != 1 {
		t.Fatalf("DUPLICATION HAZARD: manifest deleted while the committed output %s and the inputs are live (manifests: %v)",
			job.OutputStorageKey, left)
	}
	rig.assertInputsPresent(t)
}
