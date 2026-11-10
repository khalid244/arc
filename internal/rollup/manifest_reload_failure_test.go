package rollup

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// The reload warning must say WHICH cube's manifest failed and why, with the list capped.
func TestReadManifestsNamesFailingCubes(t *testing.T) {
	stg := newFakeStorage()
	m := testManager(stg)
	ctx := context.Background()

	good := m.freshManifest(CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}}})
	gb, err := good.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	stg.objs["_arc/rollup/default/events/by_site/manifest.json"] = gb
	stg.objs["_arc/rollup/default/events/corrupt/manifest.json"] = []byte("{not json")
	stg.objs["_arc/rollup/default/events/empty/manifest.json"] = nil

	files := []string{
		"s3://arc-test/_arc/rollup/default/events/by_site/manifest.json",
		"s3://arc-test/_arc/rollup/default/events/corrupt/manifest.json",
		"s3://arc-test/_arc/rollup/default/events/empty/manifest.json",
		"s3://arc-test/_arc/rollup/default/events/missing/manifest.json",
	}
	loaded, failed := m.readManifests(ctx, files)
	if len(loaded) != 1 {
		t.Fatalf("loaded = %d, want 1", len(loaded))
	}
	if len(failed) != 3 {
		t.Fatalf("failed = %v, want 3 entries", failed)
	}
	for i, want := range []string{"default/events/corrupt: ", "default/events/empty: empty manifest", "default/events/missing: not found"} {
		if !strings.Contains(failed[i], want) {
			t.Errorf("failed[%d] = %q, want it to contain %q", i, failed[i], want)
		}
	}

	many := make([]string, 12)
	for i := range many {
		many[i] = fmt.Sprintf("c%d: boom", i)
	}
	capped := capFailures(many, maxLoggedManifestFailures)
	if len(capped) != maxLoggedManifestFailures+1 || capped[maxLoggedManifestFailures] != "... and 7 more" {
		t.Fatalf("capFailures = %v", capped)
	}
}
