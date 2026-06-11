package rollup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// The multi-pod workload tests. In production, queries land on route-only
// replicas while planSpecs runs on the single builder pod, so the observed
// workload must cross pods through object storage: each pod persists its OWN
// recordings as absolute counters to <prefix>/_workload/<pod>.json and the
// builder re-merges every file each tick. The merge must be idempotent (the
// same file is merged every tick) and a pod must never re-persist counts that
// belong to another pod (that would echo them back and inflate every cycle).

// recShape builds a minimal recordable shape: one grouped dim on a source.
func recShape(source, dim string) QueryShape {
	return QueryShape{Source: source, Dims: []string{dim}}
}

// dimsBytes records n queries for (source, dim) on a fresh Workload and
// serializes it — the exact content a route-only pod would persist.
func dimsBytes(t *testing.T, source, dim string, n int) []byte {
	t.Helper()
	w := NewWorkload()
	for i := 0; i < n; i++ {
		w.Record(recShape(source, dim))
	}
	b, err := w.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	return b
}

// TestWorkloadMergeNamespaceIdempotent proves the builder can merge the same
// per-pod file every tick without double-counting: a re-merge REPLACES the
// namespace, and an updated file (absolute counters grew) updates rather than
// accumulates.
func TestWorkloadMergeNamespaceIdempotent(t *testing.T) {
	w := NewWorkload()
	w.Record(recShape("posthog.events", "event")) // builder's own observation

	pod := dimsBytes(t, "posthog.events", "event", 10)
	for i := 0; i < 3; i++ { // three ticks merging the SAME file
		if err := w.MergeNamespaceBytes("_arc/rollup/_workload/pod-a.json", pod); err != nil {
			t.Fatalf("merge %d: %v", i, err)
		}
	}
	if got := w.DimCounts("posthog.events")["event"]; got != 11 {
		t.Fatalf("after repeated merges of one file: event = %d, want 11 (1 own + 10 pod, no double-count)", got)
	}

	// The pod's counters grew (absolute counters): re-merge must REPLACE.
	grown := dimsBytes(t, "posthog.events", "event", 25)
	if err := w.MergeNamespaceBytes("_arc/rollup/_workload/pod-a.json", grown); err != nil {
		t.Fatalf("merge grown: %v", err)
	}
	if got := w.DimCounts("posthog.events")["event"]; got != 26 {
		t.Fatalf("after grown re-merge: event = %d, want 26 (1 own + 25 pod)", got)
	}
}

// TestWorkloadMergeNamespaceAdditiveAcrossPods proves counts from DIFFERENT
// pods add up — two replicas each seeing half the traffic must together rank a
// dim as high as one pod seeing all of it.
func TestWorkloadMergeNamespaceAdditiveAcrossPods(t *testing.T) {
	w := NewWorkload()
	if err := w.MergeNamespaceBytes("pod-a", dimsBytes(t, "posthog.events", "event", 7)); err != nil {
		t.Fatal(err)
	}
	if err := w.MergeNamespaceBytes("pod-b", dimsBytes(t, "posthog.events", "event", 5)); err != nil {
		t.Fatal(err)
	}
	if got := w.DimCounts("posthog.events")["event"]; got != 12 {
		t.Fatalf("cross-pod sum: event = %d, want 12", got)
	}
}

// TestWorkloadBytesExcludesMergedNamespaces pins the no-feedback-loop property:
// what a pod persists (Bytes) is its own recordings plus its own resumed seed —
// NEVER counts merged from other pods' files. If Bytes leaked remote counts,
// the builder's _workload.json (and any pod file) would re-export other pods'
// observations and every merge generation would inflate them.
func TestWorkloadBytesExcludesMergedNamespaces(t *testing.T) {
	w := NewWorkload()
	if err := w.LoadBytes(dimsBytes(t, "posthog.events", "event", 5)); err != nil { // resumed own history
		t.Fatal(err)
	}
	w.Record(recShape("posthog.events", "event")) // new own observation
	if err := w.MergeNamespaceBytes("pod-x", dimsBytes(t, "posthog.events", "event", 100)); err != nil {
		t.Fatal(err)
	}

	// Live view sums everything.
	if got := w.DimCounts("posthog.events")["event"]; got != 106 {
		t.Fatalf("DimCounts = %d, want 106 (5 seed + 1 own + 100 remote)", got)
	}

	// Persisted view excludes the remote namespace.
	b, err := w.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	fresh := NewWorkload()
	if err := fresh.LoadBytes(b); err != nil {
		t.Fatal(err)
	}
	if got := fresh.DimCounts("posthog.events")["event"]; got != 6 {
		t.Fatalf("round-tripped Bytes = %d, want 6 (seed+own only; remote must not leak)", got)
	}
}

// TestWorkloadSeqTracksOwnRecordsOnly pins the dirty flag the route-only
// persist loop uses: Seq advances on Record (new data worth a PUT) but not on
// LoadBytes or namespace merges (nothing new observed BY this pod).
func TestWorkloadSeqTracksOwnRecordsOnly(t *testing.T) {
	w := NewWorkload()
	if w.Seq() != 0 {
		t.Fatalf("fresh Seq = %d, want 0", w.Seq())
	}
	if err := w.LoadBytes(dimsBytes(t, "default.downloads", "site", 3)); err != nil {
		t.Fatal(err)
	}
	if err := w.MergeNamespaceBytes("pod-a", dimsBytes(t, "default.downloads", "site", 3)); err != nil {
		t.Fatal(err)
	}
	if w.Seq() != 0 {
		t.Fatalf("Seq after load+merge = %d, want 0 (no own observation)", w.Seq())
	}
	w.Record(QueryShape{}) // empty source is dropped — must not look dirty
	if w.Seq() != 0 {
		t.Fatalf("Seq after dropped record = %d, want 0", w.Seq())
	}
	w.Record(recShape("default.downloads", "site"))
	if w.Seq() != 1 {
		t.Fatalf("Seq after record = %d, want 1", w.Seq())
	}
}

// TestManagerMergesPodWorkloadFiles proves the builder-side tick merge:
// per-pod files under <prefix>/_workload/ are discovered (DuckDB glob over the
// local uriRoot, same as production globs S3), read through the storage
// backend, and merged additively across pods / idempotently across ticks.
func TestManagerMergesPodWorkloadFiles(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	root := t.TempDir()
	stg := newFakeStorage()
	m := driftManager(t, db, root, stg)
	m.instanceID = "builder-0"

	dir := filepath.Join(root, "_arc/rollup/_workload")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	pods := map[string][]byte{
		"pod-a.json": dimsBytes(t, "posthog.events", "event", 4),
		"pod-b.json": dimsBytes(t, "posthog.events", "event", 6),
	}
	for name, b := range pods {
		// On disk for the glob discovery, in fakeStorage for the Read.
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
		stg.objs["_arc/rollup/_workload/"+name] = b
	}

	ctx := context.Background()
	m.mergePodWorkloads(ctx)
	if got := m.workload.DimCounts("posthog.events")["event"]; got != 10 {
		t.Fatalf("after merge: event = %d, want 10 (4+6 across pods)", got)
	}
	m.mergePodWorkloads(ctx) // next tick re-merges the same files
	if got := m.workload.DimCounts("posthog.events")["event"]; got != 10 {
		t.Fatalf("after re-merge: event = %d, want 10 (idempotent)", got)
	}

	// The builder's own persist must not absorb the pod counts (they stay owned
	// by the per-pod files), or a builder restart would count them twice: once
	// from the resumed _workload.json and once from the still-present files.
	m.persistWorkload(ctx)
	resumed := NewWorkload()
	if err := resumed.LoadBytes(stg.objs[m.workloadKey()]); err != nil {
		t.Fatal(err)
	}
	if got := resumed.DimCounts("posthog.events")["event"]; got != 0 {
		t.Fatalf("_workload.json absorbed %d pod counts; restart would double-count", got)
	}
}

// TestPersistPodWorkloadIfDirty pins the route-only persist: no PUT until a
// query is recorded, exactly one PUT per batch of new recordings (object
// storage pressure — Hetzner 503s — is why every-tick unconditional PUTs are
// not acceptable), and the written counters are this pod's absolute view.
func TestPersistPodWorkloadIfDirty(t *testing.T) {
	stg := newFakeStorage()
	m := testManager(stg)
	m.instanceID = "pod-a"
	ctx := context.Background()
	key := m.podWorkloadKey()
	if key != "_arc/rollup/_workload/pod-a.json" {
		t.Fatalf("podWorkloadKey = %q", key)
	}

	m.persistPodWorkloadIfDirty(ctx)
	if _, ok := stg.objs[key]; ok {
		t.Fatal("persisted before anything was recorded")
	}

	m.workload.Record(recShape("posthog.events", "event"))
	m.persistPodWorkloadIfDirty(ctx)
	got := NewWorkload()
	if err := got.LoadBytes(stg.objs[key]); err != nil {
		t.Fatalf("persisted file unreadable: %v", err)
	}
	if n := got.DimCounts("posthog.events")["event"]; n != 1 {
		t.Fatalf("persisted event = %d, want 1", n)
	}

	// No new recordings -> no rewrite (sentinel must survive).
	stg.objs[key] = []byte("sentinel")
	m.persistPodWorkloadIfDirty(ctx)
	if string(stg.objs[key]) != "sentinel" {
		t.Fatal("re-persisted identical data (pointless PUT)")
	}

	// New recording -> rewrite with the grown absolute counter.
	m.workload.Record(recShape("posthog.events", "event"))
	m.persistPodWorkloadIfDirty(ctx)
	got = NewWorkload()
	if err := got.LoadBytes(stg.objs[key]); err != nil {
		t.Fatalf("re-persisted file unreadable: %v", err)
	}
	if n := got.DimCounts("posthog.events")["event"]; n != 2 {
		t.Fatalf("persisted event = %d, want 2 (absolute counters)", n)
	}
}

// TestRunRouteOnlyFlushesWorkloadOnShutdown pins the route-only loop wiring:
// recordings made on a route-only pod reach object storage (here via the
// shutdown flush — the per-tick persist is the same call). Before this fix,
// runRouteOnly never persisted, so reader-pod recordings died in memory and
// prod's _workload.json stayed {"dims":{}} forever.
func TestRunRouteOnlyFlushesWorkloadOnShutdown(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	root := t.TempDir()
	stg := newFakeStorage()
	m := driftManager(t, db, root, stg)
	m.instanceID = "pod-route-0"

	m.workload.Record(recShape("posthog.events", "event"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // first select hits ctx.Done: reload once, flush, return
	m.runRouteOnly(ctx)

	b, ok := stg.objs[m.podWorkloadKey()]
	if !ok {
		t.Fatal("route-only shutdown did not flush the recorded workload")
	}
	got := NewWorkload()
	if err := got.LoadBytes(b); err != nil {
		t.Fatalf("flushed file unreadable: %v", err)
	}
	if n := got.DimCounts("posthog.events")["event"]; n != 1 {
		t.Fatalf("flushed event = %d, want 1", n)
	}
}
