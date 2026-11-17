//go:build duckdb_arrow

package compaction

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// cutDeleteErr is the production failure (2026-11-15, 74 jobs): Hetzner
// answered DeleteObjects with 200 and deleted the keys, and the HTTP/2 stream
// was cancelled while the SDK read the body.
var cutDeleteErr = errors.New("failed to delete batch from S3: operation error S3: DeleteObjects, " +
	"https response error StatusCode: 200, RequestID: tx0, HostID: , deserialization failed, " +
	"failed to decode response body, stream error: stream ID 353; CANCEL; received from peer")

// deleteFaultBackend wraps a LocalBackend and scripts DeleteBatch: each call
// takes the next mode (the last repeats). "cut" deletes the keys and then
// fails like the production error; "fail" fails without deleting; "ok"
// delegates. Every call's key list is recorded.
type deleteFaultBackend struct {
	storage.Backend
	modes  []string
	onCall func()

	mu    sync.Mutex
	calls [][]string
}

func (b *deleteFaultBackend) DeleteBatch(ctx context.Context, paths []string) error {
	b.mu.Lock()
	n := len(b.calls)
	b.calls = append(b.calls, slices.Clone(paths))
	mode := "ok"
	if len(b.modes) > 0 {
		mode = b.modes[min(n, len(b.modes)-1)]
	}
	b.mu.Unlock()
	if b.onCall != nil {
		b.onCall()
	}
	switch mode {
	case "cut":
		if err := b.Backend.DeleteBatch(ctx, paths); err != nil {
			return err
		}
		return cutDeleteErr
	case "fail":
		return errors.New("failed to delete batch from S3: exceeded maximum number of attempts, 3, StatusCode: 503")
	}
	return b.Backend.DeleteBatch(ctx, paths)
}

func (b *deleteFaultBackend) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	return b.Backend.(storage.ObjectLister).ListObjects(ctx, prefix)
}

func (b *deleteFaultBackend) deleteCalls() [][]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.calls)
}

func shortInputDeleteBackoff(t *testing.T) {
	t.Helper()
	old := inputDeleteRetryBackoff
	inputDeleteRetryBackoff = time.Millisecond
	t.Cleanup(func() { inputDeleteRetryBackoff = old })
}

type deleteRetryOutcome struct {
	job       *Job
	backend   *deleteFaultBackend
	rig       *manifestFirstRig
	manifests []string
	bystander string
}

// runDeleteRetryJob runs a real hourly job over the manifest-first rig's three
// inputs with DeleteBatch scripted by modes. A file in the same partition that
// the job was not given stands in for everything the job does not own.
func runDeleteRetryJob(t *testing.T, retries int, modes ...string) deleteRetryOutcome {
	t.Helper()
	shortInputDeleteBackoff(t)
	rig := newManifestFirstRig(t)
	ctx := context.Background()
	bystander := manifestFirstPartition + "/not-an-input.parquet"
	if err := rig.local.Write(ctx, bystander, []byte("raw")); err != nil {
		t.Fatal(err)
	}
	backend := &deleteFaultBackend{Backend: rig.local, modes: modes}
	mm := NewManifestManager(backend, zerolog.Nop())
	job := NewJob(&JobConfig{
		Measurement: "cpu", PartitionPath: manifestFirstPartition, Files: rig.inputs,
		StorageBackend: backend, Database: "testdb", Tier: "hourly",
		TempDirectory: t.TempDir(), Logger: zerolog.Nop(), DB: rig.db,
		ManifestManager: mm, JobID: "delete-retry", InputDeleteRetries: retries,
	})
	if err := job.Run(ctx); err != nil {
		t.Fatalf("Job.Run: %v", err)
	}
	left, err := mm.ListManifests(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return deleteRetryOutcome{job: job, backend: backend, rig: rig, manifests: left, bystander: bystander}
}

func (o deleteRetryOutcome) exists(t *testing.T, key string) bool {
	t.Helper()
	ok, err := o.rig.local.Exists(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// Every DeleteBatch call, the retries included, names exactly the job's own
// inputs, and nothing else in the partition is touched.
func (o deleteRetryOutcome) assertOnlyOwnInputsDeleted(t *testing.T) {
	t.Helper()
	want := slices.Clone(o.rig.inputs)
	slices.Sort(want)
	for i, call := range o.backend.deleteCalls() {
		got := slices.Clone(call)
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("DeleteBatch call %d = %v; want exactly the job's inputs %v", i+1, call, want)
		}
	}
	if !o.exists(t, o.bystander) {
		t.Fatal("a file the job was not given was deleted")
	}
	if !o.exists(t, o.job.OutputStorageKey) {
		t.Fatal("output missing after the job")
	}
}

// Default 0: today's behaviour. The cut response fails the delete phase, the
// job keeps its manifest for recovery and does not report sources deleted.
func TestJobRun_InputDeleteRetry_DefaultKeepsTodaysBehaviour(t *testing.T) {
	o := runDeleteRetryJob(t, 0, "cut")
	if n := len(o.backend.deleteCalls()); n != 1 {
		t.Fatalf("DeleteBatch calls = %d; want 1 (no retry by default)", n)
	}
	if o.job.SourcesDeleted {
		t.Fatal("SourcesDeleted = true after a failed delete phase")
	}
	if len(o.manifests) != 1 {
		t.Fatalf("manifests left = %v; want the job's manifest kept for recovery", o.manifests)
	}
	o.assertOnlyOwnInputsDeleted(t)
}

// The fix: the same keys are sent again, the store answers cleanly (deleting
// a deleted key succeeds), and the job finishes normally and removes its own
// manifest, so recovery has nothing to do.
func TestJobRun_InputDeleteRetry_CutResponseRetriedJobRemovesManifest(t *testing.T) {
	o := runDeleteRetryJob(t, 2, "cut", "ok")
	if n := len(o.backend.deleteCalls()); n != 2 {
		t.Fatalf("DeleteBatch calls = %d; want 2", n)
	}
	if !o.job.SourcesDeleted {
		t.Fatal("SourcesDeleted = false after the retry succeeded")
	}
	if len(o.manifests) != 0 {
		t.Fatalf("manifests left = %v; want none: the job removes its own manifest", o.manifests)
	}
	for _, in := range o.rig.inputs {
		if o.exists(t, in) {
			t.Fatalf("input %s survived", in)
		}
	}
	o.assertOnlyOwnInputsDeleted(t)
}

// A delete that failed without deleting anything is completed by the retry.
func TestJobRun_InputDeleteRetry_UncommittedDeleteCompletedByRetry(t *testing.T) {
	o := runDeleteRetryJob(t, 2, "fail", "ok")
	if n := len(o.backend.deleteCalls()); n != 2 {
		t.Fatalf("DeleteBatch calls = %d; want 2", n)
	}
	if !o.job.SourcesDeleted || len(o.manifests) != 0 {
		t.Fatalf("SourcesDeleted = %v, manifests = %v; want true and none", o.job.SourcesDeleted, o.manifests)
	}
	for _, in := range o.rig.inputs {
		if o.exists(t, in) {
			t.Fatalf("input %s survived", in)
		}
	}
	o.assertOnlyOwnInputsDeleted(t)
}

// Retries are bounded; when they run out the job falls back to today's
// handling: manifest kept, recovery finishes it.
func TestJobRun_InputDeleteRetry_ExhaustedKeepsManifest(t *testing.T) {
	o := runDeleteRetryJob(t, 2, "fail")
	if n := len(o.backend.deleteCalls()); n != 3 {
		t.Fatalf("DeleteBatch calls = %d; want 1 + 2 retries", n)
	}
	if o.job.SourcesDeleted || len(o.manifests) != 1 {
		t.Fatalf("SourcesDeleted = %v, manifests = %v; want false and the manifest kept", o.job.SourcesDeleted, o.manifests)
	}
	o.assertOnlyOwnInputsDeleted(t)
}

// No retry once the job's context is done: a cancelled job must stop issuing
// storage mutations.
func TestDeleteOldFiles_NoRetryAfterCancel(t *testing.T) {
	shortInputDeleteBackoff(t)
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backend := &deleteFaultBackend{Backend: local, modes: []string{"fail"}, onCall: cancel}
	job := &Job{
		StorageBackend:     backend,
		InputDeleteRetries: 5,
		compactedFiles:     []string{"db/cpu/2026/11/10/04/a.parquet"},
		logger:             zerolog.Nop(),
	}
	if err := job.deleteOldFiles(ctx); err == nil {
		t.Fatal("deleteOldFiles succeeded although every delete failed")
	}
	if n := len(backend.deleteCalls()); n != 1 {
		t.Fatalf("DeleteBatch calls = %d after cancellation; want 1", n)
	}
}

// compaction.input_delete_retries reaches the job: Manager -> subprocess
// config (JSON, the parent/child boundary) -> JobConfig -> Job.
func TestInputDeleteRetries_Plumbing(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(&ManagerConfig{StorageBackend: local, TempDirectory: t.TempDir(), InputDeleteRetries: 2, Logger: zerolog.Nop()})
	if m.InputDeleteRetries != 2 {
		t.Fatalf("Manager.InputDeleteRetries = %d; want 2", m.InputDeleteRetries)
	}
	raw, err := json.Marshal(&SubprocessJobConfig{InputDeleteRetries: 2})
	if err != nil {
		t.Fatal(err)
	}
	var back SubprocessJobConfig
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.InputDeleteRetries != 2 {
		t.Fatalf("SubprocessJobConfig JSON round trip: %d; want 2 (%s)", back.InputDeleteRetries, raw)
	}
	job := NewJob(jobConfigFromSubprocess(&back, local, nil, nil, zerolog.Nop()))
	if job.InputDeleteRetries != 2 {
		t.Fatalf("Job.InputDeleteRetries = %d; want 2", job.InputDeleteRetries)
	}
	// An older parent sends no field: the child keeps today's behaviour.
	var legacy SubprocessJobConfig
	if err := json.Unmarshal([]byte(`{"database":"db"}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if NewJob(jobConfigFromSubprocess(&legacy, local, nil, nil, zerolog.Nop())).InputDeleteRetries != 0 {
		t.Fatal("missing input_delete_retries must mean 0")
	}
}
