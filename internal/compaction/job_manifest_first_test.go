//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

// uploadFaultBackend wraps a LocalBackend (as a plain storage.Backend, so the
// job streams the output through WriteReader instead of adopting the file) and
// injects faults on the compacted output's key only.
type uploadFaultBackend struct {
	storage.Backend
	t *testing.T

	mu sync.Mutex
	// commitThenFail stores the object and then reports an error — the
	// S3 PUT that timed out after the server committed it.
	commitThenFail bool
	// failUpload reports an error without storing anything.
	failUpload bool
	// failOutputDelete makes deleting the output key fail.
	failOutputDelete bool
	// outputDeleteErr, when set, is the error a failing output delete returns.
	outputDeleteErr error
	// manifestAtUpload records the manifests visible when the output upload
	// started (path -> decoded body).
	manifestAtUpload map[string]Manifest
}

func isCompactedOutputKey(key string) bool {
	return strings.HasSuffix(key, "_compacted.parquet")
}

func (b *uploadFaultBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	if !isCompactedOutputKey(path) {
		return b.Backend.WriteReader(ctx, path, reader, size)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.manifestAtUpload = map[string]Manifest{}
	keys, err := b.Backend.List(ctx, ManifestBasePath+"/")
	if err != nil {
		b.t.Fatalf("list manifests at upload: %v", err)
	}
	for _, k := range keys {
		raw, err := b.Backend.Read(ctx, k)
		if err != nil {
			b.t.Fatalf("read manifest at upload: %v", err)
		}
		var m Manifest
		if err := json.Unmarshal(raw, &m); err != nil {
			b.t.Fatalf("decode manifest at upload: %v", err)
		}
		b.manifestAtUpload[k] = m
	}
	switch {
	case b.failUpload:
		return errors.New("injected: upload failed")
	case b.commitThenFail:
		if err := b.Backend.WriteReader(ctx, path, reader, size); err != nil {
			return err
		}
		return errors.New("injected: upload timed out after the object was committed")
	}
	return b.Backend.WriteReader(ctx, path, reader, size)
}

func (b *uploadFaultBackend) Delete(ctx context.Context, path string) error {
	b.mu.Lock()
	fail := b.failOutputDelete && isCompactedOutputKey(path)
	failErr := b.outputDeleteErr
	b.mu.Unlock()
	if fail {
		if failErr != nil {
			return failErr
		}
		return errors.New("injected: output delete failed")
	}
	return b.Backend.Delete(ctx, path)
}

func (b *uploadFaultBackend) DeleteBatch(ctx context.Context, paths []string) error {
	for _, p := range paths {
		if err := b.Delete(ctx, p); err != nil {
			return err
		}
	}
	return nil
}

func (b *uploadFaultBackend) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	return b.Backend.(storage.ObjectLister).ListObjects(ctx, prefix)
}

type manifestFirstRig struct {
	db      *sql.DB
	local   *storage.LocalBackend
	backend *uploadFaultBackend
	mm      *ManifestManager
	inputs  []string
}

const manifestFirstPartition = "testdb/cpu/2026/04/11/14"

func newManifestFirstRig(t *testing.T) *manifestFirstRig {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { local.Close() })
	rig := &manifestFirstRig{db: db, local: local, backend: &uploadFaultBackend{Backend: local, t: t}}
	rig.mm = NewManifestManager(rig.backend, zerolog.Nop())
	for i := 0; i < 3; i++ {
		fixture := filepath.Join(t.TempDir(), "in.parquet")
		writeFixtureParquet(t, ctx, db, fixture, fmt.Sprintf(
			"TIMESTAMPTZ '2026-04-11 14:00:0%dZ' AS time, 'h%d' AS host, 1.0 AS value", i, i))
		data, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("%s/in%d.parquet", manifestFirstPartition, i)
		if err := local.Write(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		rig.inputs = append(rig.inputs, key)
	}
	return rig
}

func (r *manifestFirstRig) run(t *testing.T, jobID string) (*Job, error) {
	t.Helper()
	job := NewJob(&JobConfig{
		Measurement: "cpu", PartitionPath: manifestFirstPartition, Files: r.inputs,
		StorageBackend: r.backend, Database: "testdb", Tier: "hourly",
		TempDirectory: t.TempDir(), Logger: zerolog.Nop(), DB: r.db,
		ManifestManager: r.mm, JobID: jobID,
	})
	return job, job.Run(context.Background())
}

func (r *manifestFirstRig) assertInputsPresent(t *testing.T) {
	t.Helper()
	for _, k := range r.inputs {
		if ok, _ := r.local.Exists(context.Background(), k); !ok {
			t.Fatalf("input %s deleted although the job did not commit", k)
		}
	}
}

// The manifest is written BEFORE the output upload starts, with
// OutputUploaded=false (so query pods keep showing the sources), and is gone
// after a successful job.
func TestJobRun_ManifestWrittenBeforeUploadNotYetUploaded(t *testing.T) {
	rig := newManifestFirstRig(t)
	job, err := rig.run(t, "manifest-first-ok")
	if err != nil {
		t.Fatalf("Job.Run: %v", err)
	}
	if len(rig.backend.manifestAtUpload) != 1 {
		t.Fatalf("manifests visible when the upload started = %d, want 1 (manifest-first)", len(rig.backend.manifestAtUpload))
	}
	for _, m := range rig.backend.manifestAtUpload {
		if m.OutputUploaded {
			t.Fatal("manifest claimed OutputUploaded before the upload ran: query pods would hide the sources while the output is absent")
		}
		if m.OutputPath != job.OutputStorageKey || len(m.InputFiles) != len(rig.inputs) {
			t.Fatalf("manifest does not describe this job: %+v", m)
		}
	}
	if left, _ := rig.mm.ListManifests(context.Background()); len(left) != 0 {
		t.Fatalf("manifest left after a successful job: %v", left)
	}
	if !job.SourcesDeleted {
		t.Fatal("sources not reported deleted")
	}
}

// An upload that fails cleanly leaves storage exactly as before the job:
// inputs intact, no output, no manifest — so the adaptive splitter may retry.
func TestJobRun_FailedUploadUnwindsManifest(t *testing.T) {
	rig := newManifestFirstRig(t)
	rig.backend.failUpload = true
	job, err := rig.run(t, "manifest-first-fail")
	if err == nil {
		t.Fatal("Job.Run succeeded despite the failed upload")
	}
	if strings.Contains(err.Error(), outputUploadUnresolvedMarker) {
		t.Fatalf("resolved failure reported as unresolved: %v", err)
	}
	if recoverable, _ := ClassifySubprocessError(err, ""); !recoverable {
		t.Fatalf("a cleanly failed upload must stay retryable: %v", err)
	}
	if left, _ := rig.mm.ListManifests(context.Background()); len(left) != 0 {
		t.Fatalf("manifest left after the upload failure was unwound: %v", left)
	}
	if ok, _ := rig.local.Exists(context.Background(), job.OutputStorageKey); ok {
		t.Fatal("output present after a failed upload")
	}
	rig.assertInputsPresent(t)
}

// A PUT that committed but reported an error (a timeout after the server
// stored the object) is removed before the manifest is dropped, so a retry
// cannot add a second copy of the rows next to it.
func TestJobRun_AmbiguousUploadRemovesCommittedOutput(t *testing.T) {
	rig := newManifestFirstRig(t)
	rig.backend.commitThenFail = true
	job, err := rig.run(t, "manifest-first-ambiguous")
	if err == nil {
		t.Fatal("Job.Run succeeded despite the upload error")
	}
	if ok, _ := rig.local.Exists(context.Background(), job.OutputStorageKey); ok {
		t.Fatal("committed output left in the partition next to its inputs: a retry would duplicate every row")
	}
	if left, _ := rig.mm.ListManifests(context.Background()); len(left) != 0 {
		t.Fatalf("manifest left after the output was removed: %v", left)
	}
	rig.assertInputsPresent(t)
}

// When the possibly-committed output cannot be removed either, the manifest is
// KEPT and the error is non-recoverable, so the adaptive splitter does not
// re-compact the same inputs into more outputs. Recovery then resolves it from
// storage: the full-size output is kept and the inputs are deleted — one copy
// of every row.
func TestJobRun_UnresolvedUploadKeepsManifestForRecovery(t *testing.T) {
	rig := newManifestFirstRig(t)
	rig.backend.commitThenFail = true
	rig.backend.failOutputDelete = true
	job, err := rig.run(t, "manifest-first-unresolved")
	if err == nil {
		t.Fatal("Job.Run succeeded despite the upload error")
	}
	if !strings.Contains(err.Error(), outputUploadUnresolvedMarker) {
		t.Fatalf("error does not carry the unresolved marker: %v", err)
	}
	if recoverable, reason := ClassifySubprocessError(err, ""); recoverable || reason != "upload_outcome_unresolved" {
		t.Fatalf("ClassifySubprocessError = (%v, %q), want (false, upload_outcome_unresolved)", recoverable, reason)
	}
	ctx := context.Background()
	left, _ := rig.mm.ListManifests(ctx)
	if len(left) != 1 {
		t.Fatalf("manifest not kept for recovery: %v", left)
	}
	rig.assertInputsPresent(t)

	// The inputs stay excluded from compaction while the manifest exists.
	held, err := rig.mm.GetFilesInManifests(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range rig.inputs {
		if _, ok := held[k]; !ok {
			t.Fatalf("input %s not held back by the retained manifest", k)
		}
	}

	// Age the manifest past ManifestMinRecoveryAge and recover.
	m, err := rig.mm.ReadManifest(ctx, left[0])
	if err != nil {
		t.Fatal(err)
	}
	m.CreatedAt = time.Now().UTC().Add(-2 * ManifestMinRecoveryAge)
	raw, _ := json.Marshal(m)
	if err := rig.local.Write(ctx, left[0], raw); err != nil {
		t.Fatal(err)
	}
	rig.backend.failOutputDelete = false
	if recovered, err := rig.mm.RecoverOrphanedManifests(ctx, nil, nil); err != nil || recovered != 1 {
		t.Fatalf("recovery: recovered=%d err=%v", recovered, err)
	}
	if ok, _ := rig.local.Exists(ctx, job.OutputStorageKey); !ok {
		t.Fatal("recovery dropped the full-size committed output")
	}
	for _, k := range rig.inputs {
		if ok, _ := rig.local.Exists(ctx, k); ok {
			t.Fatalf("input %s still present next to the kept output: duplicate rows", k)
		}
	}
}
