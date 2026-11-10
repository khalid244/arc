package compaction

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// A job that uploaded its output and deleted its inputs but never deleted its
// manifest (a failed or interrupted DeleteManifest, a failed receipt mark)
// leaves a manifest with output_uploaded=true whose inputs are gone. Query pods
// re-read it every 10 s and add its inputs to every raw query's NOT IN list;
// the candidate scanner holds its output back from compaction.

type finishFixture struct {
	ctx     context.Context
	backend *storage.LocalBackend
	mm      *ManifestManager
}

func newFinishFixture(t *testing.T) *finishFixture {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return &finishFixture{ctx: context.Background(), backend: b, mm: NewManifestManager(b, zerolog.Nop())}
}

// manifest writes a manifest for job id in db/cpu/2026/11/10/04. output and
// inputs say which of its objects exist; the output body is outputBody.
func (f *finishFixture) manifest(t *testing.T, id string, uploaded bool, age time.Duration, outputBody string, output bool, inputs ...bool) (path string, m *Manifest) {
	t.Helper()
	part := "db/cpu/2026/11/10/04"
	m = &Manifest{
		OutputPath:     part + "/" + id + "_compacted.parquet",
		OutputSize:     int64(len(outputBody)),
		OutputUploaded: uploaded,
		Database:       "db",
		Measurement:    "cpu",
		PartitionPath:  part,
		Tier:           "hourly",
		Status:         ManifestStatusPending,
		CreatedAt:      time.Now().UTC().Add(-age),
		JobID:          id,
	}
	if output {
		f.put(t, m.OutputPath, outputBody)
	}
	for i, exists := range inputs {
		in := part + "/" + id + "-in" + string(rune('a'+i)) + ".parquet"
		m.InputFiles = append(m.InputFiles, in)
		if exists {
			f.put(t, in, "raw")
		}
	}
	path, err := f.mm.WriteManifest(f.ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	return path, m
}

func (f *finishFixture) put(t *testing.T, key, body string) {
	t.Helper()
	if err := f.backend.Write(f.ctx, key, []byte(body)); err != nil {
		t.Fatal(err)
	}
}

func (f *finishFixture) exists(t *testing.T, key string) bool {
	t.Helper()
	ok, err := f.backend.Exists(f.ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

const oldManifest = 2 * ManifestMinRecoveryAge

func TestFinishCompletedManifests_DeletesFinishedManifestKeepsOutput(t *testing.T) {
	f := newFinishFixture(t)
	path, m := f.manifest(t, "done", true, oldManifest, "output", true, false, false, false)

	var kept []string
	var consumed [][]string
	n, err := f.mm.finishCompletedManifests(f.ctx, recoveryScope{},
		func(k string) { kept = append(kept, k) },
		func(in []string) error { consumed = append(consumed, in); return nil },
		recoveryHooks{})
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if n != 1 {
		t.Fatalf("finished = %d, want 1", n)
	}
	if f.exists(t, path) {
		t.Fatal("finished manifest was not deleted")
	}
	if !f.exists(t, m.OutputPath) {
		t.Fatal("output deleted: finishing a manifest must never touch its output")
	}
	if len(kept) != 1 || kept[0] != m.OutputPath {
		t.Fatalf("onKeptOutput = %v, want [%s]", kept, m.OutputPath)
	}
	if len(consumed) != 1 || len(consumed[0]) != 3 {
		t.Fatalf("onConsumedInputs = %v, want the 3 inputs once", consumed)
	}
	files, err := f.mm.GetInputFilesForUploadedOutputs(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("query exclusion still lists %d inputs of the finished manifest", len(files))
	}
}

func TestFinishCompletedManifests_LeavesEveryOtherManifestAlone(t *testing.T) {
	cases := []struct {
		name     string
		uploaded bool
		age      time.Duration
		output   bool
		inputs   []bool
	}{
		{"output not marked uploaded", false, oldManifest, true, []bool{false, false}},
		{"an input still exists", true, oldManifest, true, []bool{false, true}},
		{"every input still exists", true, oldManifest, true, []bool{true, true}},
		{"output missing", true, oldManifest, false, []bool{false, false}},
		{"younger than the recovery min age", true, ManifestMinRecoveryAge / 2, true, []bool{false, false}},
		{"no inputs recorded", true, oldManifest, true, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFinishFixture(t)
			path, m := f.manifest(t, "j", tc.uploaded, tc.age, "output", tc.output, tc.inputs...)
			n, err := f.mm.finishCompletedManifests(f.ctx, recoveryScope{}, nil, nil, recoveryHooks{})
			if err != nil {
				t.Fatalf("finish: %v", err)
			}
			if n != 0 {
				t.Fatalf("finished = %d, want 0", n)
			}
			if !f.exists(t, path) {
				t.Fatal("manifest deleted")
			}
			if f.exists(t, m.OutputPath) != tc.output {
				t.Fatal("output changed")
			}
			for i, in := range m.InputFiles {
				if f.exists(t, in) != tc.inputs[i] {
					t.Fatalf("input %s changed", in)
				}
			}
		})
	}
}

func TestFinishCompletedManifests_ScopeAndMarkFailure(t *testing.T) {
	f := newFinishFixture(t)
	path, _ := f.manifest(t, "j", true, oldManifest, "output", true, false)

	n, err := f.mm.finishCompletedManifests(f.ctx, recoveryScope{Databases: []string{"other"}}, nil, nil, recoveryHooks{})
	if err != nil || n != 0 || !f.exists(t, path) {
		t.Fatalf("out-of-scope manifest: finished=%d err=%v exists=%v, want 0, nil, true", n, err, f.exists(t, path))
	}

	markErr := errors.New("receipt store down")
	n, err = f.mm.finishCompletedManifests(f.ctx, recoveryScope{}, nil, func([]string) error { return markErr }, recoveryHooks{})
	if !errors.Is(err, markErr) || n != 0 {
		t.Fatalf("mark failure: finished=%d err=%v, want 0 and the mark error", n, err)
	}
	if !f.exists(t, path) {
		t.Fatal("manifest deleted although the consumed-inputs mark failed: recovery could not re-fire it")
	}

	var order []string
	hooks := recoveryHooks{
		beforeInputDelete: func(context.Context, *Manifest) error { order = append(order, "before"); return nil },
		afterInputDelete:  func(context.Context, *Manifest) error { order = append(order, "after"); return nil },
	}
	n, err = f.mm.finishCompletedManifests(f.ctx, recoveryScope{Databases: []string{"db"}, Measurement: "cpu"}, nil,
		func([]string) error { order = append(order, "consumed"); return nil }, hooks)
	if err != nil || n != 1 {
		t.Fatalf("finish: finished=%d err=%v", n, err)
	}
	if got := len(order); got != 3 || order[0] != "before" || order[1] != "after" || order[2] != "consumed" {
		t.Fatalf("hook order = %v, want [before after consumed] as on recovery's kept-output branch", order)
	}
	if f.exists(t, path) {
		t.Fatal("manifest not deleted")
	}
}

// existsFailBackend fails Exists for one key, like an S3 HEAD that exhausts its
// retries.
type existsFailBackend struct {
	storage.Backend
	failKey string
	calls   atomic.Int32
}

func (b *existsFailBackend) Exists(ctx context.Context, key string) (bool, error) {
	b.calls.Add(1)
	if key == b.failKey {
		return false, errors.New("HeadObject: retries exhausted")
	}
	return b.Backend.Exists(ctx, key)
}

func TestFinishCompletedManifests_InputCheckErrorKeepsManifest(t *testing.T) {
	f := newFinishFixture(t)
	path, m := f.manifest(t, "j", true, oldManifest, "output", true, false, false)
	fb := &existsFailBackend{Backend: f.backend, failKey: m.InputFiles[1]}
	mm := NewManifestManager(fb, zerolog.Nop())

	n, err := mm.finishCompletedManifests(f.ctx, recoveryScope{}, nil, nil, recoveryHooks{})
	if err == nil || n != 0 {
		t.Fatalf("finished=%d err=%v, want 0 and the Exists error", n, err)
	}
	if !f.exists(t, path) {
		t.Fatal("manifest deleted although one input could not be checked")
	}
}

// Recovery, too, finishes such a manifest without the partial-upload branch:
// with the inputs gone the output holds the only copy of their rows, so a size
// that disagrees with the manifest must not get the output deleted.
func TestRecoverOrphanedManifests_FinishedManifestNeverDiscardsOutput(t *testing.T) {
	f := newFinishFixture(t)
	path, m := f.manifest(t, "j", true, oldManifest, "output", true, false, false)
	f.put(t, m.OutputPath, "output rewritten to another size")

	n, err := f.mm.RecoverOrphanedManifests(f.ctx, nil, nil)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if n != 1 {
		t.Fatalf("recovered = %d, want 1", n)
	}
	if !f.exists(t, m.OutputPath) {
		t.Fatal("recovery deleted the output of a manifest whose inputs are gone: those rows are lost")
	}
	if f.exists(t, path) {
		t.Fatal("finished manifest not deleted")
	}
}

// A size mismatch while the inputs still exist is still a partial upload:
// recovery keeps discarding it so the job reruns from the inputs.
func TestRecoverOrphanedManifests_PartialOutputWithInputsStillDiscarded(t *testing.T) {
	f := newFinishFixture(t)
	path, m := f.manifest(t, "j", true, oldManifest, "output", true, true, false)
	f.put(t, m.OutputPath, "out")

	if _, err := f.mm.RecoverOrphanedManifests(f.ctx, nil, nil); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if f.exists(t, m.OutputPath) || f.exists(t, path) {
		t.Fatal("partial output with live inputs: want output and manifest discarded for a rerun")
	}
	if !f.exists(t, m.InputFiles[0]) {
		t.Fatal("input deleted next to a partial output")
	}
}

// The root cause of the stuck manifests: a daily cycle runs for hours, hourly
// ticks run as interleaved passes inside it, and those skipped recovery
// entirely, so a manifest left behind after the daily cycle's own recovery
// stayed until the daily cycle ended. The interleaved pass now finishes the
// ones whose output is uploaded and whose inputs are gone, and still leaves
// every other manifest to the next full recovery.
func TestInterleave_FinishesCompletedManifests(t *testing.T) {
	d1 := ilCandidate("daily", "db/cpu/2026/11/01", 10)
	d2 := ilCandidate("daily", "db/cpu/2026/11/02", 10)
	h := ilCandidate("hourly", "db/cpu/2026/11/08/03", 10)
	rig := newILRig(t, 1, true, []Candidate{d1, d2}, []Candidate{h})
	mm := NewManifestManager(rig.m.StorageBackend, rig.m.logger)
	rig.m.ManifestManager = mm
	ctx := context.Background()
	exists := func(path string) bool {
		ok, err := rig.m.StorageBackend.Exists(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	write := func(id string, uploaded, inputExists bool) (manifestPath string, m *Manifest) {
		t.Helper()
		part := "db/cpu/2026/11/08/04"
		m = &Manifest{
			Database: "db", Measurement: "cpu", Tier: "hourly", JobID: id,
			PartitionPath: part, OutputPath: part + "/" + id + "_compacted.parquet",
			OutputSize: 3, OutputUploaded: uploaded,
			InputFiles: []string{part + "/" + id + "-in.parquet"},
			CreatedAt:  time.Now().Add(-oldManifest),
			Status:     ManifestStatusPending,
		}
		if err := rig.m.StorageBackend.Write(ctx, m.OutputPath, []byte("out")); err != nil {
			t.Fatal(err)
		}
		if inputExists {
			if err := rig.m.StorageBackend.Write(ctx, m.InputFiles[0], []byte("in")); err != nil {
				t.Fatal(err)
			}
		}
		p, err := mm.WriteManifest(ctx, m)
		if err != nil {
			t.Fatal(err)
		}
		return p, m
	}

	var mu sync.Mutex
	var finishedSeen []string
	var finished, live, notUploaded string
	rig.onStart = func(b Candidate) {
		if b.Tier != "hourly" {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		finishedSeen = append(finishedSeen, map[bool]string{true: "present", false: "gone"}[exists(finished)])
	}
	started, release := rig.hold("db/cpu/2026/11/01#1")
	done := rig.startCycle(ctx, "daily")
	waitClosed(t, started, "daily 01")

	mu.Lock()
	var out *Manifest
	finished, out = write("finished", true, false)
	live, _ = write("live", true, true)
	notUploaded, _ = write("not-uploaded", false, false)
	mu.Unlock()
	rig.requestHourly()
	release()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}

	if n := len(rig.recordsBySource(cycleSourceInterleaved)); n != 1 {
		t.Fatalf("interleaved records = %d, want 1", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(finishedSeen) != 1 || finishedSeen[0] != "gone" {
		t.Fatalf("finished manifest when the interleaved hourly batch ran: %v, want [gone]", finishedSeen)
	}
	if exists(finished) {
		t.Fatal("interleaved pass left a manifest whose output is uploaded and whose inputs are gone")
	}
	if !exists(out.OutputPath) {
		t.Fatal("output of the finished manifest deleted")
	}
	if !exists(live) {
		t.Fatal("manifest with a live input deleted mid-cycle")
	}
	if !exists(notUploaded) {
		t.Fatal("manifest whose output was never marked uploaded deleted mid-cycle: that is full recovery's call")
	}
}
