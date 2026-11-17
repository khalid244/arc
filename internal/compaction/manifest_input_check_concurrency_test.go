package compaction

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Production evidence (2026-11-15..17): every manifest start-of-cycle recovery
// processed had output_uploaded=true and inputs that were already gone (the
// job's DeleteObjects committed but its response body was cut, so the job kept
// its manifest). jobCompleted proves "inputs gone" with one HEAD per input, one
// at a time: 150-200 HEADs per hourly manifest at 0.2-9 s each on Hetzner,
// 2.7 h for 17 manifests. compaction.manifest_recovery_input_check_concurrency
// runs those HEADs concurrently; 1 (the default) keeps them serial.

// headBackend wraps a backend and instruments Exists: it records the peak
// number of concurrent calls, counts calls per key, can fail or report present
// chosen keys, and adds a small latency so concurrent calls overlap.
type headBackend struct {
	storage.Backend
	delay    time.Duration
	failKeys map[string]bool
	onCall   func(n int32) // called with the running call count, before the call

	mu       sync.Mutex
	inFlight int
	peak     int
	calls    map[string]int
	total    atomic.Int32
}

func newHeadBackend(b storage.Backend, delay time.Duration) *headBackend {
	return &headBackend{Backend: b, delay: delay, failKeys: map[string]bool{}, calls: map[string]int{}}
}

func (b *headBackend) Exists(ctx context.Context, key string) (bool, error) {
	if n := b.total.Add(1); b.onCall != nil {
		b.onCall(n)
	}
	b.mu.Lock()
	b.inFlight++
	if b.inFlight > b.peak {
		b.peak = b.inFlight
	}
	b.calls[key]++
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.inFlight--
		b.mu.Unlock()
	}()
	if b.delay > 0 {
		select {
		case <-time.After(b.delay):
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	if b.failKeys[key] {
		return false, errors.New("HeadObject: exceeded maximum number of attempts, 3, StatusCode: 503")
	}
	return b.Backend.Exists(ctx, key)
}

func (b *headBackend) peakInFlight() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peak
}

func (b *headBackend) callsFor(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls[key]
}

// wideManifest writes an old, uploaded manifest whose output exists, with n
// inputs; present lists the input indexes that still exist.
func (f *finishFixture) wideManifest(t *testing.T, id string, n int, present ...int) (string, *Manifest) {
	t.Helper()
	part := "db/cpu/2026/11/10/04"
	m := &Manifest{
		OutputPath:     part + "/" + id + "_compacted.parquet",
		OutputSize:     int64(len("output")),
		OutputUploaded: true,
		Database:       "db",
		Measurement:    "cpu",
		PartitionPath:  part,
		Tier:           "hourly",
		Status:         ManifestStatusPending,
		CreatedAt:      time.Now().UTC().Add(-oldManifest),
		JobID:          id,
	}
	f.put(t, m.OutputPath, "output")
	live := map[int]bool{}
	for _, i := range present {
		live[i] = true
	}
	for i := 0; i < n; i++ {
		in := fmt.Sprintf("%s/%s-in%03d.parquet", part, id, i)
		m.InputFiles = append(m.InputFiles, in)
		if live[i] {
			f.put(t, in, "raw")
		}
	}
	path, err := f.mm.WriteManifest(f.ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	return path, m
}

func TestManifestInputCheckConcurrency_DefaultIsSerial(t *testing.T) {
	f := newFinishFixture(t)
	_, m := f.wideManifest(t, "j", 24)
	hb := newHeadBackend(f.backend, time.Millisecond)
	mm := NewManifestManager(hb, zerolog.Nop())

	completed, err := mm.jobCompleted(f.ctx, m)
	if err != nil || !completed {
		t.Fatalf("jobCompleted = %v, %v; want true, nil", completed, err)
	}
	if got := hb.peakInFlight(); got != 1 {
		t.Fatalf("peak concurrent HEADs = %d with the default; want 1 (today's serial check)", got)
	}
}

func TestManifestInputCheckConcurrency_BoundedParallelAllGone(t *testing.T) {
	f := newFinishFixture(t)
	_, m := f.wideManifest(t, "j", 64)
	hb := newHeadBackend(f.backend, 5*time.Millisecond)
	mm := NewManifestManager(hb, zerolog.Nop())
	mm.SetInputCheckConcurrency(8)

	completed, err := mm.jobCompleted(f.ctx, m)
	if err != nil || !completed {
		t.Fatalf("jobCompleted = %v, %v; want true, nil", completed, err)
	}
	peak := hb.peakInFlight()
	if peak < 2 || peak > 8 {
		t.Fatalf("peak concurrent HEADs = %d; want 2..8", peak)
	}
	for _, in := range m.InputFiles {
		if n := hb.callsFor(in); n != 1 {
			t.Fatalf("input %s checked %d times; want exactly once", in, n)
		}
	}
	if n := hb.callsFor(m.OutputPath); n != 1 {
		t.Fatalf("output checked %d times; want once", n)
	}
}

func TestManifestInputCheckConcurrency_LiveInputIsNotCompleted(t *testing.T) {
	for _, idx := range []int{0, 31, 63} {
		t.Run(fmt.Sprintf("live input %d", idx), func(t *testing.T) {
			f := newFinishFixture(t)
			_, m := f.wideManifest(t, "j", 64, idx)
			mm := NewManifestManager(newHeadBackend(f.backend, time.Millisecond), zerolog.Nop())
			mm.SetInputCheckConcurrency(8)

			completed, err := mm.jobCompleted(f.ctx, m)
			if completed {
				t.Fatal("jobCompleted = true while an input still exists")
			}
			if err != nil {
				t.Fatalf("err = %v; a live input is an answer, not an error", err)
			}
		})
	}
}

func TestManifestInputCheckConcurrency_CheckErrorIsNotCompleted(t *testing.T) {
	f := newFinishFixture(t)
	_, m := f.wideManifest(t, "j", 64)
	hb := newHeadBackend(f.backend, time.Millisecond)
	hb.failKeys[m.InputFiles[40]] = true
	mm := NewManifestManager(hb, zerolog.Nop())
	mm.SetInputCheckConcurrency(8)

	completed, err := mm.jobCompleted(f.ctx, m)
	if completed || err == nil {
		t.Fatalf("jobCompleted = %v, %v; want false and the HEAD error", completed, err)
	}
}

// A live input decides "not completed" even when another input's HEAD fails.
func TestManifestInputCheckConcurrency_LiveInputAndErrorIsNotCompleted(t *testing.T) {
	f := newFinishFixture(t)
	_, m := f.wideManifest(t, "j", 64, 50)
	hb := newHeadBackend(f.backend, time.Millisecond)
	hb.failKeys[m.InputFiles[10]] = true
	mm := NewManifestManager(hb, zerolog.Nop())
	mm.SetInputCheckConcurrency(8)

	if completed, _ := mm.jobCompleted(f.ctx, m); completed {
		t.Fatal("jobCompleted = true with a live input and a failed check")
	}
}

func TestManifestInputCheckConcurrency_CancelledContextIsNotCompleted(t *testing.T) {
	f := newFinishFixture(t)
	_, m := f.wideManifest(t, "j", 64)
	hb := newHeadBackend(f.backend, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	// Call 1 is the output HEAD (a failure there is "not completed" with no
	// error, as before); cancel once input checks are under way.
	hb.onCall = func(n int32) {
		if n == 4 {
			cancel()
		}
	}
	mm := NewManifestManager(hb, zerolog.Nop())
	mm.SetInputCheckConcurrency(8)

	completed, err := mm.jobCompleted(ctx, m)
	if completed || err == nil {
		t.Fatalf("jobCompleted = %v, %v after cancellation; want false and an error", completed, err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want it to wrap context.Canceled", err)
	}
}

// End to end through both entry points: the finished manifest is finished
// without touching its output, the one with a live input is left to the
// kept-output branch (full recovery deletes its inputs) or to the next full
// recovery (the interleaved pass leaves it), exactly as with serial checks.
func TestManifestInputCheckConcurrency_RecoveryOutcomesUnchanged(t *testing.T) {
	for _, conc := range []int{1, 8} {
		t.Run(fmt.Sprintf("full recovery concurrency %d", conc), func(t *testing.T) {
			f := newFinishFixture(t)
			donePath, done := f.wideManifest(t, "done", 40)
			livePath, live := f.wideManifest(t, "live", 40, 7, 39)
			mm := NewManifestManager(newHeadBackend(f.backend, time.Millisecond), zerolog.Nop())
			mm.SetInputCheckConcurrency(conc)

			n, err := mm.RecoverOrphanedManifests(f.ctx, nil, nil)
			if err != nil || n != 2 {
				t.Fatalf("recovered = %d, err = %v; want 2, nil", n, err)
			}
			if f.exists(t, donePath) || f.exists(t, livePath) {
				t.Fatal("a recovered manifest was not deleted")
			}
			if !f.exists(t, done.OutputPath) || !f.exists(t, live.OutputPath) {
				t.Fatal("recovery deleted a valid output")
			}
			for _, in := range live.InputFiles {
				if f.exists(t, in) {
					t.Fatalf("input %s of a kept output survived recovery", in)
				}
			}
		})
		t.Run(fmt.Sprintf("interleaved finish concurrency %d", conc), func(t *testing.T) {
			f := newFinishFixture(t)
			donePath, done := f.wideManifest(t, "done", 40)
			livePath, live := f.wideManifest(t, "live", 40, 39)
			mm := NewManifestManager(newHeadBackend(f.backend, time.Millisecond), zerolog.Nop())
			mm.SetInputCheckConcurrency(conc)

			n, err := mm.finishCompletedManifests(f.ctx, recoveryScope{}, nil, nil, recoveryHooks{})
			if err != nil || n != 1 {
				t.Fatalf("finished = %d, err = %v; want 1, nil", n, err)
			}
			if f.exists(t, donePath) {
				t.Fatal("finished manifest not deleted")
			}
			if !f.exists(t, done.OutputPath) || !f.exists(t, live.OutputPath) {
				t.Fatal("finishing deleted an output")
			}
			if !f.exists(t, livePath) || !f.exists(t, live.InputFiles[39]) {
				t.Fatal("manifest with a live input was finished mid-cycle")
			}
		})
	}
}

// A failed input check keeps the manifest under concurrency too.
func TestManifestInputCheckConcurrency_FinishKeepsManifestOnCheckError(t *testing.T) {
	f := newFinishFixture(t)
	path, m := f.wideManifest(t, "j", 40)
	hb := newHeadBackend(f.backend, time.Millisecond)
	hb.failKeys[m.InputFiles[25]] = true
	mm := NewManifestManager(hb, zerolog.Nop())
	mm.SetInputCheckConcurrency(8)

	n, err := mm.finishCompletedManifests(f.ctx, recoveryScope{}, nil, nil, recoveryHooks{})
	if err == nil || n != 0 {
		t.Fatalf("finished = %d, err = %v; want 0 and the HEAD error", n, err)
	}
	if !f.exists(t, path) {
		t.Fatal("manifest deleted although one input could not be checked")
	}
}

func TestManifestInputCheckConcurrency_SetterClampsToSerial(t *testing.T) {
	mm := NewManifestManager(nil, zerolog.Nop())
	for _, n := range []int{-3, 0, 1} {
		mm.SetInputCheckConcurrency(n)
		if got := mm.inputCheckConcurrency(); got != 1 {
			t.Fatalf("SetInputCheckConcurrency(%d) -> %d; want 1", n, got)
		}
	}
	mm.SetInputCheckConcurrency(16)
	if got := mm.inputCheckConcurrency(); got != 16 {
		t.Fatalf("SetInputCheckConcurrency(16) -> %d", got)
	}
}

// The Manager hands the configured value to its ManifestManager.
func TestNewManager_WiresManifestRecoveryInputCheckConcurrency(t *testing.T) {
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ in, want int }{{0, 1}, {1, 1}, {12, 12}} {
		m := NewManager(&ManagerConfig{
			StorageBackend:                        b,
			TempDirectory:                         t.TempDir(),
			ManifestRecoveryInputCheckConcurrency: tc.in,
			Logger:                                zerolog.Nop(),
		})
		if got := m.ManifestManager.inputCheckConcurrency(); got != tc.want {
			t.Fatalf("ManifestRecoveryInputCheckConcurrency %d -> %d; want %d", tc.in, got, tc.want)
		}
	}
}
