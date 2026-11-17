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
	fastKeys map[string]bool // answered without the delay
	onCall   func(n int32)   // called with the running call count, before the call

	mu       sync.Mutex
	inFlight int
	peak     int
	calls    map[string]int
	total    atomic.Int32
}

func newHeadBackend(b storage.Backend, delay time.Duration) *headBackend {
	return &headBackend{Backend: b, delay: delay, failKeys: map[string]bool{}, fastKeys: map[string]bool{}, calls: map[string]int{}}
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
	if b.delay > 0 && !b.failKeys[key] && !b.fastKeys[key] {
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

func (b *headBackend) inputCalls(inputs []string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, in := range inputs {
		n += b.calls[in]
	}
	return n
}

// The first present input or failed check stops the dispatch of the rest:
// a 200-input manifest with a live first input costs a handful of HEADs, not
// 200. The deciding key answers at once; the others take a while and give up
// when the check is cancelled. At most limit HEADs can be in flight when the
// answer arrives, so the count is bounded by idx + limit; 2*limit of slack
// absorbs scheduler jitter and is still far below the 64 a check that does
// not stop would issue.
func TestManifestInputCheckConcurrency_StopsEarly(t *testing.T) {
	const limit, inputs = 4, 64
	for _, tc := range []struct {
		name string
		idx  int
		fail bool
	}{
		{"live input first", 0, false},
		{"live input in the middle", 20, false},
		{"failed check first", 0, true},
		{"failed check in the middle", 20, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFinishFixture(t)
			var present []int
			if !tc.fail {
				present = []int{tc.idx}
			}
			_, m := f.wideManifest(t, "j", inputs, present...)
			hb := newHeadBackend(f.backend, 10*time.Millisecond)
			key := m.InputFiles[tc.idx]
			if tc.fail {
				hb.failKeys[key] = true
			} else {
				hb.fastKeys[key] = true
			}
			mm := NewManifestManager(hb, zerolog.Nop())
			mm.SetInputCheckConcurrency(limit)

			completed, err := mm.jobCompleted(f.ctx, m)
			if completed {
				t.Fatal("jobCompleted = true")
			}
			if tc.fail != (err != nil) {
				t.Fatalf("err = %v; want an error only for the failed check", err)
			}
			got := hb.inputCalls(m.InputFiles)
			if max := tc.idx + 2*limit; got > max {
				t.Fatalf("%d input HEADs; want <= %d: the check did not stop at the first answer", got, max)
			}
			if hb.callsFor(key) != 1 {
				t.Fatalf("deciding input checked %d times; want 1", hb.callsFor(key))
			}
		})
	}
}

// A caller whose context is already done gets its error and no HEADs.
func TestInputsAllGone_CancelledBeforeDispatch(t *testing.T) {
	f := newFinishFixture(t)
	_, m := f.wideManifest(t, "j", 16)
	hb := newHeadBackend(f.backend, 0)
	mm := NewManifestManager(hb, zerolog.Nop())
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()

	completed, err := mm.inputsAllGone(ctx, m.InputFiles, 4)
	if completed || !errors.Is(err, context.Canceled) {
		t.Fatalf("inputsAllGone = %v, %v; want false, context.Canceled", completed, err)
	}
	if n := hb.inputCalls(m.InputFiles); n != 0 {
		t.Fatalf("%d HEADs after cancellation; want 0", n)
	}
}

// The final guards, directly: only "every input answered not-found" is true.
func TestInputCheckVerdict(t *testing.T) {
	headErr := errors.New("HeadObject 503")
	for _, tc := range []struct {
		name     string
		present  bool
		firstErr error
		ctxErr   error
		gone     int
		want     bool
		wantErr  error
	}{
		{"all gone", false, nil, nil, 5, true, nil},
		{"present wins over error", true, headErr, nil, 3, false, nil},
		{"present wins over cancellation", true, nil, context.Canceled, 3, false, nil},
		{"check error", false, headErr, nil, 4, false, headErr},
		{"check error wins over cancellation", false, headErr, context.Canceled, 4, false, headErr},
		{"cancelled, nothing failed", false, nil, context.Canceled, 2, false, context.Canceled},
		{"cancelled with every input gone", false, nil, context.DeadlineExceeded, 5, false, context.DeadlineExceeded},
		{"partial check", false, nil, nil, 4, false, nil},
		{"no input checked", false, nil, nil, 0, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := inputCheckVerdict(tc.present, tc.firstErr, tc.ctxErr, tc.gone, 5)
			if got != tc.want {
				t.Fatalf("verdict = %v; want %v", got, tc.want)
			}
			switch {
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("err = %v; want %v", err, tc.wantErr)
			case tc.wantErr == nil && !tc.want && !tc.present && err == nil:
				t.Fatal("a partial check returned no error")
			case tc.present && err != nil:
				t.Fatalf("err = %v; a present input is an answer, not an error", err)
			}
		})
	}
}
