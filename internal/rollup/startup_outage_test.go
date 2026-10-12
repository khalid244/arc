package rollup

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// outageStorage behaves like an object store that is black-holed: every call
// blocks until its context ends. Once healed it serves from fakeStorage.
type outageStorage struct {
	*fakeStorage
	healed atomic.Bool
}

func (o *outageStorage) wait(ctx context.Context) error {
	if o.healed.Load() {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (o *outageStorage) Read(ctx context.Context, p string) ([]byte, error) {
	if err := o.wait(ctx); err != nil {
		return nil, err
	}
	return o.fakeStorage.Read(ctx, p)
}

func (o *outageStorage) Write(ctx context.Context, p string, d []byte) error {
	if err := o.wait(ctx); err != nil {
		return err
	}
	return o.fakeStorage.Write(ctx, p, d)
}

func (o *outageStorage) StatFile(ctx context.Context, p string) (int64, error) {
	if err := o.wait(ctx); err != nil {
		return 0, err
	}
	return o.fakeStorage.StatFile(ctx, p)
}

func (o *outageStorage) ListDirectories(ctx context.Context, p string) ([]string, error) {
	if err := o.wait(ctx); err != nil {
		return nil, err
	}
	return o.fakeStorage.ListDirectories(ctx, p)
}

// A pod restarted while object storage hangs must still finish NewManager
// (which runs before the HTTP server listens) within the startup budget.
func TestNewManager_DoesNotBlockOnHungStorage(t *testing.T) {
	old := initialLoadTimeout
	initialLoadTimeout = 200 * time.Millisecond
	defer func() { initialLoadTimeout = old }()

	stg := &outageStorage{fakeStorage: newFakeStorage()}
	start := time.Now()
	m, err := NewManager(Config{}, S3Params{Bucket: "arc-test"}, stg, zerolog.New(io.Discard))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer m.Close()
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("NewManager blocked %v on hung storage; startup must be bounded", took)
	}
	if !m.workloadResumePending {
		t.Fatal("a resume that never got an answer must stay pending")
	}
}

// A route-only pod whose startup resume failed must not overwrite its saved
// workload with the partial in-memory counts; once storage answers, the saved
// counts are merged in first and then persisted together with new recordings.
func TestPodWorkload_PersistWaitsForResume(t *testing.T) {
	old := initialLoadTimeout
	initialLoadTimeout = 100 * time.Millisecond
	defer func() { initialLoadTimeout = old }()

	stg := &outageStorage{fakeStorage: newFakeStorage()}
	m := testManager(stg)
	m.instanceID = "pod-a"
	saved := []byte(`{"dims":{"db.events":{"country":7}}}`)
	stg.objs[m.podWorkloadKey()] = saved

	ctx, cancel := context.WithTimeout(context.Background(), initialLoadTimeout)
	m.workloadResumePending = !m.resumeWorkload(ctx)
	cancel()
	if !m.workloadResumePending {
		t.Fatal("resume during the outage must be pending")
	}
	m.workload.Record(QueryShape{Source: "db.events", Grain: "hour", Dims: []string{"country"}})

	m.persistPodWorkloadIfDirty(context.Background())
	if got := string(stg.objs[m.podWorkloadKey()]); got != string(saved) {
		t.Fatalf("persist during the outage overwrote the saved workload: %s", got)
	}

	stg.healed.Store(true)
	m.persistPodWorkloadIfDirty(context.Background())
	if m.workloadResumePending {
		t.Fatal("resume must complete once storage answers")
	}
	w := NewWorkload()
	if err := w.LoadBytes(stg.objs[m.podWorkloadKey()]); err != nil {
		t.Fatalf("persisted workload unreadable: %v", err)
	}
	b, _ := w.Bytes()
	if !containsCount(string(b), "country", 8) {
		t.Fatalf("persisted workload lost the saved counts: %s", b)
	}
}

func containsCount(doc, dim string, atLeast int) bool {
	w := NewWorkload()
	if err := w.LoadBytes([]byte(doc)); err != nil {
		return false
	}
	for _, dm := range w.seed {
		if dm[dim] >= atLeast {
			return true
		}
	}
	return false
}

// A builder that could not load its saved workload skips the tick instead of
// planning cubes from partial counts.
func TestTick_SkipsWhileWorkloadResumePending(t *testing.T) {
	old := initialLoadTimeout
	initialLoadTimeout = 50 * time.Millisecond
	defer func() { initialLoadTimeout = old }()

	stg := &outageStorage{fakeStorage: newFakeStorage()}
	m := testManager(stg)
	m.cfg.Builder = true
	m.workloadResumePending = true
	m.tick(context.Background())
	if _, ok := stg.objs[m.workloadKey()]; ok {
		t.Fatal("tick persisted a workload while the saved one was never loaded")
	}
}
