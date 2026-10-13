package ingest

// The shutdown drain (P3-H2). Without a handoff peer whatever the final drain
// leaves staged is lost with the pod's emptyDir, so it must use its whole
// budget: a pass ends at the first availability failure, and a 1-2 s RGW blip
// at shutdown must not end the drain while there is budget left.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// blipBackend fails the first n data PUTs with a 503 (a short RGW blip that
// outlived the resilient wrapper's retries), then works. Probes always work.
type blipBackend struct {
	storage.Backend
	left atomic.Int64
}

func (b *blipBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if !strings.HasPrefix(path, "_arc/") && b.left.Add(-1) >= 0 {
		return fmt.Errorf("put %s: %w", path, statusErr{503})
	}
	return b.Backend.WriteReader(ctx, path, r, size)
}

func TestStagerFinalDrain_RetriesAfterATransientFailure(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	bb := &blipBackend{Backend: inner}
	bb.left.Store(1)
	ls, err := NewLateStager(&LateStagerConfig{Storage: bb, StageDir: filepath.Join(root, "late"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a"}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]struct{}{}
	for _, m := range []string{"m1", "m2", "m3", "m4"} {
		if err := ls.Stage("db", m, makeParquet(t, m+"-%d", 3, expected)); err != nil {
			t.Fatal(err)
		}
	}
	ls.SetCloseBudget(20 * time.Second)
	start := time.Now()
	if err := ls.Close(); err != nil {
		p, _ := ls.Pending()
		t.Fatalf("Close() after %v = %v, %d staged files left: one 503 ended the shutdown drain with budget left", time.Since(start).Round(time.Millisecond), err, p)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Close took %v for one transient failure", took)
	}
	rows, distinct, _ := countRowsUnder(t, objRoot)
	if rows != int64(len(expected)) || distinct != int64(len(expected)) {
		t.Fatalf("rows=%d distinct=%d, want %d", rows, distinct, len(expected))
	}
}

// The store is marked unhealthy at shutdown and comes back a little later: the
// first probes fail, a later one succeeds, and the drain still runs.
func TestStagerFinalDrain_ProbesAgainUntilTheStoreIsBack(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	ob := newOutageBackend(inner)
	health := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-a", MinBackoff: time.Hour, MaxBackoff: time.Hour, Logger: zerolog.Nop()})
	defer health.Close()
	ls, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, "late"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a", Health: health}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]struct{}{}
	if err := ls.Stage("db", "m", makeParquet(t, "r-%d", 5, expected)); err != nil {
		t.Fatal(err)
	}
	ob.setDown(true)
	health.ReportFailure(errConnRefused)
	go func() {
		time.Sleep(1200 * time.Millisecond)
		ob.setDown(false)
	}()
	ls.SetCloseBudget(10 * time.Second)
	if err := ls.Close(); err != nil {
		t.Fatalf("Close() = %v: the store came back 1.2s into a 10s shutdown budget", err)
	}
	if n := len(ob.probeTimes()); n < 2 {
		t.Fatalf("%d probes; want the failed probe retried", n)
	}
	rows, distinct, _ := countRowsUnder(t, objRoot)
	if rows != int64(len(expected)) || distinct != int64(len(expected)) {
		t.Fatalf("rows=%d distinct=%d, want %d", rows, distinct, len(expected))
	}
}

// A store that stays down still bounds Close by the budget.
func TestStagerFinalDrain_BoundedWhileTheStoreStaysDown(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	ob := newOutageBackend(inner)
	ob.setDown(true)
	ls, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, "late"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ls.Stage("db", "m", makeParquet(t, "r-%d", 2, map[string]struct{}{})); err != nil {
		t.Fatal(err)
	}
	ls.SetCloseBudget(time.Second)
	start := time.Now()
	err = ls.Close()
	if took := time.Since(start); took > 2500*time.Millisecond {
		t.Fatalf("Close took %v with a 1s budget", took)
	}
	if !errors.Is(err, ErrStagedNotUploaded) {
		t.Fatalf("Close() = %v, want ErrStagedNotUploaded", err)
	}
}

// With a handoff peer, a store that still fails its probe at shutdown is down,
// not blipping: the drain must not spend the peer's share of the budget
// retrying it, but hand the work over at once.
func TestStagerFinalDrain_HandsOffAtOnceWhileTheStoreIsDown(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	ob := newOutageBackend(inner)
	ob.setDown(true)
	hB := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-b", MinBackoff: time.Hour, MaxBackoff: time.Hour, Logger: zerolog.Nop()})
	defer hB.Close()
	b, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, "b"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-b", Health: hB}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.abandon()
	hB.ReportFailure(errConnRefused)
	peer := newImportPeer(t, map[string]*stagerEngine{"late": b.stagerEngine})

	hA := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-a", MinBackoff: time.Hour, MaxBackoff: time.Hour, Logger: zerolog.Nop()})
	defer hA.Close()
	a, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, "a"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a", Health: hA, HandoffURL: peer.srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Stage("db", "m", makeParquet(t, "r-%d", 2, map[string]struct{}{})); err != nil {
		t.Fatal(err)
	}
	hA.ReportFailure(errConnRefused)
	a.SetCloseBudget(10 * time.Second)
	start := time.Now()
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Close took %v: the drain kept retrying a store that failed its probe instead of handing off", took)
	}
	if p, _ := b.Pending(); p != 1 {
		t.Fatalf("peer holds %d files, want 1", p)
	}
}
