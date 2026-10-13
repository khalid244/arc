package ingest

// Jobs that keep failing (P3-H5, P3-H6).
//
// Persisted jobs are resumed first in every pass, and a pass stops at the
// first availability failure. A job whose PUT keeps failing for a reason of
// its own (a key- or size-specific 5xx/timeout) while the store answers the
// health probe must not end every pass for every other group and for the
// other stager sharing the health tracker. A job that cannot upload for a
// non-availability reason (403, ...) must not be joined by a new job of the
// same group on every pass, each keeping its own merged copy on disk.

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// poisonBackend answers 503 to every PUT under prefix and works for
// everything else (probes included).
type poisonBackend struct {
	storage.Backend
	prefix string
}

func (p *poisonBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if strings.HasPrefix(path, p.prefix) {
		return fmt.Errorf("put %s: %w", path, statusErr{503})
	}
	return p.Backend.WriteReader(ctx, path, r, size)
}

func waitRows(t *testing.T, dir string, want int64, within time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		rows, _, _ := countRowsUnder(t, dir)
		if rows >= want || time.Now().After(deadline) {
			return rows
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestStagerJobFailures_PoisonedJobDoesNotStarveOthers(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	pb := &poisonBackend{Backend: inner, prefix: "db/x_late/"}
	h := NewS3Health(S3HealthConfig{Backend: pb, PodID: "ingest-a", MinBackoff: 20 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, Logger: zerolog.Nop()})
	defer h.Close()
	opts := StagerOptions{PodID: "ingest-a", Health: h}
	ls, err := NewLateStager(&LateStagerConfig{Storage: pb, StageDir: filepath.Join(root, "late"), FlushAge: 200 * time.Millisecond, Logger: zerolog.Nop(), Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer ls.abandon()
	fs, err := NewFreshStager(&FreshStagerConfig{Storage: pb, StageDir: filepath.Join(root, "fresh"), FlushAge: 200 * time.Millisecond, Logger: zerolog.Nop(), Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.abandon()

	if err := ls.Stage("db", "x", makeParquet(t, "x-%d", 3, map[string]struct{}{})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // x is the oldest: its job runs first
	lateY, freshZ := map[string]struct{}{}, map[string]struct{}{}
	for i := 0; i < 3; i++ {
		if err := ls.Stage("db", "y", makeParquet(t, fmt.Sprintf("y%d-%%d", i), 3, lateY)); err != nil {
			t.Fatal(err)
		}
		if err := fs.Stage("db", "z", makeParquet(t, fmt.Sprintf("z%d-%%d", i), 3, freshZ)); err != nil {
			t.Fatal(err)
		}
	}
	if rows := waitRows(t, filepath.Join(objRoot, "db", "y_late"), int64(len(lateY)), 5*time.Second); rows != int64(len(lateY)) {
		t.Fatalf("late group db/y uploaded %d of %d rows in 5s: the job whose key keeps failing starves it", rows, len(lateY))
	}
	if rows := waitRows(t, filepath.Join(objRoot, "db", "z"), int64(len(freshZ)), 5*time.Second); rows != int64(len(freshZ)) {
		t.Fatalf("fresh stager uploaded %d of %d rows in 5s: the late stager's failing job stops it through the shared tracker", rows, len(freshZ))
	}
	// Steady state: new data keeps flowing while the poisoned job keeps failing.
	for i := 3; i < 5; i++ {
		if err := ls.Stage("db", "y", makeParquet(t, fmt.Sprintf("y%d-%%d", i), 3, lateY)); err != nil {
			t.Fatal(err)
		}
	}
	if rows := waitRows(t, filepath.Join(objRoot, "db", "y_late"), int64(len(lateY)), 3*time.Second); rows != int64(len(lateY)) {
		t.Fatalf("late group db/y uploaded %d of %d rows", rows, len(lateY))
	}
	if n := h.failureCount(); n > 2 {
		t.Fatalf("the poisoned job marked the store unavailable %d times although the store answered every probe", n)
	}
	if !h.Healthy() {
		t.Fatal("tracker unhealthy although only one job's key fails")
	}
	st := ls.pendingStats()
	if st.FailingJobs != 1 || st.OldestFailingJobAgeSeconds <= 0 {
		t.Fatalf("failing job metrics: %+v, want 1 failing job with an age", st)
	}
	if rows, _, _ := countRowsUnder(t, filepath.Join(objRoot, "db", "x_late")); rows != 0 {
		t.Fatalf("poisoned group uploaded %d rows", rows)
	}
}

// The order of resumed jobs: a job that failed in an earlier pass goes after
// the jobs that did not, so a job that hangs until its timeout does not delay
// the others in every pass.
func TestStagerJobFailures_FailedJobsResumeLast(t *testing.T) {
	jobs := []*stagerJob{
		{ID: "a", CreatedUnixNano: 1},
		{ID: "b", CreatedUnixNano: 2},
		{ID: "c", CreatedUnixNano: 3},
	}
	e := &stagerEngine{}
	e.recordJobFailure(jobs[0], errStagerUploadFailed, time.Now())
	e.orderJobsForResume(jobs)
	var got []string
	for _, j := range jobs {
		got = append(got, j.ID)
	}
	if strings.Join(got, ",") != "b,c,a" {
		t.Fatalf("resume order %v, want b,c,a", got)
	}
}

func TestStagerMetrics_FailingJobGauges(t *testing.T) {
	unregister := metrics.Get().RegisterStagerPending("fresh", func() metrics.StagerPendingStats {
		return metrics.StagerPendingStats{FailingJobs: 2, OldestFailingJobAgeSeconds: 42}
	})
	defer unregister()
	if v := promValue(t, `arc_stager_failing_jobs{kind="fresh"}`); v != 2 {
		t.Errorf("arc_stager_failing_jobs = %v, want 2", v)
	}
	if v := promValue(t, `arc_stager_oldest_failing_job_age_seconds{kind="fresh"}`); v != 42 {
		t.Errorf("arc_stager_oldest_failing_job_age_seconds = %v, want 42", v)
	}
}

// switchBackend answers 403 to data PUTs while deny is set.
type switchBackend struct {
	storage.Backend
	deny atomic.Bool
}

func (s *switchBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if s.deny.Load() && !strings.HasPrefix(path, "_arc/") {
		return fmt.Errorf("put %s: %w", path, statusErr{403})
	}
	return s.Backend.WriteReader(ctx, path, r, size)
}

// stageDirUsage reports the persisted jobs and the bytes of merged outputs
// kept in a stage dir.
func stageDirUsage(t *testing.T, dir string) (jobs int, outBytes int64) {
	t.Helper()
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		switch {
		case strings.HasPrefix(rel, stagerOutDirPrefix):
			outBytes += info.Size()
		case strings.HasPrefix(rel, stagerJobFilePrefix):
			jobs++
		}
		return nil
	})
	return jobs, outBytes
}

// A store refusing uploads with a non-availability error (403) leaves the
// tracker healthy. Each pass used to build another job of the group from the
// files staged since, each keeping a merged copy next to its sources.
func TestStagerJobFailures_OneFailingJobPerGroup(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	sb := &switchBackend{Backend: inner}
	sb.deny.Store(true)
	dir := filepath.Join(root, "fresh")
	fs, err := NewFreshStager(&FreshStagerConfig{Storage: sb, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.abandon()
	expected := map[string]struct{}{}
	for pass := 0; pass < 5; pass++ {
		for i := 0; i < 3; i++ {
			if err := fs.Stage("db", "m", makeParquet(t, fmt.Sprintf("p%d-%d-%%d", pass, i), 50, expected)); err != nil {
				t.Fatal(err)
			}
		}
		_ = fs.Flush(context.Background())
		if jobs, out := stageDirUsage(t, dir); jobs > 1 {
			t.Fatalf("pass %d: %d persisted jobs of one group holding %d bytes of merged copies while the store refuses uploads", pass, jobs, out)
		}
	}
	if !fs.health.Healthy() {
		t.Fatal("a 403 marked the store unavailable")
	}
	// The store accepts uploads again: the failed job goes to its recorded
	// keys and everything staged since follows.
	sb.deny.Store(false)
	for i := 0; i < 3; i++ {
		_ = fs.Flush(context.Background())
	}
	if p, _ := fs.Pending(); p != 0 {
		t.Fatalf("%d staged files left after the store recovered", p)
	}
	rows, distinct, _ := countRowsUnder(t, objRoot)
	if rows != int64(len(expected)) || distinct != int64(len(expected)) {
		t.Fatalf("rows=%d distinct=%d, want %d once", rows, distinct, len(expected))
	}
	if jobs, out := stageDirUsage(t, dir); jobs != 0 || out != 0 {
		t.Fatalf("%d jobs and %d output bytes left", jobs, out)
	}
}

// A job that cannot be re-merged (a source vanished) needs an operator; it
// must not also hold back every new file of its group.
func TestStagerJobFailures_StuckRemergeDoesNotBlockTheGroup(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	sb := &switchBackend{Backend: inner}
	sb.deny.Store(true)
	dir := filepath.Join(root, "late")
	ls, err := NewLateStager(&LateStagerConfig{Storage: sb, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	defer ls.abandon()
	for i := 0; i < 2; i++ {
		if err := ls.Stage("db", "m", makeParquet(t, fmt.Sprintf("old%d-%%d", i), 3, map[string]struct{}{})); err != nil {
			t.Fatal(err)
		}
	}
	_ = ls.Flush(context.Background()) // job persisted, 403
	ls.mu.Lock()
	jobs, _ := ls.loadJobsLocked()
	ls.mu.Unlock()
	if len(jobs) != 1 {
		t.Fatalf("setup: %d jobs", len(jobs))
	}
	// Lose the job's outputs and one source: it can no longer be rebuilt.
	_ = os.RemoveAll(ls.outDir(jobs[0].ID))
	if err := os.Remove(filepath.Join(dir, jobs[0].Sources[0])); err != nil {
		t.Fatal(err)
	}
	sb.deny.Store(false)
	fresh := map[string]struct{}{}
	if err := ls.Stage("db", "m", makeParquet(t, "new-%d", 3, fresh)); err != nil {
		t.Fatal(err)
	}
	_ = ls.Flush(context.Background())
	_ = ls.Flush(context.Background())
	rows, _, _ := countRowsUnder(t, objRoot)
	if rows != int64(len(fresh)) {
		t.Fatalf("store holds %d rows, want the %d new rows (the stuck job must not block its group)", rows, len(fresh))
	}
}
