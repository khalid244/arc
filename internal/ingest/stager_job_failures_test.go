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
	"path/filepath"
	"strings"
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
	e.recordJobFailure("a", errStagerUploadFailed, time.Now())
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
