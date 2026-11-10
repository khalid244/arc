package compaction

import (
	"context"
	"errors"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// #1343: the parent counts each attempt's outcome, and the storage I/O the
// subprocess did, from its result; the subprocess's own registry is never
// scraped.

func snap() map[string]int64 {
	out := map[string]int64{}
	for k, v := range metrics.Get().Snapshot() {
		if n, ok := v.(int64); ok {
			out[k] = n
		}
	}
	return out
}

func delta(before, after map[string]int64, key string) int64 { return after[key] - before[key] }

// stubSubprocess replaces the compaction subprocess for the test.
func stubSubprocess(t *testing.T, fn func(context.Context, *SubprocessJobConfig) (*SubprocessJobResult, error)) {
	t.Helper()
	old := runJobInSubprocess
	t.Cleanup(func() { runJobInSubprocess = old })
	runJobInSubprocess = func(ctx context.Context, c *SubprocessJobConfig, _ zerolog.Logger, _ ...string) (*SubprocessJobResult, error) {
		return fn(ctx, c)
	}
}

func metricsCandidate(t *testing.T, m *Manager) Candidate {
	t.Helper()
	files := []string{"db/cpu/2026/10/13/10/a.parquet", "db/cpu/2026/10/13/10/b.parquet"}
	for _, f := range files {
		if err := m.StorageBackend.Write(context.Background(), f, []byte("12345")); err != nil {
			t.Fatal(err)
		}
	}
	return Candidate{Database: "db", Measurement: "cpu", PartitionPath: "cpu/2026/10/13/10", Files: files, BatchNumber: 1, Tier: "hourly"}
}

func runStubbed(t *testing.T, m *Manager, ctx context.Context, res *SubprocessJobResult, err error, inspect func(*SubprocessJobConfig)) error {
	t.Helper()
	candidate := metricsCandidate(t, m)
	stubSubprocess(t, func(_ context.Context, c *SubprocessJobConfig) (*SubprocessJobResult, error) {
		if inspect != nil {
			inspect(c)
		}
		return res, err
	})
	return m.compactPartition(ctx, candidate, jobAttribution{})
}

func TestParentCountsJobOutcomes(t *testing.T) {
	cases := []struct {
		name   string
		res    *SubprocessJobResult
		err    error
		cancel bool
		want   map[string]int64
	}{
		{"success", &SubprocessJobResult{Success: true, FilesCompacted: 2, BytesBefore: 10, BytesAfter: 4,
			Metrics: map[string]int64{"storage_writes": 3, "storage_write_bytes": 40, "unknown_key": 9}},
			nil, false, map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_success": 1, "compaction_files_compacted": 2,
				"compaction_bytes_read": 10, "compaction_bytes_written": 4, "storage_writes_total": 3, "storage_write_bytes_total": 40,
				"compaction_jobs_failed": 0, "compaction_jobs_interrupted": 0}},
		{"failure", &SubprocessJobResult{Success: false, Error: "boom", Metrics: map[string]int64{"storage_errors": 1}},
			nil, false, map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_failed": 1, "storage_errors_total": 1,
				"compaction_jobs_success": 0, "compaction_files_compacted": 0}},
		{"crash, no result", nil, errors.New("exit status 2"), false,
			map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_failed": 1}},
		{"killed for a cancelled cycle", nil, context.Canceled, true,
			map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_interrupted": 1, "compaction_jobs_failed": 0}},
		{"stopped by the same signal, reported a failure", &SubprocessJobResult{Success: false, Error: "context canceled"}, nil, true,
			map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_interrupted": 1, "compaction_jobs_failed": 0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := activeJobsRig(t, localBackend(t))
			ctx := context.Background()
			if c.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			var before map[string]int64
			// Taken as the "subprocess" runs: the rig's own writes are not
			// the job's.
			_ = runStubbed(t, m, ctx, c.res, c.err, func(*SubprocessJobConfig) { before = snap() })
			after := snap()
			for k, want := range c.want {
				if got := delta(before, after, k); got != want {
					t.Errorf("%s moved by %d, want %d", k, got, want)
				}
			}
		})
	}
}

// A partition already locked is no attempt: nothing is counted.
func TestParentDoesNotCountALockSkip(t *testing.T) {
	m := activeJobsRig(t, localBackend(t))
	candidate := metricsCandidate(t, m)
	stubSubprocess(t, func(context.Context, *SubprocessJobConfig) (*SubprocessJobResult, error) {
		t.Fatal("a locked partition ran a subprocess")
		return nil, nil
	})
	key := candidate.Database + "/" + candidate.PartitionPath
	if !m.LockManager.AcquireLock(key) {
		t.Fatal("lock")
	}
	defer m.LockManager.ReleaseLock(key)
	before := snap()
	if err := m.compactPartition(context.Background(), candidate, jobAttribution{}); err != nil {
		t.Fatal(err)
	}
	if got := delta(before, snap(), "compaction_jobs_total"); got != 0 {
		t.Fatalf("a lock skip moved compaction_jobs_total by %d", got)
	}
}

// An interrupted attempt returns ctx's error even when the subprocess
// reported its own failure: the cycle classifies by errors.Is(err, ctx.Err()).
func TestInterruptedAttemptReturnsTheContextError(t *testing.T) {
	m := activeJobsRig(t, localBackend(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := runStubbed(t, m, ctx, &SubprocessJobResult{Success: false, Error: "context canceled by SIGTERM"}, nil, nil)
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("err = %v, want it to wrap context.Canceled", got)
	}
}

// A failed attempt under a live context returns the subprocess's own error,
// unwrapped, as before.
func TestFailedAttemptReturnsTheSubprocessError(t *testing.T) {
	m := activeJobsRig(t, localBackend(t))
	got := runStubbed(t, m, context.Background(), &SubprocessJobResult{Success: false, Error: "Out of Memory Error: x"}, nil, nil)
	if got == nil || got.Error() != "Out of Memory Error: x" {
		t.Fatalf("err = %v", got)
	}
	if got := runStubbed(t, m, context.Background(), &SubprocessJobResult{Success: true}, nil, nil); got != nil {
		t.Fatalf("a successful attempt returned %v", got)
	}
}
