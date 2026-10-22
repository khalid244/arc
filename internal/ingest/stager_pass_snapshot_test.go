package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// IC3(a): a scheduled pass takes only the files staged when it started. A
// file staged while the pass's job merges and uploads used to become a tiny
// follow-up job (staged_files=1) seconds later, and with it one more object in
// every hour partition the follow-up touched. It now waits for the next pass.

func newSnapshotTestStager(t *testing.T) (*FreshStager, string) {
	t.Helper()
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.WarnLevel)
	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	fs, err := NewFreshStager(&FreshStagerConfig{
		Storage: store, StageDir: filepath.Join(tmpDir, "_fresh_stager"),
		FlushAge: time.Hour, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	return fs, tmpDir
}

// stageDuringFirstJob stages one more file from inside the first job of each
// pass, the way ingest keeps staging while a job runs.
func stageDuringFirstJob(t *testing.T, fs *FreshStager, expected map[string]struct{}) (jobs func() int) {
	var mu sync.Mutex
	n, extra := 0, 0
	fs.hooks.onJob = func(db, m string, sources []string, bytes int64) {
		mu.Lock()
		defer mu.Unlock()
		n++
		if n == 1 {
			p := makeParquet(t, fmt.Sprintf("during-%d-%%d", extra), 20, expected)
			extra++
			if err := fs.Stage("posthog", "events", p); err != nil {
				t.Errorf("stage during job: %v", err)
			}
		}
	}
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		return n
	}
}

func TestStagerPassSnapshot_ScheduledPassRunsNoFollowUpJob(t *testing.T) {
	fs, tmpDir := newSnapshotTestStager(t)
	defer fs.Close()

	expected := make(map[string]struct{})
	for g := 0; g < 3; g++ {
		if err := fs.Stage("posthog", "events", makeParquet(t, fmt.Sprintf("g%d-%%d", g), 50, expected)); err != nil {
			t.Fatal(err)
		}
	}
	jobs := stageDuringFirstJob(t, fs, expected)

	if err := fs.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := jobs(); got != 1 {
		t.Fatalf("scheduled pass ran %d jobs, want 1 (a file staged during the job must wait for the next pass)", got)
	}
	if n, err := fs.Pending(); err != nil || n != 1 {
		t.Fatalf("pending after pass = %d (%v), want 1 (the file staged during the job)", n, err)
	}
	// makeParquet spreads rows over 3 hours: one object per hour, no more.
	if got := countOutputParquets(t, tmpDir); got != 3 {
		t.Fatalf("objects after first pass = %d, want 3 (one per touched hour)", got)
	}

	// The next pass takes it: nothing is lost or left behind.
	fs.hooks.onJob = nil
	if err := fs.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, err := fs.Pending(); err != nil || n != 0 {
		t.Fatalf("pending after second pass = %d (%v), want 0", n, err)
	}
	assertExactlyOnce(t, tmpDir, expected)
}

// The shutdown drain (force) still takes everything, including files staged
// while its own jobs run: shutdown semantics are unchanged.
func TestStagerPassSnapshot_ForcedDrainTakesLateStagedFiles(t *testing.T) {
	fs, tmpDir := newSnapshotTestStager(t)
	defer fs.Close()

	expected := make(map[string]struct{})
	if err := fs.Stage("posthog", "events", makeParquet(t, "a-%d", 50, expected)); err != nil {
		t.Fatal(err)
	}
	jobs := stageDuringFirstJob(t, fs, expected)

	if err := fs.drain(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if got := jobs(); got != 2 {
		t.Fatalf("forced drain ran %d jobs, want 2 (it must also take the file staged during its first job)", got)
	}
	if n, err := fs.Pending(); err != nil || n != 0 {
		t.Fatalf("pending after forced drain = %d (%v), want 0", n, err)
	}
	assertExactlyOnce(t, tmpDir, expected)
}

// A backlog larger than one job still drains completely in one scheduled
// pass: the snapshot bounds what a pass takes, not how many jobs it runs.
func TestStagerPassSnapshot_BacklogStillDrainsInOnePass(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.WarnLevel)
	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stageDir := filepath.Join(tmpDir, "_fresh_stager")
	files := writeStagedBacklog(t, stageDir, freshStagerSpec().marker, "posthog", "events", 7, 10)
	want := files * 10
	fs, err := NewFreshStager(&FreshStagerConfig{
		Storage: store, StageDir: stageDir, FlushAge: time.Hour, Logger: logger,
		Options: StagerOptions{JobMaxFiles: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	rec := recordJobs(fs.stagerEngine)
	if err := fs.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(*rec); got != 3 {
		t.Fatalf("jobs = %d, want 3 (7 files at 3 per job)", got)
	}
	if n, err := fs.Pending(); err != nil || n != 0 {
		t.Fatalf("pending = %d (%v), want 0", n, err)
	}
	rows, distinct, _ := countRowsUnder(t, filepath.Join(tmpDir, "posthog"))
	if rows != int64(want) || distinct != int64(want) {
		t.Fatalf("rows=%d distinct=%d, want %d", rows, distinct, want)
	}
}

func assertExactlyOnce(t *testing.T, tmpDir string, expected map[string]struct{}) {
	t.Helper()
	ids, err := collectAllRowIDs(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]int, len(ids))
	for _, id := range ids {
		seen[id]++
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("row %s uploaded %d times", id, c)
		}
	}
	if len(seen) != len(expected) {
		t.Fatalf("uploaded %d distinct rows, want %d", len(seen), len(expected))
	}
}
