package ingest

// IW-05 + partial multi-partition uploads: a stager merge job is exactly-once.
// Before the first PUT the job file records the job id, its sources and the
// fixed object key of every output; each output's status is persisted after
// its PUT (or after a HEAD finds it present following a failed PUT); sources
// are deleted only when every output is done, then the job file. A process
// killed at ANY step resumes the same job with the same keys, so the backlog
// lands exactly once: no duplicate objects, no lost rows.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

var errTestKill = errors.New("test: process killed")

// killAt returns a crash hook that "kills" the stager the n-th time the named
// point is reached: the job stops there, with nothing cleaned up.
func killAt(point string, n int) func(string) error {
	var seen atomic.Int32
	return func(p string) error {
		if p == point && int(seen.Add(1)) == n {
			return errTestKill
		}
		return nil
	}
}

func readJobFiles(t *testing.T, stageDir string) []stagerJob {
	t.Helper()
	entries, err := os.ReadDir(stageDir)
	if err != nil {
		t.Fatal(err)
	}
	var jobs []stagerJob
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), stagerJobFilePrefix) || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(stageDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var j stagerJob
		if err := json.Unmarshal(b, &j); err != nil {
			t.Fatalf("job file %s: %v", e.Name(), err)
		}
		jobs = append(jobs, j)
	}
	return jobs
}

func objectKeysUnder(t *testing.T, root string) []string {
	t.Helper()
	var keys []string
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	sort.Strings(keys)
	return keys
}

func leftoverJobState(t *testing.T, stageDir string) []string {
	t.Helper()
	entries, _ := os.ReadDir(stageDir)
	var left []string
	for _, e := range entries {
		n := e.Name()
		if strings.HasPrefix(n, stagerJobFilePrefix) || strings.HasPrefix(n, stagerOutDirPrefix) {
			left = append(left, n)
		}
	}
	return left
}

type jobKillCase struct {
	name  string
	point string
	nth   int
	// jobPersisted: the job file exists after the kill, so the restart must
	// reuse its keys.
	jobPersisted bool
	// uploadedBeforeKill is how many outputs are in the store at the kill.
	uploadedBeforeKill int
}

// TestStagerJobs_FreshKillAtEachStep kills a three-partition fresh job at each
// step, restarts a new stager on the same stage dir and object store, and
// checks the outcome is exactly the rows staged, as exactly one object per
// partition, with the recorded keys.
func TestStagerJobs_FreshKillAtEachStep(t *testing.T) {
	cases := []jobKillCase{
		{name: "after merge, before the job file", point: stagerPointMerged, nth: 1},
		{name: "after the job file, before any PUT", point: stagerPointJobPersisted, nth: 1, jobPersisted: true},
		{name: "after 1 of 3 PUTs", point: stagerPointOutputDone, nth: 1, jobPersisted: true, uploadedBeforeKill: 1},
		{name: "after 2 of 3 PUTs", point: stagerPointOutputDone, nth: 2, jobPersisted: true, uploadedBeforeKill: 2},
		{name: "after all PUTs, before source delete", point: stagerPointUploaded, nth: 1, jobPersisted: true, uploadedBeforeKill: 3},
		{name: "part-way through source delete", point: stagerPointSourceDeleted, nth: 2, jobPersisted: true, uploadedBeforeKill: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			objRoot := filepath.Join(root, "s3")
			store, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			stageDir := filepath.Join(root, "fresh")
			const rowsPerFile = 5
			n := writeStagedBacklog(t, stageDir, "fresh_stage_", "db", "m", 6, rowsPerFile)
			want := int64(n * rowsPerFile)
			opts := StagerOptions{PodID: "ingest-a", UploadConcurrency: 1}

			a, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: stageDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
			if err != nil {
				t.Fatal(err)
			}
			a.hooks.crash = killAt(tc.point, tc.nth)
			_ = a.Flush(context.Background())
			a.abandon()

			jobs := readJobFiles(t, stageDir)
			if tc.jobPersisted != (len(jobs) == 1) {
				t.Fatalf("after the kill: %d job file(s), want persisted=%v", len(jobs), tc.jobPersisted)
			}
			if got := len(objectKeysUnder(t, objRoot)); got != tc.uploadedBeforeKill {
				t.Fatalf("after the kill: %d objects, want %d", got, tc.uploadedBeforeKill)
			}
			var recorded []string
			if tc.jobPersisted {
				if len(jobs[0].Outputs) != 3 {
					t.Fatalf("job records %d outputs, want 3 partitions", len(jobs[0].Outputs))
				}
				for _, o := range jobs[0].Outputs {
					recorded = append(recorded, o.Key)
				}
				sort.Strings(recorded)
			}

			b, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: stageDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			if err := b.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}

			if p, _ := b.Pending(); p != 0 {
				t.Errorf("restart left %d staged files", p)
			}
			if left := leftoverJobState(t, stageDir); len(left) != 0 {
				t.Errorf("restart left job state behind: %v", left)
			}
			keys := objectKeysUnder(t, objRoot)
			if len(keys) != 3 {
				t.Errorf("DUPLICATE/LOSS: %d objects for 3 partitions: %v", len(keys), keys)
			}
			if tc.jobPersisted && strings.Join(keys, ",") != strings.Join(recorded, ",") {
				t.Errorf("the resumed job used other keys than it recorded:\n got  %v\n want %v", keys, recorded)
			}
			rows, distinct, _ := countRowsUnder(t, objRoot)
			if rows != want || distinct != want {
				t.Errorf("rows=%d distinct=%d, want exactly %d", rows, distinct, want)
			}
		})
	}
}

// TestStagerJobs_LateKillAfterPutBeforeDelete: the late stager's single output
// is not re-uploaded after a kill between its PUT and the source delete.
func TestStagerJobs_LateKillAfterPutBeforeDelete(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	store, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stageDir := filepath.Join(root, "late")
	n := writeStagedBacklog(t, stageDir, "late_stage_", "db", "m", 4, 3)
	opts := StagerOptions{PodID: "ingest-a"}
	a, err := NewLateStager(&LateStagerConfig{Storage: store, StageDir: stageDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	a.hooks.crash = killAt(stagerPointUploaded, 1)
	_ = a.Flush(context.Background())
	a.abandon()
	before := objectKeysUnder(t, objRoot)
	if len(before) != 1 {
		t.Fatalf("precondition: 1 object before the kill, got %v", before)
	}

	b, err := NewLateStager(&LateStagerConfig{Storage: store, StageDir: stageDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := objectKeysUnder(t, objRoot)
	if len(after) != 1 || after[0] != before[0] {
		t.Errorf("objects after restart %v, want only %v", after, before)
	}
	if p, _ := b.Pending(); p != 0 {
		t.Errorf("restart left %d staged files", p)
	}
	rows, distinct, _ := countRowsUnder(t, objRoot)
	if want := int64(n * 3); rows != want || distinct != want {
		t.Errorf("rows=%d distinct=%d, want %d", rows, distinct, want)
	}
}

// TestStagerJobs_ResumeRemergesMissingOutputs: if the local merge outputs of a
// persisted job are gone (a job handed over without them, or a wiped scratch
// dir), the resume merges the job's sources again and still uploads to the
// recorded keys only.
func TestStagerJobs_ResumeRemergesMissingOutputs(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	store, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stageDir := filepath.Join(root, "fresh")
	n := writeStagedBacklog(t, stageDir, "fresh_stage_", "db", "m", 6, 4)
	opts := StagerOptions{PodID: "ingest-a", UploadConcurrency: 1}
	a, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: stageDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	a.hooks.crash = killAt(stagerPointOutputDone, 1)
	_ = a.Flush(context.Background())
	a.abandon()
	jobs := readJobFiles(t, stageDir)
	if len(jobs) != 1 {
		t.Fatalf("want 1 persisted job, got %d", len(jobs))
	}
	if err := os.RemoveAll(filepath.Join(stageDir, stagerOutDirPrefix+jobs[0].ID)); err != nil {
		t.Fatal(err)
	}
	var recorded []string
	for _, o := range jobs[0].Outputs {
		recorded = append(recorded, o.Key)
	}
	sort.Strings(recorded)

	b, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: stageDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	keys := objectKeysUnder(t, objRoot)
	if strings.Join(keys, ",") != strings.Join(recorded, ",") {
		t.Errorf("keys after the re-merge %v, want the recorded %v", keys, recorded)
	}
	rows, distinct, _ := countRowsUnder(t, objRoot)
	if want := int64(n * 4); rows != want || distinct != want {
		t.Errorf("rows=%d distinct=%d, want %d", rows, distinct, want)
	}
}

// TestStagerJobs_KeysCarryPodAndJob: object keys carry the pod id and the job
// id, so two pods (or two jobs) never write the same key, and keep the
// <YYYYMMDD>_<HHMMSS>_<digits>.parquet tail the reorganizer, the daily
// compactor and file-time pruning parse.
func TestStagerJobs_KeysCarryPodAndJob(t *testing.T) {
	tail := regexp.MustCompile(`_(\d{8})_(\d{6})_\d+\.parquet$`)
	reorgRE := regexp.MustCompile(`^[^/]*?(\d{8})_(\d{6})_\d+\.parquet$`) // compaction/reorg.go filenameTimeRE
	run := func(t *testing.T, pod string, fresh bool) []string {
		root := t.TempDir()
		objRoot := filepath.Join(root, "s3")
		store, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		opts := StagerOptions{PodID: pod}
		if fresh {
			dir := filepath.Join(root, "fresh")
			writeStagedBacklog(t, dir, "fresh_stage_", "db", "m", 3, 2)
			s, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			_ = s.Flush(context.Background())
		} else {
			dir := filepath.Join(root, "late")
			writeStagedBacklog(t, dir, "late_stage_", "db", "m", 3, 2)
			s, err := NewLateStager(&LateStagerConfig{Storage: store, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			_ = s.Flush(context.Background())
		}
		return objectKeysUnder(t, objRoot)
	}
	for _, fresh := range []bool{true, false} {
		a, b := run(t, "ingest-a", fresh), run(t, "ingest-b", fresh)
		if len(a) == 0 || len(b) == 0 {
			t.Fatalf("fresh=%v: nothing uploaded", fresh)
		}
		for _, k := range a {
			base := filepath.Base(k)
			if !strings.Contains(base, "ingest-a") {
				t.Errorf("key %s does not carry the pod id", k)
			}
			if !tail.MatchString(base) {
				t.Errorf("key %s lacks the _YYYYMMDD_HHMMSS_<digits>.parquet tail", k)
			}
			if !fresh && !reorgRE.MatchString(base) {
				t.Errorf("late key %s is not parsed by the reorganizer", k)
			}
			if fresh && !strings.HasPrefix(base, "m_") {
				t.Errorf("fresh key %s does not start with the measurement", k)
			}
			if !fresh && (!strings.HasPrefix(k, "db/m_late/") || !strings.HasPrefix(base, "m_late_")) {
				t.Errorf("late key %s is not in the flat db/m_late/ sidecar", k)
			}
			for _, k2 := range b {
				if filepath.Base(k2) == base {
					t.Errorf("pods ingest-a and ingest-b produced the same object name %s", base)
				}
			}
		}
	}
	if got := sanitizeStagerPodID("ingest_0.default/x"); strings.ContainsAny(got, "_./") || got == "" {
		t.Errorf("sanitizeStagerPodID kept separators: %q", got)
	}
	if got := fmt.Sprint(sanitizeStagerPodID("")); got == "" {
		t.Errorf("empty pod id must fall back to a non-empty name")
	}
}
