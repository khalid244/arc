package compaction

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// The byte-budget splitter packs files greedily in listing order. A day that
// already holds sealed _daily files close to the budget gets re-selected when a
// reorg file lands next to them (Case 4 bypasses MinFiles), and each sealed
// file that cannot share a batch came out as a batch of its own. That 1-file
// batch failed in compactFilesAdaptively ("batch size 1 below minimum 2") on
// every cycle and counted as a failed batch, while it had nothing to do:
// re-merging one sealed file alone rewrites the same rows.

func sizedCandidate(files map[string]int64, order ...string) Candidate {
	c := Candidate{Database: "db", Measurement: "cpu", PartitionPath: "db/cpu/2026/10/20", Tier: "daily"}
	for _, f := range order {
		c.Files = append(c.Files, f)
		c.FileSizes = append(c.FileSizes, files[f])
	}
	c.FileCount = len(c.Files)
	return c
}

func batchFiles(bs []Candidate) [][]string {
	out := make([][]string, 0, len(bs))
	for _, b := range bs {
		fs := append([]string(nil), b.Files...)
		slices.Sort(fs)
		out = append(out, fs)
	}
	return out
}

func assertWithinCaps(t *testing.T, bs []Candidate, maxFiles int, budget int64) {
	t.Helper()
	for i, b := range bs {
		var sum int64
		for _, s := range b.FileSizes {
			sum += s
		}
		if sum > budget {
			t.Errorf("batch %d: %d bytes over the %d budget", i, sum, budget)
		}
		if maxFiles > 0 && len(b.Files) > maxFiles {
			t.Errorf("batch %d: %d files over the cap %d", i, len(b.Files), maxFiles)
		}
		if len(b.Files) != len(b.FileSizes) {
			t.Errorf("batch %d: Files/FileSizes length mismatch", i)
		}
		if b.BatchNumber != i+1 || b.TotalBatches != len(bs) {
			t.Errorf("batch %d numbered %d of %d, want %d of %d", i, b.BatchNumber, b.TotalBatches, i+1, len(bs))
		}
	}
}

// Greedy order isolated the reorg file the re-merge exists for: A, then B
// (A+B over budget), then R (B+R over budget). A has room for R.
func TestSplitCandidateByBudget_FoldsLoneFileIntoBatchWithRoom(t *testing.T) {
	sizes := map[string]int64{"a_daily": 700, "b_daily": 1000, "r_reorg": 30}
	c := sizedCandidate(sizes, "a_daily", "b_daily", "r_reorg")

	got := SplitCandidateByBudget(c, 0, 1024)

	assertWithinCaps(t, got, 0, 1024)
	want := [][]string{{"a_daily", "r_reorg"}, {"b_daily"}}
	if fmt.Sprint(batchFiles(got)) != fmt.Sprint(want) {
		t.Fatalf("batches = %v, want %v", batchFiles(got), want)
	}
}

func TestSplitCandidateByBudget_FoldRespectsBudgetAndFileCap(t *testing.T) {
	t.Run("no room", func(t *testing.T) {
		sizes := map[string]int64{"a_daily": 700, "b_daily": 700, "r_reorg": 5}
		got := SplitCandidateByBudget(sizedCandidate(sizes, "a_daily", "b_daily", "r_reorg"), 0, 1024)
		assertWithinCaps(t, got, 0, 1024)
		want := [][]string{{"a_daily"}, {"b_daily", "r_reorg"}}
		if fmt.Sprint(batchFiles(got)) != fmt.Sprint(want) {
			t.Fatalf("batches = %v, want %v", batchFiles(got), want)
		}
	})
	t.Run("file cap", func(t *testing.T) {
		sizes := map[string]int64{"a": 10, "b": 10, "c": 1020, "d": 10}
		got := SplitCandidateByBudget(sizedCandidate(sizes, "a", "b", "c", "d"), 2, 1024)
		assertWithinCaps(t, got, 2, 1024)
		want := [][]string{{"a", "b"}, {"c"}, {"d"}}
		if fmt.Sprint(batchFiles(got)) != fmt.Sprint(want) {
			t.Fatalf("batches = %v, want %v", batchFiles(got), want)
		}
	})
	t.Run("two lone files pair up", func(t *testing.T) {
		sizes := map[string]int64{"x": 600, "y": 900, "z": 300}
		got := SplitCandidateByBudget(sizedCandidate(sizes, "x", "y", "z"), 0, 1024)
		assertWithinCaps(t, got, 0, 1024)
		want := [][]string{{"x", "z"}, {"y"}}
		if fmt.Sprint(batchFiles(got)) != fmt.Sprint(want) {
			t.Fatalf("batches = %v, want %v", batchFiles(got), want)
		}
	})
}

func TestWithoutLoneBatches(t *testing.T) {
	mk := func(n int, files ...string) Candidate {
		return Candidate{PartitionPath: "p", Files: files, FileSizes: make([]int64, len(files)), BatchNumber: n, TotalBatches: 3}
	}
	kept, dropped := withoutLoneBatches([]Candidate{mk(1, "a"), mk(2, "b", "c"), mk(3, "d")})
	if len(kept) != 1 || kept[0].BatchNumber != 1 || kept[0].TotalBatches != 1 || fmt.Sprint(kept[0].Files) != "[b c]" {
		t.Fatalf("kept = %+v, want [b c] as batch 1 of 1", kept)
	}
	if fmt.Sprint(batchFiles(dropped)) != "[[a] [d]]" {
		t.Fatalf("dropped = %v, want [[a] [d]]", batchFiles(dropped))
	}

	all := []Candidate{mk(1, "a", "b"), mk(2, "c", "d")}
	kept, dropped = withoutLoneBatches(all)
	if len(dropped) != 0 || len(kept) != 2 || kept[1].BatchNumber != 2 || kept[1].TotalBatches != 3 {
		t.Fatalf("nothing to drop must leave the batches as they are: kept=%+v dropped=%+v", kept, dropped)
	}
}

// budgetTier is a daily tier with a byte budget, so the manager takes the
// SplitCandidateByBudget path.
type budgetTier struct {
	interleaveTier
	budget int64
}

func (t budgetTier) GetMaxOutputBytes() int64 { return t.budget }

func (budgetTier) IsCompactedFile(f string) bool { return strings.HasSuffix(f, "_daily.parquet") }

// The daily cycle's view of the same day: the sealed file that cannot share a
// batch is skipped before it becomes a job, the reorg file is still folded, and
// nothing counts as a failed batch.
func TestDailyCycle_LoneSealedFileIsSkippedNotFailed(t *testing.T) {
	rig := newILRig(t, 1, false, nil, nil)
	ctx := context.Background()
	day := "db/cpu/2026/10/20"
	a := day + "/cpu_20261021_031500_aaaaaaaa_daily.parquet"
	b := day + "/cpu_20261021_031900_bbbbbbbb_daily.parquet"
	r := day + "/cpu_reorg_job1_0.parquet"
	for f, n := range map[string]int{a: 700, b: 700, r: 5} {
		if err := rig.m.StorageBackend.Write(ctx, f, []byte(strings.Repeat("x", n))); err != nil {
			t.Fatal(err)
		}
	}
	c := Candidate{Database: "db", Measurement: "cpu", PartitionPath: day, Tier: "daily", Files: []string{a, b, r}, FileCount: 3}
	rig.m.Tiers = []Tier{budgetTier{
		interleaveTier: interleaveTier{name: "daily", find: func(_ context.Context, meas string) ([]Candidate, error) {
			if meas != "cpu" {
				return nil, nil
			}
			return []Candidate{c}, nil
		}},
		budget: 1024,
	}}
	var mu sync.Mutex
	var ran [][]string
	rig.m.compactBatchForTest = func(_ context.Context, batch Candidate) error {
		mu.Lock()
		defer mu.Unlock()
		ran = append(ran, append([]string(nil), batch.Files...))
		if len(batch.Files) < MinFilesPerBatch {
			return fmt.Errorf("compaction failed: batch size %d below minimum %d", len(batch.Files), MinFilesPerBatch)
		}
		return nil
	}

	logs := &lockedLog{}
	rig.m.logger = zerolog.New(logs).Level(zerolog.DebugLevel)

	if err := waitDone(t, rig.startCycle(ctx, "daily")); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}

	// The skipped batch recurs every cycle: its own line is Debug, the cycle
	// summary carries the count.
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "Skipping one-file batches") && !strings.Contains(line, `"level":"debug"`) {
			t.Fatalf("one-file batch line must be Debug: %s", line)
		}
		if strings.Contains(line, "Compaction cycle finished") && !strings.Contains(line, `"skipped_one_file_batches":1`) {
			t.Fatalf("cycle summary must count the skipped batch: %s", line)
		}
	}
	if !strings.Contains(logs.String(), "Compaction cycle finished") {
		t.Fatal("no cycle summary logged")
	}

	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(ran) != fmt.Sprint([][]string{{b, r}}) {
		t.Fatalf("batches run = %v, want only [%s %s]", ran, b, r)
	}
	recs := rig.records()
	if len(recs) != 1 {
		t.Fatalf("cycle records = %d, want 1", len(recs))
	}
	if recs[0].Failed != 0 {
		t.Fatalf("failed batches = %d, want 0: a lone sealed file is nothing to do, not a failure", recs[0].Failed)
	}
	if recs[0].Succeeded != 1 {
		t.Fatalf("succeeded batches = %d, want 1", recs[0].Succeeded)
	}
}

// A candidate left with one file (after the manifest filter, say) is not a job
// either: one file has nothing to merge with.
func TestDailyCycle_SingleFileCandidateIsSkipped(t *testing.T) {
	rig := newILRig(t, 1, false, nil, nil)
	ctx := context.Background()
	c := ilCandidate("daily", "db/cpu/2026/10/24", 1)
	rig.m.Tiers = []Tier{budgetTier{
		interleaveTier: interleaveTier{name: "daily", find: func(_ context.Context, meas string) ([]Candidate, error) {
			if meas != "cpu" {
				return nil, nil
			}
			return []Candidate{c}, nil
		}},
	}}
	var ran int
	rig.m.compactBatchForTest = func(context.Context, Candidate) error { ran++; return nil }

	if err := waitDone(t, rig.startCycle(ctx, "daily")); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}
	if ran != 0 {
		t.Fatalf("a 1-file candidate ran %d batch(es), want 0", ran)
	}
	if recs := rig.records(); len(recs) != 1 || recs[0].Failed != 0 || recs[0].Discovered != 0 {
		t.Fatalf("records = %+v, want one cycle with nothing discovered or failed", recs)
	}
}

type lockedLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
