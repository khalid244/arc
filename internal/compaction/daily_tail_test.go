package compaction

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Tail-only re-merge (compaction.daily_tail_only_ratio). The reorganizer adds
// a few late rows to each of the last ~31 days on every run, so Case 4 selects
// those days in every daily cycle, and the budget split packed each day's
// sealed _daily file (~1 GB) with ~1 MB of reorg files: the whole day was
// rewritten to absorb them.

const mib = int64(1) << 20

func tailTier(ratio float64, sealedMin, budget int64) *DailyTier {
	return NewDailyTier(&DailyTierConfig{
		MinFiles:           8,
		MaxOutputBytes:     budget,
		TailOnlyRatio:      ratio,
		TailSealedMinBytes: sealedMin,
		Enabled:            true,
		Logger:             zerolog.Nop(),
	})
}

type dayFile struct {
	name string
	size int64
}

func tailDayCandidate(day string, files ...dayFile) Candidate {
	c := Candidate{Database: "posthog", Measurement: "events", PartitionPath: day, Tier: "daily"}
	for _, f := range files {
		c.Files = append(c.Files, day+"/"+f.name)
		c.FileSizes = append(c.FileSizes, f.size)
	}
	c.FileCount = len(c.Files)
	return c
}

func reorgFiles(n int, size int64) []dayFile {
	out := make([]dayFile, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, dayFile{fmt.Sprintf("events_reorg_job%02d_0.parquet", i), size})
	}
	return out
}

const (
	sealedA   = "events_20261022_031500_aaaaaaaa_daily.parquet"
	sealedB   = "events_20261022_034000_bbbbbbbb_daily.parquet"
	remainder = "events_20261022_041000_cccccccc_daily.parquet"
)

// An old day: two sealed daily files, the small remainder output of an earlier
// merge, and 20 reorg files. Only the remainder and the reorg files merge.
func TestDailyTailOnly_OldDayMergesTailWithoutSealedFiles(t *testing.T) {
	day := "posthog/events/2026/10/21"
	files := append([]dayFile{{sealedA, 1000 * mib}, {sealedB, 900 * mib}, {remainder, 30 * mib}}, reorgFiles(20, mib)...)
	c := tailDayCandidate(day, files...)
	sealed := []string{day + "/" + sealedA, day + "/" + sealedB}

	tier := tailTier(0.15, 0, 1024*mib)
	if tier.TailSealedMinBytes != 512*mib {
		t.Fatalf("sealed size = %d, want half the budget", tier.TailSealedMinBytes)
	}
	if !tier.ShouldCompact(c.Files, time.Now().AddDate(0, 0, -27)) {
		t.Fatal("the day must be a candidate (reorg files next to a sealed file)")
	}

	// Today's behaviour, the reason for the rule: a sealed file shares a batch.
	off := tailTier(0, 0, 1024*mib)
	if _, ok := off.selectTail(c); ok {
		t.Fatal("ratio 0 must leave the candidate alone")
	}
	before, _ := withoutLoneBatches(SplitCandidateByBudget(c, 30, 1024*mib))
	if !slices.ContainsFunc(before, func(b Candidate) bool { return slices.Contains(b.Files, sealed[1]) }) {
		t.Fatalf("precondition: without the rule a sealed file is rewritten; batches = %v", batchFiles(before))
	}

	got, ok := tier.selectTail(c)
	if !ok {
		t.Fatal("tail-only must apply: tail 50 MiB < 15% of 1900 MiB sealed")
	}
	want := c.Files[2:]
	if !slices.Equal(got.Files, want) || !slices.Equal(got.FileSizes, c.FileSizes[2:]) || got.FileCount != len(want) {
		t.Fatalf("tail = %v (%d), want the remainder and the 20 reorg files", got.Files, got.FileCount)
	}
	if got.PartitionPath != c.PartitionPath || got.Tier != c.Tier || got.Database != c.Database {
		t.Fatalf("tail lost candidate fields: %+v", got)
	}

	batches, lone := withoutLoneBatches(SplitCandidateByBudget(got, 30, 1024*mib))
	if len(lone) != 0 || len(batches) != 1 || !slices.Equal(batches[0].Files, want) {
		t.Fatalf("batches = %v (lone %v), want one batch of the 21 tail files", batchFiles(batches), batchFiles(lone))
	}
}

// A day the daily tier has not merged yet (hour files only) has no sealed file:
// nothing changes. Nor does a day whose daily outputs are all below the sealed
// size, or one with no tail at all (Case 3's re-merge of sealed outputs: both
// files are at least 512 MiB, and 600 is over 15% of 700).
func TestDailyTailOnly_NoSealedFileOrNoTailIsUnchanged(t *testing.T) {
	tier := tailTier(0.15, 0, 1024*mib)
	day := "posthog/events/2026/11/15"

	var hours []dayFile
	for h := 0; h < 24; h++ {
		hours = append(hours, dayFile{fmt.Sprintf("%02d/events_20261115_%02d0500_1_compacted.parquet", h, h+1), 100 * mib})
	}
	smallDaily := append([]dayFile{{remainder, 150 * mib}}, reorgFiles(3, mib)...)
	sealedOnly := []dayFile{{sealedA, 600 * mib}, {sealedB, 700 * mib}}

	for name, files := range map[string][]dayFile{"new day": hours, "small daily": smallDaily, "sealed only": sealedOnly} {
		t.Run(name, func(t *testing.T) {
			c := tailDayCandidate(day, files...)
			got, ok := tier.selectTail(c)
			if ok || !slices.Equal(got.Files, c.Files) {
				t.Fatalf("selectTail = (%v, %v), want the candidate unchanged", got.Files, ok)
			}
		})
	}

	// Sizes unknown (the backend cannot list them): unchanged.
	c := tailDayCandidate(day, append([]dayFile{{sealedA, 900 * mib}}, reorgFiles(3, mib)...)...)
	c.FileSizes = nil
	if _, ok := tier.selectTail(c); ok {
		t.Fatal("without sizes the rule cannot tell sealed from tail and must not apply")
	}
}

// Once the tail reaches the ratio it folds into the sealed file as before.
func TestDailyTailOnly_TailAtThresholdFoldsIntoSealed(t *testing.T) {
	day := "posthog/lifecycle/2026/10/21"
	tier := tailTier(0.15, 0, 1024*mib)

	// 100 + 20 = 120 MiB = 15% of 800 MiB: fold.
	c := tailDayCandidate(day, append([]dayFile{{sealedA, 800 * mib}, {remainder, 100 * mib}}, reorgFiles(20, mib)...)...)
	if got, ok := tier.selectTail(c); ok {
		t.Fatalf("tail at the ratio must fold, got tail %v", got.Files)
	}
	batches, _ := withoutLoneBatches(SplitCandidateByBudget(c, 30, 1024*mib))
	if len(batches) != 1 || !slices.Equal(batches[0].Files, c.Files) {
		t.Fatalf("batches = %v, want sealed + tail in one batch", batchFiles(batches))
	}

	// One MiB less stays tail-only.
	c = tailDayCandidate(day, append([]dayFile{{sealedA, 800 * mib}, {remainder, 99 * mib}}, reorgFiles(20, mib)...)...)
	if got, ok := tier.selectTail(c); !ok || slices.Contains(got.Files, day+"/"+sealedA) {
		t.Fatalf("tail below the ratio must leave the sealed file out: (%v, %v)", got.Files, ok)
	}

	// An explicit sealed size: a 300 MiB daily file counts as sealed at 256.
	c = tailDayCandidate(day, append([]dayFile{{sealedA, 1000 * mib}, {remainder, 300 * mib}}, reorgFiles(5, mib)...)...)
	got, ok := tailTier(0.15, 256*mib, 1024*mib).selectTail(c)
	if !ok || len(got.Files) != 5 {
		t.Fatalf("with a 256 MiB sealed size only the reorg files are tail: (%v, %v)", got.Files, ok)
	}
}

// Only _daily outputs can be sealed. A big hour-level file stays tail, which
// here puts the tail over the ratio: the day folds.
func TestDailyTailOnly_LargeHourFileIsTail(t *testing.T) {
	day := "posthog/events/2026/10/21"
	hour := dayFile{"05/events_20261021_060500_1_compacted.parquet", 600 * mib}
	c := tailDayCandidate(day, append([]dayFile{{sealedA, 1000 * mib}, hour}, reorgFiles(3, mib)...)...)
	if got, ok := tailTier(0.15, 0, 1024*mib).selectTail(c); ok {
		t.Fatalf("the 600 MiB hour file is tail (603 MiB >= 15%% of 1000): want a fold, got tail %v", got.Files)
	}
}

// Sealing needs the size floor and the ratio, both inclusive. The ratio bound
// keeps a tail output tail until it reaches the fold size: with the floor
// alone a 140 MiB tail would turn sealed at 128 MiB and never fold.
func TestDailyTailOnly_SealedBounds(t *testing.T) {
	day := "posthog/lifecycle/2026/10/21"
	tailOut := "events_20261019_031500_dddddddd_daily.parquet" // listed before the sealed file

	t.Run("exactly the floor is sealed", func(t *testing.T) {
		c := tailDayCandidate(day, append([]dayFile{{sealedA, 512 * mib}}, reorgFiles(3, mib)...)...)
		got, ok := tailTier(0.15, 0, 1024*mib).selectTail(c)
		if !ok || slices.Contains(got.Files, day+"/"+sealedA) {
			t.Fatalf("a 512 MiB daily file is sealed at a 512 MiB floor: (%v, %v)", got.Files, ok)
		}
	})
	t.Run("above the floor, below the ratio: tail", func(t *testing.T) {
		c := tailDayCandidate(day, append([]dayFile{{tailOut, 140 * mib}, {sealedA, 1000 * mib}}, reorgFiles(5, mib)...)...)
		got, ok := tailTier(0.15, 128*mib, 1024*mib).selectTail(c)
		if !ok || !slices.Equal(got.Files, append([]string{c.Files[0]}, c.Files[2:]...)) {
			t.Fatalf("140 MiB < 15%% of 1000: the tail output must stay tail: (%v, %v)", got.Files, ok)
		}
	})
	t.Run("exactly the ratio is sealed", func(t *testing.T) {
		c := tailDayCandidate(day, append([]dayFile{{tailOut, 150 * mib}, {sealedA, 1000 * mib}}, reorgFiles(5, mib)...)...)
		got, ok := tailTier(0.15, 128*mib, 1024*mib).selectTail(c)
		if !ok || !slices.Equal(got.Files, c.Files[2:]) {
			t.Fatalf("150 MiB = 15%% of 1000: sealed, only the reorg files are tail: (%v, %v)", got.Files, ok)
		}
	})
}

// A fold that finds no sealed file with room merges the tail on its own; the
// output is at the ratio, so the next cycle counts it as sealed. Listing
// order as in storage: the sealed file, the newer tail output, reorg files.
func TestDailyTailOnly_FoldWithoutRoomSealsTheTail(t *testing.T) {
	day := "posthog/lifecycle/2026/10/21"
	tier := tailTier(0.15, 128*mib, 1024*mib)

	c := tailDayCandidate(day, append([]dayFile{{sealedA, 1000 * mib}, {remainder, 140 * mib}}, reorgFiles(20, mib)...)...)
	if _, ok := tier.selectTail(c); ok {
		t.Fatal("160 MiB >= 15% of 1000: want a fold")
	}
	batches, _ := withoutLoneBatches(SplitCandidateByBudget(c, 30, 1024*mib))
	if len(batches) != 1 || !slices.Equal(batches[0].Files, c.Files[1:]) {
		t.Fatalf("batches = %v, want the tail alone (no room next to 1000 MiB)", batchFiles(batches))
	}

	next := tailDayCandidate(day, append([]dayFile{{sealedA, 1000 * mib}, {remainder, 160 * mib}}, reorgFiles(2, mib)...)...)
	got, ok := tier.selectTail(next)
	if !ok || !slices.Equal(got.Files, next.Files[2:]) {
		t.Fatalf("the 160 MiB output is sealed now, only new reorg files are tail: (%v, %v)", got.Files, ok)
	}
}

// A tail of one file (the earlier tail output, or one reorg file) has nothing
// to merge with: no job, and not the sealed file either.
func TestDailyTailOnly_SingleFileTailMakesNoJob(t *testing.T) {
	day := "posthog/lifecycle/2026/10/21"
	tier := tailTier(0.15, 0, 1024*mib)
	for name, tail := range map[string]dayFile{
		"tail output": {remainder, 40 * mib},
		"reorg file":  {"events_reorg_job01_0.parquet", mib},
	} {
		t.Run(name, func(t *testing.T) {
			c := tailDayCandidate(day, dayFile{sealedA, 840 * mib}, tail)
			got, ok := tier.selectTail(c)
			if !ok || !slices.Equal(got.Files, []string{day + "/" + tail.name}) {
				t.Fatalf("selectTail = (%v, %v), want the one tail file", got.Files, ok)
			}
			batches, _ := withoutLoneBatches(SplitCandidateByBudget(got, 30, 1024*mib))
			if len(batches) != 0 {
				t.Fatalf("batches = %v, want none", batchFiles(batches))
			}
		})
	}
}

// The manager applies the rule between the size lookup and the budget split:
// a real daily tier over real files (sizes in bytes, budget 1024 bytes).
func TestDailyCycle_TailOnly(t *testing.T) {
	day := "db/cpu/2025/10/21"
	write := func(rig *ilRig, files map[string]int) {
		for f, n := range files {
			if err := rig.m.StorageBackend.Write(context.Background(), day+"/"+f, []byte(strings.Repeat("x", n))); err != nil {
				t.Fatal(err)
			}
		}
	}
	run := func(rig *ilRig, ratio float64) [][]string {
		rig.m.MaxFilesPerBatch = 30
		rig.m.Tiers = []Tier{NewDailyTier(&DailyTierConfig{
			StorageBackend: rig.m.StorageBackend,
			MinFiles:       8,
			MaxOutputBytes: 1024,
			TailOnlyRatio:  ratio,
			Enabled:        true,
			Logger:         zerolog.Nop(),
		})}
		var mu sync.Mutex
		var ran [][]string
		rig.m.compactBatchForTest = func(_ context.Context, b Candidate) error {
			mu.Lock()
			defer mu.Unlock()
			ran = append(ran, append([]string(nil), b.Files...))
			return nil
		}
		if err := waitDone(t, rig.startCycle(context.Background(), "daily")); err != nil {
			t.Fatalf("daily cycle: %v", err)
		}
		if recs := rig.records(); len(recs) == 0 || recs[len(recs)-1].Failed != 0 {
			t.Fatalf("cycle records = %+v, want a cycle without failures", recs)
		}
		mu.Lock()
		defer mu.Unlock()
		return ran
	}

	oldDay := func() (files map[string]int, tail []string) {
		files = map[string]int{"cpu_20251022_031500_aaaaaaaa_daily.parquet": 1000, "cpu_20251022_034000_bbbbbbbb_daily.parquet": 900, "cpu_20251022_041000_cccccccc_daily.parquet": 30}
		for i := 0; i < 20; i++ {
			name := fmt.Sprintf("cpu_reorg_job%02d_0.parquet", i)
			files[name] = 1
			tail = append(tail, day+"/"+name)
		}
		return files, append(tail, day+"/cpu_20251022_041000_cccccccc_daily.parquet")
	}

	t.Run("ratio 0 keeps the sealed file in the batch", func(t *testing.T) {
		rig := newILRig(t, 1, false, nil, nil)
		files, _ := oldDay()
		write(rig, files)
		ran := run(rig, 0)
		if len(ran) != 1 || !slices.Contains(ran[0], day+"/cpu_20251022_034000_bbbbbbbb_daily.parquet") {
			t.Fatalf("batches run = %v, want one that rewrites the 900-byte sealed file", ran)
		}
	})

	t.Run("old day", func(t *testing.T) {
		rig := newILRig(t, 1, false, nil, nil)
		files, tail := oldDay()
		write(rig, files)

		ran := run(rig, 0.15)
		if len(ran) != 1 {
			t.Fatalf("batches run = %v, want one", ran)
		}
		got := slices.Clone(ran[0])
		slices.Sort(got)
		slices.Sort(tail)
		if !slices.Equal(got, tail) {
			t.Fatalf("batch = %v, want the 21 tail files only", got)
		}
	})

	t.Run("single-file tail, two cycles", func(t *testing.T) {
		rig := newILRig(t, 1, false, nil, nil)
		write(rig, map[string]int{"cpu_20251022_031500_aaaaaaaa_daily.parquet": 840, "cpu_reorg_job01_0.parquet": 1})
		for i := 0; i < 2; i++ {
			if ran := run(rig, 0.15); len(ran) != 0 {
				t.Fatalf("cycle %d ran %v, want nothing", i+1, ran)
			}
		}
	})
}
