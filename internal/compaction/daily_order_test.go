package compaction

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// dayCandidate builds a daily candidate for db/<meas>/<day> ("2026/11/13")
// holding hour hour-level files, sealed _daily outputs and reorg day-level
// files, in that order.
func dayCandidate(t *testing.T, meas, day string, hour, sealed, reorg int) Candidate {
	t.Helper()
	pt, err := time.Parse("2006/01/02", day)
	if err != nil {
		t.Fatal(err)
	}
	path := "db/" + meas + "/" + day
	c := Candidate{Database: "db", Measurement: meas, PartitionPath: path, PartitionTime: pt, Tier: dailyTierName}
	stamp := pt.Format("20060102")
	for i := 0; i < hour; i++ {
		c.Files = append(c.Files, fmt.Sprintf("%s/%02d/%s_%s_%02d0000_%d.parquet", path, i%24, meas, stamp, i%24, i))
	}
	for i := 0; i < sealed; i++ {
		c.Files = append(c.Files, fmt.Sprintf("%s/%s_%s_000000_%d_daily.parquet", path, meas, stamp, i))
	}
	for i := 0; i < reorg; i++ {
		c.Files = append(c.Files, fmt.Sprintf("%s/%s_reorg_job%d_0.parquet", path, meas, i))
	}
	c.FileCount = len(c.Files)
	return c
}

func queuedDay(t *testing.T, meas, day string, hour, sealed, reorg, perBatch int) queuedPartition {
	t.Helper()
	c := dayCandidate(t, meas, day, hour, sealed, reorg)
	return queuedPartition{path: c.PartitionPath, batches: SplitCandidateIntoBatches(c, perBatch), files: c.Files}
}

func queuedPaths(q []queuedPartition) []string {
	out := make([]string, len(q))
	for i, p := range q {
		out[i] = p.path
	}
	return out
}

// Days still holding hour-level files come first, newest day first, across
// every measurement; a day cut off after its first batch (a _daily file next
// to hour folders) still counts as one. Re-merges follow, most unsealed files
// first, so an old day with many late reorg files is not starved by newer
// days that only need one folded in.
func TestDailyPriorityOrder_CrossMeasurement(t *testing.T) {
	queued := []queuedPartition{
		queuedDay(t, "events", "2026/11/10", 0, 1, 1, 30),
		queuedDay(t, "lifecycle", "2026/10/20", 0, 1, 6, 30),
		queuedDay(t, "events", "2026/11/13", 101, 0, 0, 40),
		queuedDay(t, "sort_order_support_report", "2026/11/11", 0, 2, 0, 30),
		queuedDay(t, "lifecycle", "2026/11/14", 30, 0, 0, 30),
		queuedDay(t, "sort_order_support_report", "2026/11/12", 60, 1, 0, 30),
	}
	orderDailyByPriority(queued)

	want := []string{
		"db/lifecycle/2026/11/14",
		"db/events/2026/11/13",
		"db/sort_order_support_report/2026/11/12", // cut off mid-way: first merge
		"db/lifecycle/2026/10/20",                 // 6 reorg files
		"db/events/2026/11/10",                    // 1 reorg file
		"db/sort_order_support_report/2026/11/11", // fragmented, nothing unsealed
	}
	if got := queuedPaths(queued); !slices.Equal(got, want) {
		t.Fatalf("order =\n%v\nwant\n%v", got, want)
	}

	// The batches of one day stay one entry, in batch order.
	ev := queued[1]
	if len(ev.batches) != 3 {
		t.Fatalf("events 11-13 batches = %d, want 3", len(ev.batches))
	}
	for i, b := range ev.batches {
		if b.BatchNumber != i+1 || b.PartitionPath != ev.path {
			t.Fatalf("events 11-13 batch %d = %s#%d", i, b.PartitionPath, b.BatchNumber)
		}
	}
}

// A day cut off after some of its batches holds _daily outputs AND hour
// folders. It still needs its first merge finished, so it ranks with the
// never-merged days, ahead of a newer re-merge with many reorg files.
func TestDailyPriorityOrder_CutOffMultiBatchDay(t *testing.T) {
	queued := []queuedPartition{
		queuedDay(t, "events", "2026/11/14", 0, 1, 20, 30),
		queuedDay(t, "events", "2026/11/09", 50, 2, 0, 30),
	}
	orderDailyByPriority(queued)
	want := []string{"db/events/2026/11/09", "db/events/2026/11/14"}
	if got := queuedPaths(queued); !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

// Ties: first merges of one day across measurements, and re-merges with the
// same unsealed count, go newest day first and then by path, whatever order
// discovery delivered them in.
func TestDailyPriorityOrder_Ties(t *testing.T) {
	build := func() []queuedPartition {
		return []queuedPartition{
			queuedDay(t, "b", "2026/11/13", 12, 0, 0, 30),
			queuedDay(t, "a", "2026/11/13", 40, 0, 0, 30),
			queuedDay(t, "a", "2026/11/12", 90, 0, 0, 30), // more files, older day
			queuedDay(t, "a", "2026/11/01", 0, 1, 2, 30),
			queuedDay(t, "b", "2026/11/05", 0, 1, 2, 30),
			queuedDay(t, "a", "2026/11/05", 0, 1, 2, 30),
		}
	}
	want := []string{
		"db/a/2026/11/13",
		"db/b/2026/11/13",
		"db/a/2026/11/12",
		"db/a/2026/11/05",
		"db/b/2026/11/05",
		"db/a/2026/11/01",
	}
	base := build()
	for shift := range base {
		queued := append(append([]queuedPartition{}, base[shift:]...), base[:shift]...)
		slices.Reverse(queued[:shift])
		orderDailyByPriority(queued)
		if got := queuedPaths(queued); !slices.Equal(got, want) {
			t.Fatalf("shift %d: order =\n%v\nwant\n%v", shift, got, want)
		}
	}
}

// Only the files the batches will merge count: a file dropped from every
// batch (a lone file, an oversize one) does not rank its day.
func TestDailyPriorityOrder_RanksBatchFilesOnly(t *testing.T) {
	remerge := queuedDay(t, "events", "2026/11/14", 0, 1, 1, 30)
	remerge.files = append(remerge.files, "db/events/2026/11/14/03/events_20261114_030000_1.parquet")
	queued := []queuedPartition{
		remerge,
		queuedDay(t, "events", "2026/11/02", 0, 1, 3, 30),
	}
	orderDailyByPriority(queued)
	want := []string{"db/events/2026/11/02", "db/events/2026/11/14"}
	if got := queuedPaths(queued); !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

// With compaction.daily_order = "priority" the manager holds every daily
// batch until discovery of all measurements ends and launches them in the
// global order, with or without hourly_during_daily.
func TestDailyPriorityOrder_ManagerLaunchOrder(t *testing.T) {
	for _, interleave := range []bool{false, true} {
		t.Run(fmt.Sprintf("hourly_during_daily=%v", interleave), func(t *testing.T) {
			cpu := []Candidate{
				dayCandidate(t, "cpu", "2026/11/10", 0, 1, 1),
				dayCandidate(t, "cpu", "2026/11/13", 25, 0, 0),
			}
			rig := newILRig(t, 1, interleave, cpu, nil)
			rig.m.DailyOrder = DailyOrderPriority
			others := map[string][]Candidate{
				"mem": {
					dayCandidate(t, "mem", "2026/10/20", 0, 1, 4),
					dayCandidate(t, "mem", "2026/11/14", 12, 0, 0),
				},
				"disk": {dayCandidate(t, "disk", "2026/11/12", 12, 1, 0)},
			}
			for meas := range others {
				if err := rig.m.StorageBackend.Write(context.Background(), "db/"+meas+"/2026/11/01/00/seed.parquet", []byte("x")); err != nil {
					t.Fatal(err)
				}
			}
			rig.dailyFind = func(_ context.Context, measurement string) ([]Candidate, error) {
				return others[measurement], nil
			}

			if err := waitDone(t, rig.startCycle(context.Background(), "daily")); err != nil {
				t.Fatalf("daily cycle: %v", err)
			}
			var starts []string
			for _, ev := range rig.snapshot() {
				if s, ok := strings.CutSuffix(ev, ":start"); ok {
					starts = append(starts, s)
				}
			}
			want := []string{
				"daily:db/mem/2026/11/14#1", "daily:db/mem/2026/11/14#2",
				"daily:db/cpu/2026/11/13#1", "daily:db/cpu/2026/11/13#2", "daily:db/cpu/2026/11/13#3",
				"daily:db/disk/2026/11/12#1", "daily:db/disk/2026/11/12#2",
				"daily:db/mem/2026/10/20#1",
				"daily:db/cpu/2026/11/10#1",
			}
			if !slices.Equal(starts, want) {
				t.Fatalf("batch starts =\n%v\nwant\n%v", starts, want)
			}
		})
	}
}

// A DELETE rewrite (<logical>_rewrite_<n>.parquet) keeps its file's role: a
// rewritten _daily output stays sealed, rewritten hour-level and reorg files
// stay unsealed.
func TestDailyPriorityOrder_RewrittenFiles(t *testing.T) {
	rewrite := func(f string) string {
		return strings.TrimSuffix(f, ".parquet") + "_rewrite_1731715200000000000.parquet"
	}

	sealedRewritten := queuedDay(t, "events", "2026/11/14", 0, 1, 1, 30)
	for i, f := range sealedRewritten.batches[0].Files {
		if strings.HasSuffix(f, "_daily.parquet") {
			sealedRewritten.batches[0].Files[i] = rewrite(f)
		}
	}
	reorgRewritten := queuedDay(t, "events", "2026/11/03", 0, 1, 2, 30)
	hourRewritten := queuedDay(t, "events", "2026/11/01", 2, 1, 0, 30)
	for _, q := range []queuedPartition{reorgRewritten, hourRewritten} {
		for i, f := range q.batches[0].Files {
			if !strings.HasSuffix(f, "_daily.parquet") {
				q.batches[0].Files[i] = rewrite(f)
			}
		}
	}

	if r := rankDailyPartition(sealedRewritten); r.firstMerge || r.unsealed != 1 {
		t.Fatalf("rewritten _daily day rank = %+v, want a re-merge with 1 unsealed file", r)
	}
	if r := rankDailyPartition(reorgRewritten); r.firstMerge || r.unsealed != 2 {
		t.Fatalf("rewritten reorg day rank = %+v, want a re-merge with 2 unsealed files", r)
	}
	if r := rankDailyPartition(hourRewritten); !r.firstMerge || r.unsealed != 2 {
		t.Fatalf("rewritten hour-file day rank = %+v, want a first merge with 2 unsealed files", r)
	}

	queued := []queuedPartition{sealedRewritten, reorgRewritten, hourRewritten}
	orderDailyByPriority(queued)
	want := []string{"db/events/2026/11/01", "db/events/2026/11/03", "db/events/2026/11/14"}
	if got := queuedPaths(queued); !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

// blockedMemRig is a daily rig with one more measurement, mem, whose
// discovery blocks until released: cpu's candidates are found while mem is
// still listing.
func blockedMemRig(t *testing.T, order string, cpu []Candidate) (rig *ilRig, memEntered <-chan struct{}, releaseMem func()) {
	t.Helper()
	rig = newILRig(t, 1, false, cpu, nil)
	rig.m.DailyOrder = order
	if err := rig.m.StorageBackend.Write(context.Background(), "db/mem/2026/11/01/00/seed.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	rig.dailyFind = func(ctx context.Context, measurement string) ([]Candidate, error) {
		if measurement != "mem" {
			return nil, nil
		}
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []Candidate{dayCandidate(t, "mem", "2026/11/14", 12, 0, 0)}, nil
	}
	return rig, entered, func() { once.Do(func() { close(release) }) }
}

// Without hourly_during_daily only "priority" holds daily batches until
// discovery ends; "discovery" starts each as soon as it is found.
func TestDailyOrder_HoldsUntilDiscoveryEndsOnlyInPriority(t *testing.T) {
	t.Run("discovery", func(t *testing.T) {
		rig, memEntered, releaseMem := blockedMemRig(t, DailyOrderDiscovery, []Candidate{dayCandidate(t, "cpu", "2026/11/10", 0, 1, 1)})
		started, release := rig.hold("db/cpu/2026/11/10#1")
		defer release()
		defer releaseMem()
		done := rig.startCycle(context.Background(), "daily")
		waitClosed(t, memEntered, "mem discovery")
		waitClosed(t, started, "cpu batch while mem discovery is still listing")
		release()
		releaseMem()
		if err := waitDone(t, done); err != nil {
			t.Fatalf("daily cycle: %v", err)
		}
	})

	t.Run("priority", func(t *testing.T) {
		rig, memEntered, releaseMem := blockedMemRig(t, DailyOrderPriority, []Candidate{dayCandidate(t, "cpu", "2026/11/10", 0, 1, 1)})
		defer releaseMem()
		done := rig.startCycle(context.Background(), "daily")
		waitClosed(t, memEntered, "mem discovery")
		time.Sleep(60 * time.Millisecond)
		if ev := rig.snapshot(); len(ev) != 0 {
			t.Fatalf("batches ran while daily discovery was still listing: %v", ev)
		}
		releaseMem()
		if err := waitDone(t, done); err != nil {
			t.Fatalf("daily cycle: %v", err)
		}
		want := []string{
			"daily:db/mem/2026/11/14#1:start", "daily:db/mem/2026/11/14#1:end",
			"daily:db/mem/2026/11/14#2:start", "daily:db/mem/2026/11/14#2:end",
			"daily:db/cpu/2026/11/10#1:start", "daily:db/cpu/2026/11/10#1:end",
		}
		if got := rig.snapshot(); !slices.Equal(got, want) {
			t.Fatalf("events =\n%v\nwant\n%v", got, want)
		}
	})
}

// A pause or cancel in priority mode without hourly_during_daily: during
// discovery no held batch launches; during a batch the next batch and the
// next partition stay unstarted.
func TestDailyPriorityOrder_PauseAndCancel(t *testing.T) {
	cpu := func(t *testing.T) []Candidate {
		return []Candidate{
			dayCandidate(t, "cpu", "2026/11/10", 0, 1, 1),
			dayCandidate(t, "cpu", "2026/11/13", 12, 0, 0), // two batches, ranked first
		}
	}

	t.Run("pause during discovery", func(t *testing.T) {
		rig, memEntered, releaseMem := blockedMemRig(t, DailyOrderPriority, cpu(t))
		defer releaseMem()
		done := rig.startCycle(context.Background(), "daily")
		waitClosed(t, memEntered, "mem discovery")
		rig.paused.Store(true)
		releaseMem()
		if err := waitDone(t, done); !errors.Is(err, ErrCompactionPaused) {
			t.Fatalf("daily cycle err = %v, want ErrCompactionPaused", err)
		}
		if ev := rig.snapshot(); len(ev) != 0 {
			t.Fatalf("held batches launched under the pause: %v", ev)
		}
	})

	t.Run("cancel during discovery", func(t *testing.T) {
		rig, memEntered, releaseMem := blockedMemRig(t, DailyOrderPriority, cpu(t))
		defer releaseMem()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := rig.startCycle(ctx, "daily")
		waitClosed(t, memEntered, "mem discovery")
		cancel()
		if err := waitDone(t, done); !errors.Is(err, context.Canceled) {
			t.Fatalf("daily cycle err = %v, want context.Canceled", err)
		}
		if ev := rig.snapshot(); len(ev) != 0 {
			t.Fatalf("held batches launched after cancellation: %v", ev)
		}
	})

	t.Run("pause during a batch", func(t *testing.T) {
		rig := newILRig(t, 1, false, cpu(t), nil)
		rig.m.DailyOrder = DailyOrderPriority
		started, release := rig.hold("db/cpu/2026/11/13#1")
		defer release()
		done := rig.startCycle(context.Background(), "daily")
		waitClosed(t, started, "first-ranked batch")
		rig.paused.Store(true)
		release()
		if err := waitDone(t, done); !errors.Is(err, ErrCompactionPaused) {
			t.Fatalf("daily cycle err = %v, want ErrCompactionPaused", err)
		}
		want := []string{"daily:db/cpu/2026/11/13#1:start", "daily:db/cpu/2026/11/13#1:end"}
		if got := rig.snapshot(); !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
	})

	t.Run("cancel during a batch", func(t *testing.T) {
		rig := newILRig(t, 1, false, cpu(t), nil)
		rig.m.DailyOrder = DailyOrderPriority
		started, _ := rig.hold("db/cpu/2026/11/13#1") // returns only when the cycle is cancelled
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := rig.startCycle(ctx, "daily")
		waitClosed(t, started, "first-ranked batch")
		cancel()
		if err := waitDone(t, done); !errors.Is(err, context.Canceled) {
			t.Fatalf("daily cycle err = %v, want context.Canceled", err)
		}
		want := []string{"daily:db/cpu/2026/11/13#1:start", "daily:db/cpu/2026/11/13#1:end"}
		if got := rig.snapshot(); !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
	})
}
