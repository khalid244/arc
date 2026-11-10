package compaction

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
)

// Fork: an interleaved hourly pass (hourly_during_daily) is a cycle body of
// its own; it must record a too-wide strike like a scheduled hourly cycle and
// skip a remembered partition, without changing when it runs.
func TestInterleavedHourlyPassHonoursTheTooWideMemo(t *testing.T) {
	d := ilCandidate("daily", "db/cpu/2026/11/01", 20) // two batches: a yield point between them
	h := ilCandidate("hourly", "db/cpu/2026/11/08/03", 4)
	rig := newILRig(t, 1, true, []Candidate{d}, []Candidate{h})
	m := rig.m

	// Hourly batches go through the real splitter, every attempt out of
	// memory; daily batches stay on the rig.
	var hourlyAttempts atomic.Int32
	m.attemptForTest = func(context.Context, Candidate) error {
		hourlyAttempts.Add(1)
		return errMemory()
	}
	m.compactBatchForTest = func(ctx context.Context, b Candidate) error {
		if b.Tier == "hourly" {
			return m.compactFilesAdaptively(ctx, b, b.Files, 0, "", 0)
		}
		return rig.runBatch(ctx, b)
	}

	interleavedDaily := func() CycleRecord {
		t.Helper()
		before := len(rig.recordsBySource(cycleSourceInterleaved))
		started, release := rig.hold("db/cpu/2026/11/01#1")
		done := rig.startCycle(context.Background(), "daily")
		waitClosed(t, started, "first daily batch")
		rig.requestHourly()
		release()
		if err := waitDone(t, done); err != nil {
			t.Fatalf("daily cycle: %v", err)
		}
		inter := rig.recordsBySource(cycleSourceInterleaved)
		if len(inter) != before+1 {
			t.Fatalf("interleaved passes = %d, want %d", len(inter), before+1)
		}
		if n := len(rig.recordsBySource(cycleSourceCoalesced)); n != 0 {
			t.Fatalf("hourly ran as a coalesced drain (%d), not inside the daily cycle", n)
		}
		if got := rig.pendingSnapshot(); len(got) != 0 {
			t.Fatalf("pending after the daily cycle = %v", got)
		}
		newest := inter[0]
		for _, r := range inter {
			if r.CycleID > newest.CycleID {
				newest = r
			}
		}
		return newest
	}

	// First daily cycle: the interleaved pass tries the hour (4 files, then
	// 2 + 2: the first 2 is as small as the splitter gets) and strikes it.
	ir := interleavedDaily()
	if ir.Failed != 1 || ir.SkippedTooWide != 0 || hourlyAttempts.Load() != 2 {
		t.Fatalf("first interleaved pass: %+v, attempts %d", ir, hourlyAttempts.Load())
	}
	e := m.TooWidePartitions()
	if len(e) != 1 || e[0].Tier != "hourly" || e[0].Partition != h.PartitionPath || !e[0].Memoized() || e[0].Reason != "memory_error" {
		t.Fatalf("memo after the interleaved pass: %+v", e)
	}
	if !slices.Equal(e[0].Files, h.Files) {
		t.Fatalf("memo files %v, want %v", e[0].Files, h.Files)
	}

	// Second daily cycle: the interleaved pass leaves the hour alone, and
	// still runs at the same point (between the two daily batches).
	ir = interleavedDaily()
	if ir.SkippedTooWide != 1 || ir.Discovered != 0 || ir.Started != 0 || hourlyAttempts.Load() != 2 {
		t.Fatalf("second interleaved pass: %+v, attempts %d", ir, hourlyAttempts.Load())
	}
	events := rig.snapshot()
	if i, j := indexOf(events, "daily:db/cpu/2026/11/01#1:end"), indexOf(events, "daily:db/cpu/2026/11/01#2:start"); i < 0 || j < i {
		t.Fatalf("daily batches out of order: %v", events)
	}
	if len(m.TooWidePartitions()) != 1 {
		t.Fatal("the interleaved pass pruned an hour it discovered")
	}

	// A scheduled hourly cycle shares the memo.
	id, err := m.RunCompactionCycleForTiers(context.Background(), []string{"hourly"})
	if err != nil {
		t.Fatal(err)
	}
	if rec, _, _, _, _ := m.CycleLookup(id); rec.SkippedTooWide != 1 || hourlyAttempts.Load() != 2 {
		t.Fatalf("hourly cycle after the interleaved strike: %+v, attempts %d", rec, hourlyAttempts.Load())
	}
}

// An interleaved hourly pass skips hours inside a day the daily cycle has not
// finished before anything else; such an hour is still "seen", so the pass
// does not forget its memo entry.
func TestInterleavedHourlyPassKeepsTheMemoOfAnHourInsideAnOpenDay(t *testing.T) {
	d := ilCandidate("daily", "db/cpu/2026/11/01", 20)
	h := ilCandidate("hourly", "db/cpu/2026/11/01/05", 4) // inside the open day
	rig := newILRig(t, 1, true, []Candidate{d}, []Candidate{h})
	m := rig.m
	m.tooWide.strike("hourly", h.PartitionPath, h.Files, "memory_error", m.TooWideSettings())

	started, release := rig.hold("db/cpu/2026/11/01#1")
	done := rig.startCycle(context.Background(), "daily")
	waitClosed(t, started, "first daily batch")
	rig.requestHourly()
	release()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}
	inter := rig.recordsBySource(cycleSourceInterleaved)
	if len(inter) != 1 || inter[0].SkippedTooWide != 0 || inter[0].Discovered != 0 {
		t.Fatalf("interleaved pass: %+v", inter)
	}
	if len(m.TooWidePartitions()) != 1 {
		t.Fatal("an hour skipped as inside an open day was forgotten")
	}
}
