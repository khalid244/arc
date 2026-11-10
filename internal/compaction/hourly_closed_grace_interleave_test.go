package compaction

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// The closed-hour grace inside a daily cycle (hourly_during_daily): the
// interleaved pass runs the real hourly tier, so a closed hour is merged at the
// next yield point with its stragglers younger than the grace held back, and an
// hour the grace made eligible is still left alone while it lies inside a day
// partition the daily cycle has listed and not finished.
func TestInterleave_ClosedGraceHourlyPass(t *testing.T) {
	now := time.Date(2026, 11, 9, 1, 30, 0, 0, time.UTC)
	d1 := ilCandidate("daily", "db/cpu/2026/11/01", 20)
	dOpen := ilCandidate("daily", "db/cpu/2026/11/08", 10)
	rig := newILRig(t, 1, true, []Candidate{d1, dOpen}, nil)
	ctx := context.Background()

	raw := func(h, w time.Time, i int) string {
		t.Helper()
		p := fmt.Sprintf("db/cpu/%04d/%02d/%02d/%02d/cpu_pod-a_%s_%09d.parquet",
			h.Year(), int(h.Month()), h.Day(), h.Hour(), w.Format("20060102_150405"), i)
		if err := rig.m.StorageBackend.Write(ctx, p, []byte("PAR1FIXTUREPAR1")); err != nil {
			t.Fatal(err)
		}
		return p
	}
	hour00 := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	settled := []string{
		raw(hour00, hour00.Add(10*time.Minute), 1),
		raw(hour00, hour00.Add(50*time.Minute), 2),
		raw(hour00, hour00.Add(75*time.Minute), 3),
	}
	straggler := raw(hour00, now.Add(-5*time.Minute), 4)
	hour23 := time.Date(2026, 11, 8, 23, 0, 0, 0, time.UTC)
	raw(hour23, hour23.Add(20*time.Minute), 1)
	raw(hour23, now.Add(-40*time.Minute), 2)
	// Hour 22 of the open day: an output plus one old raw file, which only the
	// lone-file re-merge selects.
	hour22 := time.Date(2026, 11, 8, 22, 0, 0, 0, time.UTC)
	if err := rig.m.StorageBackend.Write(ctx, "db/cpu/2026/11/08/22/cpu_20261109_002000_0000000a_compacted.parquet", []byte("PAR1FIXTUREPAR1")); err != nil {
		t.Fatal(err)
	}
	raw(hour22, hour22.Add(30*time.Minute), 1)

	rig.m.Tiers[0] = graceTier(rig.m.StorageBackend, nil, 10*time.Minute, now)
	if cs, err := rig.m.Tiers[0].FindCandidates(ctx, "db", "cpu"); err != nil {
		t.Fatal(err)
	} else if !slices.Contains(candidatePaths(cs), "db/cpu/2026/11/08/22") {
		t.Fatalf("precondition: the tier must select hour 22, got %v", candidatePaths(cs))
	}

	var mu sync.Mutex
	hourly := map[string][]string{}
	rig.onStart = func(b Candidate) {
		if b.Tier != "hourly" {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		hourly[b.PartitionPath] = append([]string(nil), b.Files...)
	}
	started, release := rig.hold("db/cpu/2026/11/01#1")
	done := rig.startCycle(ctx, "daily")
	waitClosed(t, started, "daily 01 batch 1")
	rig.requestHourly()
	release()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}

	if n := len(rig.recordsBySource(cycleSourceInterleaved)); n != 1 {
		t.Fatalf("interleaved records = %d, want 1", n)
	}
	mu.Lock()
	defer mu.Unlock()
	got, ok := hourly["db/cpu/2026/11/09/00"]
	if !ok {
		t.Fatalf("closed hour 00 not merged by the interleaved pass; hourly batches: %v", hourly)
	}
	slices.Sort(got)
	slices.Sort(settled)
	if !slices.Equal(got, settled) {
		t.Fatalf("hour 00 batch = %v, want the settled files %v (the straggler %s waits for the next tick)", got, settled, straggler)
	}
	if files, ok := hourly["db/cpu/2026/11/08/23"]; ok {
		t.Fatalf("hour 23 lies in day 11/08, which the daily cycle has listed and not finished; the interleaved pass ran it with %v", files)
	}
	if files, ok := hourly["db/cpu/2026/11/08/22"]; ok {
		t.Fatalf("hour 22 lies in the unfinished day 11/08; the interleaved pass re-merged it with %v", files)
	}
	events := rig.snapshot()
	hs := indexOf(events, "hourly:db/cpu/2026/11/09/00#1:start")
	if hs < 0 || hs < indexOf(events, "daily:db/cpu/2026/11/01#1:end") || hs > indexOf(events, "daily:db/cpu/2026/11/01#2:start") {
		t.Fatalf("hourly batch did not run between the two daily 01 batches: %v", events)
	}
}
