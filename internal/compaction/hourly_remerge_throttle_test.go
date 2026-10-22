package compaction

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// The grace path re-merges an hour's output with the raw files that arrived
// since. Unthrottled, that ran on every */10 tick (6-7 rewrites of the hour
// file between H+1h10m and H+3h10m). The throttle holds such a re-merge until
// RemergeMinFiles raw files are queued or the oldest has waited RemergeMaxWait.

func throttledTier(f *graceFixture, cache *PartitionCache, now time.Time, minFiles int, maxWait time.Duration) *HourlyTier {
	tier := graceTier(f.backend, cache, 10*time.Minute, now)
	tier.RemergeMinFiles = minFiles
	tier.RemergeMaxWait = maxWait
	return tier
}

// rawRun writes n raw files into hour h, spread one per 10s from w.
func (f *graceFixture) rawRun(t *testing.T, h, w time.Time, n int) []string {
	t.Helper()
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, f.raw(t, h, w.Add(time.Duration(i)*10*time.Second), 100+i))
	}
	return out
}

func TestHourlyRemergeThrottle_BelowThresholdWaits(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h := at(11, 0)
		f.compacted(t, h, at(12, 11), 1)
		f.rawRun(t, h, at(12, 12), 10) // 12:12:00..12:13:30, oldest 18m old
		f.raw(t, h, at(12, 25), 1)     // younger than the grace
		got, err := throttledTier(f, cache(), graceNow, 30, 30*time.Minute).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		if c := candidateFor(got, h); c != nil {
			t.Fatalf("10 raw files (< 30), oldest 18m (< 30m): re-merge must wait; got %d files", len(c.Files))
		}

		// The same listing without the throttle re-merges (the Fix B behavior).
		got, err = throttledTier(f, cache(), graceNow, 0, 30*time.Minute).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		if candidateFor(got, h) == nil {
			t.Fatal("throttle off: hour 11 must re-merge on this tick")
		}
	})
}

func TestHourlyRemergeThrottle_ThresholdMetMerges(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h := at(11, 0)
		out := f.compacted(t, h, at(12, 11), 1)
		raw := f.rawRun(t, h, at(12, 12), 30) // 12:12:00..12:16:50, oldest 18m old
		f.raw(t, h, at(12, 25), 1)            // younger than the grace: not counted, not taken
		got, err := throttledTier(f, cache(), graceNow, 30, 30*time.Minute).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		cand := candidateFor(got, h)
		if cand == nil {
			t.Fatal("30 settled raw files: re-merge must run")
		}
		sameFiles(t, cand, append([]string{out}, raw...)...)
	})
}

func TestHourlyRemergeThrottle_MaxWaitMerges(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h := at(11, 0)
		now := at(12, 45)
		out := f.compacted(t, h, at(12, 11), 1)
		a := f.raw(t, h, at(12, 14), 1) // 31m old: past the max wait
		b := f.raw(t, h, at(12, 20), 2)
		c := f.raw(t, h, at(12, 30), 3)
		f.raw(t, h, at(12, 40), 4) // younger than the grace
		got, err := throttledTier(f, cache(), now, 30, 30*time.Minute).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		cand := candidateFor(got, h)
		if cand == nil {
			t.Fatal("oldest raw file waited 31m (max 30m): re-merge must run")
		}
		sameFiles(t, cand, out, a, b, c)

		// One minute earlier the oldest has waited 29m: still held.
		got, err = throttledTier(f, cache(), at(12, 43), 30, 30*time.Minute).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		if c := candidateFor(got, h); c != nil {
			t.Fatalf("oldest raw file waited 29m: re-merge must wait; got %v", c.Files)
		}
	})
}

// The first pass (no output yet) and fragment re-merges (no raw files) are
// never held.
func TestHourlyRemergeThrottle_FirstPassAndFragmentsNotHeld(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h11, h10 := at(11, 0), at(10, 0)
		a := f.raw(t, h11, at(12, 5), 1)
		b := f.raw(t, h11, at(12, 15), 2)
		o1 := f.compacted(t, h10, at(12, 25), 1)
		o2 := f.compacted(t, h10, at(12, 25), 2)
		f.raw(t, h10, at(12, 26), 3) // keeps hour 10 off the newest-file gate
		got, err := throttledTier(f, cache(), graceNow, 30, 30*time.Minute).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		if c := candidateFor(got, h11); c == nil {
			t.Fatal("first pass of hour 11 must not be throttled")
		} else {
			sameFiles(t, c, a, b)
		}
		if c := candidateFor(got, h10); c == nil {
			t.Fatal("fragment re-merge of hour 10 must not be throttled")
		} else {
			sameFiles(t, c, o1, o2)
		}
	})
}

// Once the newest-file gate passes, the throttle is out of the picture: the
// partition is selected exactly as without a grace. A max wait longer than
// the gate would hold these files if the throttle applied.
func TestHourlyRemergeThrottle_NewestFileGatePathUnaffected(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h := at(11, 0)
		now := at(13, 30) // cutoff 12:30
		out := f.compacted(t, h, at(12, 11), 1)
		a := f.raw(t, h, at(12, 12), 1)
		b := f.raw(t, h, at(12, 15), 2)
		for _, grace := range []time.Duration{0, 10 * time.Minute} {
			tier := throttledTier(f, cache(), now, 30, 3*time.Hour)
			tier.ClosedGrace = grace
			got, err := tier.FindCandidates(context.Background(), "db", "m")
			if err != nil {
				t.Fatal(err)
			}
			cand := candidateFor(got, h)
			if cand == nil {
				t.Fatalf("grace %v: newest file 1h15m old, hour 11 must be a candidate", grace)
			}
			sameFiles(t, cand, out, a, b)
		}
	})
}

func TestHourlyRemergeThrottle_TierConfig(t *testing.T) {
	tier := NewHourlyTier(&HourlyTierConfig{
		MinAgeHours: 1, MinFiles: 3, Enabled: true, Logger: zerolog.Nop(),
		ClosedGraceMinutes: 10, ClosedRemergeMinFiles: 30, ClosedRemergeMaxWaitMinutes: 30,
	})
	if tier.RemergeMinFiles != 30 || tier.RemergeMaxWait != 30*time.Minute {
		t.Fatalf("throttle = %d/%v, want 30/30m", tier.RemergeMinFiles, tier.RemergeMaxWait)
	}
	off := NewHourlyTier(&HourlyTierConfig{
		MinAgeHours: 1, MinFiles: 3, Enabled: true, Logger: zerolog.Nop(),
		ClosedRemergeMinFiles: 30, ClosedRemergeMaxWaitMinutes: 30,
	})
	if off.ClosedGrace != 0 || off.RemergeMinFiles != 0 || off.RemergeMaxWait != 0 {
		t.Fatalf("no grace: throttle must stay off, got grace %v throttle %d/%v", off.ClosedGrace, off.RemergeMinFiles, off.RemergeMaxWait)
	}
}
