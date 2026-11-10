package compaction

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A stager whose PUT of key K committed while the client saw an error, and
// whose HEAD check failed too, PUTs K again later (ingest stager_jobs.go
// uploadOutput). With the closed-hour grace the compactor can merge and delete
// K in between, so the re-PUT brings K's rows back next to the output that
// already holds them. One such file is below hourly_min_files, so nothing
// merged it again until the daily merge the next day.

func repuTier(f *graceFixture, cache *PartitionCache, now time.Time) *HourlyTier {
	tier := throttledTier(f, cache, now, 200, 30*time.Minute)
	tier.MinFiles = 3
	return tier
}

func TestHourlyClosedGrace_LoneRawNextToOutputRemergedWithinMaxWait(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h := at(11, 0)
		out := f.compacted(t, h, at(12, 11), 1)
		k := f.raw(t, h, at(12, 5), 7) // a key the 12:11 merge consumed, PUT again
		find := func(now time.Time) *Candidate {
			t.Helper()
			got, err := repuTier(f, cache(), now).FindCandidates(context.Background(), "db", "m")
			if err != nil {
				t.Fatal(err)
			}
			return candidateFor(got, h)
		}

		if c := find(at(12, 30)); c != nil {
			t.Fatalf("K waited 25m (< 30m): the re-merge must wait; got %v", c.Files)
		}
		if c := find(at(12, 36)); c != nil {
			t.Fatalf("the output is 25m old: at most one re-merge per max wait; got %v", c.Files)
		}
		c := find(at(12, 41))
		if c == nil {
			t.Fatal("one raw file next to the output, waited 36m (max 30m): it must be re-merged with the output although 1 < hourly_min_files (3); it stays double counted until the daily merge otherwise")
		}
		sameFiles(t, c, out, k)

		// Past the newest-file gate the throttle is out of the picture; the
		// lone file still goes.
		c = find(at(13, 30))
		if c == nil {
			t.Fatal("newest file past hourly_min_age_hours: the lone raw file must still be re-merged with the output")
		}
		sameFiles(t, c, out, k)
	})
}

func TestHourlyClosedGrace_LoneRawRemergeScope(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h11, h10 := at(11, 0), at(10, 0)
		f.compacted(t, h11, at(12, 11), 1)
		f.raw(t, h11, at(12, 5), 7)
		// No output yet: a first pass still needs hourly_min_files.
		f.raw(t, h10, at(10, 20), 1)
		f.raw(t, h10, at(10, 40), 2)
		now := at(13, 30)

		off := repuTier(f, cache(), now)
		off.ClosedGrace = 0
		got, err := off.FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("grace off: selection must be unchanged (nothing below hourly_min_files), got %v", candidatePaths(got))
		}

		got, err = repuTier(f, cache(), now).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		if candidateFor(got, h11) == nil {
			t.Fatal("grace on: hour 11 (output + one old raw file) must be re-merged")
		}
		if c := candidateFor(got, h10); c != nil {
			t.Fatalf("hour 10 has no output: 2 raw files stay below hourly_min_files, got %v", c.Files)
		}
	})

	// An hour not yet closed for the grace is never forced, whatever its
	// files' ages.
	tier := repuTier(newGraceFixture(t), nil, at(12, 55))
	open := []string{
		"db/m/2026/10/20/12/m_20261020_121000_000000001_compacted.parquet",
		"db/m/2026/10/20/12/m_pod-a_20261020_120500_000000002.parquet",
	}
	if tier.ShouldCompact(open, at(12, 0)) {
		t.Fatal("open hour 12: an output plus one old raw file must not be forced")
	}
	tier = repuTier(newGraceFixture(t), nil, at(12, 5))
	justClosed := []string{
		"db/m/2026/10/20/11/m_20261020_111000_000000001_compacted.parquet",
		"db/m/2026/10/20/11/m_pod-a_20261020_111500_000000002.parquet",
	}
	if tier.ShouldCompact(justClosed, at(11, 0)) {
		t.Fatal("hour 11 closed 5m ago (grace 10m): must not be forced")
	}
}

// A raw file the job can never consume (the job skips a file with bad Parquet
// magic and leaves it in place) keeps the rule firing. The output's own age
// limits that to one rewrite per max wait instead of one per tick.
func TestHourlyClosedGrace_StuckRawRewritesAtMostOncePerMaxWait(t *testing.T) {
	s := newRemergeSim(t, at(11, 0))
	s.arrive(at(12, 11), s.outputKey(at(12, 11)))
	stuck := s.rawKey(at(12, 5))
	s.arrive(at(12, 5), stuck)
	s.keep = map[string]bool{stuck: true}
	s.run(at(12, 20), at(18, 0))
	for i := 1; i < len(s.merges); i++ {
		if gap := s.merges[i].Sub(s.merges[i-1]); gap < 30*time.Minute {
			t.Fatalf("rewrites %v and %v are %v apart, want >= the 30m max wait", s.merges[i-1], s.merges[i], gap)
		}
	}
	if len(s.merges) == 0 || len(s.merges) > 12 {
		t.Fatalf("rewrites over 5h40m = %d, want 1..12", len(s.merges))
	}
}

// remergeSim drives the real HourlyTier.FindCandidatesFromListing tick by tick
// over one closed hour, with the deploy values (hourly_min_files 3, grace 10,
// re-merge min files 200, max wait 30, */10 ticks), and stands in for the job:
// a selected candidate's files are replaced by one output named at the tick.
type remergeSim struct {
	t      *testing.T
	hour   time.Time
	tier   *HourlyTier
	now    time.Time
	listed map[string]bool
	future []simFile
	keep   map[string]bool // files the "job" never consumes
	merges []time.Time
	gone   map[string]time.Time // key -> tick that consumed it
	seq    int
}

type simFile struct {
	at  time.Time
	key string
}

func newRemergeSim(t *testing.T, hour time.Time) *remergeSim {
	s := &remergeSim{t: t, hour: hour, listed: map[string]bool{}, gone: map[string]time.Time{}}
	s.tier = NewHourlyTier(&HourlyTierConfig{
		MinAgeHours: 1, MinFiles: 3, Enabled: true, Logger: zerolog.Nop(),
		ClosedGraceMinutes: 10, ClosedRemergeMinFiles: 200, ClosedRemergeMaxWaitMinutes: 30,
	})
	s.tier.clock = func() time.Time { return s.now }
	return s
}

func (s *remergeSim) dir() string {
	h := s.hour
	return fmt.Sprintf("db/m/%04d/%02d/%02d/%02d/", h.Year(), int(h.Month()), h.Day(), h.Hour())
}

func (s *remergeSim) rawKey(w time.Time) string {
	s.seq++
	return fmt.Sprintf("%sm_pod-%d_%s_%09d.parquet", s.dir(), s.seq%3, w.Format("20060102_150405"), s.seq)
}

func (s *remergeSim) outputKey(w time.Time) string {
	s.seq++
	return fmt.Sprintf("%sm_%s_%08x_compacted.parquet", s.dir(), w.Format("20060102_150405"), s.seq)
}

func (s *remergeSim) arrive(at time.Time, key string) {
	s.future = append(s.future, simFile{at: at, key: key})
}

func (s *remergeSim) run(from, until time.Time) {
	sort.SliceStable(s.future, func(i, j int) bool { return s.future[i].at.Before(s.future[j].at) })
	for tick := from; !tick.After(until); tick = tick.Add(10 * time.Minute) {
		s.now = tick
		for len(s.future) > 0 && !s.future[0].at.After(tick) {
			s.listed[s.future[0].key] = true
			s.future = s.future[1:]
		}
		objs := make([]string, 0, len(s.listed))
		for k := range s.listed {
			objs = append(objs, k)
		}
		sort.Strings(objs)
		c := candidateFor(s.tier.FindCandidatesFromListing("db", "m", objs), s.hour)
		if c == nil {
			continue
		}
		for _, f := range c.Files {
			if s.keep[f] {
				continue
			}
			delete(s.listed, f)
			s.gone[f] = tick
		}
		s.listed[s.outputKey(tick)] = true
		s.merges = append(s.merges, tick)
	}
}

// body writes the hour's own files (3 pods, one file a minute each) and late
// files every lateEvery from H+1h until the fresh stager stops writing H at
// H+2h. It returns one body key, for a re-PUT.
func (s *remergeSim) body(lateEvery time.Duration) string {
	var first string
	for m := 0; m < 60; m++ {
		for p := 0; p < 3; p++ {
			w := s.hour.Add(time.Duration(m)*time.Minute + time.Duration(p)*time.Second)
			k := s.rawKey(w)
			if first == "" {
				first = k
			}
			s.arrive(w, k)
		}
	}
	if lateEvery > 0 {
		for w := s.hour.Add(time.Hour + lateEvery); w.Before(s.hour.Add(2 * time.Hour)); w = w.Add(lateEvery) {
			s.arrive(w, s.rawKey(w))
		}
	}
	return first
}

// The reviewer's C1 model: about 3 late files a minute. With min files 200
// the 30m wait governs and a closed hour is written 3 times; the lone-file
// rule must not add rewrites there, and at lower late rates it stays bounded
// and ends once the late window has.
func TestHourlyClosedRemerge_RewritesPerClosedHour(t *testing.T) {
	h := at(10, 0)
	cases := []struct {
		name      string
		lateEvery time.Duration
		want      int // exact count, 0 = only bounded
	}{
		{"3 late files a minute", 20 * time.Second, 3},
		{"one late file every 5m", 5 * time.Minute, 0},
		{"one late file every 20m", 20 * time.Minute, 0},
		{"no late files", 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newRemergeSim(t, h)
			s.body(tc.lateEvery)
			s.run(h.Add(time.Hour), h.Add(8*time.Hour))
			t.Logf("rewrites at %v", s.merges)
			if tc.want > 0 && len(s.merges) != tc.want {
				t.Fatalf("rewrites = %d, want %d", len(s.merges), tc.want)
			}
			if len(s.merges) == 0 || len(s.merges) > 4 {
				t.Fatalf("rewrites = %d, want 1..4", len(s.merges))
			}
			if last := s.merges[len(s.merges)-1]; last.After(h.Add(2*time.Hour + 40*time.Minute)) {
				t.Fatalf("last rewrite at %v: later than the late window + max wait + one tick", last)
			}
			if raw := s.rawLeft(); len(raw) != 0 {
				t.Fatalf("raw files left after 8h: %v", raw)
			}
		})
	}
}

// A re-PUT of an already merged key, with no other late file: before the fix
// it was never merged again by the hourly tier.
func TestHourlyClosedRemerge_RePutMergedWithinMaxWait(t *testing.T) {
	h := at(10, 0)
	for _, reput := range []time.Duration{75 * time.Minute, 2 * time.Hour, 3*time.Hour + 30*time.Minute} {
		t.Run(reput.String(), func(t *testing.T) {
			s := newRemergeSim(t, h)
			k := s.body(0)
			s.run(h.Add(time.Hour), h.Add(time.Hour+10*time.Minute))
			if _, ok := s.gone[k]; !ok {
				t.Fatalf("first pass at H+1h10m did not take %s", k)
			}
			delete(s.gone, k)
			s.arrive(h.Add(reput), k)
			s.run(h.Add(time.Hour+20*time.Minute), h.Add(8*time.Hour))
			merged, ok := s.gone[k]
			if !ok {
				t.Fatalf("re-PUT of %s at H+%v is never merged again: its rows stay double counted until the daily merge", k, reput)
			}
			if life := merged.Sub(h.Add(reput)); life > 40*time.Minute {
				t.Fatalf("re-PUT at H+%v merged at %v: duplicate lived %v, want <= max wait + one tick", reput, merged, life)
			}
			if len(s.merges) != 2 {
				t.Fatalf("rewrites = %v, want the first pass plus one re-merge", s.merges)
			}
		})
	}
}

func (s *remergeSim) rawLeft() []string {
	var out []string
	for k := range s.listed {
		if !s.tier.IsCompactedFile(k) && !s.keep[k] {
			out = append(out, k)
		}
	}
	return out
}
