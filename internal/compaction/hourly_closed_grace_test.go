package compaction

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// IC2: a closed hour is compacted once it has been closed for ClosedGrace,
// even while late files still trickle into it. Before, eligibility waited
// until the partition's NEWEST file was MinAgeHours old, and the fresh
// stager writes into hour H until H+2h, so H stayed raw until about H+3h.

var graceNow = time.Date(2026, 10, 20, 12, 30, 0, 0, time.UTC)

type graceFixture struct {
	backend *storage.LocalBackend
}

func newGraceFixture(t *testing.T) *graceFixture {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return &graceFixture{backend: b}
}

// raw writes a raw file into hour partition h whose name carries write time w
// (the shape ingest and the fresh stager use: <m>_..._<YYYYMMDD>_<HHMMSS>_<nanos>).
func (f *graceFixture) raw(t *testing.T, h, w time.Time, i int) string {
	t.Helper()
	p := fmt.Sprintf("db/m/%04d/%02d/%02d/%02d/m_pod-a_%s_%09d.parquet",
		h.Year(), int(h.Month()), h.Day(), h.Hour(), w.Format("20060102_150405"), i)
	f.write(t, p)
	return p
}

// compacted writes an hourly tier output into h, named at write time w.
func (f *graceFixture) compacted(t *testing.T, h, w time.Time, i int) string {
	t.Helper()
	p := fmt.Sprintf("db/m/%04d/%02d/%02d/%02d/m_%s_%09d_compacted.parquet",
		h.Year(), int(h.Month()), h.Day(), h.Hour(), w.Format("20060102_150405"), i)
	f.write(t, p)
	return p
}

func (f *graceFixture) write(t *testing.T, p string) {
	t.Helper()
	content := append(append([]byte("PAR1"), []byte("FIXTURE")...), []byte("PAR1")...)
	if err := f.backend.Write(context.Background(), p, content); err != nil {
		t.Fatal(err)
	}
}

func graceTier(b storage.Backend, cache *PartitionCache, grace time.Duration, now time.Time) *HourlyTier {
	tier := newHourlyTierForScan(b, cache, 1)
	tier.ClosedGrace = grace
	tier.clock = func() time.Time { return now }
	return tier
}

func at(hh, mm int) time.Time { return time.Date(2026, 10, 20, hh, mm, 0, 0, time.UTC) }

func candidateFor(cs []Candidate, h time.Time) *Candidate {
	want := fmt.Sprintf("db/m/%04d/%02d/%02d/%02d", h.Year(), int(h.Month()), h.Day(), h.Hour())
	for i := range cs {
		if cs[i].PartitionPath == want {
			return &cs[i]
		}
	}
	return nil
}

func sortedFiles(c *Candidate) []string {
	out := append([]string(nil), c.Files...)
	sort.Strings(out)
	return out
}

func sameFiles(t *testing.T, got *Candidate, want ...string) {
	t.Helper()
	sort.Strings(want)
	g := sortedFiles(got)
	if fmt.Sprint(g) != fmt.Sprint(want) {
		t.Fatalf("candidate files:\n got  %v\n want %v", g, want)
	}
}

// Both discovery paths (flat scan without a cache, cached incremental scan)
// must apply the same eligibility.
func forBothScans(t *testing.T, fn func(t *testing.T, cache func() *PartitionCache)) {
	t.Run("flat", func(t *testing.T) { fn(t, func() *PartitionCache { return nil }) })
	t.Run("cached", func(t *testing.T) {
		fn(t, func() *PartitionCache { return NewPartitionCache(24*time.Hour, 30) })
	})
}

func TestHourlyClosedGrace_DisabledKeepsNewestFileGate(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h := at(11, 0)
		f.raw(t, h, at(11, 10), 1)
		f.raw(t, h, at(11, 40), 2)
		f.raw(t, h, at(12, 25), 3) // a straggler 5 minutes ago
		got, err := graceTier(f.backend, cache(), 0, graceNow).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		if c := candidateFor(got, h); c != nil {
			t.Fatalf("grace disabled: hour 11 must wait for its newest file to be 1h old; got candidate %v", c.Files)
		}
	})
}

func TestHourlyClosedGrace_ClosedHourCompactsWhileStragglersArrive(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h := at(11, 0)
		a := f.raw(t, h, at(11, 10), 1)
		b := f.raw(t, h, at(11, 40), 2)
		c := f.raw(t, h, at(12, 5), 3)
		f.raw(t, h, at(12, 25), 4) // younger than the grace: left for the next tick
		f.raw(t, at(12, 0), at(12, 20), 5)
		f.raw(t, at(12, 0), at(12, 21), 6)
		got, err := graceTier(f.backend, cache(), 10*time.Minute, graceNow).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		cand := candidateFor(got, h)
		if cand == nil {
			t.Fatalf("hour 11 closed 30m ago (grace 10m) must be a candidate; got %v", candidatePaths(got))
		}
		sameFiles(t, cand, a, b, c)
		if cand.FileCount != 3 {
			t.Fatalf("FileCount = %d, want 3", cand.FileCount)
		}
		if candidateFor(got, at(12, 0)) != nil {
			t.Fatal("the current hour must never be a candidate")
		}
	})
}

func TestHourlyClosedGrace_NotBeforeGraceEnds(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h := at(11, 0)
		f.raw(t, h, at(11, 10), 1)
		f.raw(t, h, at(11, 20), 2)
		f.raw(t, h, at(11, 59), 3)
		now := at(12, 5) // closed 5 minutes ago, grace 10
		got, err := graceTier(f.backend, cache(), 10*time.Minute, now).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		if c := candidateFor(got, h); c != nil {
			t.Fatalf("hour 11 at 12:05 with grace 10m must not be a candidate; got %v", c.Files)
		}
	})
}

// The second pass: the first pass's output plus the stragglers that arrived
// since (Case 2) is re-merged, the newest straggler waits.
func TestHourlyClosedGrace_StragglersFoldIntoEarlierOutput(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h := at(11, 0)
		out := f.compacted(t, h, at(12, 11), 1)
		a := f.raw(t, h, at(12, 12), 1)
		b := f.raw(t, h, at(12, 15), 2)
		c := f.raw(t, h, at(12, 19), 3)
		f.raw(t, h, at(12, 28), 4)
		got, err := graceTier(f.backend, cache(), 10*time.Minute, graceNow).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		cand := candidateFor(got, h)
		if cand == nil {
			t.Fatalf("hour 11 with an output and 3 settled stragglers must be a candidate")
		}
		sameFiles(t, cand, out, a, b, c)
	})
}

// Fragmented outputs of a first pass (batches) re-merge on the next tick: tier
// outputs are written only by the compactor, so the grace does not hold them.
func TestHourlyClosedGrace_FragmentsRemergeWithoutWaiting(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h := at(11, 0)
		o1 := f.compacted(t, h, at(12, 25), 1)
		o2 := f.compacted(t, h, at(12, 25), 2)
		got, err := graceTier(f.backend, cache(), 10*time.Minute, graceNow).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		cand := candidateFor(got, h)
		if cand == nil {
			t.Fatalf("two fresh fragments of a closed hour must re-merge")
		}
		sameFiles(t, cand, o1, o2)
	})
}

// Too few settled files: the young ones do not count toward MinFiles.
func TestHourlyClosedGrace_YoungFilesDoNotCountTowardMinFiles(t *testing.T) {
	forBothScans(t, func(t *testing.T, cache func() *PartitionCache) {
		f := newGraceFixture(t)
		h := at(11, 0)
		f.compacted(t, h, at(12, 11), 1)
		f.raw(t, h, at(12, 15), 1)
		f.raw(t, h, at(12, 24), 2)
		f.raw(t, h, at(12, 26), 3)
		got, err := graceTier(f.backend, cache(), 10*time.Minute, graceNow).FindCandidates(context.Background(), "db", "m")
		if err != nil {
			t.Fatal(err)
		}
		if c := candidateFor(got, h); c != nil {
			t.Fatalf("1 output + 1 settled straggler (MinFiles 2 uncompacted) must wait; got %v", c.Files)
		}
	})
}

// The listing-reuse path applies the same rule.
func TestHourlyClosedGrace_FromListing(t *testing.T) {
	f := newGraceFixture(t)
	h := at(11, 0)
	a := f.raw(t, h, at(11, 10), 1)
	b := f.raw(t, h, at(12, 5), 2)
	young := f.raw(t, h, at(12, 25), 3)
	tier := graceTier(f.backend, nil, 10*time.Minute, graceNow)
	got := tier.FindCandidatesFromListing("db", "m", []string{a, b, young})
	cand := candidateFor(got, h)
	if cand == nil {
		t.Fatal("listing path: hour 11 must be a candidate")
	}
	sameFiles(t, cand, a, b)
}
