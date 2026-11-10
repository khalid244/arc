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

	"github.com/rs/zerolog"
)

// N4: a forced re-merge (closedRemergeDue: a raw file below MinFiles next to a
// closed hour's output) that yields no job, or whose job fails, must not come
// back on every tick. The hour backs off, doubling per such pass.

type selectCounter struct {
	*HourlyTier
	mu       sync.Mutex
	selected int
}

func (s *selectCounter) FindCandidates(ctx context.Context, database, measurement string) ([]Candidate, error) {
	c, err := s.HourlyTier.FindCandidates(ctx, database, measurement)
	s.mu.Lock()
	s.selected += len(c)
	s.mu.Unlock()
	return c, err
}

func (s *selectCounter) take() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.selected
	s.selected = 0
	return n
}

type backoffRig struct {
	t     *testing.T
	m     *Manager
	tier  *selectCounter
	now   time.Time
	mu    sync.Mutex
	runs  int
	fail  bool
	k     string
	start time.Time
}

// newBackoffRig: hour 09 of a day, two outputs written at 10:40 and a raw file
// K named 09:50; the tier runs the deploy values (min_files 3, grace 10,
// re-merge 200 / 30m). outputSize sets the outputs' size against the 1000-byte
// budget: above it, K has nothing to pair with.
func newBackoffRig(t *testing.T, outputSize int) *backoffRig {
	t.Helper()
	m, backend, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	m.ManifestManager = nil
	m.MaxConcurrent = 1
	m.logger = zerolog.Nop()
	ctx := context.Background()
	part := "testdb/cpu/2026/11/10/09"
	r := &backoffRig{t: t, m: m, k: part + "/cpu_pod-a-k_20261110_095000_000000050.parquet"}
	for key, n := range map[string]int{
		part + "/cpu_20261110_104000_aaaaaaaa_compacted.parquet": outputSize,
		part + "/cpu_20261110_104001_bbbbbbbb_compacted.parquet": outputSize,
		r.k: 100,
	} {
		if err := backend.Write(ctx, key, []byte(strings.Repeat("x", n))); err != nil {
			t.Fatal(err)
		}
	}
	hourly := NewHourlyTier(&HourlyTierConfig{
		StorageBackend: backend, MinAgeHours: 1, MinFiles: 3, Enabled: true, Logger: zerolog.Nop(),
		MaxOutputBytes:     1000,
		ClosedGraceMinutes: 10, ClosedRemergeMinFiles: 200, ClosedRemergeMaxWaitMinutes: 30,
		DropConsumedInputs: true,
	})
	r.start = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	r.now = r.start
	hourly.clock = func() time.Time {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.now
	}
	r.tier = &selectCounter{HourlyTier: hourly}
	m.Tiers = []Tier{r.tier}
	m.compactBatchForTest = func(_ context.Context, b Candidate) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.runs++
		if r.fail && slices.Contains(b.Files, r.k) {
			return errors.New("injected: K cannot be read next to the output")
		}
		return nil
	}
	return r
}

// tick runs one hourly cycle at start+at and reports whether the hour was
// selected and how many batches ran.
func (r *backoffRig) tick(at time.Duration) (selected bool, runs int) {
	r.t.Helper()
	r.mu.Lock()
	r.now = r.start.Add(at)
	r.runs = 0
	r.mu.Unlock()
	if _, err := r.m.RunCompactionCycleForTiers(context.Background(), []string{"hourly"}); err != nil && !(r.fail && strings.Contains(err.Error(), "batch failures")) {
		r.t.Fatalf("cycle at +%v: %v", at, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tier.take() > 0, r.runs
}

func (r *backoffRig) expect(want map[time.Duration]bool, until time.Duration) {
	r.t.Helper()
	var got []string
	for at := time.Duration(0); at <= until; at += 10 * time.Minute {
		sel, _ := r.tick(at)
		if sel != want[at] {
			got = append(got, fmt.Sprintf("+%v selected=%v want %v", at, sel, want[at]))
		}
	}
	if len(got) > 0 {
		r.t.Fatalf("forced re-merge schedule:\n%s", strings.Join(got, "\n"))
	}
}

// Every output is over the budget: K is a lone batch, no job runs. The hour
// is retried after 30m, then 60m, then 120m.
func TestHourlyClosedRemerge_EmptyForcedPassBacksOff(t *testing.T) {
	r := newBackoffRig(t, 2000)
	r.expect(map[time.Duration]bool{
		0:                 true,
		30 * time.Minute:  true,
		90 * time.Minute:  true,
		210 * time.Minute: true,
	}, 220*time.Minute)
}

// A job that fails next to the output is retried after 30m, then 60m, and a
// success ends the back-off. The stub job deletes nothing, so after the
// success the hour is still due and is forced again on the next tick.
func TestHourlyClosedRemerge_FailedForcedPassBacksOffUntilSuccess(t *testing.T) {
	r := newBackoffRig(t, 300)
	r.fail = true
	for _, step := range []struct {
		at       time.Duration
		selected bool
	}{
		{0, true},
		{10 * time.Minute, false},
		{20 * time.Minute, false},
		{30 * time.Minute, true},
		{40 * time.Minute, false},
		{80 * time.Minute, false},
		{90 * time.Minute, true},
	} {
		sel, runs := r.tick(step.at)
		if sel != step.selected {
			t.Fatalf("+%v: selected=%v, want %v", step.at, sel, step.selected)
		}
		if sel && runs != 1 {
			t.Fatalf("+%v: %d batches ran, want 1", step.at, runs)
		}
		if !sel && runs != 0 {
			t.Fatalf("+%v: %d batches ran while backing off", step.at, runs)
		}
	}
	// The job works again: the next allowed pass succeeds and clears the
	// back-off, so the following tick may force the hour again at once.
	r.fail = false
	if sel, runs := r.tick(210 * time.Minute); !sel || runs != 1 {
		t.Fatalf("+210m: selected=%v runs=%d, want the retry to run", sel, runs)
	}
	if sel, _ := r.tick(220 * time.Minute); !sel {
		t.Fatal("+220m: a successful pass must clear the back-off")
	}
}
