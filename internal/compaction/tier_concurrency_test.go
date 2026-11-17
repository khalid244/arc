package compaction

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// tierTracker wraps a batch runner and records, per tier and per partition,
// how many batches ran at once, and the order each partition's batches started.
type tierTracker struct {
	mu        sync.Mutex
	inFlight  map[string]int
	peak      map[string]int
	partFlt   map[string]int
	partPeak  map[string]int
	partOrder map[string][]int
}

func trackTiers(m *Manager) *tierTracker {
	tr := &tierTracker{
		inFlight: map[string]int{}, peak: map[string]int{},
		partFlt: map[string]int{}, partPeak: map[string]int{}, partOrder: map[string][]int{},
	}
	next := m.compactBatchForTest
	m.compactBatchForTest = func(ctx context.Context, b Candidate) error {
		tr.mu.Lock()
		tr.inFlight[b.Tier]++
		tr.peak[b.Tier] = max(tr.peak[b.Tier], tr.inFlight[b.Tier])
		tr.partFlt[b.PartitionPath]++
		tr.partPeak[b.PartitionPath] = max(tr.partPeak[b.PartitionPath], tr.partFlt[b.PartitionPath])
		tr.partOrder[b.PartitionPath] = append(tr.partOrder[b.PartitionPath], b.BatchNumber)
		tr.mu.Unlock()
		defer func() {
			tr.mu.Lock()
			tr.inFlight[b.Tier]--
			tr.partFlt[b.PartitionPath]--
			tr.mu.Unlock()
		}()
		return next(ctx, b)
	}
	return tr
}

func (tr *tierTracker) peakOf(tier string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.peak[tier]
}

func hourCandidates(n, files int) []Candidate {
	var out []Candidate
	for h := 1; h <= n; h++ {
		out = append(out, ilCandidate("hourly", fmt.Sprintf("db/cpu/2026/11/08/%02d", h), files))
	}
	return out
}

// holdAll holds the first batch of every candidate.
func holdAll(rig *ilRig, cands []Candidate) (started []<-chan struct{}, releaseAll func()) {
	var releases []func()
	for _, c := range cands {
		s, r := rig.hold(c.PartitionPath + "#1")
		started = append(started, s)
		releases = append(releases, r)
	}
	return started, func() {
		for _, r := range releases {
			r()
		}
	}
}

func assertNotStarted(t *testing.T, chans []<-chan struct{}, what string) {
	t.Helper()
	// Long enough for a free semaphore slot to launch a waiting worker.
	time.Sleep(60 * time.Millisecond)
	for i, ch := range chans {
		select {
		case <-ch:
			t.Fatalf("%s %d started past the concurrency limit", what, i)
		default:
		}
	}
}

// compaction.hourly_max_concurrent sizes the hourly tier's semaphore on its
// own: three hourly jobs run at once while max_concurrent stays 1.
func TestHourlyMaxConcurrent_HourlyTierRunsInParallel(t *testing.T) {
	hourly := hourCandidates(5, 10)
	rig := newILRig(t, 1, false, nil, hourly)
	rig.m.HourlyMaxConcurrent = 3
	tr := trackTiers(rig.m)

	started, release := holdAll(rig, hourly)
	defer release()
	done := rig.startCycle(context.Background(), "hourly")
	for i := 0; i < 3; i++ {
		waitClosed(t, started[i], fmt.Sprintf("hourly %d", i))
	}
	assertNotStarted(t, started[3:], "hourly")
	release()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("hourly cycle: %v", err)
	}
	if got := tr.peakOf("hourly"); got != 3 {
		t.Fatalf("hourly peak = %d, want 3", got)
	}
	if rec := rig.records(); len(rec) != 1 || rec[0].Succeeded != 5 {
		t.Fatalf("cycle records = %+v, want one with 5 succeeded", rec)
	}
}

// The daily tier keeps max_concurrent (1) in the same cycle, and the tiers
// still never overlap.
func TestHourlyMaxConcurrent_DailyStaysSerial(t *testing.T) {
	hourly := hourCandidates(3, 10)
	daily := []Candidate{
		ilCandidate("daily", "db/cpu/2026/11/01", 10),
		ilCandidate("daily", "db/cpu/2026/11/02", 10),
		ilCandidate("daily", "db/cpu/2026/11/03", 10),
	}
	rig := newILRig(t, 1, false, daily, hourly)
	rig.m.HourlyMaxConcurrent = 3
	tr := trackTiers(rig.m)

	hStarted, hRelease := holdAll(rig, hourly)
	dStarted, dRelease := holdAll(rig, daily)
	defer hRelease()
	defer dRelease()
	done := rig.startCycle(context.Background(), "hourly", "daily")
	for i, ch := range hStarted {
		waitClosed(t, ch, fmt.Sprintf("hourly %d", i))
	}
	assertNotStarted(t, dStarted, "daily (while hourly ran)")
	hRelease()
	waitClosed(t, dStarted[0], "daily 0")
	assertNotStarted(t, dStarted[1:], "daily")
	dRelease()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("cycle: %v", err)
	}
	if h, d := tr.peakOf("hourly"), tr.peakOf("daily"); h != 3 || d != 1 {
		t.Fatalf("peaks hourly=%d daily=%d, want 3 and 1", h, d)
	}
	if overlap, _ := rig.overlaps(); len(overlap) > 0 {
		t.Fatalf("tiers overlapped: %v", overlap)
	}
}

// An hourly pass interleaved into the daily cycle runs its jobs in parallel
// under the hourly limit, still never alongside a daily batch, and the daily
// tier stays serial around it.
func TestHourlyMaxConcurrent_InterleavedPassUsesHourlyLimit(t *testing.T) {
	daily := []Candidate{
		ilCandidate("daily", "db/cpu/2026/11/01", 20), // two batches
		ilCandidate("daily", "db/cpu/2026/11/02", 10),
	}
	hourly := hourCandidates(4, 10)
	rig := newILRig(t, 1, true, daily, hourly)
	rig.m.HourlyMaxConcurrent = 3
	tr := trackTiers(rig.m)

	d1Started, d1Release := rig.hold("db/cpu/2026/11/01#1")
	defer d1Release()
	hStarted, hRelease := holdAll(rig, hourly)
	defer hRelease()
	done := rig.startCycle(context.Background(), "daily")
	waitClosed(t, d1Started, "daily 01#1")
	rig.requestHourly()
	d1Release()
	for i := 0; i < 3; i++ {
		waitClosed(t, hStarted[i], fmt.Sprintf("interleaved hourly %d", i))
	}
	assertNotStarted(t, hStarted[3:], "interleaved hourly")
	hRelease()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}
	if overlap, _ := rig.overlaps(); len(overlap) > 0 {
		t.Fatalf("hourly and daily batches overlapped: %v", overlap)
	}
	if h, d := tr.peakOf("hourly"), tr.peakOf("daily"); h != 3 || d != 1 {
		t.Fatalf("peaks hourly=%d daily=%d, want 3 and 1", h, d)
	}
	if n := len(rig.recordsBySource(cycleSourceInterleaved)); n != 1 {
		t.Fatalf("interleaved records = %d, want 1", n)
	}
}

// Unset (0) is today's behaviour: the hourly tier shares max_concurrent.
func TestHourlyMaxConcurrent_DefaultUsesMaxConcurrent(t *testing.T) {
	hourly := hourCandidates(3, 10)
	rig := newILRig(t, 1, false, nil, hourly)
	tr := trackTiers(rig.m)

	started, release := holdAll(rig, hourly)
	defer release()
	done := rig.startCycle(context.Background(), "hourly")
	waitClosed(t, started[0], "hourly 0")
	assertNotStarted(t, started[1:], "hourly")
	release()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("hourly cycle: %v", err)
	}
	if got := tr.peakOf("hourly"); got != 1 {
		t.Fatalf("hourly peak = %d, want 1 (max_concurrent)", got)
	}

	for _, tc := range []struct{ maxC, hourlyC, wantHourly, wantDaily int }{
		{0, 0, 2, 2}, // NewManager defaults max_concurrent to 2
		{1, 0, 1, 1},
		{4, 0, 4, 4},
		{1, 3, 3, 1},
		{4, 1, 1, 4},
		{1, -2, 1, 1}, // negative is not a limit; config rejects it, the manager ignores it
	} {
		m := NewManager(&ManagerConfig{MaxConcurrent: tc.maxC, HourlyMaxConcurrent: tc.hourlyC, TempDirectory: t.TempDir()})
		if h, d := m.tierMaxConcurrent(hourlyTierName), m.tierMaxConcurrent(dailyTierName); h != tc.wantHourly || d != tc.wantDaily {
			t.Errorf("max_concurrent=%d hourly_max_concurrent=%d: hourly=%d daily=%d, want %d and %d",
				tc.maxC, tc.hourlyC, h, d, tc.wantHourly, tc.wantDaily)
		}
	}
}

// Parallel hourly jobs never share a partition: a partition's batches run one
// after another in one worker while other partitions run beside it.
func TestHourlyMaxConcurrent_NoTwoJobsOnOnePartition(t *testing.T) {
	hourly := hourCandidates(3, 30) // three batches each
	rig := newILRig(t, 1, false, nil, hourly)
	rig.m.HourlyMaxConcurrent = 4
	tr := trackTiers(rig.m)

	started, release := holdAll(rig, hourly)
	defer release()
	var second []<-chan struct{}
	rig.mu.Lock()
	for _, c := range hourly {
		s := make(chan struct{}) // closed when the batch starts; not held
		rig.started[c.PartitionPath+"#2"] = s
		second = append(second, s)
	}
	rig.mu.Unlock()
	done := rig.startCycle(context.Background(), "hourly")
	for i, ch := range started {
		waitClosed(t, ch, fmt.Sprintf("first batch of hour %d", i))
	}
	// A free slot (4 > 3 partitions) must not start a held partition's next batch.
	assertNotStarted(t, second, "second batch")
	release()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("hourly cycle: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, c := range hourly {
		if p := tr.partPeak[c.PartitionPath]; p != 1 {
			t.Errorf("%s: %d jobs at once, want 1", c.PartitionPath, p)
		}
		if got := tr.partOrder[c.PartitionPath]; !slices.Equal(got, []int{1, 2, 3}) {
			t.Errorf("%s: batch order %v, want [1 2 3]", c.PartitionPath, got)
		}
	}
	if tr.peak["hourly"] != 3 {
		t.Errorf("hourly peak = %d, want 3 (one per partition)", tr.peak["hourly"])
	}
}

// Through the real attempt path the partition lock is the second guard: even a
// partition discovered twice never runs two subprocesses at once.
func TestHourlyMaxConcurrent_PartitionLockHoldsUnderParallelHourly(t *testing.T) {
	p := ilCandidate("hourly", "db/cpu/2026/11/08/01", 10)
	q := ilCandidate("hourly", "db/cpu/2026/11/08/02", 10)
	rig := newILRig(t, 1, false, nil, []Candidate{p, p, q})
	rig.m.HourlyMaxConcurrent = 3
	rig.m.compactBatchForTest = nil

	var mu sync.Mutex
	inFlight, peak, calls := map[string]int{}, map[string]int{}, map[string]int{}
	qStarted := make(chan struct{})
	stubSubprocess(t, func(ctx context.Context, c *SubprocessJobConfig) (*SubprocessJobResult, error) {
		mu.Lock()
		inFlight[c.PartitionPath]++
		peak[c.PartitionPath] = max(peak[c.PartitionPath], inFlight[c.PartitionPath])
		calls[c.PartitionPath]++
		mu.Unlock()
		if c.PartitionPath == q.PartitionPath {
			close(qStarted)
		} else {
			// Hold P until the duplicate worker, launched before Q, has
			// had its attempt.
			waitClosed(t, qStarted, "Q")
			time.Sleep(50 * time.Millisecond)
		}
		mu.Lock()
		inFlight[c.PartitionPath]--
		mu.Unlock()
		return &SubprocessJobResult{Success: true, FilesCompacted: len(c.Files)}, nil
	})
	if _, err := rig.m.RunCompactionCycleForTiers(context.Background(), []string{"hourly"}); err != nil {
		t.Fatalf("hourly cycle: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if peak[p.PartitionPath] != 1 || peak[q.PartitionPath] != 1 {
		t.Fatalf("per-partition peaks = %v, want 1 each", peak)
	}
	if calls[p.PartitionPath] != 1 {
		t.Fatalf("partition discovered twice ran %d subprocesses, want 1 (lock skip)", calls[p.PartitionPath])
	}
}

// Hourly jobs get the hourly_* sizing; daily jobs keep the compaction.* one.
func TestHourlyJobLimits_PerTierSubprocessSizing(t *testing.T) {
	m, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	m.ManifestManager = nil
	m.MemoryLimit, m.Threads, m.MaxTempDirectorySize = "12GB", 4, "60GiB"

	run := func(tier string) *SubprocessJobConfig {
		t.Helper()
		var got *SubprocessJobConfig
		stubSubprocess(t, func(_ context.Context, c *SubprocessJobConfig) (*SubprocessJobResult, error) {
			cp := *c
			got = &cp
			return &SubprocessJobResult{Success: true}, nil
		})
		c := ilCandidate(tier, "db/cpu/2026/11/08/03", 3)
		c.BatchNumber = 1
		if err := m.CompactPartition(context.Background(), c); err != nil {
			t.Fatal(err)
		}
		return got
	}
	check := func(label string, c *SubprocessJobConfig, mem string, threads int, temp string) {
		t.Helper()
		if c == nil || c.MemoryLimit != mem || c.Threads != threads || c.MaxTempDirectorySize != temp {
			t.Errorf("%s: subprocess sizing = %+v, want (%s, %d, %s)", label, c, mem, threads, temp)
		}
	}

	check("hourly, no overrides", run("hourly"), "12GB", 4, "60GiB")

	m.HourlyMemoryLimit, m.HourlyThreads, m.HourlyMaxTempDirectorySize = "4GB", 2, "12GiB"
	check("hourly, overrides", run("hourly"), "4GB", 2, "12GiB")
	check("daily, overrides set", run("daily"), "12GB", 4, "60GiB")

	m.HourlyMemoryLimit, m.HourlyThreads, m.HourlyMaxTempDirectorySize = "", 0, "12GiB"
	check("hourly, spill cap only", run("hourly"), "12GB", 4, "12GiB")

	// Negative values (refused by config) inherit, never reach the subprocess.
	m.HourlyThreads, m.HourlyMaxTempDirectorySize = -1, ""
	check("hourly, negative threads", run("hourly"), "12GB", 4, "60GiB")
}

// An hourly partition's too-wide warning names the hourly_* keys that size
// its jobs; a daily one keeps the compaction.* keys.
func TestTooWideRemedy_NamesTheTierKeys(t *testing.T) {
	for _, tc := range []struct{ tier, reason, want, not string }{
		{"hourly", spillCapReason, "compaction.hourly_max_temp_directory_size", ""},
		{"hourly", "oom", "compaction.hourly_memory_limit", ""},
		{"hourly", "oom", "compaction.hourly_max_concurrent", ""},
		{"daily", spillCapReason, "compaction.max_temp_directory_size", "hourly_"},
		{"daily", "oom", "compaction.memory_limit", "hourly_"},
	} {
		got := tooWideRemedy(tc.tier, tc.reason)
		if !strings.Contains(got, tc.want) || (tc.not != "" && strings.Contains(got, tc.not)) {
			t.Errorf("tooWideRemedy(%s, %s) = %q, want %q without %q", tc.tier, tc.reason, got, tc.want, tc.not)
		}
	}
}

// The too-wide memo keys a failure to the job sizing. Unset hourly keys leave
// the fingerprint as it was (no memo entry is retried by the upgrade); set ones
// are part of it, since they size the hourly jobs.
func TestHourlyJobLimits_TooWideSettings(t *testing.T) {
	m, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	m.Tiers = nil
	m.MemoryLimit, m.Threads, m.MaxConcurrent, m.MaxTempDirectorySize = "12GB", 4, 1, "60GiB"
	base := m.TooWideSettings()
	if strings.Contains(base, "hourly_") {
		t.Fatalf("fingerprint without hourly overrides mentions them: %q", base)
	}
	m.HourlyMaxConcurrent = 3
	withConc := m.TooWideSettings()
	if withConc == base || !strings.Contains(withConc, "hourly_max_concurrent=3") {
		t.Fatalf("fingerprint ignores hourly_max_concurrent: %q", withConc)
	}
	m.HourlyMemoryLimit, m.HourlyThreads, m.HourlyMaxTempDirectorySize = "4GB", 2, "12GiB"
	got := m.TooWideSettings()
	for _, want := range []string{"hourly_memory_limit=4GB", "hourly_threads=2", "hourly_max_temp_directory_size=12GiB"} {
		if !strings.Contains(got, want) {
			t.Errorf("fingerprint %q lacks %s", got, want)
		}
	}
}

func TestNewManager_WiresHourlyTierLimits(t *testing.T) {
	m := NewManager(&ManagerConfig{
		MaxConcurrent: 1, MemoryLimit: "12GB", Threads: 4, MaxTempDirectorySize: "60GiB",
		HourlyMaxConcurrent: 3, HourlyMemoryLimit: "4GB", HourlyThreads: 2, HourlyMaxTempDirectorySize: "12GiB",
		TempDirectory: t.TempDir(),
	})
	if m.HourlyMaxConcurrent != 3 || m.HourlyMemoryLimit != "4GB" || m.HourlyThreads != 2 || m.HourlyMaxTempDirectorySize != "12GiB" {
		t.Fatalf("manager hourly sizing = (%d, %q, %d, %q)", m.HourlyMaxConcurrent, m.HourlyMemoryLimit, m.HourlyThreads, m.HourlyMaxTempDirectorySize)
	}
}
