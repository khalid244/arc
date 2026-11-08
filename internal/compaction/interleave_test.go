package compaction

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type interleaveTier struct {
	Tier
	name string
	find func(ctx context.Context, measurement string) ([]Candidate, error)
}

func (t interleaveTier) GetTierName() string { return t.name }
func (interleaveTier) IsEnabled() bool       { return true }
func (interleaveTier) GetMinFiles() int      { return 2 }
func (interleaveTier) GetStats() map[string]interface{} {
	return map[string]interface{}{}
}

func (t interleaveTier) FindCandidates(ctx context.Context, database, measurement string) ([]Candidate, error) {
	if database != "db" || t.find == nil {
		return nil, nil
	}
	return t.find(ctx, measurement)
}

// ilCandidate builds a candidate of n files; with MaxFilesPerBatch 10 a
// 20-file candidate splits into two batches.
func ilCandidate(tier, partition string, n int) Candidate {
	c := Candidate{Database: "db", Measurement: "cpu", PartitionPath: partition, Tier: tier}
	for i := 0; i < n; i++ {
		c.Files = append(c.Files, fmt.Sprintf("%s/f%02d.parquet", partition, i))
	}
	c.FileCount = len(c.Files)
	return c
}

type ilRig struct {
	t *testing.T
	m *Manager

	mu       sync.Mutex
	events   []string
	inFlight map[string]int
	maxTotal int
	overlap  []string
	holds    map[string]chan struct{}
	started  map[string]chan struct{}
	onStart  func(batch Candidate)

	weeklyScans atomic.Int32
	paused      atomic.Bool
	// dailyFind, when set, answers daily discovery for measurements other
	// than cpu.
	dailyFind func(ctx context.Context, measurement string) ([]Candidate, error)
}

func batchKey(c Candidate) string { return fmt.Sprintf("%s#%d", c.PartitionPath, c.BatchNumber) }

func newILRig(t *testing.T, maxConcurrent int, interleave bool, daily, hourly []Candidate) *ilRig {
	t.Helper()
	m, backend, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	if err := backend.Write(context.Background(), "db/cpu/2026/11/08/03/seed.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	m.ManifestManager = nil
	m.MaxConcurrent = maxConcurrent
	m.HourlyDuringDaily = interleave

	rig := &ilRig{
		t:        t,
		m:        m,
		inFlight: map[string]int{},
		holds:    map[string]chan struct{}{},
		started:  map[string]chan struct{}{},
	}
	m.SetPauseGate(rig.paused.Load)
	cpuOnly := func(c []Candidate) func(context.Context, string) ([]Candidate, error) {
		return func(_ context.Context, measurement string) ([]Candidate, error) {
			if measurement != "cpu" {
				return nil, nil
			}
			return c, nil
		}
	}
	m.Tiers = []Tier{
		interleaveTier{name: "hourly", find: cpuOnly(hourly)},
		interleaveTier{name: "daily", find: func(ctx context.Context, measurement string) ([]Candidate, error) {
			if measurement != "cpu" && rig.dailyFind != nil {
				return rig.dailyFind(ctx, measurement)
			}
			return cpuOnly(daily)(ctx, measurement)
		}},
		interleaveTier{name: "weekly", find: func(_ context.Context, measurement string) ([]Candidate, error) {
			if measurement == "cpu" {
				rig.weeklyScans.Add(1)
			}
			return nil, nil
		}},
	}
	m.compactBatchForTest = rig.runBatch
	return rig
}

// hold makes the named batch block until the returned release is called (or
// the cycle context ends), and returns a channel closed when it starts.
func (r *ilRig) hold(key string) (started <-chan struct{}, release func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := make(chan struct{})
	s := make(chan struct{})
	r.holds[key] = h
	r.started[key] = s
	var once sync.Once
	return s, func() { once.Do(func() { close(h) }) }
}

func (r *ilRig) runBatch(ctx context.Context, batch Candidate) error {
	key := batchKey(batch)
	r.mu.Lock()
	for tier, n := range r.inFlight {
		if n > 0 && tier != batch.Tier {
			r.overlap = append(r.overlap, fmt.Sprintf("%s started while %d %s batch(es) ran", key, n, tier))
		}
	}
	r.inFlight[batch.Tier]++
	total := 0
	for _, n := range r.inFlight {
		total += n
	}
	if total > r.maxTotal {
		r.maxTotal = total
	}
	r.events = append(r.events, batch.Tier+":"+key+":start")
	hold, started, onStart := r.holds[key], r.started[key], r.onStart
	r.mu.Unlock()

	if onStart != nil {
		onStart(batch)
	}
	if started != nil {
		close(started)
	}
	var err error
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			err = ctx.Err()
		}
	} else {
		time.Sleep(2 * time.Millisecond)
	}

	r.mu.Lock()
	r.inFlight[batch.Tier]--
	r.events = append(r.events, batch.Tier+":"+key+":end")
	r.mu.Unlock()
	return err
}

func (r *ilRig) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *ilRig) overlaps() ([]string, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.overlap...), r.maxTotal
}

func (r *ilRig) startCycle(ctx context.Context, tiers ...string) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := r.m.RunCompactionCycleForTiers(ctx, tiers)
		done <- err
	}()
	return done
}

func (r *ilRig) requestHourly() {
	r.t.Helper()
	id, err := r.m.RunCompactionCycleForTiers(context.Background(), []string{"hourly"})
	if err != nil || id != 0 {
		r.t.Fatalf("hourly request = (%d, %v), want coalesced (0, nil)", id, err)
	}
}

func (r *ilRig) pendingSnapshot() []string {
	r.m.pendMu.Lock()
	defer r.m.pendMu.Unlock()
	return append([]string(nil), r.m.pending...)
}

func (r *ilRig) records() []CycleRecord {
	return r.m.CyclePage(CycleHistoryLimit).Cycles
}

func (r *ilRig) recordsBySource(source string) []CycleRecord {
	var out []CycleRecord
	for _, rec := range r.records() {
		if rec.Source == source {
			out = append(out, rec)
		}
	}
	return out
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("cycle did not finish")
		return nil
	}
}

func indexOf(events []string, ev string) int {
	return slices.Index(events, ev)
}

// The coalesced hourly request runs at the next yield point INSIDE the daily
// cycle -- between two batches of one day partition -- not after the cycle.
func TestInterleave_HourlyRunsBetweenDailyBatches(t *testing.T) {
	d1 := ilCandidate("daily", "db/cpu/2026/11/01", 20) // two batches
	d2 := ilCandidate("daily", "db/cpu/2026/11/02", 10)
	h := ilCandidate("hourly", "db/cpu/2026/11/08/03", 10)
	rig := newILRig(t, 1, true, []Candidate{d1, d2}, []Candidate{h})

	var hourlyStampedMidCycle, dailyStampedMidCycle atomic.Bool
	rig.onStart = func(b Candidate) {
		if batchKey(b) == "db/cpu/2026/11/01#2" {
			hourlyStampedMidCycle.Store(!rig.m.LastSuccessfulRun("hourly").IsZero())
			dailyStampedMidCycle.Store(!rig.m.LastSuccessfulRun("daily").IsZero())
		}
	}
	started, release := rig.hold("db/cpu/2026/11/01#1")
	done := rig.startCycle(context.Background(), "daily")
	waitClosed(t, started, "first daily batch")

	rig.requestHourly()
	if got := rig.pendingSnapshot(); !slices.Equal(got, []string{"hourly"}) {
		t.Fatalf("pending = %v, want [hourly]", got)
	}
	release()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}

	want := []string{
		"daily:db/cpu/2026/11/01#1:start", "daily:db/cpu/2026/11/01#1:end",
		"hourly:db/cpu/2026/11/08/03#1:start", "hourly:db/cpu/2026/11/08/03#1:end",
		"daily:db/cpu/2026/11/01#2:start", "daily:db/cpu/2026/11/01#2:end",
		"daily:db/cpu/2026/11/02#1:start", "daily:db/cpu/2026/11/02#1:end",
	}
	if got := rig.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("events =\n%v\nwant\n%v", got, want)
	}
	if overlap, maxTotal := rig.overlaps(); len(overlap) > 0 || maxTotal != 1 {
		t.Fatalf("overlap=%v maxTotal=%d, want none and 1", overlap, maxTotal)
	}
	if !hourlyStampedMidCycle.Load() {
		t.Error("interleaved hourly pass did not stamp hourly last-run before the daily cycle ended")
	}
	if dailyStampedMidCycle.Load() {
		t.Error("daily last-run stamped before the daily tier finished")
	}

	inter := rig.recordsBySource(cycleSourceInterleaved)
	if len(inter) != 1 {
		t.Fatalf("interleaved records = %d, want 1 (all: %+v)", len(inter), rig.records())
	}
	ir := inter[0]
	if !slices.Equal(ir.Tiers, []string{"hourly"}) || ir.Status != "completed" ||
		ir.Discovered != 1 || ir.Started != 1 || ir.Succeeded != 1 {
		t.Errorf("interleaved record = %+v", ir)
	}
	var daily CycleRecord
	for _, rec := range rig.records() {
		if slices.Equal(rec.Tiers, []string{"daily"}) {
			daily = rec
		}
	}
	if daily.CycleID == 0 || daily.Status != "completed" {
		t.Fatalf("daily record = %+v", daily)
	}
	if daily.Discovered != 3 || daily.Started != 3 || daily.Succeeded != 3 {
		t.Errorf("daily counters include the hourly pass: %+v", daily)
	}
	if ir.CycleID <= daily.CycleID {
		t.Errorf("interleaved id %d not after daily id %d", ir.CycleID, daily.CycleID)
	}
	if ir.StartedAt.Before(daily.StartedAt) || ir.FinishedAt.After(daily.FinishedAt) {
		t.Errorf("interleaved pass [%v,%v] not inside daily [%v,%v]", ir.StartedAt, ir.FinishedAt, daily.StartedAt, daily.FinishedAt)
	}
	if len(rig.recordsBySource(cycleSourceCoalesced)) != 0 {
		t.Error("hourly also ran as a coalesced drain; it should have been consumed by the interleaved pass")
	}
	if got := rig.pendingSnapshot(); len(got) != 0 {
		t.Errorf("pending after cycle = %v, want empty", got)
	}
	if rig.m.IsCycleRunning() {
		t.Error("claim not released")
	}
}

// Two daily workers: the hourly pass waits until NO daily batch is running,
// and no daily batch starts while hourly batches run.
func TestInterleave_NeverOverlapsDailyBatchesWithTwoWorkers(t *testing.T) {
	var daily []Candidate
	for d := 1; d <= 4; d++ {
		daily = append(daily, ilCandidate("daily", fmt.Sprintf("db/cpu/2026/11/%02d", d), 10))
	}
	var hourly []Candidate
	for h := 1; h <= 3; h++ {
		hourly = append(hourly, ilCandidate("hourly", fmt.Sprintf("db/cpu/2026/11/08/%02d", h), 10))
	}
	rig := newILRig(t, 2, true, daily, hourly)

	s1, r1 := rig.hold("db/cpu/2026/11/01#1")
	s2, r2 := rig.hold("db/cpu/2026/11/02#1")
	done := rig.startCycle(context.Background(), "daily")
	waitClosed(t, s1, "daily 01")
	waitClosed(t, s2, "daily 02")

	rig.requestHourly()
	r1()
	// A worker is now at a yield point with hourly pending, but daily 02 is
	// still running: the hourly pass must not start.
	time.Sleep(60 * time.Millisecond)
	for _, ev := range rig.snapshot() {
		if ev[:6] == "hourly" {
			t.Fatalf("hourly batch started while daily 02 was in flight: %v", rig.snapshot())
		}
	}
	r2()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}

	events := rig.snapshot()
	if overlap, maxTotal := rig.overlaps(); len(overlap) > 0 || maxTotal > 2 {
		t.Fatalf("overlap=%v maxTotal=%d", overlap, maxTotal)
	}
	lastHourlyEnd := -1
	for _, h := range hourly {
		i := indexOf(events, "hourly:"+batchKey(SplitCandidateIntoBatches(h, 10)[0])+":end")
		if i < 0 {
			t.Fatalf("hourly %s never ran: %v", h.PartitionPath, events)
		}
		lastHourlyEnd = max(lastHourlyEnd, i)
	}
	for _, d := range []string{"db/cpu/2026/11/03#1", "db/cpu/2026/11/04#1"} {
		i := indexOf(events, "daily:"+d+":start")
		if i < 0 {
			t.Fatalf("daily %s never ran: %v", d, events)
		}
		if i < lastHourlyEnd {
			t.Fatalf("daily %s started before the hourly pass finished: %v", d, events)
		}
	}
	if n := len(rig.recordsBySource(cycleSourceInterleaved)); n != 1 {
		t.Fatalf("interleaved records = %d, want 1", n)
	}
}

// Hourly candidates inside a day partition the daily cycle discovered but has
// not finished (running or still queued) are skipped; one inside a finished
// day or an unrelated day runs.
func TestInterleave_SkipsHoursOfUnfinishedDailyPartitions(t *testing.T) {
	daily := []Candidate{
		ilCandidate("daily", "db/cpu/2026/11/02", 10), // finishes before the yield
		ilCandidate("daily", "db/cpu/2026/11/01", 20), // yield happens before its first batch
		ilCandidate("daily", "db/cpu/2026/11/03", 10), // still queued at the yield
	}
	hourly := []Candidate{
		ilCandidate("hourly", "db/cpu/2026/11/01/05", 10),
		ilCandidate("hourly", "db/cpu/2026/11/02/07", 10),
		ilCandidate("hourly", "db/cpu/2026/11/03/09", 10),
		ilCandidate("hourly", "db/cpu/2026/10/30/01", 10),
		ilCandidate("hourly", "db/cpu/2026/11/08/03", 10),
	}
	rig := newILRig(t, 1, true, daily, hourly)

	started, release := rig.hold("db/cpu/2026/11/02#1")
	done := rig.startCycle(context.Background(), "daily")
	waitClosed(t, started, "daily 02")
	rig.requestHourly()
	release()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}

	var ranHourly []string
	for _, ev := range rig.snapshot() {
		if len(ev) > 6 && ev[:7] == "hourly:" && ev[len(ev)-6:] == ":start" {
			ranHourly = append(ranHourly, ev)
		}
	}
	slices.Sort(ranHourly)
	want := []string{
		"hourly:db/cpu/2026/10/30/01#1:start",
		"hourly:db/cpu/2026/11/02/07#1:start",
		"hourly:db/cpu/2026/11/08/03#1:start",
	}
	if !slices.Equal(ranHourly, want) {
		t.Fatalf("hourly batches run = %v, want %v", ranHourly, want)
	}
	for _, d := range []string{"db/cpu/2026/11/02#1", "db/cpu/2026/11/01#1", "db/cpu/2026/11/01#2", "db/cpu/2026/11/03#1"} {
		if indexOf(rig.snapshot(), "daily:"+d+":end") < 0 {
			t.Errorf("daily batch %s did not run", d)
		}
	}
	inter := rig.recordsBySource(cycleSourceInterleaved)
	if len(inter) != 1 || inter[0].Discovered != 3 {
		t.Fatalf("interleaved records = %+v, want one with 3 discovered batches", inter)
	}
}

func TestOpenDayPartitionsCovers(t *testing.T) {
	var s openDayPartitions
	s.add("db/cpu/2026/11/01")
	s.add("db/cpu/2026/11/01")
	s.add("spoke/db/cpu/2026/11/02")
	for path, want := range map[string]bool{
		"db/cpu/2026/11/01/05":       true,
		"db/cpu/2026/11/01":          true,
		"db/cpu/2026/11/011/05":      false,
		"db/cpu/2026/11/0":           false,
		"db/cpu/2026/11/02/05":       false,
		"spoke/db/cpu/2026/11/02/23": true,
		"db/cpu2/2026/11/01/05":      false,
		"other/db/cpu/2026/11/01/05": false,
		"db/cpu/2026/11/01/05/extra": true,
		"":                           false,
	} {
		if got := s.covers(path); got != want {
			t.Errorf("covers(%q) = %v, want %v", path, got, want)
		}
	}
	s.remove("db/cpu/2026/11/01")
	if !s.covers("db/cpu/2026/11/01/05") {
		t.Error("registered twice, removed once: must still be covered")
	}
	s.remove("db/cpu/2026/11/01")
	if s.covers("db/cpu/2026/11/01/05") {
		t.Error("removed as often as added: must no longer be covered")
	}
}

// The interleaved pass does not run manifest recovery: an orphan written after
// the daily cycle's own recovery survives the hourly pass.
func TestInterleave_DoesNotRunManifestRecovery(t *testing.T) {
	d1 := ilCandidate("daily", "db/cpu/2026/11/01", 10)
	d2 := ilCandidate("daily", "db/cpu/2026/11/02", 10)
	h := ilCandidate("hourly", "db/cpu/2026/11/08/03", 10)
	rig := newILRig(t, 1, true, []Candidate{d1, d2}, []Candidate{h})
	rig.m.ManifestManager = NewManifestManager(rig.m.StorageBackend, rig.m.logger)
	ctx := context.Background()

	writeOrphan := func(id string) string {
		t.Helper()
		path, err := rig.m.ManifestManager.WriteManifest(ctx, &Manifest{
			Database: "db", Measurement: "cpu", Tier: "hourly", JobID: id,
			PartitionPath: "db/cpu/2020/01/01/00",
			OutputPath:    "db/cpu/2020/01/01/00/" + id + "_compacted.parquet",
			OutputSize:    3,
			InputFiles:    []string{"db/cpu/2020/01/01/00/" + id + "-in.parquet"},
			CreatedAt:     time.Now().Add(-2 * ManifestMinRecoveryAge),
			Status:        ManifestStatusPending,
		})
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
	exists := func(path string) bool {
		ok, err := rig.m.StorageBackend.Exists(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}

	before := writeOrphan("before-daily")
	var orphanSeenByHourly atomic.Int32
	var during string
	var duringMu sync.Mutex
	rig.onStart = func(b Candidate) {
		if b.Tier != "hourly" {
			return
		}
		duringMu.Lock()
		p := during
		duringMu.Unlock()
		if exists(p) {
			orphanSeenByHourly.Add(1)
		}
	}
	started, release := rig.hold("db/cpu/2026/11/01#1")
	done := rig.startCycle(ctx, "daily")
	waitClosed(t, started, "daily 01")
	if exists(before) {
		t.Fatal("the daily cycle's own recovery did not run (control)")
	}
	duringMu.Lock()
	during = writeOrphan("during-daily")
	duringMu.Unlock()
	rig.requestHourly()
	release()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}

	if n := len(rig.recordsBySource(cycleSourceInterleaved)); n != 1 {
		t.Fatalf("interleaved records = %d, want 1", n)
	}
	if orphanSeenByHourly.Load() != 1 {
		t.Fatal("orphan manifest was gone when the interleaved hourly batch ran: recovery ran mid-cycle")
	}
	if !exists(during) {
		t.Fatal("orphan manifest recovered during the daily cycle: the interleaved pass ran recovery")
	}
}

// Flag off: today's behaviour -- hourly waits in pending until the daily cycle
// ends, then runs as a coalesced drain.
func TestInterleave_FlagOffKeepsHourlyPendingUntilDailyEnds(t *testing.T) {
	d1 := ilCandidate("daily", "db/cpu/2026/11/01", 20)
	d2 := ilCandidate("daily", "db/cpu/2026/11/02", 10)
	h := ilCandidate("hourly", "db/cpu/2026/11/08/03", 10)
	rig := newILRig(t, 1, false, []Candidate{d1, d2}, []Candidate{h})

	var pendingAtLastDaily []string
	rig.onStart = func(b Candidate) {
		if batchKey(b) == "db/cpu/2026/11/02#1" {
			pendingAtLastDaily = rig.pendingSnapshot()
		}
	}
	started, release := rig.hold("db/cpu/2026/11/01#1")
	done := rig.startCycle(context.Background(), "daily")
	waitClosed(t, started, "daily 01")
	rig.requestHourly()
	release()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}

	want := []string{
		"daily:db/cpu/2026/11/01#1:start", "daily:db/cpu/2026/11/01#1:end",
		"daily:db/cpu/2026/11/01#2:start", "daily:db/cpu/2026/11/01#2:end",
		"daily:db/cpu/2026/11/02#1:start", "daily:db/cpu/2026/11/02#1:end",
		"hourly:db/cpu/2026/11/08/03#1:start", "hourly:db/cpu/2026/11/08/03#1:end",
	}
	if got := rig.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("events =\n%v\nwant\n%v", got, want)
	}
	if !slices.Equal(pendingAtLastDaily, []string{"hourly"}) {
		t.Errorf("pending at the last daily batch = %v, want [hourly]", pendingAtLastDaily)
	}
	if n := len(rig.recordsBySource(cycleSourceInterleaved)); n != 0 {
		t.Errorf("interleaved records = %d with the flag off", n)
	}
	if n := len(rig.recordsBySource(cycleSourceCoalesced)); n != 1 {
		t.Errorf("coalesced records = %d, want 1", n)
	}
}

// A pause that lands before the yield point: no interleaved pass starts and
// hourly stays pending for the normal drain.
func TestInterleave_PauseLeavesHourlyPending(t *testing.T) {
	d1 := ilCandidate("daily", "db/cpu/2026/11/01", 20)
	d2 := ilCandidate("daily", "db/cpu/2026/11/02", 10)
	h := ilCandidate("hourly", "db/cpu/2026/11/08/03", 10)
	rig := newILRig(t, 1, true, []Candidate{d1, d2}, []Candidate{h})

	started, release := rig.hold("db/cpu/2026/11/01#1")
	done := rig.startCycle(context.Background(), "daily")
	waitClosed(t, started, "daily 01")
	rig.requestHourly()
	rig.paused.Store(true)
	release()
	err := waitDone(t, done)
	if !errors.Is(err, ErrCompactionPaused) {
		t.Fatalf("daily cycle err = %v, want ErrCompactionPaused", err)
	}

	for _, ev := range rig.snapshot() {
		if ev[:6] == "hourly" {
			t.Fatalf("hourly ran under the pause: %v", rig.snapshot())
		}
	}
	if n := len(rig.recordsBySource(cycleSourceInterleaved)); n != 0 {
		t.Fatalf("interleaved pass started under the pause (%d records)", n)
	}
	if got := rig.pendingSnapshot(); !slices.Equal(got, []string{"hourly"}) {
		t.Fatalf("pending = %v, want [hourly]", got)
	}
}

// Cancellation while a worker waits at the yield point behind a running daily
// batch: no interleaved pass starts and hourly stays pending.
func TestInterleave_CancelLeavesHourlyPending(t *testing.T) {
	d1 := ilCandidate("daily", "db/cpu/2026/11/01", 20)
	d2 := ilCandidate("daily", "db/cpu/2026/11/02", 10)
	h := ilCandidate("hourly", "db/cpu/2026/11/08/03", 10)
	rig := newILRig(t, 2, true, []Candidate{d1, d2}, []Candidate{h})

	s1, r1 := rig.hold("db/cpu/2026/11/01#1")
	s2, _ := rig.hold("db/cpu/2026/11/02#1") // returns only when the cycle is cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := rig.startCycle(ctx, "daily")
	waitClosed(t, s1, "daily 01")
	waitClosed(t, s2, "daily 02")
	rig.requestHourly()
	r1()
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := waitDone(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("daily cycle err = %v, want context.Canceled", err)
	}

	for _, ev := range rig.snapshot() {
		if ev[:6] == "hourly" {
			t.Fatalf("hourly ran after cancellation: %v", rig.snapshot())
		}
	}
	if n := len(rig.recordsBySource(cycleSourceInterleaved)); n != 0 {
		t.Fatalf("interleaved pass started after cancellation (%d records)", n)
	}
	if got := rig.pendingSnapshot(); !slices.Equal(got, []string{"hourly"}) {
		t.Fatalf("pending = %v, want [hourly]", got)
	}
}

// The interleaved pass takes only "hourly" out of pending; other coalesced
// tiers stay queued for the normal drain.
func TestInterleave_LeavesOtherPendingTiers(t *testing.T) {
	d1 := ilCandidate("daily", "db/cpu/2026/11/01", 20)
	h := ilCandidate("hourly", "db/cpu/2026/11/08/03", 10)
	rig := newILRig(t, 1, true, []Candidate{d1}, []Candidate{h})

	var pendingDuringHourly []string
	var weeklyScansDuringHourly int32 = -1
	rig.onStart = func(b Candidate) {
		if b.Tier == "hourly" {
			pendingDuringHourly = rig.pendingSnapshot()
			weeklyScansDuringHourly = rig.weeklyScans.Load()
		}
	}
	started, release := rig.hold("db/cpu/2026/11/01#1")
	done := rig.startCycle(context.Background(), "daily")
	waitClosed(t, started, "daily 01")
	if id, err := rig.m.RunCompactionCycleForTiers(context.Background(), []string{"weekly"}); err != nil || id != 0 {
		t.Fatalf("weekly request = (%d, %v)", id, err)
	}
	rig.requestHourly()
	if got := rig.pendingSnapshot(); !slices.Equal(got, []string{"weekly", "hourly"}) {
		t.Fatalf("pending = %v, want [weekly hourly]", got)
	}
	release()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}

	if !slices.Equal(pendingDuringHourly, []string{"weekly"}) {
		t.Fatalf("pending during the interleaved pass = %v, want [weekly]", pendingDuringHourly)
	}
	if weeklyScansDuringHourly != 0 {
		t.Fatalf("weekly scanned %d times before/while the interleaved hourly pass ran", weeklyScansDuringHourly)
	}
	if rig.weeklyScans.Load() != 1 {
		t.Fatalf("weekly scans = %d, want 1 (drained after the daily cycle)", rig.weeklyScans.Load())
	}
	co := rig.recordsBySource(cycleSourceCoalesced)
	if len(co) != 1 || !slices.Equal(co[0].Tiers, []string{"weekly"}) {
		t.Fatalf("coalesced records = %+v, want one weekly drain", co)
	}
	if got := rig.pendingSnapshot(); len(got) != 0 {
		t.Fatalf("pending after drain = %v", got)
	}
}

// No daily batch -- and so no yield -- starts until discovery has finished for
// every measurement of the daily tier, so no daily LIST runs alongside the
// hourly pass's writes and deletes.
func TestInterleave_WaitsForAllDailyDiscovery(t *testing.T) {
	d1 := ilCandidate("daily", "db/cpu/2026/11/01", 20)
	h := ilCandidate("hourly", "db/cpu/2026/11/08/03", 10)
	rig := newILRig(t, 1, true, []Candidate{d1}, []Candidate{h})
	if err := rig.m.StorageBackend.Write(context.Background(), "db/mem/2026/11/01/00/seed.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	dm := ilCandidate("daily", "db/mem/2026/11/01", 10)
	dm.Measurement = "mem"
	memEntered := make(chan struct{})
	memRelease := make(chan struct{})
	rig.dailyFind = func(ctx context.Context, measurement string) ([]Candidate, error) {
		if measurement != "mem" {
			return nil, nil
		}
		close(memEntered)
		select {
		case <-memRelease:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []Candidate{dm}, nil
	}

	done := rig.startCycle(context.Background(), "daily")
	waitClosed(t, memEntered, "mem discovery")
	rig.requestHourly()
	time.Sleep(60 * time.Millisecond)
	if ev := rig.snapshot(); len(ev) != 0 {
		t.Fatalf("batches ran while daily discovery was still listing: %v", ev)
	}
	close(memRelease)
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daily cycle: %v", err)
	}

	want := []string{
		"hourly:db/cpu/2026/11/08/03#1:start", "hourly:db/cpu/2026/11/08/03#1:end",
		"daily:db/cpu/2026/11/01#1:start", "daily:db/cpu/2026/11/01#1:end",
		"daily:db/cpu/2026/11/01#2:start", "daily:db/cpu/2026/11/01#2:end",
		"daily:db/mem/2026/11/01#1:start", "daily:db/mem/2026/11/01#1:end",
	}
	got := rig.snapshot()
	if !slices.Equal(got, want) {
		// Discovery order between measurements is not fixed.
		alt := append(append([]string{}, want[:2]...), want[6:]...)
		alt = append(alt, want[2:6]...)
		if !slices.Equal(got, alt) {
			t.Fatalf("events =\n%v\nwant\n%v", got, want)
		}
	}
}
