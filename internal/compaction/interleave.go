package compaction

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	hourlyTierName = "hourly"
	dailyTierName  = "daily"
)

// dailyInterleave is what one daily tier pass shares with the hourly passes it
// runs between its batches (compaction.hourly_during_daily).
//
// Exclusivity invariant: no hourly batch runs while a daily batch is in flight,
// and no daily batch starts while an interleaved pass runs. Every daily batch
// holds gate's read side for exactly the batch; a pass holds the write side.
// Deadlock-free because a worker holds the read side only around one batch --
// never while it yields or waits for yieldMu -- so a yielder waiting for the
// write side waits only for batches already running, and RWMutex blocks new
// readers behind it so it cannot starve. yieldMu admits one yielder at a time;
// the rest find hourly gone from pending once they get it.
type dailyInterleave struct {
	cycleID int64
	yieldMu sync.Mutex
	gate    sync.RWMutex
	open    openDayPartitions

	mu     sync.Mutex
	queued []queuedPartition
}

type queuedPartition struct {
	path    string
	batches []Candidate
}

// enqueue holds a discovered partition until discovery ends, marking its day
// open so an interleaved pass leaves its hours alone until its worker ends.
func (il *dailyInterleave) enqueue(path string, batches []Candidate) {
	il.open.add(path)
	il.mu.Lock()
	il.queued = append(il.queued, queuedPartition{path: path, batches: batches})
	il.mu.Unlock()
}

// batchStart and batchEnd bracket one daily batch; no-ops without interleave.
func (il *dailyInterleave) batchStart() {
	if il != nil {
		il.gate.RLock()
	}
}

func (il *dailyInterleave) batchEnd() {
	if il != nil {
		il.gate.RUnlock()
	}
}

// openDayPartitions counts the day partitions a daily pass discovered and has
// not finished. Daily listed their hour-level files and will read and delete
// them, so an hourly pass must not compact those hours meanwhile.
type openDayPartitions struct {
	mu   sync.Mutex
	days map[string]int
}

func (s *openDayPartitions) add(day string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.days == nil {
		s.days = make(map[string]int)
	}
	s.days[day]++
}

func (s *openDayPartitions) remove(day string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.days[day] <= 1 {
		delete(s.days, day)
		return
	}
	s.days[day]--
}

// covers reports whether partitionPath is an open day or lies under one
// (day + "/" prefix).
func (s *openDayPartitions) covers(partitionPath string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for p := partitionPath; p != ""; {
		if s.days[p] > 0 {
			return true
		}
		i := strings.LastIndexByte(p, '/')
		if i < 0 {
			break
		}
		p = p[:i]
	}
	return false
}

// interleavedPass marks a cycle body run by yieldToHourly.
type interleavedPass struct {
	open    *openDayPartitions
	skipped atomic.Int64
}

func (m *Manager) hourlyPending() bool {
	m.pendMu.Lock()
	defer m.pendMu.Unlock()
	return slices.Contains(m.pending, hourlyTierName)
}

// withoutTier returns tiers minus name in a fresh slice (nil when empty), and
// whether name was present.
func withoutTier(tiers []string, name string) ([]string, bool) {
	i := slices.Index(tiers, name)
	if i < 0 {
		return tiers, false
	}
	if len(tiers) == 1 {
		return nil, true
	}
	out := make([]string, 0, len(tiers)-1)
	out = append(out, tiers[:i]...)
	return append(out, tiers[i+1:]...), true
}

// yieldToHourly runs a coalesced hourly request inside the daily tier pass il,
// under the claim that pass's cycle holds, as its own cycle record. Called by
// a daily worker between batches, holding no gate. Takes only "hourly" out of
// pending; a paused or cancelled cycle leaves it there for the normal drain.
// Full recovery is skipped (only completed manifests are finished) and hours
// inside open day partitions are left alone.
func (m *Manager) yieldToHourly(ctx context.Context, il *dailyInterleave) {
	if !m.hourlyPending() {
		return
	}
	il.yieldMu.Lock()
	defer il.yieldMu.Unlock()
	if ctx.Err() != nil || m.Paused() || !m.hourlyPending() {
		return
	}
	il.gate.Lock()
	defer il.gate.Unlock()

	m.pendMu.Lock()
	if ctx.Err() != nil || m.Paused() {
		m.pendMu.Unlock()
		return
	}
	var took bool
	m.pending, took = withoutTier(m.pending, hourlyTierName)
	m.pendMu.Unlock()
	if !took {
		return
	}

	id := m.nextCycleID()
	pass := &interleavedPass{open: &il.open}
	began := time.Now()
	m.logger.Info().
		Int64("cycle_id", id).
		Int64("interleaved_in", il.cycleID).
		Msg("Running coalesced hourly request inside the daily cycle")
	err := m.runCycleBody(ctx, id, cycleSourceInterleaved, nil, []string{hourlyTierName}, "", pass)
	if errors.Is(err, ErrCompactionPaused) || ctx.Err() != nil {
		m.pendMu.Lock()
		m.pending = unionTiers(m.pending, []string{hourlyTierName})
		m.pendMu.Unlock()
	}
	m.logger.Info().
		Int64("cycle_id", id).
		Int64("interleaved_in", il.cycleID).
		Int64("skipped_partitions", pass.skipped.Load()).
		Dur("duration", time.Since(began)).
		Msg("Interleaved hourly pass finished")
}
