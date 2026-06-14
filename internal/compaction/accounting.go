package compaction

import (
	"sort"
	"time"
)

// unionTiers merges b into a, de-duplicating, preserving a's order then
// appending any new names from b. Used to coalesce colliding cycle requests
// into a single pending set so the queue can't grow unbounded.
func unionTiers(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, xs := range [][]string{a, b} {
		for _, x := range xs {
			if _, ok := seen[x]; ok {
				continue
			}
			seen[x] = struct{}{}
			out = append(out, x)
		}
	}
	return out
}

// recordTierRun stamps the time a tier last completed a scan in a cycle.
func (m *Manager) recordTierRun(tier string) {
	m.lastRunMu.Lock()
	m.lastRun[tier] = time.Now()
	m.lastRunMu.Unlock()
}

// LastSuccessfulRun returns the wall-clock time the named tier last completed a
// scan successfully, or the zero time if it has never run.
func (m *Manager) LastSuccessfulRun(tier string) time.Time {
	m.lastRunMu.Lock()
	defer m.lastRunMu.Unlock()
	return m.lastRun[tier]
}

// StaleTiers returns the names (sorted) of enabled tiers whose last successful
// run is older than now-threshold. A tier that has never run counts as stale
// (zero time is always before the cutoff). This is the starvation backstop: an
// alert on StaleTiers(26h, now) containing "daily" would have caught the
// scheduler-collision outage on day two instead of after a month.
func (m *Manager) StaleTiers(threshold time.Duration, now time.Time) []string {
	cutoff := now.Add(-threshold)

	m.lastRunMu.Lock()
	defer m.lastRunMu.Unlock()

	var stale []string
	for _, tier := range m.Tiers {
		if !tier.IsEnabled() {
			continue
		}
		name := tier.GetTierName()
		if m.lastRun[name].Before(cutoff) {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	return stale
}

// tierLastSuccessSnapshot returns a copy of the per-tier last-run map for
// Stats() (and the Prometheus gauge that reads it).
func (m *Manager) tierLastSuccessSnapshot() map[string]time.Time {
	m.lastRunMu.Lock()
	defer m.lastRunMu.Unlock()
	out := make(map[string]time.Time, len(m.lastRun))
	for k, v := range m.lastRun {
		out[k] = v
	}
	return out
}
