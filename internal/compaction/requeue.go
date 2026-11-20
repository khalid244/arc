package compaction

import (
	"context"
	"errors"
	"time"
)

// Retrying a tier whose cycle failed before it did any work
// (compaction.requeue_failed_tiers), and the coalesced drain's own budget
// (compaction.coalesced_fresh_timeout).
//
// Why requeue: a drained request has already left pending, so a drain that
// fails at once loses it. On 2026-11-17 the 03:00Z daily coalesced behind an
// hourly cycle, and its drain (cycle 441) failed in the same second with
// "circuit breaker is open" while listing databases; nothing asked for daily
// again until the next 03:00Z.

const defaultRequeueMinDelay = 30 * time.Minute

// requeueState is one tier's retry. Guarded by pendMu.
type requeueState struct {
	retryAfter time.Time
	// attempts counts the tier's consecutive failed runs, the first included.
	attempts int
	lastErr  string
	// waiting is true until the retry is promoted into pending.
	waiting bool
}

// RequeuedTier is a tier's retry as Stats reports it (requeued_tiers).
type RequeuedTier struct {
	RetryAfter time.Time `json:"retry_after"`
	Attempts   int       `json:"attempts"`
	LastError  string    `json:"last_error"`
	// Waiting is false while the retry runs.
	Waiting bool `json:"waiting"`
}

// settleRequeues runs in a cycle's finalizer, with failErr the cycle's error
// when it failed (nil when it succeeded, or was cancelled, timed out or paused).
//
// A tier the cycle settled drops its retry: it did its work, and running the
// retry too would run it twice. A tier settles when it started at least one
// batch -- even if most of its discovery failed (prod cycles 408 and 426 had
// that shape), so a partial failure is left to the next scheduled run, not
// retried -- or when it scanned every database and namespace without a
// discovery error.
//
// When the cycle failed, every requested tier it did not settle -- the cycle
// failed before reaching it, or its discovery failed and it started nothing --
// is queued to run again no sooner than RequeueMinDelay after this failure, so
// during a long storage outage each tier is tried once per delay instead of at
// every drain. A promoted retry that ends neither way (cancelled, timed out,
// paused) is dropped: those are never retried.
//
// Only unscoped cycles take part: pending holds only unscoped work, and an
// interleaved hourly pass is retried by yieldToHourly's own rules. Only
// scheduled and coalesced cycles queue a retry: an operator's trigger reports
// its own failure.
func (m *Manager) settleRequeues(source string, unscoped bool, tierNames []string, settled map[string]bool, failErr error) {
	if !m.RequeueFailedTiers || !unscoped {
		return
	}
	if source != cycleSourceScheduler && source != cycleSourceCoalesced {
		failErr = nil
	}
	retryAt := time.Now().Add(m.RequeueMinDelay)
	type retried struct {
		name     string
		attempts int
	}
	var retry []retried
	m.pendMu.Lock()
	for _, name := range tierNames {
		st := m.requeued[name]
		switch {
		case settled[name]:
			delete(m.requeued, name)
		case failErr != nil && m.tierEnabled(name):
			if st == nil {
				if m.requeued == nil {
					m.requeued = make(map[string]*requeueState)
				}
				st = &requeueState{}
				m.requeued[name] = st
			}
			st.attempts++
			st.retryAfter = retryAt
			st.lastErr = truncateCycleError(failErr.Error())
			st.waiting = true
			retry = append(retry, retried{name, st.attempts})
		case st != nil && !st.waiting:
			delete(m.requeued, name)
		}
	}
	m.pendMu.Unlock()
	for _, r := range retry {
		m.logger.Warn().Err(failErr).
			Str("tier", r.name).
			Int("attempts", r.attempts).
			Time("retry_after", retryAt).
			Msg("Compaction tier failed before starting any batch; re-queued")
	}
}

func (m *Manager) tierEnabled(name string) bool {
	for _, tier := range m.Tiers {
		if tier.GetTierName() == name {
			return tier.IsEnabled()
		}
	}
	return false
}

// promoteDueRequeuesLocked moves waiting retries whose delay has passed into
// pending, in tier order. Caller holds pendMu.
func (m *Manager) promoteDueRequeuesLocked(now time.Time) {
	if len(m.requeued) == 0 {
		return
	}
	for _, tier := range m.Tiers {
		name := tier.GetTierName()
		if st := m.requeued[name]; st != nil && st.waiting && !now.Before(st.retryAfter) {
			st.waiting = false
			m.pending = unionTiers(m.pending, []string{name})
		}
	}
}

// requeueSnapshot is requeued_tiers in Stats: always a map, so JSON shows {}.
func (m *Manager) requeueSnapshot() map[string]RequeuedTier {
	m.pendMu.Lock()
	defer m.pendMu.Unlock()
	out := make(map[string]RequeuedTier, len(m.requeued))
	for name, st := range m.requeued {
		out[name] = RequeuedTier{RetryAfter: st.retryAfter, Attempts: st.attempts, LastError: st.lastErr, Waiting: st.waiting}
	}
	return out
}

// SetCompactGate wires the cluster compactor gate (CanCompact) the drain loop
// re-checks; nil (OSS) always allows.
func (m *Manager) SetCompactGate(gate func() bool) {
	if gate == nil {
		m.compactGate.Store(nil)
		return
	}
	m.compactGate.Store(&gate)
}

// mayDrain re-checks the compactor gate before each coalesced drain or
// promoted retry. The scheduler checks it only before the owner's own cycle,
// and with fresh budgets and retries one owner can keep draining for days; a
// node that lost the compactor lease meanwhile must stop instead of compacting
// alongside the new holder. Work stays pending for this node's next owner, as
// on a pause. Checked only with a fork drain flag on, so the default keeps
// today's behaviour.
func (m *Manager) mayDrain() bool {
	if !m.RequeueFailedTiers && !m.CoalescedFreshTimeout {
		return true
	}
	gate := m.compactGate.Load()
	return gate == nil || (*gate)()
}

// ownerStopsDrain reports whether the owner's context ends the drain loop.
// With CoalescedFreshTimeout a drain does not spend the owner's budget, so an
// owner that has used its own up no longer holds the queue back; cancellation
// still does.
func (m *Manager) ownerStopsDrain(ctx context.Context) bool {
	if m.CoalescedFreshTimeout {
		return errors.Is(ctx.Err(), context.Canceled)
	}
	return ctx.Err() != nil
}

// coalescedContext is the context a coalesced drain runs on. Off, it is the
// owner's, so a drain that starts hours into its owner's cycle gets only the
// rest of that budget: a daily coalesced behind an hourly cycle that began at
// 02:40Z died at 02:40Z the next day after ~14.8 h of its own work. On, the
// drain gets CycleTimeout measured from its own start, keeps the owner's
// values, and is still cancelled when the owner is cancelled (shutdown, a
// cancelled trigger) -- but not when the owner merely reaches its deadline.
func (m *Manager) coalescedContext(owner context.Context) (context.Context, context.CancelFunc) {
	if !m.CoalescedFreshTimeout {
		return owner, func() {}
	}
	timeout := m.CycleTimeout
	if timeout <= 0 {
		timeout = defaultCycleTimeout
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(owner), timeout)
	stop := context.AfterFunc(owner, func() {
		if errors.Is(owner.Err(), context.Canceled) {
			cancel()
		}
	})
	return ctx, func() {
		stop()
		cancel()
	}
}
