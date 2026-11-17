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

// settleRequeues runs in a cycle's finalizer. A tier the cycle settled drops
// any retry it had queued: it has done its work, and running the retry too
// would run it twice. When the cycle failed, every requested tier it did not
// settle -- the cycle failed before reaching it, or its discovery failed and it
// started no batch -- is queued to run again no sooner than RequeueMinDelay.
// The delay is from this failure, so during a long storage outage each tier is
// tried once per delay instead of at every drain.
//
// Only unscoped cycles take part: pending holds only unscoped work, and an
// interleaved hourly pass is retried by yieldToHourly's own rules. Only
// scheduled and coalesced cycles queue a retry: an operator's trigger reports
// its own failure.
func (m *Manager) settleRequeues(source string, unscoped bool, tierNames []string, settled map[string]bool, failed bool) {
	if !m.RequeueFailedTiers || !unscoped {
		return
	}
	failed = failed && (source == cycleSourceScheduler || source == cycleSourceCoalesced)
	retryAt := time.Now().Add(m.RequeueMinDelay)
	var retry []string
	m.pendMu.Lock()
	for _, name := range tierNames {
		switch {
		case settled[name]:
			delete(m.requeued, name)
		case failed && m.tierEnabled(name):
			if m.requeued == nil {
				m.requeued = make(map[string]time.Time)
			}
			m.requeued[name] = retryAt
			retry = append(retry, name)
		}
	}
	m.pendMu.Unlock()
	if len(retry) > 0 {
		m.logger.Warn().
			Strs("tiers", retry).
			Time("retry_after", retryAt).
			Msg("Compaction tiers failed before starting any batch; re-queued")
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

// promoteDueRequeuesLocked moves requeued tiers whose delay has passed into
// pending, in tier order. Caller holds pendMu.
func (m *Manager) promoteDueRequeuesLocked(now time.Time) {
	if len(m.requeued) == 0 {
		return
	}
	for _, tier := range m.Tiers {
		name := tier.GetTierName()
		if at, ok := m.requeued[name]; ok && !now.Before(at) {
			delete(m.requeued, name)
			m.pending = unionTiers(m.pending, []string{name})
		}
	}
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
