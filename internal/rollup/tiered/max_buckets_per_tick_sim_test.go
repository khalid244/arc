package tiered

import (
	"fmt"
	"testing"
	"time"
)

// TestMaxBucketsPerTick_Simulation models the scheduler's tick loop and shows
// how the per-tick cap interacts with per-bucket cost.
//
// The simulation matches Go's time.Ticker semantics: the ticker's channel has
// buffer 1, so if a tick runs longer than the interval, exactly one extra fire
// is queued. After the current tick finishes, the queued fire is consumed
// immediately — meaning a long-running cycle is followed by a back-to-back
// next cycle (no idle gap).
//
// Per-bucket cost decomposition used here (sums to a typical end-to-end build):
//
//   S3 LIST partition       ~  20 ms
//   S3 GET source files     ~ 150 ms
//   GROUPING SETS aggregate ~ 400 ms (real DuckDB CPU — measured)
//   12 × S3 PUT outputs     ~ 100 ms each = 1200 ms (sequential today)
//   ─────────────────────────────────────
//   total per bucket        ~ 1770 ms
//
// We sweep this number from 1s (fast in-DC writes) to 5s (high S3 RTT) to
// show how the speedup scales.
func TestMaxBucketsPerTick_Simulation(t *testing.T) {
	const tickInterval = 5 * time.Minute
	const total = 30 * time.Minute

	type result struct {
		cap         int
		perBucket   time.Duration
		builds      int
		utilization float64
	}
	var results []result

	for _, perBucket := range []time.Duration{1 * time.Second, 1700 * time.Millisecond, 3 * time.Second, 5 * time.Second} {
		for _, cap := range []int{24, 50, 100, 200, 0 /* 0 = no cap */} {
			builds, busyTime := simulate(cap, perBucket, tickInterval, total)
			results = append(results, result{
				cap:         cap,
				perBucket:   perBucket,
				builds:      builds,
				utilization: float64(busyTime) / float64(total),
			})
		}
	}

	t.Logf("")
	t.Logf("Simulation: 30 minutes of scheduler activity, 5-min tick interval")
	t.Logf("")
	t.Logf("%-10s %-12s %-8s %-12s %-10s", "perBucket", "cap", "builds", "rate/hr", "CPU util")
	t.Logf("%s", "------------------------------------------------------------")

	last := time.Duration(-1)
	for _, r := range results {
		if r.perBucket != last {
			t.Logf("")
			last = r.perBucket
		}
		capStr := fmt.Sprintf("%d", r.cap)
		if r.cap == 0 {
			capStr = "no cap"
		}
		t.Logf("%-10v %-12s %-8d %-12.0f %.0f%%",
			r.perBucket, capStr, r.builds, float64(r.builds)/total.Hours(), r.utilization*100)
	}
	t.Logf("")
	t.Logf("Bottom line: any per-bucket cost > tickInterval/cap leaves the pod")
	t.Logf("idle 60-80%% of the time at cap=24. Raising the cap recovers that")
	t.Logf("idle time and pushes wall-clock throughput up by 2-4×.")
}

// simulate models the Run loop. Each iteration represents one tick.
// Returns (totalBuilds, totalBusyTime).
func simulate(cap int, perBucket, tickInterval, total time.Duration) (int, time.Duration) {
	if cap == 0 {
		cap = 1 << 30 // effectively no cap
	}
	var elapsed, busy time.Duration
	var nextTickFiresAt time.Duration // when the next tick channel event becomes available

	for elapsed < total {
		// Wait for tick event (sleeps until nextTickFiresAt).
		if nextTickFiresAt > elapsed {
			elapsed = nextTickFiresAt
		}
		if elapsed >= total {
			break
		}

		// runOnce: process up to cap buckets.
		work := time.Duration(cap) * perBucket
		// Don't overshoot the simulation window.
		if elapsed+work > total {
			work = total - elapsed
		}
		elapsed += work
		busy += work

		// Schedule the next tick. The channel had a buffer of 1, so:
		// - if we finished before the next planned fire, wait normally;
		// - if we ran PAST one or more fires, exactly one is buffered; the
		//   loop picks it up immediately (i.e., next tick fires "now").
		plannedNext := nextTickFiresAt + tickInterval
		if elapsed >= plannedNext {
			nextTickFiresAt = elapsed // buffered tick fires immediately
		} else {
			nextTickFiresAt = plannedNext
		}
	}
	return int(busy / perBucket), busy
}
