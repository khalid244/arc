package compaction

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// compaction.daily_order: the order a daily tier pass launches its day
// partitions in.
const (
	// DailyOrderDiscovery launches them in the order discovery finds them,
	// which across measurements is whichever LIST returns first.
	DailyOrderDiscovery = "discovery"
	// DailyOrderPriority holds them until discovery ends and launches them
	// in orderDailyByPriority order.
	DailyOrderPriority = "priority"
)

// dailyRank is what orderDailyByPriority sorts one held day partition by,
// taken from the files its batches will merge.
type dailyRank struct {
	// firstMerge: the day still holds hour-level files. A day whose first
	// merge was cut off after some batches has _daily outputs too, so "no
	// _daily file yet" would rank it as a re-merge.
	firstMerge bool
	// unsealed counts the daily inputs (hour-level and reorg files): what a
	// re-merge folds into the sealed file.
	unsealed int
	day      time.Time
}

func rankDailyPartition(q queuedPartition) dailyRank {
	r := dailyRank{}
	if len(q.batches) > 0 {
		r.day = q.batches[0].PartitionTime
	}
	for _, b := range q.batches {
		for _, f := range b.Files {
			logical := storage.StripRewriteSuffix(f)
			if strings.HasSuffix(logical, "_daily.parquet") || !isDailyInputFile(logical) {
				continue
			}
			r.unsealed++
			if isHourLevelFile(logical) {
				r.firstMerge = true
			}
		}
	}
	return r
}

// orderDailyByPriority sorts the day partitions a daily tier pass holds, over
// every database and measurement, and returns how many need a first merge.
//
// A day still in hour folders is what slows queries most, and a cycle the
// timeout cuts off must reach it before old days that only fold in a few late
// rows, each by rewriting a sealed file of up to daily_max_output_size_mb. So
// first merges go first, newest day first. Re-merges follow, most unsealed
// files first: newest-first alone would leave the oldest days behind every
// cycle. Remaining ties go newest day first, then by path, so the order does
// not depend on which LIST returned first.
//
// The batches of one day stay one entry, in order: one worker runs them.
func orderDailyByPriority(queued []queuedPartition) int {
	type ranked struct {
		q queuedPartition
		r dailyRank
	}
	rs := make([]ranked, len(queued))
	firstMerges := 0
	for i, q := range queued {
		rs[i] = ranked{q: q, r: rankDailyPartition(q)}
		if rs[i].r.firstMerge {
			firstMerges++
		}
	}
	slices.SortStableFunc(rs, func(a, b ranked) int {
		if a.r.firstMerge != b.r.firstMerge {
			if a.r.firstMerge {
				return -1
			}
			return 1
		}
		if !a.r.firstMerge {
			if c := cmp.Compare(b.r.unsealed, a.r.unsealed); c != 0 {
				return c
			}
		}
		if c := b.r.day.Compare(a.r.day); c != 0 {
			return c
		}
		return strings.Compare(a.q.path, b.q.path)
	})
	for i := range rs {
		queued[i] = rs[i].q
	}
	return firstMerges
}
