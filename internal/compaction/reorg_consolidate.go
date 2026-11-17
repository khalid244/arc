package compaction

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// drainConsolidated is runOne's drain loop when ConsolidateOutputs is set: it
// drains the closed buckets (hours, oldest first) as groups of consecutive
// buckets, each group ONE processChunk call. The per-bucket drain writes one
// output per (bucket, event day); every bucket of a drain spreads its rows
// over the same ~31 event days, so a 6-bucket drain put 6 small files into
// each of those days, and every such file makes its day a daily-compaction
// candidate (Case 4) and costs every query one more open.
//
// Crash safety is processChunk's, unchanged: a group is one chunk with one
// manifest listing every source of every bucket in it and every output.
// Sources are deleted only after all outputs are uploaded and the manifest is
// marked uploaded; a crash or failure before that leaves a pending manifest
// that recovery rolls back as a whole (every output of the jobID deleted and
// HEAD-verified gone before the sources are released). While the manifest
// exists the drain's source filter keeps all of its sources out and the
// compaction filter keeps all of its outputs out of daily merges. Nothing in
// recovery depends on a manifest covering one hour: BucketHour (the oldest
// bucket) only names the manifest key, and the jobID keeps it unique.
//
// Unchanged invariants: only closed buckets are drained (runOne selects them),
// outputs are named <m>_reorg_<base>_<n>_<seq>.parquet in the event day like
// per-bucket outputs (the Case 4 marker and the day-level layout daily reads),
// and outputs go to the base measurement, never a _late one.
//
// Memory: the partitioned COPY buffers the rows it reads, so its peak RSS
// grows with the group's input and is NOT reliably held by memory_limit
// (replayed on real posthog.events rows at memory_limit=8GB: 150 MiB of
// source peaked at 8.1 GB RSS, 300 MiB at 11.5 GB; at a 3GB limit 300 MiB
// still reached 6.0 GB without an error). ConsolidateMaxSourceBytes is
// therefore the memory bound, next to MaxFilesPerBatch files (as for a
// chunk) and ConsolidateMaxBuckets buckets; it also bounds the scratch disk
// (sources + outputs). Groups run one at a time, so the in-process DuckDB
// never holds more than one group. A bucket that alone exceeds a cap is
// drained by processBucket exactly as without consolidation.
//
// Failure isolation matches the per-bucket drain. A group that fails before
// its manifest is written (download, COPY, audit), or that drains nothing
// (phantom keys only, or skipped because its first file's `time` is not a
// TIMESTAMP), wrote nothing to storage, so its buckets are drained one at a
// time in the same cycle and only a failing bucket waits. After such a
// failure the sidecar drains one bucket at a time for
// consolidateFailureBackoff, so a failure of the group itself (memory, a
// schema conflict between buckets) does not repeat every cycle. A group that fails
// from its manifest write on may have outputs in storage and its sources held
// by a pending manifest: those wait for recovery and then drain one at a
// time (markSolo).
//
// MaxBucketsPerRun keeps its meaning: a group takes at most the slots left,
// and only buckets that moved a source or failed keep theirs.
func (r *Reorganizer) drainConsolidated(ctx context.Context, db, measurement, lateName, prefix string, hours []time.Time, buckets map[time.Time][]string) error {
	maxFiles := r.MaxFilesPerBatch
	if maxFiles <= 0 {
		maxFiles = 2000
	}
	maxBuckets := r.ConsolidateMaxBuckets
	if until, paused := r.groupingPausedUntil(db, lateName); paused {
		r.Logger.Info().
			Str("late_measurement", lateName).
			Time("until", until).
			Msg("Reorg: grouping paused after a consolidated group failed before its manifest; draining one bucket at a time")
		maxBuckets = 1
	}
	var sizes map[string]int64
	if r.ConsolidateMaxSourceBytes > 0 {
		// nil when the listing is unavailable: every size is then unknown
		// and planGroup drains each bucket alone instead of guessing.
		sizes, _ = r.sourceSizes(ctx, prefix)
	}
	solo := func(h time.Time) bool { return r.isSolo(db, lateName, h) }
	limit := r.MaxBucketsPerRun
	used, noops := 0, 0

	account := func(h time.Time, files int, moved bool, err error) {
		if err != nil {
			used++
			metrics.Get().IncReorgBucketsFailed()
			r.Logger.Error().Err(err).Time("hour", h).Int("files", files).Msg("Reorg bucket failed")
			return
		}
		metrics.Get().IncReorgBucketsSuccess()
		if moved {
			used++
		} else {
			noops++
		}
	}

	for next := 0; next < len(hours) && (limit <= 0 || used < limit); {
		if err := ctx.Err(); err != nil {
			return err
		}
		capBuckets := maxBuckets
		if limit > 0 && (capBuckets <= 0 || capBuckets > limit-used) {
			capBuckets = limit - used
		}
		n := planGroup(hours[next:], buckets, sizes, capBuckets, maxFiles, r.ConsolidateMaxSourceBytes, solo)
		group := hours[next : next+n]
		next += n

		if n == 1 {
			h := group[0]
			moved, err := r.processBucket(ctx, db, measurement, lateName, h, buckets[h])
			account(h, len(buckets[h]), moved > 0, err)
			continue
		}

		var sources []string
		var groupBytes int64
		hourOf := make(map[string]time.Time)
		for _, h := range group {
			for _, k := range buckets[h] {
				sources = append(sources, k)
				hourOf[k] = h
				groupBytes += sizes[k]
			}
		}
		// Same <base>_<index> shape as processBucket's chunk jobIDs.
		jobID := fmt.Sprintf("%d_0", r.nextJobBase())
		moved, err := r.processChunk(ctx, db, measurement, lateName, group[0], sources, jobID)
		if err != nil && (ctx.Err() != nil || errors.As(err, new(commitPhaseError))) {
			// Cancelled (the next cycle regroups; a pending manifest is
			// recovered as usual), or failed with this attempt's outputs
			// possibly in storage: the sources are not drained again before
			// recovery has resolved the manifest.
			if ctx.Err() == nil {
				r.markSolo(db, lateName, group)
			}
			r.Logger.Error().Err(err).
				Str("late_measurement", lateName).
				Time("first_hour", group[0]).
				Time("last_hour", group[n-1]).
				Int("buckets", n).
				Int("files", len(sources)).
				Msg("Reorg consolidated group failed after its manifest was written (or was cancelled); recovery resolves it and its buckets then drain one at a time")
			for _, h := range group {
				account(h, len(buckets[h]), false, err)
			}
			continue
		}
		if err != nil || len(moved) == 0 {
			if err != nil {
				// A group-level failure (a COPY over the memory limit, a
				// schema conflict between buckets) would recur in the next
				// group too, costing a failed group plus this fallback on
				// every cycle; pause grouping for this sidecar instead.
				r.pauseGrouping(db, lateName)
			}
			r.Logger.Warn().Err(err).
				Str("late_measurement", lateName).
				Time("first_hour", group[0]).
				Time("last_hour", group[n-1]).
				Int("buckets", n).
				Msg("Reorg consolidated group failed before its manifest or drained nothing; draining its buckets one at a time now")
			for _, h := range group {
				if ctx.Err() != nil {
					break
				}
				// Keys this group just proved phantom are not fetched again.
				keys, _ := r.withoutRememberedPhantoms(buckets[h])
				var bucketMoved int
				var bucketErr error
				if len(keys) > 0 {
					bucketMoved, bucketErr = r.processBucket(ctx, db, measurement, lateName, h, keys)
				}
				account(h, len(buckets[h]), bucketMoved > 0, bucketErr)
				// Keep a bucket that does not drain alone out of later groups.
				if bucketMoved == 0 && ctx.Err() == nil {
					r.markSolo(db, lateName, []time.Time{h})
				}
			}
			continue
		}
		movedHours := make(map[time.Time]bool, n)
		for _, k := range moved {
			movedHours[hourOf[k]] = true
		}
		for _, h := range group {
			account(h, len(buckets[h]), movedHours[h], nil)
		}
		r.Logger.Info().
			Str("database", db).
			Str("measurement", measurement).
			Str("job_id", jobID).
			Time("first_hour", group[0]).
			Time("last_hour", group[n-1]).
			Int("buckets", n).
			Int("sources", len(sources)).
			Int("moved", len(moved)).
			Int64("source_bytes", groupBytes).
			Msg("Reorg consolidated group drained")
	}
	if noops > 0 {
		r.Logger.Info().
			Str("database", db).
			Str("late_measurement", lateName).
			Int("noop_buckets", noops).
			Int("drained_or_failed_buckets", used).
			Msg("Reorg: buckets that drained nothing (only phantom keys, or skipped) did not count toward max_buckets_per_run")
	}
	return ctx.Err()
}

// planGroup returns how many of hours (closed buckets, oldest first) the next
// drain unit takes; at least 1 when hours is not empty. Later buckets join
// while the group stays within maxBuckets buckets, maxFiles source files and
// maxBytes source bytes (maxBuckets/maxBytes <= 0: no cap). With a byte cap,
// a bucket with a key missing from sizes has an unknown size (sizes nil, or a
// listing that does not match the keys) and never joins a group, so the cap
// cannot be bypassed silently. The first bucket is always taken: when it
// alone reaches a cap, has an unknown size, or solo reports it, nothing joins
// it and the caller drains it like the per-bucket path does. A solo bucket
// never joins a group either.
func planGroup(hours []time.Time, buckets map[time.Time][]string, sizes map[string]int64, maxBuckets, maxFiles int, maxBytes int64, solo func(time.Time) bool) int {
	if len(hours) == 0 {
		return 0
	}
	if solo(hours[0]) {
		return 1
	}
	n, files := 0, 0
	var bytes int64
	for _, h := range hours {
		if n > 0 && (solo(h) || (maxBuckets > 0 && n >= maxBuckets)) {
			break
		}
		var b int64
		known := true
		for _, k := range buckets[h] {
			sz, ok := sizes[k]
			known = known && ok
			b += sz
		}
		if maxBytes > 0 && !known {
			if n == 0 {
				return 1
			}
			break
		}
		f := len(buckets[h])
		if n > 0 && (files+f > maxFiles || (maxBytes > 0 && bytes+b > maxBytes)) {
			break
		}
		n++
		files += f
		bytes += b
	}
	return n
}

// sourceSizes returns the size of every object under prefix from one
// ObjectLister listing (one LIST, like runOne's). ok is false when the backend
// cannot list sizes or the listing failed.
func (r *Reorganizer) sourceSizes(ctx context.Context, prefix string) (map[string]int64, bool) {
	ol, ok := r.Backend.(storage.ObjectLister)
	if !ok {
		r.Logger.Warn().Str("prefix", prefix).Msg("Reorg: backend cannot list object sizes; consolidate_max_source_mb cannot be checked, draining one bucket at a time")
		return nil, false
	}
	objs, err := ol.ListObjects(ctx, prefix)
	if err != nil {
		r.Logger.Warn().Err(err).Str("prefix", prefix).Msg("Reorg: listing source sizes failed; consolidate_max_source_mb cannot be checked, draining one bucket at a time this cycle")
		return nil, false
	}
	sizes := make(map[string]int64, len(objs))
	for _, o := range objs {
		sizes[filepath.ToSlash(o.Path)] = o.Size
	}
	return sizes, true
}

func soloKey(db, lateName string, h time.Time) string {
	return db + "/" + lateName + "/" + h.UTC().Format(time.RFC3339)
}

// markSolo marks buckets to be drained one at a time from then on: the
// buckets of a group that failed after its manifest write (they drain once
// recovery releases them), and buckets that did not drain alone in a
// same-cycle fallback. A failure caused by one bucket's data (a corrupt
// source, a schema the COPY rejects, a non-TIMESTAMP `time`) repeats on every
// attempt, so regrouping it would cost a failed group attempt every cycle.
// Marks are dropped once the bucket is no longer listed (pruneSolo). Process
// memory only: after a restart a bucket is tried grouped once more, and its
// group falls back to one bucket at a time again.
func (r *Reorganizer) markSolo(db, lateName string, hours []time.Time) {
	r.soloMu.Lock()
	defer r.soloMu.Unlock()
	if r.soloBuckets == nil {
		r.soloBuckets = make(map[string]struct{})
	}
	for _, h := range hours {
		r.soloBuckets[soloKey(db, lateName, h)] = struct{}{}
	}
}

// isSolo reports whether the bucket is marked by markSolo.
func (r *Reorganizer) isSolo(db, lateName string, h time.Time) bool {
	r.soloMu.Lock()
	defer r.soloMu.Unlock()
	_, ok := r.soloBuckets[soloKey(db, lateName, h)]
	return ok
}

// pruneSolo forgets this sidecar's solo marks whose bucket no longer has a
// listed source key. listed is every source key of the sidecar's listing,
// including keys a manifest holds: a bucket waiting for recovery of a failed
// group keeps its mark, so it does not rejoin a group once released.
func (r *Reorganizer) pruneSolo(db, lateName string, listed []string) {
	r.soloMu.Lock()
	defer r.soloMu.Unlock()
	if len(r.soloBuckets) == 0 {
		return
	}
	present := make(map[string]bool)
	for _, k := range listed {
		if ft, ok := parseFilenameTime(k); ok {
			present[soloKey(db, lateName, ft.Truncate(time.Hour))] = true
		}
	}
	sidecar := db + "/" + lateName + "/"
	for k := range r.soloBuckets {
		if strings.HasPrefix(k, sidecar) && !present[k] {
			delete(r.soloBuckets, k)
		}
	}
}

// nextJobBase returns a job-ID base unique within the process: the wall
// clock in ns, moved past the last base handed out. Output paths embed the
// jobID, so two chunks that start within one clock tick (macOS reports the
// wall clock in us; a solo bucket starts right after a group's base was
// taken) would otherwise write the same day files, and the later upload would
// silently replace the earlier chunk's rows after its sources were deleted.
func (r *Reorganizer) nextJobBase() int64 {
	for {
		last := r.lastJobBase.Load()
		next := time.Now().UnixNano()
		if next <= last {
			next = last + 1
		}
		if r.lastJobBase.CompareAndSwap(last, next) {
			return next
		}
	}
}

// consolidateFailureBackoff is how long a sidecar drains one bucket at a
// time after one of its groups failed before its manifest: four cycles at
// the 6-hourly production schedule, then grouping is tried again.
const consolidateFailureBackoff = 24 * time.Hour

func (r *Reorganizer) pauseGrouping(db, lateName string) {
	r.soloMu.Lock()
	defer r.soloMu.Unlock()
	if r.groupingPaused == nil {
		r.groupingPaused = make(map[string]time.Time)
	}
	r.groupingPaused[db+"/"+lateName] = time.Now().Add(consolidateFailureBackoff)
}

// groupingPausedUntil reports whether grouping is paused for the sidecar,
// forgetting an expired pause.
func (r *Reorganizer) groupingPausedUntil(db, lateName string) (time.Time, bool) {
	r.soloMu.Lock()
	defer r.soloMu.Unlock()
	until, ok := r.groupingPaused[db+"/"+lateName]
	if !ok {
		return time.Time{}, false
	}
	if !time.Now().Before(until) {
		delete(r.groupingPaused, db+"/"+lateName)
		return time.Time{}, false
	}
	return until, true
}

// commitPhaseError marks a processChunk failure from its manifest write on:
// outputs of that attempt may be in storage and a pending manifest may hold
// its sources, so they must not be drained again in the same cycle. Error()
// is the wrapped error's, so log text is unchanged.
type commitPhaseError struct{ err error }

func (e commitPhaseError) Error() string { return e.err.Error() }
func (e commitPhaseError) Unwrap() error { return e.err }
