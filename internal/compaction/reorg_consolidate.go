package compaction

import (
	"context"
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
// Memory: a group is bounded by MaxFilesPerBatch files (as a chunk is),
// ConsolidateMaxBuckets buckets, and ConsolidateMaxSourceBytes of source
// parquet, which bounds the rows the COPY buffers per event day and the
// scratch disk (sources + outputs). Groups run one at a time, so the
// in-process DuckDB never holds more than one group. A bucket that alone
// exceeds a cap, or that sat in a group that failed or drained nothing (see
// markSolo), is drained by processBucket exactly as without consolidation.
//
// MaxBucketsPerRun keeps its meaning: a group takes at most the slots left,
// and only buckets that moved a source or failed keep theirs.
func (r *Reorganizer) drainConsolidated(ctx context.Context, db, measurement, lateName, prefix string, hours []time.Time, buckets map[time.Time][]string) error {
	maxFiles := r.MaxFilesPerBatch
	if maxFiles <= 0 {
		maxFiles = 2000
	}
	maxBuckets := r.ConsolidateMaxBuckets
	var sizes map[string]int64
	if r.ConsolidateMaxSourceBytes > 0 {
		var ok bool
		if sizes, ok = r.sourceSizes(ctx, prefix); !ok {
			// The byte cap cannot be checked: drain one bucket per unit,
			// the bound the per-bucket drain always had, instead of guessing.
			maxBuckets = 1
		}
	}
	solo := r.soloSet(db, lateName, hours)
	limit := r.MaxBucketsPerRun
	base := time.Now().UnixNano()
	used, noops, groupIdx := 0, 0, 0

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
		jobID := fmt.Sprintf("%d_%d", base, groupIdx)
		groupIdx++
		moved, err := r.processChunk(ctx, db, measurement, lateName, group[0], sources, jobID)
		if err != nil {
			if ctx.Err() == nil {
				r.markSolo(db, lateName, group)
			}
			r.Logger.Error().Err(err).
				Str("late_measurement", lateName).
				Time("first_hour", group[0]).
				Time("last_hour", group[n-1]).
				Int("buckets", n).
				Int("files", len(sources)).
				Msg("Reorg consolidated group failed; its sources stay in the sidecar and its buckets drain one at a time from now on")
			for _, h := range group {
				account(h, len(buckets[h]), false, err)
			}
			continue
		}
		if len(moved) == 0 {
			// Nothing written: every key a proven phantom, or the group
			// skipped because its FIRST file's `time` is not a TIMESTAMP
			// (processChunk sniffs one file). Regrouped, the same group
			// would be skipped every cycle with its healthy buckets in it.
			r.markSolo(db, lateName, group)
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
// maxBytes source bytes (maxBuckets/maxBytes <= 0: no cap; a key missing from
// sizes counts 0, it is gone or a phantom). The first bucket is always taken:
// when it alone reaches a cap, or solo reports it, nothing joins it and the
// caller drains it like the per-bucket path does. A solo bucket never joins a
// group either.
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
		for _, k := range buckets[h] {
			b += sizes[k]
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

// markSolo remembers the buckets of a group that failed or drained nothing so
// they are drained one at a time from then on. A failure caused by one
// bucket's data (a corrupt source, a schema the COPY rejects, a row-count
// mismatch, a non-TIMESTAMP `time`) repeats on every attempt; regrouping the
// same oldest buckets each cycle would then keep the healthy buckets of the
// group from ever draining, which the per-bucket drain does not allow. The
// fallback waits for the next cycle because a failed group may hold a pending
// manifest over its sources. Process memory only: after a restart they are
// tried grouped once more.
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

// soloSet forgets the remembered solo buckets of this sidecar that are no
// longer among its closed buckets (drained, or held by a manifest) and returns
// a lookup over the rest.
func (r *Reorganizer) soloSet(db, lateName string, hours []time.Time) func(time.Time) bool {
	r.soloMu.Lock()
	defer r.soloMu.Unlock()
	keep := make(map[string]bool)
	if len(r.soloBuckets) > 0 {
		listed := make(map[string]bool, len(hours))
		for _, h := range hours {
			listed[soloKey(db, lateName, h)] = true
		}
		sidecar := db + "/" + lateName + "/"
		for k := range r.soloBuckets {
			if !strings.HasPrefix(k, sidecar) {
				continue
			}
			if listed[k] {
				keep[k] = true
			} else {
				delete(r.soloBuckets, k)
			}
		}
	}
	return func(h time.Time) bool { return keep[soloKey(db, lateName, h)] }
}
