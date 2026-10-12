package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Reorganizer drains the flat events_late sidecar measurements written by
// the ingest's late-routing hook and redistributes their rows back into the
// normal Y/M/D/H partition layout under the original measurement.
//
// The reorganizer is intentionally NOT a Tier — the existing Tier/Job
// framework assumes per-partition in-place compaction, while reorg does
// fan-out: one source file can contain rows spanning many target hour
// partitions. Implementing it as a sibling component avoids contorting the
// Job code for a different shape of work.
//
// Safety contract: source files are deleted ONLY after every target file
// has been written successfully AND the row-count audit (input ==
// non-null-time-output) passes. Every chunk is tracked by a manifest
// (reorg_manifest.go): a crash after the upload completes the source delete
// on recovery; a crash or failure mid-upload is rolled back on recovery by
// deleting every output of that attempt BEFORE its sources are released to
// the next drain. No data loss and no duplicate target files.
//
// Outputs land in the rows' EVENT day, which can be far in the past. The
// daily tier only revisits the last 90 days plus its reconcile cursor, so a
// _reorg_ file in an older day is never folded into that day's daily file
// (see the [reorg] notes in arc.toml): every row stays readable, but the
// extra file and any duplicate it carries stay until the reconcile window is
// widened to reach that day.
type Reorganizer struct {
	Backend       storage.Backend
	Databases     []string // databases to scan; empty = discover
	Measurements  []string // base measurement names opted into late-split (e.g. ["events"])
	MinAgeSeconds int      // skip source files whose hour bucket is fresher than this
	TempDirectory string   // local scratch dir for DuckDB + downloads
	MemoryLimit   string   // DuckDB memory limit; empty = unset (config resolves it to compaction.memory_limit)
	// MaxTempDirectorySize caps each chunk's DuckDB spill under its scratch
	// dir (e.g. "12GiB"); empty = DuckDB's default (90% of free disk). The
	// config resolves it to compaction.max_temp_directory_size. With
	// MaxConcurrent > 1 the worst case is MaxConcurrent times this.
	MaxTempDirectorySize string
	MaxConcurrent        int // max parallel bucket workers; <= 0 falls back to 1
	MaxFilesPerBatch     int // chunk size for DuckDB COPY per bucket; <= 0 falls back to 2000
	DownloadWorkers      int // parallel S3 download workers per bucket; <= 0 falls back to 8
	// MaxBucketsPerRun caps how many closed hour-buckets one Run drains,
	// oldest first (see selectBuckets). <= 0 = drain all. Bounds each cycle
	// to a completable, committed unit of work so the backlog shrinks
	// monotonically instead of timing out mid-cycle.
	MaxBucketsPerRun int
	// ManifestManager provides crash recovery. nil disables manifest tracking
	// (a partial-crash will leak duplicates that daily-tier dedup folds).
	ManifestManager *ReorgManifestManager
	// ClusterGate is the same Phase 4 role check the compaction scheduler
	// uses. When non-nil and CanCompact() reports false, Run() stays idle
	// (not an error — silent no-op so multi-pod deployments can deploy
	// reorg config to every replica and only the leader actually does work).
	// nil means "no check, run unconditionally" — used by OSS / single-pod
	// deployments and by existing tests that predate clustering.
	ClusterGate ClusterGate
	Logger      zerolog.Logger

	// running guards against overlapping drains. The cron and a manual API
	// trigger (POST /api/v1/reorg/trigger) both call Run; two concurrent runs
	// would race on pre-manifest source files (both could re-read the same
	// bucket before either writes a manifest), producing duplicate target files.
	// CompareAndSwap makes the second caller a no-op. Mirrors the compactor's
	// IsCycleRunning() guard.
	running atomic.Bool

	// lifeMu guards the shutdown state below. Shutdown sets stopped (no new
	// run starts afterwards) and cancels the in-flight run through cancelRun;
	// runDone is closed when that run has returned.
	lifeMu    sync.Mutex
	stopped   bool
	cancelRun context.CancelFunc
	runDone   chan struct{}
}

// beginRun registers a run so Shutdown can cancel it. It returns the run's
// context (a cancellable child of parent) and a finish func the run must
// call when it returns; ok is false once Shutdown has been called.
func (r *Reorganizer) beginRun(parent context.Context) (ctx context.Context, finish func(), ok bool) {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()
	if r.stopped {
		return parent, func() {}, false
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	r.cancelRun = cancel
	r.runDone = done
	return ctx, func() {
		r.lifeMu.Lock()
		r.cancelRun = nil
		r.runDone = nil
		r.lifeMu.Unlock()
		cancel()
		close(done)
	}, true
}

// Shutdown stops the reorganizer: no Run started afterwards does any work,
// and the in-flight run (cron or API-triggered) has its context cancelled.
// It then waits for that run to return, but never past ctx: a run stuck in a
// call that ignores cancellation returns ctx's error instead of holding the
// rest of the shutdown sequence until SIGKILL.
//
// A cancelled run leaves state recovery resolves without loss or duplicates:
// a chunk cancelled before its manifest exists keeps its sources untouched; a
// pending manifest (cancelled mid-upload) is rolled back once its recorded
// writer deadline has passed; an uploaded manifest has its source delete
// finished by the next recovery pass.
func (r *Reorganizer) Shutdown(ctx context.Context) error {
	r.lifeMu.Lock()
	r.stopped = true
	cancel, done := r.cancelRun, r.runDone
	r.lifeMu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("reorganizer: in-flight run did not stop before the shutdown deadline: %w", ctx.Err())
	}
}

// IsRunning reports whether a drain pass is currently executing. Used by the
// API trigger handler to return 409 instead of stacking a second drain.
func (r *Reorganizer) IsRunning() bool { return r.running.Load() }

// filenameTimeRE captures the YYYYMMDD_HHMMSS prefix in our standard filename
// pattern <measurement>_YYYYMMDD_HHMMSS_<nanos>.parquet. Anchored at start of
// filename (no `/`) so we don't accidentally match digits embedded in some
// other prefix that happens to share the date-time shape.
var filenameTimeRE = regexp.MustCompile(`^[^/]*?(\d{8})_(\d{6})_\d+\.parquet$`)

// Run executes one drain pass: for every (database, measurement) combo, bucket
// the source files in <db>/<measurement>_late/ by their filename hour and
// reorganize each closed-hour bucket. A bucket is "closed" when its hour is
// older than MinAgeSeconds, which guarantees ingest has stopped writing new
// files into that hour partition of the sidecar.
//
// If a ManifestManager is configured, recovery for stale in-flight buckets
// runs FIRST so the listing in step 2 reflects the cleaned-up state — no
// risk of double-processing source files that an earlier run already
// uploaded.
func (r *Reorganizer) Run(ctx context.Context) error {
	// Phase 4 role gate: same chokepoint pattern as compaction/scheduler.go.
	// A non-compactor node that's been deployed with reorg config (because
	// every replica gets the same TOML) silently no-ops here rather than
	// stamping `events_late/` from multiple pods at once. Logged at debug
	// to avoid flooding logs on every cron tick.
	if r.ClusterGate != nil && !r.ClusterGate.CanCompact() {
		r.Logger.Debug().
			Str("role", r.ClusterGate.Role()).
			Msg("Reorg: gated by ClusterGate; this node is not the compactor — skipping cycle")
		return nil
	}
	// Overlap guard: the cron and a manual API trigger both call Run. Acquire
	// the run flag or no-op. Registered AFTER a successful CAS so a guarded
	// (losing) caller never clears the winner's flag. Wraps recovery + drain so
	// neither runs twice concurrently.
	if !r.running.CompareAndSwap(false, true) {
		r.Logger.Info().Msg("Reorg: a drain pass is already running; skipping this trigger")
		return nil
	}
	defer r.running.Store(false)
	ctx, finish, ok := r.beginRun(ctx)
	defer finish()
	if !ok {
		r.Logger.Info().Msg("Reorg: shutting down; not starting a drain pass")
		return nil
	}

	if r.ManifestManager != nil {
		// A failed recovery PASS (the manifest listing failed, or ctx ended)
		// skips the cycle: without the listing the drain cannot know which
		// sources an uploaded-but-not-yet-deleted chunk still owns, and would
		// re-drain them into a duplicate set of target files. A single
		// manifest that could not be recovered does not stop the pass; the
		// source filter in runOne keeps its sources out of this cycle.
		if _, err := r.ManifestManager.RecoverOrphanedReorgManifests(ctx); err != nil {
			r.Logger.Warn().Err(err).Msg("Reorg recovery could not run; skipping this cycle so no in-flight chunk is re-drained")
			return fmt.Errorf("reorg recovery: %w", err)
		}
	}

	minAge := time.Duration(r.MinAgeSeconds) * time.Second
	if minAge <= 0 {
		minAge = time.Hour
	}

	databases := r.Databases
	if len(databases) == 0 {
		if dl, ok := r.Backend.(storage.DirectoryLister); ok {
			dirs, err := dl.ListDirectories(ctx, "")
			if err != nil {
				return fmt.Errorf("list databases: %w", err)
			}
			for _, d := range dirs {
				// Reserved roots (_arc, _schema, dot-prefixed, ...) are
				// Arc's own state, never databases; "compaction" is the
				// legacy state dir Manager.listDatabases also skips.
				if d != "" && !storage.IsReservedRootDir(d) && d != "compaction" {
					databases = append(databases, d)
				}
			}
		}
	}

	// Late-split is default-on for every measurement, so we can't rely on a
	// fixed opt-in list. Discover each <db>/<base>_late/ sidecar per database
	// and drain it — this covers all default-on tables plus future ones, and
	// excluded measurements simply never produce a sidecar (so discovery skips
	// them naturally). When the backend doesn't implement DirectoryLister we
	// fall back to the legacy r.Measurements iteration so nothing breaks.
	dl, canList := r.Backend.(storage.DirectoryLister)
	for _, db := range databases {
		if err := ctx.Err(); err != nil {
			return err
		}
		lateMeasurements := r.discoverLateSidecars(ctx, db, dl, canList)
		for _, lateName := range lateMeasurements {
			if err := ctx.Err(); err != nil {
				return err
			}
			base := strings.TrimSuffix(lateName, ingest.LateSuffix)
			if base == "" {
				continue
			}
			if err := r.runOne(ctx, db, base, lateName, minAge); err != nil {
				r.Logger.Error().Err(err).
					Str("database", db).
					Str("late_measurement", lateName).
					Msg("Reorganize failed")
			}
		}
	}
	return nil
}

// discoverLateSidecars returns the late-sidecar directory names (e.g.
// "events_late") under <db>/ that the drain should process. When the backend
// supports DirectoryLister it lists <db>/ and selects entries ending in
// ingest.LateSuffix; otherwise it falls back to deriving names from the
// configured r.Measurements opt-in list (legacy behaviour).
//
// A name ending in "_late" is only a candidate: a real measurement can be
// called that. runOne drains nothing but the flat, sidecar-shaped files the
// ingest's late routing writes (isSidecarSourceKey), so a real measurement's
// nested Y/M/D/H files are never moved or deleted.
func (r *Reorganizer) discoverLateSidecars(ctx context.Context, db string, dl storage.DirectoryLister, canList bool) []string {
	if canList {
		dirs, err := dl.ListDirectories(ctx, db+"/")
		if err != nil {
			r.Logger.Warn().Err(err).
				Str("database", db).
				Msg("Reorg: failed to list sidecars; skipping database this cycle")
			return nil
		}
		var sidecars []string
		for _, d := range dirs {
			if strings.HasSuffix(d, ingest.LateSuffix) && d != ingest.LateSuffix {
				sidecars = append(sidecars, d)
			}
		}
		return sidecars
	}
	// Fallback: backend can't enumerate directories — use the opt-in list.
	var sidecars []string
	for _, measurement := range r.Measurements {
		m := strings.TrimSpace(measurement)
		if m == "" {
			continue
		}
		sidecars = append(sidecars, m+ingest.LateSuffix)
	}
	return sidecars
}

// sidecarNameSuffixRE matches what follows "<lateName>" in a late-sidecar
// file name: _YYYYMMDD_HHMMSS_<digits>.parquet (ingest flatLatePath and the
// LateStager both write a 9-digit nanosecond suffix; any digit count is
// accepted so older files still drain).
var sidecarNameSuffixRE = regexp.MustCompile(`^_\d{8}_\d{6}_\d+\.parquet$`)

// isSidecarSourceKey reports whether key is a late-sidecar object the ingest
// wrote: directly under prefix (= "<db>/<lateName>/", no deeper path) and
// named <lateName>_YYYYMMDD_HHMMSS_<digits>.parquet. Everything else under the
// directory, in particular the nested Y/M/D/H files of a real measurement
// that happens to be named "<x>_late", is not the drain's to move (CS-11).
func isSidecarSourceKey(key, prefix, lateName string) bool {
	rest, ok := strings.CutPrefix(filepath.ToSlash(key), prefix)
	if !ok || rest == "" || strings.Contains(rest, "/") {
		return false
	}
	tail, ok := strings.CutPrefix(rest, lateName)
	if !ok {
		return false
	}
	return sidecarNameSuffixRE.MatchString(tail)
}

// runOne drains a single (db, late_measurement) sidecar.
func (r *Reorganizer) runOne(ctx context.Context, db, measurement, lateName string, minAge time.Duration) error {
	prefix := db + "/" + lateName + "/"
	listed, err := r.Backend.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("list %s: %w", prefix, err)
	}
	keys := make([]string, 0, len(listed))
	for _, key := range listed {
		if isSidecarSourceKey(key, prefix, lateName) {
			keys = append(keys, key)
		}
	}
	if ignored := len(listed) - len(keys); ignored > 0 {
		r.Logger.Info().
			Str("database", db).
			Str("late_measurement", lateName).
			Int("ignored", ignored).
			Int("sidecar_files", len(keys)).
			Msg("Reorg: ignoring keys under the sidecar directory that are not flat late-sidecar files (e.g. a real measurement named *_late); they are never moved or deleted")
	}
	if len(keys) == 0 {
		return nil
	}

	// Mirror the compactor's filterCandidateFiles: drop any source file an
	// existing reorg manifest already owns. Recovery (which ran first) owns
	// the delete for those buckets; re-reading them here would write a second,
	// duplicate set of target files into the queried partitions.
	if r.ManifestManager != nil {
		inFlight, err := r.ManifestManager.SourceFilesInManifests(ctx)
		if err != nil {
			// Fail closed: an unreadable manifest may own any of these keys.
			return fmt.Errorf("load in-flight reorg manifests (skipping %s this cycle to avoid re-draining an in-flight chunk): %w", prefix, err)
		}
		if len(inFlight) > 0 {
			kept := make([]string, 0, len(keys))
			for _, key := range keys {
				if _, busy := inFlight[key]; busy {
					continue
				}
				kept = append(kept, key)
			}
			if len(kept) < len(keys) {
				r.Logger.Debug().
					Int("skipped", len(keys)-len(kept)).
					Int("remaining", len(kept)).
					Msg("Reorg: skipped source files already tracked by a manifest")
			}
			keys = kept
		}
		if len(keys) == 0 {
			return nil
		}
	}

	// Bucket source files by their filename's hour (== ingest hour, since
	// late routing uses now.UTC() to name files). Only buckets that are
	// fully closed get processed this cycle.
	cutoff := time.Now().UTC().Add(-minAge)
	buckets := make(map[time.Time][]string)
	for _, key := range keys {
		ft, ok := parseFilenameTime(key)
		if !ok {
			r.Logger.Debug().Str("key", key).Msg("Reorg: skipping file with unparseable name")
			continue
		}
		hour := ft.Truncate(time.Hour)
		if hour.After(cutoff) {
			continue
		}
		buckets[hour] = append(buckets[hour], key)
	}

	if len(buckets) == 0 {
		return nil
	}

	selected := selectBuckets(buckets, r.MaxBucketsPerRun)

	r.Logger.Info().
		Str("database", db).
		Str("late_measurement", lateName).
		Int("closed_buckets", len(buckets)).
		Int("selected_buckets", len(selected)).
		Int("total_source_files", len(keys)).
		Msg("Reorganizer starting drain")

	// Bucket-level parallelism mirrors the compactor's per-partition
	// semaphore + waitgroup pattern in compaction/manager.go. Each bucket
	// is independent (disjoint source files, disjoint downloads, disjoint
	// DuckDB processes) so the only shared resource is the storage backend.
	//
	// Default MaxConcurrent=1: DuckDB runs in-process today and a second
	// concurrent bucket would double the memory pressure on the parent
	// process. Raising above 1 should wait until reorg moves to subprocess
	// isolation (same pattern as compaction/subprocess.go).
	maxConcurrent := r.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	for _, hour := range selected {
		files := buckets[hour]
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		default:
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(h time.Time, fs []string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := r.processBucket(ctx, db, measurement, lateName, h, fs); err != nil {
				metrics.Get().IncReorgBucketsFailed()
				r.Logger.Error().Err(err).
					Time("hour", h).
					Int("files", len(fs)).
					Msg("Reorg bucket failed")
			} else {
				metrics.Get().IncReorgBucketsSuccess()
			}
		}(hour, files)
	}
	wg.Wait()
	return nil
}

// selectBuckets returns the bucket hours to drain this cycle, oldest first.
// When maxBuckets > 0 the result is capped to that many oldest buckets so each
// run performs a bounded, completable amount of work (committed on success)
// rather than timing out mid-cycle on an unbounded backlog. maxBuckets <= 0
// drains every closed bucket (used during backlog catch-up with a larger
// cycle_timeout).
func selectBuckets(buckets map[time.Time][]string, maxBuckets int) []time.Time {
	hours := make([]time.Time, 0, len(buckets))
	for hr := range buckets {
		hours = append(hours, hr)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i].Before(hours[j]) })
	if maxBuckets > 0 && len(hours) > maxBuckets {
		hours = hours[:maxBuckets]
	}
	return hours
}

// processBucket drains one ingest-hour bucket by splitting its source files
// into chunks of MaxFilesPerBatch and draining each chunk as an independent,
// atomically-committed unit (see processChunk).
//
// Chunking is the resilience boundary. A bucket holding 100K+ tiny files (the
// 05-30 clock-skew storm) cannot be drained all-or-nothing: a single transient
// Ceph RGW 504 on one source download, or the cycle deadline landing mid-bucket,
// would fail the whole bucket BEFORE any manifest exists, so nothing commits and
// the next cycle re-reads every file from scratch and re-fails — a death spiral.
// Per-chunk commit makes a failure forfeit only the in-flight chunk; every
// committed chunk stays drained, so the bucket shrinks monotonically across
// cycles. The extra per-chunk day-files are folded by the daily compaction tier
// (Case-4 guarantees that fires on reorg files).
func (r *Reorganizer) processBucket(ctx context.Context, db, measurement, lateName string, hour time.Time, sources []string) error {
	batchSize := r.MaxFilesPerBatch
	if batchSize <= 0 {
		batchSize = 2000
	}
	// One base timestamp per bucket; the per-chunk jobID appends the chunk
	// index so each chunk's output paths AND manifest key are unique even if two
	// chunks start within the same clock tick — a same-jobID + same-day collision
	// would overwrite the first chunk's output file (silent data loss).
	base := time.Now().UnixNano()
	var firstErr error
	for chunkIdx := 0; chunkIdx*batchSize < len(sources); chunkIdx++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		start := chunkIdx * batchSize
		end := start + batchSize
		if end > len(sources) {
			end = len(sources)
		}
		jobID := fmt.Sprintf("%d_%d", base, chunkIdx)
		if err := r.processChunk(ctx, db, measurement, lateName, hour, sources[start:end], jobID); err != nil {
			// Keep draining the remaining chunks: they are independent, and a
			// transient per-file 504 (or one corrupt file) shouldn't strand the
			// rest of the bucket. The forfeited chunk's sources stay in
			// events_late/ for the next cycle. Record the first error so the
			// bucket is still counted failed (runOne logs + metricizes it).
			if firstErr == nil {
				firstErr = err
			}
			// A cancelled context (cycle_timeout / SIGTERM) fails every
			// subsequent chunk identically — stop now; committed chunks hold.
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
	return firstErr
}

// processChunk drains one chunk of an ingest hour's source files. The flow is
// the same two-phase commit the compactor uses (see compaction/manifest.go and
// compaction/job.go's deleteOldFiles dance):
//
//  1. Download the chunk's sources to a local scratch dir
//  2. DuckDB COPY ... PARTITION_BY produces one parquet per target day
//     (filtered to WHERE time IS NOT NULL — see step 2.5)
//     2.5 Row-count audit: count NULL-time rows separately, sum output rows,
//     refuse to delete sources if the math doesn't match.
//  3. Enumerate outputs with deterministic JobID-bound paths (so a crashed
//     run's partials are distinguishable from a re-run's outputs)
//  4. Write manifest in "pending" state — past this point recovery can
//     finish or roll back the chunk without local scratch data
//  5. Upload every output
//  6. Mark manifest "uploaded" — recovery now only has to retry the
//     source-delete step
//  7. Delete the chunk's sources
//  8. Delete manifest
//
// Failure in steps 4–8 leaves a recoverable manifest. Failure in 1–3 is
// pre-manifest: the chunk's source files stay in events_late/ and the
// next cycle re-runs them from scratch. The caller (processBucket) supplies a
// jobID unique to this chunk.
func (r *Reorganizer) processChunk(ctx context.Context, db, measurement, lateName string, hour time.Time, sources []string, jobID string) error {
	scratch, err := os.MkdirTemp(r.TempDirectory, fmt.Sprintf("reorg_%s_%s_*", lateName, hour.Format("20060102T150405")))
	if err != nil {
		return fmt.Errorf("mkdir scratch: %w", err)
	}
	defer os.RemoveAll(scratch)

	srcDir := filepath.Join(scratch, "sources")
	outDir := filepath.Join(scratch, "out")
	duckdbTmp := filepath.Join(scratch, "duckdb-tmp")
	if err := os.MkdirAll(srcDir, 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(duckdbTmp, 0700); err != nil {
		return err
	}

	// Parallel download. Mirrors compaction/job.go's downloadFiles pattern:
	// a fixed worker pool reads from a task channel and writes to a results
	// channel. Each file is small (~tens of KB), so the bottleneck is the
	// S3 round-trip count — running 8 in parallel makes a 5K-file bucket
	// take seconds instead of ~8 minutes serially.
	dlWorkers := r.DownloadWorkers
	if dlWorkers <= 0 {
		dlWorkers = 8
	}
	if dlWorkers > len(sources) {
		dlWorkers = len(sources)
	}
	type dlTask struct {
		idx int
		key string
	}
	type dlResult struct {
		idx       int
		localPath string
		phantom   bool // proven absent: GET and HEAD both answered not-found
		err       error
	}
	tasks := make(chan dlTask, len(sources))
	results := make(chan dlResult, len(sources))
	var dlWg sync.WaitGroup
	for i := 0; i < dlWorkers; i++ {
		dlWg.Add(1)
		go func() {
			defer dlWg.Done()
			for t := range tasks {
				local := filepath.Join(srcDir, filepath.Base(t.key))
				f, err := os.Create(local)
				if err != nil {
					results <- dlResult{idx: t.idx, err: fmt.Errorf("create %s: %w", local, err)}
					continue
				}
				if err := r.Backend.ReadTo(ctx, t.key, f); err != nil {
					f.Close()
					if objectGone(err) {
						// LIST showed the key but GET says it is not there.
						// Skip it only when HEAD agrees: a key that exists
						// (or cannot be checked) is never skipped.
						gone, headErr := r.sourceProvenGone(ctx, t.key)
						if gone {
							_ = os.Remove(local) // keep the empty file out of the glob
							results <- dlResult{idx: t.idx, phantom: true}
							continue
						}
						if headErr != nil {
							err = fmt.Errorf("%w (HEAD check failed: %v; not skipping the key)", err, headErr)
						} else {
							err = fmt.Errorf("%w (HEAD says the object exists; not skipping the key)", err)
						}
					}
					results <- dlResult{idx: t.idx, err: fmt.Errorf("download %s: %w", t.key, err)}
					continue
				}
				f.Close()
				results <- dlResult{idx: t.idx, localPath: local}
			}
		}()
	}
	for i, key := range sources {
		select {
		case <-ctx.Done():
			close(tasks)
			dlWg.Wait()
			close(results)
			return ctx.Err()
		default:
		}
		tasks <- dlTask{idx: i, key: key}
	}
	close(tasks)
	go func() { dlWg.Wait(); close(results) }()

	byIdx := make([]string, len(sources))
	phantom := make([]bool, len(sources))
	var firstErr error
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}
		if res.phantom {
			phantom[res.idx] = true
			continue
		}
		byIdx[res.idx] = res.localPath
	}
	if firstErr != nil {
		return firstErr
	}

	// Drop proven-absent keys (CS-10): a Ceph phantom index entry is listed
	// forever while GET and HEAD answer 404, and failing the chunk on it
	// would strand every other file of the chunk on every cycle. The key is
	// not in the manifest's SourceFiles and is not deleted by this chunk.
	kept := make([]string, 0, len(sources))
	localPaths := make([]string, 0, len(sources))
	var phantoms int
	for i, key := range sources {
		if phantom[i] {
			phantoms++
			r.Logger.Warn().
				Str("key", key).
				Str("late_measurement", lateName).
				Time("bucket_hour", hour).
				Msg("Reorg: source key is listed but GET and HEAD both answer not-found (Ceph phantom index entry); skipped. To purge it, PUT an empty object at this key and then DELETE it")
			continue
		}
		kept = append(kept, key)
		localPaths = append(localPaths, byIdx[i])
	}
	if phantoms > 0 {
		metrics.Get().IncReorgPhantomKeys(int64(phantoms))
	}
	sources = kept
	if len(sources) == 0 {
		return nil
	}

	// One DuckDB instance per bucket; closed before the next bucket starts
	// so jemalloc fully releases memory between buckets.
	d, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer d.Close()
	// One connection: the SETs below are per-database settings, and a single
	// connection keeps every statement of the chunk on the configured one.
	d.SetMaxOpenConns(1)
	if err := r.configureDuckDB(ctx, d, duckdbTmp); err != nil {
		return err
	}

	// Schema sniff uses the FIRST file only — every file in a bucket has the
	// same schema (it's the same measurement, same flush format). Sampling
	// one file avoids parsing 5K+ filenames into a single DESCRIBE query.
	firstFileList := "['" + escapeSQLPath(localPaths[0]) + "']"
	timeType, err := r.sniffTimeColumn(ctx, d, firstFileList)
	if err != nil {
		return fmt.Errorf("sniff time column: %w", err)
	}
	if !isTimestampType(timeType) {
		r.Logger.Warn().
			Str("late_measurement", lateName).
			Time("bucket_hour", hour).
			Str("time_type", timeType).
			Int("sources", len(sources)).
			Msg("Reorg: skipping bucket — `time` column is not a TIMESTAMP variant; reorg partition derivation requires TIMESTAMP")
		return nil
	}

	// Read all sources via a directory glob (single read_parquet) for both the
	// row-count audit and the COPY below. A glob avoids building a giant SQL
	// array literal for 10K+ file buckets (the reason the old code batched)
	// WITHOUT the per-batch output multiplication batching caused.
	srcGlob := "'" + escapeSQLPath(filepath.Join(srcDir, "*.parquet")) + "'"
	inputRows, nullTimeRows, err := r.countRows(ctx, d, srcGlob)
	if err != nil {
		return fmt.Errorf("count input rows: %w", err)
	}
	if nullTimeRows > 0 {
		r.Logger.Warn().
			Str("late_measurement", lateName).
			Time("bucket_hour", hour).
			Int64("null_time_rows", nullTimeRows).
			Int64("input_rows", inputRows).
			Msg("Reorg: rows with NULL time will be dropped (cannot derive partition); operator should investigate ingest source")
		metrics.Get().IncReorgRowsDroppedNullTS(nullTimeRows)
	}

	// Single COPY over all sources (glob), partitioned by event-DAY. Late events
	// span many event-hours (iOS clock-skew); the old hour-partitioned, per-batch
	// COPY emitted one tiny output PER event-hour PER batch — thousands of serial
	// uploads per bucket. Day granularity matches the daily compaction tier's
	// Y/M/D layout (already read by the partition pruner) and cuts outputs ~24x;
	// the single glob COPY drops the per-batch multiplier. WHERE time IS NOT NULL
	// avoids a __HIVE_DEFAULT_PARTITION__ dir the path parser skips; AT TIME ZONE
	// 'UTC' coerces TIMESTAMP_TZ to UTC; union_by_name accommodates per-flush
	// schema drift (same flag compaction's dedup.go uses).
	query := fmt.Sprintf(`
COPY (
  SELECT *,
    EXTRACT(YEAR  FROM time AT TIME ZONE 'UTC')::INT AS _y,
    EXTRACT(MONTH FROM time AT TIME ZONE 'UTC')::INT AS _m,
    EXTRACT(DAY   FROM time AT TIME ZONE 'UTC')::INT AS _d
  FROM %s
  WHERE time IS NOT NULL
) TO '%s' (
  FORMAT PARQUET,
  PARTITION_BY (_y, _m, _d),
  OVERWRITE_OR_IGNORE
)`, sqlutil.ReadParquet(srcGlob, "union_by_name = true"), escapeSQLPath(outDir))
	if _, err := d.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("duckdb COPY: %w", err)
	}

	planned, err := r.enumerateOutputs(outDir, db, measurement, jobID)
	if err != nil {
		return fmt.Errorf("enumerate outputs: %w", err)
	}

	// Row-count audit: sum row counts across enumerated output files and
	// require it to equal input_rows - null_time_rows. Any other shortfall
	// means DuckDB dropped data we didn't predict (HIVE_DEFAULT_PARTITION
	// from some other column, schema-evolution edge case, etc.) — refuse
	// to delete sources so an operator can investigate.
	outputRows, err := r.countOutputRows(ctx, d, planned)
	if err != nil {
		return fmt.Errorf("count output rows: %w", err)
	}
	expected := inputRows - nullTimeRows
	if outputRows != expected {
		r.Logger.Error().
			Str("late_measurement", lateName).
			Time("bucket_hour", hour).
			Int64("input_rows", inputRows).
			Int64("null_time_rows", nullTimeRows).
			Int64("expected_output_rows", expected).
			Int64("actual_output_rows", outputRows).
			Int("planned_outputs", len(planned)).
			Msg("Reorg: ROW COUNT MISMATCH — refusing to delete sources to prevent data loss; bucket will be retried next cycle once root cause is fixed")
		return fmt.Errorf("row count mismatch: input=%d null_time=%d expected_output=%d actual_output=%d",
			inputRows, nullTimeRows, expected, outputRows)
	}

	if len(planned) == 0 {
		// Audit passed (expected == 0), so all input was NULL-time. The
		// dropped count is already logged + metricized above. Safe to
		// delete sources — they hold only data we can never partition.
		if err := r.Backend.DeleteBatch(ctx, sources); err != nil {
			return fmt.Errorf("delete null-only sources: %w", err)
		}
		metrics.Get().IncReorgSourcesDrained(int64(len(sources)))
		return nil
	}

	manifestPlanned := make([]PlannedReorgOutput, 0, len(planned))
	for _, p := range planned {
		manifestPlanned = append(manifestPlanned, PlannedReorgOutput{Path: p.targetKey, Size: p.size})
	}

	var manifestKey string
	if r.ManifestManager != nil {
		manifest := &ReorgManifest{
			JobID:          jobID,
			Database:       db,
			Measurement:    measurement,
			LateName:       lateName,
			BucketHour:     hour,
			SourceFiles:    sources,
			PlannedOutputs: manifestPlanned,
			Status:         ReorgStatusPending,
		}
		// Every storage call below runs under ctx, so nothing this run
		// does can land after its deadline. Recording it lets recovery
		// roll back an incomplete chunk (delete its outputs) only once
		// this writer can no longer be uploading or deleting sources.
		if deadline, ok := ctx.Deadline(); ok {
			manifest.Deadline = deadline.UTC()
		}
		manifestKey, err = r.ManifestManager.Write(ctx, manifest)
		if err != nil {
			return fmt.Errorf("write reorg manifest: %w", err)
		}
	}

	// Upload outputs in parallel (mirrors the per-bucket download pool). On any
	// error we return WITHOUT marking the manifest uploaded or deleting sources,
	// so the manifest stays "pending" and recovery rolls back THIS attempt's
	// outputs by their jobID-bound paths once ctx's deadline has passed.
	// uploadOne passes ctx into WriteReader, so a cancelled cycle
	// (cycle_timeout/SIGTERM) aborts in-flight uploads.
	if err := r.uploadOutputs(ctx, planned); err != nil {
		return fmt.Errorf("upload outputs: %w", err)
	}

	if r.ManifestManager != nil && manifestKey != "" {
		if err := r.ManifestManager.MarkUploaded(ctx, manifestKey); err != nil {
			return fmt.Errorf("mark manifest uploaded: %w", err)
		}
	}

	if err := r.Backend.DeleteBatch(ctx, sources); err != nil {
		r.Logger.Warn().Err(err).
			Int("sources", len(sources)).
			Msg("Reorg: delete sources failed; recovery will complete on next cycle")
		return nil
	}

	if r.ManifestManager != nil && manifestKey != "" {
		if err := r.ManifestManager.Delete(ctx, manifestKey); err != nil {
			r.Logger.Warn().Err(err).Str("manifest", manifestKey).Msg("Reorg: failed to delete manifest after successful drain")
		}
	}

	metrics.Get().IncReorgSourcesDrained(int64(len(sources)))
	metrics.Get().IncReorgOutputsWritten(int64(len(planned)))

	r.Logger.Info().
		Str("database", db).
		Str("measurement", measurement).
		Time("source_hour", hour).
		Str("job_id", jobID).
		Int("sources", len(sources)).
		Int("outputs", len(planned)).
		Int64("input_rows", inputRows).
		Int64("output_rows", outputRows).
		Int64("dropped_null_time", nullTimeRows).
		Msg("Reorg bucket drained")
	return nil
}

// configureDuckDB applies the chunk DuckDB's resource limits and session
// settings. Any failure fails the chunk, which is safe here (no manifest or
// output exists yet) and is the only safe choice: a DuckDB that runs without
// its memory limit or spill cap can take the whole pod's memory, or spill
// past the pod's ephemeral-storage limit and get it evicted mid-compaction.
//
//   - memory_limit (when set): beyond it DuckDB spills instead of growing.
//   - temp_directory: a per-chunk spill dir under the chunk's scratch dir.
//     Without it concurrent chunks default to <cwd>/.tmp/ and race on
//     identical spill filenames (the subprocess.go fix for the compactor).
//   - max_temp_directory_size (when set): caps that spill; a chunk that
//     needs more fails with DuckDB's error and is retried next cycle.
//   - TimeZone=UTC: the day-partition derivation (EXTRACT(... AT TIME ZONE
//     'UTC')) must not depend on the pod's TZ, or a tz-naive TIMESTAMP
//     `time` column lands in the wrong UTC day on a non-UTC host.
func (r *Reorganizer) configureDuckDB(ctx context.Context, d *sql.DB, spillDir string) error {
	if r.MemoryLimit != "" {
		if _, err := d.ExecContext(ctx, "SET memory_limit='"+escapeSQLString(r.MemoryLimit)+"'"); err != nil {
			return fmt.Errorf("set DuckDB memory_limit=%q: %w", r.MemoryLimit, err)
		}
	}
	if _, err := d.ExecContext(ctx, "SET temp_directory='"+escapeSQLPath(spillDir)+"'"); err != nil {
		return fmt.Errorf("set DuckDB temp_directory=%q: %w", spillDir, err)
	}
	if r.MaxTempDirectorySize != "" {
		if _, err := d.ExecContext(ctx, "SET max_temp_directory_size='"+escapeSQLString(r.MaxTempDirectorySize)+"'"); err != nil {
			return fmt.Errorf("set DuckDB max_temp_directory_size=%q: %w", r.MaxTempDirectorySize, err)
		}
	}
	if _, err := d.ExecContext(ctx, "SET TimeZone='UTC'"); err != nil {
		return fmt.Errorf("set DuckDB session TimeZone=UTC: %w", err)
	}
	return nil
}

// sourceProvenGone confirms with a HEAD that a source whose GET answered
// not-found is really absent. Only a definite "does not exist" counts; an
// error or "exists" keeps the key (the chunk then fails and retries).
func (r *Reorganizer) sourceProvenGone(ctx context.Context, key string) (bool, error) {
	exists, err := r.Backend.Exists(ctx, key)
	if err != nil {
		return false, err
	}
	return !exists, nil
}

// reorgOutput is the local-scratch + target-storage pairing for one DuckDB
// output file. Kept private to the package — manifest persistence uses
// PlannedReorgOutput which has just the (target, size) bits.
type reorgOutput struct {
	localPath string
	targetKey string
	size      int64
}

// enumerateOutputs walks the DuckDB output directory and builds the list of
// reorgOutput entries the bucket will upload. Target keys embed jobID so
// rollback during recovery can selectively delete this attempt's partials
// without touching files written by earlier successful runs of the same
// target partition.
func (r *Reorganizer) enumerateOutputs(outDir, db, measurement, jobID string) ([]reorgOutput, error) {
	var outputs []reorgOutput
	seq := 0
	err := filepath.WalkDir(outDir, func(path string, info os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".parquet") {
			return nil
		}
		rel, err := filepath.Rel(outDir, path)
		if err != nil {
			return err
		}
		y, m, d, ok := parseHivePartitions(rel)
		if !ok {
			r.Logger.Warn().Str("rel", rel).Msg("Reorg: unparseable DuckDB output path; skipping")
			return nil
		}
		st, err := os.Stat(path)
		if err != nil {
			return err
		}
		// Day-granularity target path (db/meas/Y/M/D/), matching the daily
		// compaction tier's 6-part layout that the query pruner already reads.
		targetKey := fmt.Sprintf("%s/%s/%04d/%02d/%02d/%s%s%s_%d.parquet",
			db, measurement, y, m, d,
			measurement, reorgFileMarker, jobID, seq,
		)
		outputs = append(outputs, reorgOutput{
			localPath: path,
			targetKey: targetKey,
			size:      st.Size(),
		})
		seq++
		return nil
	})
	return outputs, err
}

// uploadOne streams a single output file from local scratch to the storage
// backend.
func (r *Reorganizer) uploadOne(ctx context.Context, o reorgOutput) error {
	f, err := os.Open(o.localPath)
	if err != nil {
		return fmt.Errorf("open output %s: %w", o.localPath, err)
	}
	defer f.Close()
	if err := r.Backend.WriteReader(ctx, o.targetKey, f, o.size); err != nil {
		return fmt.Errorf("upload %s: %w", o.targetKey, err)
	}
	return nil
}

// uploadOutputs uploads all enumerated output files concurrently, mirroring the
// per-bucket download worker pool: a fixed pool drains a task channel and
// reports per-file errors. It waits for all workers, then returns the first
// error seen. Callers MUST treat a non-nil return as "do NOT mark the manifest
// uploaded and do NOT delete sources" — leaving the manifest pending so recovery
// rolls back this attempt's jobID-bound partials. Concurrency is bounded by
// DownloadWorkers (S3 round-trip count is the cost, same as downloads).
func (r *Reorganizer) uploadOutputs(ctx context.Context, planned []reorgOutput) error {
	if len(planned) == 0 {
		return nil
	}
	workers := r.DownloadWorkers
	if workers <= 0 {
		workers = 8
	}
	if workers > len(planned) {
		workers = len(planned)
	}
	tasks := make(chan reorgOutput, len(planned))
	results := make(chan error, len(planned))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for o := range tasks {
				results <- r.uploadOne(ctx, o)
			}
		}()
	}
	for _, p := range planned {
		tasks <- p
	}
	close(tasks)
	go func() { wg.Wait(); close(results) }()

	var firstErr error
	for err := range results {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// countRows returns (total_rows, null_time_rows) for the source files.
// Parquet's footer metadata gives row counts at zero data-scan cost; the
// NULL count requires a scan but for the small late files that's cheap.
func (r *Reorganizer) countRows(ctx context.Context, d *sql.DB, fileList string) (int64, int64, error) {
	q := fmt.Sprintf(`
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE time IS NULL) AS null_time
FROM %s`, sqlutil.ReadParquet(fileList, "union_by_name = true"))
	row := d.QueryRowContext(ctx, q)
	var total, nullTime int64
	if err := row.Scan(&total, &nullTime); err != nil {
		return 0, 0, err
	}
	return total, nullTime, nil
}

// countOutputRows sums row counts across every enumerated output file. Uses
// parquet metadata only (no data scan) so it's cheap regardless of file size.
func (r *Reorganizer) countOutputRows(ctx context.Context, d *sql.DB, planned []reorgOutput) (int64, error) {
	if len(planned) == 0 {
		return 0, nil
	}
	fileList := "["
	for i, p := range planned {
		if i > 0 {
			fileList += ", "
		}
		fileList += "'" + escapeSQLPath(p.localPath) + "'"
	}
	fileList += "]"
	// The outputs sit under DuckDB's own _y=/_m=/_d= partition directories;
	// ReadParquet turns Hive inference off, which a COUNT(*) never needed.
	q := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, sqlutil.ReadParquet(fileList, "union_by_name = true"))
	row := d.QueryRowContext(ctx, q)
	var total int64
	if err := row.Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

// buildDuckListLiteral renders a slice of paths as a DuckDB array literal,
// escaping single quotes and backslashes. Used for both the row-count audit
// query and per-batch COPYs.
func buildDuckListLiteral(paths []string) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, p := range paths {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('\'')
		b.WriteString(escapeSQLPath(p))
		b.WriteByte('\'')
	}
	b.WriteByte(']')
	return b.String()
}

func parseFilenameTime(key string) (time.Time, bool) {
	m := filenameTimeRE.FindStringSubmatch(filepath.Base(key))
	if m == nil {
		return time.Time{}, false
	}
	t, err := time.Parse("20060102_150405", m[1]+"_"+m[2])
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// sniffTimeColumn returns the DuckDB column_type of the `time` column for the
// given list of parquet files. Returns an error if the column is absent or the
// describe query fails; returns the type string otherwise.
func (r *Reorganizer) sniffTimeColumn(ctx context.Context, d *sql.DB, fileList string) (string, error) {
	q := fmt.Sprintf(`SELECT column_type FROM (DESCRIBE SELECT * FROM %s) WHERE column_name = 'time'`, sqlutil.ReadParquet(fileList))
	row := d.QueryRowContext(ctx, q)
	var t string
	if err := row.Scan(&t); err != nil {
		return "", err
	}
	return t, nil
}

// isTimestampType reports whether a DuckDB column type can drive Arc's
// year/month/day/hour partition derivation. We accept the four parquet
// TIMESTAMP variants explicitly so adding e.g. BIGINT support later is a
// deliberate change to this allowlist rather than an accidental match.
func isTimestampType(t string) bool {
	u := strings.ToUpper(strings.TrimSpace(t))
	return u == "TIMESTAMP" ||
		u == "TIMESTAMP WITH TIME ZONE" ||
		u == "TIMESTAMP_MS" ||
		u == "TIMESTAMP_NS" ||
		u == "TIMESTAMP_S"
}

var hivePartitionRE = regexp.MustCompile(`_y=(\d+)/_m=(\d+)/_d=(\d+)/`)

func parseHivePartitions(rel string) (year, month, day int, ok bool) {
	m := hivePartitionRE.FindStringSubmatch(rel)
	if m == nil {
		return 0, 0, 0, false
	}
	fmt.Sscanf(m[1], "%d", &year)
	fmt.Sscanf(m[2], "%d", &month)
	fmt.Sscanf(m[3], "%d", &day)
	return year, month, day, true
}
