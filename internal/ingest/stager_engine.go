package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// StagerOptions tunes the merge-and-upload side of the ingest stagers. The
// zero value takes every default, so tests and callers that only set the
// storage, directory and flush age keep working.
type StagerOptions struct {
	// MergeMemoryLimit is DuckDB's memory_limit for one merge job
	// (ingest.stager_merge_memory_limit, default "1GB").
	MergeMemoryLimit string
	// MergeThreads is DuckDB's threads for one merge job
	// (ingest.stager_merge_threads, default 2).
	MergeThreads int
	// JobMaxFiles caps the staged files merged by one job
	// (ingest.stager_job_max_files, default 500).
	JobMaxFiles int
	// JobMaxBytes caps the staged bytes merged by one job
	// (ingest.stager_job_max_bytes, default 1GiB). A single file larger than
	// the cap still forms a job on its own.
	JobMaxBytes int64
	// PodID names this process in every object key (with the job id), so keys
	// never collide across pods or retries (ingest.stager_pod_id, default the
	// HOSTNAME). Sanitized to [A-Za-z0-9-].
	PodID string
	// UploadConcurrency bounds the parallel PUTs of one job
	// (ingest.stager_upload_concurrency, default 4).
	UploadConcurrency int
	// Health is the object-store health tracker shared by the stagers of a
	// process. Nil gives the stager a private one (no metric).
	Health *S3Health
	// ReportMetrics publishes this stager's backlog gauges
	// (arc_stager_pending_*{kind}); the process's real stagers set it.
	ReportMetrics bool
	// HandoffURL is ingest.stager_handoff_url: when set, staged work still
	// here after the final drain at shutdown is handed to the peer reachable
	// there (POST StagerImportPath) and deleted locally after a 2xx.
	HandoffURL string
	// HandoffToken is sent as a Bearer token with the handoff (when the peer
	// runs with auth enabled).
	HandoffToken string
}

const (
	defaultStagerMergeMemoryLimit = "1GB"
	defaultStagerMergeThreads     = 2
	defaultStagerJobMaxFiles      = 500
	defaultStagerJobMaxBytes      = int64(1) << 30

	// stagerJobTimeout bounds one merge-and-upload job of a scheduled drain.
	// A job is bounded in size, so this is a safety net, not a throughput
	// limit: the drain moves on to the next job instead of one deadline
	// covering the whole backlog.
	stagerJobTimeout = 5 * time.Minute

	// stagerDuckDBTempDir is the merge's temp_directory, under the stage dir
	// so a spilling merge uses the stage volume (whose usage is accounted)
	// instead of the container's root filesystem.
	stagerDuckDBTempDir = ".duckdb_tmp"
	// stagerOutDirPrefix prefixes a job's local merge output directory.
	stagerOutDirPrefix = ".out_"
)

func (o StagerOptions) withDefaults() StagerOptions {
	if strings.TrimSpace(o.MergeMemoryLimit) == "" {
		o.MergeMemoryLimit = defaultStagerMergeMemoryLimit
	}
	if o.MergeThreads <= 0 {
		o.MergeThreads = defaultStagerMergeThreads
	}
	if o.JobMaxFiles <= 0 {
		o.JobMaxFiles = defaultStagerJobMaxFiles
	}
	if o.JobMaxBytes <= 0 {
		o.JobMaxBytes = defaultStagerJobMaxBytes
	}
	if strings.TrimSpace(o.PodID) == "" {
		o.PodID = defaultStagerPodID()
	}
	o.PodID = sanitizeStagerPodID(o.PodID)
	if o.UploadConcurrency <= 0 {
		o.UploadConcurrency = defaultStagerUploadConcurrency
	}
	return o
}

// stagerHooks are test seams. Nil hooks are skipped.
type stagerHooks struct {
	// onMergeConn runs on the merge connection after the bounded settings are
	// applied and before the COPY.
	onMergeConn func(ctx context.Context, conn *sql.Conn)
	// onJob reports each job's selected sources before it merges them.
	onJob func(db, measurement string, sources []string, bytes int64)
	// crash, when it returns an error at a named step, stops the drain there
	// with nothing cleaned up, like a killed process (stagerPoint*).
	crash func(point string) error
}

// mergedOutput is one local file a merge produced, destined for one object.
type mergedOutput struct {
	// partition identifies the output within its job: "YYYY/MM/DD/HH" for the
	// fresh stager, "late" for the late stager.
	partition string
	path      string
	size      int64
}

// stagerKindSpec is what differs between the fresh and the late stager.
type stagerKindSpec struct {
	kind    string // "fresh" | "late"
	marker  string // "fresh_stage_" | "late_stage_"
	logName string // "FreshStager" | "LateStager"
	// merge merges sources into outDir on conn (bounded settings already
	// applied) and returns exactly one output per partition.
	merge func(ctx context.Context, conn *sql.Conn, measurement string, sources []string, outDir string) ([]mergedOutput, error)
	// jobKey names the object a job output is uploaded to. It is fixed when
	// the job is created and carries the pod and job id.
	jobKey func(db, measurement, partition, jobID string, created time.Time) string
}

// stagedFile is one staged source waiting for a merge job.
type stagedFile struct {
	path  string
	size  int64
	mtime time.Time
	seq   int64 // parsed from the name; -1 when the name carries no number
}

// stagerBatch is the set of sources one job merges: oldest first, bounded by
// the job caps, all of one (database, measurement).
type stagerBatch struct {
	group       string
	database    string
	measurement string
	files       []stagedFile
	bytes       int64
}

// stagerEngine is the spool shared by FreshStager and LateStager: durable
// local staging, bounded merge jobs, uploads.
type stagerEngine struct {
	spec     stagerKindSpec
	storage  storage.Backend
	stageDir string
	flushAge time.Duration
	logger   zerolog.Logger
	opts     StagerOptions
	hooks    stagerHooks

	// mu serializes Stage/unstage with the source scan, so a scan never sees
	// a half-written source and an unstage never removes a claimed one.
	mu sync.Mutex
	// claimed holds the sources of the job currently being merged/uploaded.
	claimed map[string]struct{}
	// drainMu allows one drain pass at a time.
	drainMu sync.Mutex

	seq atomic.Int64

	// closeBudget bounds the final drain in Close (nanoseconds; 0 = default).
	closeBudget atomic.Int64

	runCtx    context.Context
	runCancel context.CancelFunc
	stopCh    chan struct{}
	doneCh    chan struct{}
	stopOnce  sync.Once
	// skipFinalDrain makes the loop exit without the final drain (tests that
	// simulate a killed process).
	skipFinalDrain atomic.Bool

	// health gates merging and uploading; ownHealth marks a private tracker
	// this engine must close.
	health    *S3Health
	ownHealth bool

	unregisterMetrics func()

	// closing refuses imports once Close started.
	closing atomic.Bool
	// kickCh asks the loop for an immediate drain (after an import).
	kickCh chan struct{}
}

func newStagerEngine(spec stagerKindSpec, backend storage.Backend, stageDir string, flushAge time.Duration, logger zerolog.Logger, opts StagerOptions) (*stagerEngine, error) {
	if backend == nil {
		return nil, fmt.Errorf("%s stager: storage backend is required", spec.kind)
	}
	if stageDir == "" {
		stageDir = "./data/ingest/" + spec.kind + "-stager"
	}
	if flushAge <= 0 {
		flushAge = 60 * time.Second
	}
	if err := os.MkdirAll(stageDir, 0700); err != nil {
		return nil, fmt.Errorf("%s stager: mkdir %s: %w", spec.kind, stageDir, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &stagerEngine{
		spec:      spec,
		storage:   backend,
		stageDir:  stageDir,
		flushAge:  flushAge,
		logger:    logger.With().Str("component", spec.kind+"-stager").Logger(),
		opts:      opts.withDefaults(),
		claimed:   make(map[string]struct{}),
		runCtx:    ctx,
		runCancel: cancel,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		kickCh:    make(chan struct{}, 1),
	}
	// A surviving stage dir (emptyDir outlives a container restart) may already
	// hold files; restarting the counter at 1 would overwrite them. An emptied
	// dir must not restart it either: a peer de-duplicates handed-off files by
	// <pod>-<seq> (plus their content hash), so a restarted pod (same
	// HOSTNAME) reusing sequence numbers would look like a re-send. Seeding
	// from the clock keeps every incarnation's numbers above the last one's.
	e.seq.Store(max(seedStagerSeq(stageDir, spec.marker), time.Now().UnixNano()))
	e.health = e.opts.Health
	if e.health == nil {
		e.health = NewS3Health(S3HealthConfig{Backend: backend, PodID: e.opts.PodID, Logger: logger})
		e.ownHealth = true
	}
	e.removeMergeLeftovers()
	return e, nil
}

// start launches the background drain loop.
func (e *stagerEngine) start() {
	if e.opts.ReportMetrics {
		e.unregisterMetrics = metrics.Get().RegisterStagerPending(e.spec.kind, e.pendingStats)
	}
	go e.runLoop()
	e.logger.Info().
		Str("stage_dir", e.stageDir).
		Dur("flush_age", e.flushAge).
		Int64("seq_seed", e.seq.Load()).
		Str("merge_memory_limit", e.opts.MergeMemoryLimit).
		Int("merge_threads", e.opts.MergeThreads).
		Int("job_max_files", e.opts.JobMaxFiles).
		Int64("job_max_bytes", e.opts.JobMaxBytes).
		Msg(e.spec.logName + " started")
}

// removeMergeLeftovers deletes local merge state a crashed process left
// behind that no persisted job owns: out dirs of jobs that never reached their
// job file (the sources are still unjobbed), half-written job files, and the
// pre-job-file merge scratch of older binaries. Out dirs of persisted jobs are
// kept for their resume.
func (e *stagerEngine) removeMergeLeftovers() {
	entries, err := os.ReadDir(e.stageDir)
	if err != nil {
		return
	}
	ownOut := stagerOutDirPrefix + e.spec.kind + "_"
	for _, en := range entries {
		name := en.Name()
		path := filepath.Join(e.stageDir, name)
		switch {
		case en.IsDir() && strings.HasPrefix(name, ownOut):
			id := strings.TrimPrefix(name, ownOut)
			_, errLocal := os.Stat(e.jobFilePath(id))
			_, errImported := os.Stat(filepath.Join(e.importJobDir(id), "job.json"))
			if errLocal != nil && errImported != nil {
				_ = os.RemoveAll(path)
			}
		case en.IsDir() && strings.HasPrefix(name, ".merge_out_"):
			_ = os.RemoveAll(path)
		case !en.IsDir() && strings.HasPrefix(name, ".merged_") && strings.HasSuffix(name, ".parquet"):
			_ = os.Remove(path)
		case !en.IsDir() && strings.HasPrefix(name, "."+stagerJobFilePrefix) && strings.HasSuffix(name, ".json.tmp"):
			_ = os.Remove(path)
		}
	}
	e.removeImportLeftovers()
}

// Stage durably writes parquetData to the stage dir (see writeStageFileAtomic).
// A nil return means the file is fsynced and will be uploaded by a later drain.
func (e *stagerEngine) Stage(database, measurement string, parquetData []byte) error {
	_, err := e.stageFile(database, measurement, parquetData)
	return err
}

// stageFile is Stage that also returns the staged path, so a multi-hour flush
// that fails after staging can take the file back (unstage).
func (e *stagerEngine) stageFile(database, measurement string, parquetData []byte) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := os.MkdirAll(e.stageDir, 0700); err != nil {
		return "", fmt.Errorf("%s stager: mkdir: %w", e.spec.kind, err)
	}
	seq := e.seq.Add(1)
	name := fmt.Sprintf("%s__%s__%s%d.parquet", encodeStagerSegment(database), encodeStagerSegment(measurement), e.spec.marker, seq)
	path := filepath.Join(e.stageDir, name)
	if err := writeStageFileAtomic(path, parquetData); err != nil {
		return "", fmt.Errorf("%s stager: write %s: %w", e.spec.kind, path, err)
	}
	return path, nil
}

// unstage removes a file returned by stageFile. Serialized with Stage and the
// source scan. A file a running job already claimed is reported as an error
// (it may be uploaded), and so is one that is already gone.
func (e *stagerEngine) unstage(path string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	name := filepath.Base(path)
	if _, taken := e.claimed[name]; taken {
		return fmt.Errorf("staged file %s was already taken by a stager job", path)
	}
	owned, err := e.jobSourceSetLocked()
	if err != nil {
		return err
	}
	if owned[name] {
		return fmt.Errorf("staged file %s already belongs to a stager merge job", path)
	}
	return removeStagedFile(path)
}

// Pending reports how many staged files are waiting for upload. The shutdown
// WAL purge must not run while this is non-zero: those rows are no longer in
// the WAL.
func (e *stagerEngine) Pending() (int, error) {
	n, err := countStagedFiles(e.stageDir, e.spec.marker)
	if err != nil {
		return n, err
	}
	imported, _, _ := e.importedJobSourceStats()
	return n + imported, nil
}

// PendingStats reports the backlog: staged sources not yet uploaded (job
// sources included), their bytes and the age of the oldest.
func (e *stagerEngine) PendingStats() (files int, bytes int64, oldest time.Duration, err error) {
	entries, err := os.ReadDir(e.stageDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, 0, nil
		}
		return 0, 0, 0, err
	}
	now := time.Now()
	for _, en := range entries {
		name := en.Name()
		if en.IsDir() || strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".parquet") {
			continue
		}
		parts := strings.SplitN(name, "__", 3)
		if len(parts) != 3 || !strings.HasPrefix(parts[2], e.spec.marker) {
			continue
		}
		info, ierr := en.Info()
		if ierr != nil {
			continue
		}
		files++
		bytes += info.Size()
		if age := now.Sub(info.ModTime()); age > oldest {
			oldest = age
		}
	}
	impFiles, impBytes, impOldest := e.importedJobSourceStats()
	files += impFiles
	bytes += impBytes
	if !impOldest.IsZero() {
		if age := now.Sub(impOldest); age > oldest {
			oldest = age
		}
	}
	return files, bytes, oldest, nil
}

func (e *stagerEngine) pendingStats() metrics.StagerPendingStats {
	files, bytes, oldest, err := e.PendingStats()
	if err != nil {
		return metrics.StagerPendingStats{}
	}
	return metrics.StagerPendingStats{Files: int64(files), Bytes: bytes, OldestAgeSeconds: oldest.Seconds()}
}

// SetCloseBudget bounds the final drain performed by Close. main.go passes the
// share of server.shutdown_timeout it reserves for the stagers; without it the
// drain gets defaultStagerCloseBudget (30s).
func (e *stagerEngine) SetCloseBudget(d time.Duration) {
	if d > 0 {
		e.closeBudget.Store(int64(d))
	}
}

func (e *stagerEngine) closeDrainBudget() time.Duration {
	if d := time.Duration(e.closeBudget.Load()); d > 0 {
		return d
	}
	return defaultStagerCloseBudget
}

// Flush drains the stage dir: it repeatedly takes the oldest staged files of
// one (database, measurement), at most JobMaxFiles files and JobMaxBytes
// bytes, merges and uploads them, and stops when nothing is left, when ctx
// ends, or when every remaining group failed in this pass. A group whose job
// fails is skipped for the rest of the pass (its sources stay staged for the
// next one) so one bad group cannot spin the loop. Per-job failures are
// logged, not returned: only an unreadable stage dir is an error.
func (e *stagerEngine) Flush(ctx context.Context) error {
	return e.drain(ctx, false)
}

// drain is Flush. While the object store is unhealthy it does nothing (no
// merge churn during an outage) unless force is set, and a pass stops as soon
// as one of its uploads reports the store unavailable.
func (e *stagerEngine) drain(ctx context.Context, force bool) error {
	e.drainMu.Lock()
	defer e.drainMu.Unlock()
	if !force && !e.health.Healthy() {
		e.logger.Debug().Msg(e.spec.logName + ": object store unhealthy; drain deferred until it recovers")
		return nil
	}
	ctx = withFailureBaseline(ctx, e.health.failureCount())
	// Persisted jobs first, with their recorded keys.
	if err := e.resumeJobs(ctx); err != nil {
		if isStagerCrash(err) {
			return nil
		}
		return err
	}
	failed := make(map[string]bool)
	// tried holds every source a job of this pass already took. A source the
	// job did not consume (left in place as unreadable-for-now, or kept by a
	// failed job) is not offered again until the next pass, so the loop
	// always terminates.
	tried := make(map[string]bool)
	for ctx.Err() == nil && !e.storeFailedSince(ctx) {
		batch, err := e.nextBatch(failed, tried)
		if err != nil {
			return err
		}
		if batch == nil {
			return nil
		}
		for _, f := range batch.files {
			tried[filepath.Base(f.path)] = true
		}
		if err := e.runBatch(ctx, batch); err != nil {
			if isStagerCrash(err) {
				return nil
			}
			failed[batch.group] = true
			e.logger.Error().Err(err).
				Str("database", batch.database).
				Str("measurement", batch.measurement).
				Int("staged_files", len(batch.files)).
				Msg(e.spec.logName + ": merge job failed; sources remain staged for retry")
		}
	}
	return nil
}

// listStaged returns the staged sources in the stage dir that are neither
// claimed by the running job nor owned by a persisted job, grouped by
// (database, measurement). Caller holds e.mu.
func (e *stagerEngine) listStaged() (map[string]*stagerBatch, error) {
	owned, err := e.jobSourceSetLocked()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(e.stageDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s stager: read stage dir: %w", e.spec.kind, err)
	}
	groups := make(map[string]*stagerBatch)
	for _, en := range entries {
		name := en.Name()
		if en.IsDir() || strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".parquet") {
			continue
		}
		// Filename: <db>__<measurement>__<marker><seq>.parquet
		parts := strings.SplitN(name, "__", 3)
		if len(parts) != 3 || !strings.HasPrefix(parts[2], e.spec.marker) {
			continue
		}
		if _, taken := e.claimed[name]; taken || owned[name] {
			continue
		}
		info, err := en.Info()
		if err != nil {
			continue // vanished or unreadable; the next scan sees it again
		}
		key := parts[0] + "/" + parts[1]
		g, ok := groups[key]
		if !ok {
			g = &stagerBatch{group: key, database: decodeStagerSegment(parts[0]), measurement: decodeStagerSegment(parts[1])}
			groups[key] = g
		}
		seq := int64(-1)
		if n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(parts[2], e.spec.marker), ".parquet"), 10, 64); err == nil {
			seq = n
		}
		g.files = append(g.files, stagedFile{path: filepath.Join(e.stageDir, name), size: info.Size(), mtime: info.ModTime(), seq: seq})
	}
	return groups, nil
}

// olderStaged orders staged sources oldest first: by mtime (the stage time),
// then by sequence number, then by name.
func olderStaged(a, b stagedFile) bool {
	if !a.mtime.Equal(b.mtime) {
		return a.mtime.Before(b.mtime)
	}
	if a.seq != b.seq {
		return a.seq < b.seq
	}
	return a.path < b.path
}

// nextBatch claims the next job's sources: the group holding the oldest
// staged file (skipping groups that failed in this pass), and from it the
// oldest files up to the job caps. Returns nil when nothing is left.
func (e *stagerEngine) nextBatch(skip, tried map[string]bool) (*stagerBatch, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	groups, err := e.listStaged()
	if err != nil {
		return nil, err
	}
	var pick *stagerBatch
	for key, g := range groups {
		if skip[key] {
			continue
		}
		fresh := g.files[:0]
		for _, f := range g.files {
			if !tried[filepath.Base(f.path)] {
				fresh = append(fresh, f)
			}
		}
		g.files = fresh
		if len(g.files) == 0 {
			continue
		}
		sort.Slice(g.files, func(i, j int) bool { return olderStaged(g.files[i], g.files[j]) })
		if pick == nil || olderStaged(g.files[0], pick.files[0]) {
			pick = g
		}
	}
	if pick == nil {
		return nil, nil
	}
	batch := &stagerBatch{group: pick.group, database: pick.database, measurement: pick.measurement}
	for _, f := range pick.files {
		if len(batch.files) >= e.opts.JobMaxFiles {
			break
		}
		if len(batch.files) > 0 && batch.bytes+f.size > e.opts.JobMaxBytes {
			break
		}
		batch.files = append(batch.files, f)
		batch.bytes += f.size
	}
	for _, f := range batch.files {
		e.claimed[filepath.Base(f.path)] = struct{}{}
	}
	return batch, nil
}

func (e *stagerEngine) release(files []stagedFile) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, f := range files {
		delete(e.claimed, filepath.Base(f.path))
	}
}

// mergeSettings bound one merge: memory, threads, a temp directory on the
// stage volume, no ordering guarantee (lets DuckDB stream), and UTC so the
// fresh merge files rows under their UTC hour.
func (e *stagerEngine) mergeSettings(tempDir string) []string {
	return []string{
		"SET memory_limit=" + sqlutil.QuoteStringLiteral(e.opts.MergeMemoryLimit),
		"SET threads=" + strconv.Itoa(e.opts.MergeThreads),
		"SET temp_directory=" + sqlutil.QuoteStringLiteral(tempDir),
		"SET preserve_insertion_order=false",
		// The partition hour is extract(hour FROM time). Arc writes time as a
		// UTC-adjusted timestamp, which DuckDB reads as TIMESTAMPTZ and
		// extracts in the SESSION time zone, so on a host whose TZ is not UTC
		// every row would be filed under the wrong hour (or day). Refuse to
		// merge if pinning fails: a misfiled partition is silent, a failed
		// merge keeps the sources for a retry.
		"SET TimeZone='UTC'",
	}
}

// runMerge opens a private DuckDB, applies the bounded settings on one
// connection (SET is per connection and the pool may hand the COPY another),
// and runs the kind's merge into outDir, which is cleared first: a retry must
// start clean.
func (e *stagerEngine) runMerge(ctx context.Context, measurement string, paths []string, outDir string) ([]mergedOutput, error) {
	if err := os.RemoveAll(outDir); err != nil {
		return nil, fmt.Errorf("clear merge outdir: %w", err)
	}
	if err := os.MkdirAll(outDir, 0700); err != nil {
		return nil, fmt.Errorf("recreate merge outdir: %w", err)
	}
	tempDir := filepath.Join(e.stageDir, stagerDuckDBTempDir)
	if err := os.MkdirAll(tempDir, 0700); err != nil {
		return nil, fmt.Errorf("create merge temp dir: %w", err)
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer d.Close()
	conn, err := d.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("duckdb conn: %w", err)
	}
	defer conn.Close()
	for _, stmt := range e.mergeSettings(tempDir) {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("apply merge setting %q: %w", stmt, err)
		}
	}
	if e.hooks.onMergeConn != nil {
		e.hooks.onMergeConn(ctx, conn)
	}
	outputs, err := e.spec.merge(ctx, conn, measurement, paths, outDir)
	if err != nil {
		return nil, err
	}
	for i := range outputs {
		if err := fsyncFile(outputs[i].path); err != nil {
			return nil, fmt.Errorf("fsync merge output %s: %w", outputs[i].path, err)
		}
		fi, err := os.Stat(outputs[i].path)
		if err != nil {
			return nil, err
		}
		outputs[i].size = fi.Size()
	}
	if err := syncDir(outDir); err != nil {
		return nil, fmt.Errorf("fsync merge outdir: %w", err)
	}
	return outputs, nil
}

func fsyncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// loopFlush runs one drain pass for the loop and repeats it while the store
// failed during the pass but is healthy again by its end: its recovery signal
// fired while the pass was still busy (another PUT hanging on the outage), so
// the select would otherwise miss it and the backlog would wait for the next
// tick. Each repeat needs a fresh recovery (3 successful probes), so this
// cannot spin.
func (e *stagerEngine) loopFlush(what string) {
	for {
		before := e.health.failureCount()
		if err := e.Flush(e.runCtx); err != nil {
			e.logger.Error().Err(err).Msg(e.spec.logName + ": " + what + " failed")
		}
		if e.runCtx.Err() != nil || e.health.failureCount() == before || !e.health.Healthy() {
			return
		}
		e.logger.Info().Msg(e.spec.logName + ": object store recovered during the pass; draining again now")
	}
}

// abandon stops the loop WITHOUT the final drain, leaving the stage dir as a
// killed process would (tests).
func (e *stagerEngine) abandon() {
	e.closing.Store(true)
	e.skipFinalDrain.Store(true)
	e.stopOnce.Do(func() {
		e.runCancel()
		close(e.stopCh)
	})
	<-e.doneCh
	if e.ownHealth {
		e.health.Close()
	}
	if e.unregisterMetrics != nil {
		e.unregisterMetrics()
	}
}

func (e *stagerEngine) runLoop() {
	defer close(e.doneCh)
	ticker := time.NewTicker(e.flushAge)
	defer ticker.Stop()
	for {
		recovered := e.health.Recovered()
		select {
		case <-ticker.C:
			e.loopFlush("scheduled flush")
		case <-e.kickCh:
			e.loopFlush("drain after import")
		case <-recovered:
			// The object store is back: drain the backlog now, oldest first,
			// instead of waiting for the next tick.
			e.logger.Info().Msg(e.spec.logName + ": object store recovered; draining staged backlog now")
			e.loopFlush("recovery drain")
		case <-e.stopCh:
			if e.skipFinalDrain.Load() {
				return
			}
			e.finalDrain()
			return
		}
	}
}

// Backoff between the passes of the shutdown drain: short, unlike the prober's
// 1-30s schedule, because the budget is seconds and every staged file left at
// the end is lost with the pod (without a handoff peer).
const (
	finalDrainMinBackoff = 250 * time.Millisecond
	finalDrainMaxBackoff = time.Second
)

// finalDrain is the shutdown drain, bounded by the close budget. It repeats
// drain passes until nothing is staged or its budget ends (drainUntilEmpty).
//
// With a handoff peer configured the drain gets at most half the budget, and
// whatever is still staged afterwards is handed to the peer within the rest;
// a failed probe (the store is down, not blipping) ends the drain early so the
// handoff gets that time too.
func (e *stagerEngine) finalDrain() {
	budget := e.closeDrainBudget()
	deadline := time.Now().Add(budget)
	drainBudget := budget
	if e.opts.HandoffURL != "" {
		drainBudget = budget / 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), drainBudget)
	e.drainUntilEmpty(ctx, e.opts.HandoffURL != "")
	cancel()
	if e.opts.HandoffURL == "" {
		return
	}
	if n, err := e.Pending(); err == nil && n == 0 {
		return
	}
	hctx, hcancel := context.WithDeadline(context.Background(), deadline)
	defer hcancel()
	e.handoff(hctx)
}

// drainUntilEmpty runs forced drain passes until nothing is staged or ctx
// ends. A pass stops at its first availability failure, so one transient 503
// would otherwise end the shutdown drain with budget left. Between passes it
// waits a short backoff and, while the store is marked unhealthy, probes it
// first (the prober may not have noticed a recovery yet, and a probe costs one
// small request when the store is still down). With stopOnFailedProbe (a
// handoff peer is configured) a failed probe ends the drain: the store is
// down, and the peer can take the work now and upload it when it recovers.
func (e *stagerEngine) drainUntilEmpty(ctx context.Context, stopOnFailedProbe bool) {
	backoff := finalDrainMinBackoff
	for pass := 1; ; pass++ {
		if !e.health.Healthy() {
			if err := e.health.Probe(ctx); err != nil {
				if stopOnFailedProbe {
					e.logger.Warn().Err(err).Msg(e.spec.logName + ": object store unavailable at shutdown; handing staged work to the peer")
					return
				}
				e.logger.Warn().Err(err).Int("pass", pass).
					Msg(e.spec.logName + ": object store still unavailable at shutdown; retrying within the drain budget")
			} else if err := e.drain(ctx, true); err != nil {
				e.logger.Error().Err(err).Msg(e.spec.logName + ": shutdown flush failed (staged files remain on disk)")
			}
		} else if err := e.drain(ctx, true); err != nil {
			e.logger.Error().Err(err).Msg(e.spec.logName + ": shutdown flush failed (staged files remain on disk)")
		}
		if n, err := e.Pending(); err == nil && n == 0 {
			return
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, finalDrainMaxBackoff)
	}
}

// Close stops the background loop after the final drain (and handoff) bounded
// by the close budget. It returns an error wrapping ErrStagedNotUploaded when staged files
// remain: their rows are no longer in the WAL, so the caller must not report a
// clean shutdown (and must not purge the WAL on the strength of one).
func (e *stagerEngine) Close() error {
	e.closing.Store(true)
	e.stopOnce.Do(func() {
		e.runCancel() // abort a scheduled pass so the final drain starts now
		close(e.stopCh)
	})
	<-e.doneCh
	if e.ownHealth {
		e.health.Close()
	}
	if e.unregisterMetrics != nil {
		e.unregisterMetrics()
	}
	n, err := e.Pending()
	if err != nil {
		return fmt.Errorf("%s stager: cannot verify %s is drained: %w", e.spec.kind, e.stageDir, err)
	}
	if n > 0 {
		e.logger.Error().Int("staged_files", n).Str("stage_dir", e.stageDir).
			Msg(e.spec.logName + ": staged files were not uploaded before shutdown; they are the only copy of their rows")
		return fmt.Errorf("%s stager: %d staged file(s) in %s: %w", e.spec.kind, n, e.stageDir, ErrStagedNotUploaded)
	}
	return nil
}
