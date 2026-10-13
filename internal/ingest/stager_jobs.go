package ingest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// A stager merge job is the unit of exactly-once upload (IW-05).
//
// Life cycle, each step durable before the next:
//
//  1. Claim the oldest staged sources of one (database, measurement) (in
//     memory) and merge them into the local out dir, one file per output.
//  2. Persist job_<id>.json (temp file, fsync, rename, directory fsync): the
//     job id, its sources and, per output, the FIXED object key and status.
//     This is the commit point: from here on the sources belong to the job.
//  3. PUT every output not yet done. On a PUT error, HEAD the key through the
//     unwrapped backend (past the circuit breaker): a present object is the
//     one this job wrote (the key is unique to the pod and the job), so the
//     output is done. Persist the status after each output.
//  4. When every output is done, delete the sources, then the job file, then
//     the out dir.
//
// A crash or failure at any step resumes the SAME job with the SAME keys:
// before the job file nothing was uploaded and the sources are still
// unjobbed; after it, a resumed PUT overwrites or skips its own key and never
// adds a new object. Nothing is ever rolled back with a Delete, which fails
// during the very outage that broke the upload.

const (
	stagerJobVersion    = 1
	stagerJobFilePrefix = "job_"

	defaultStagerUploadConcurrency = 4

	// Crash points, for the kill-at-each-step tests (hooks.crash).
	stagerPointMerged        = "merged"
	stagerPointJobPersisted  = "job-persisted"
	stagerPointOutputDone    = "output-done"
	stagerPointUploaded      = "uploaded"
	stagerPointSourceDeleted = "source-deleted"
)

// stagerJob is the persisted state of one merge job.
type stagerJob struct {
	Version         int               `json:"version"`
	ID              string            `json:"id"`
	Kind            string            `json:"kind"`
	Pod             string            `json:"pod"`
	Database        string            `json:"database"`
	Measurement     string            `json:"measurement"`
	CreatedUnixNano int64             `json:"created_unix_nano"`
	Sources         []string          `json:"sources"` // file names in dir
	SourceBytes     int64             `json:"source_bytes"`
	Outputs         []stagerJobOutput `json:"outputs"`

	dir      string // directory holding the sources
	file     string // job file path
	imported bool   // handed over by a peer pod (lives in importjob_<id>/)
}

type stagerJobOutput struct {
	Partition string `json:"partition"`
	Key       string `json:"key"`
	Size      int64  `json:"size"`
	Done      bool   `json:"done"`
}

func (j *stagerJob) created() time.Time { return time.Unix(0, j.CreatedUnixNano).UTC() }

func (j *stagerJob) pending() []int {
	var idx []int
	for i, o := range j.Outputs {
		if !o.Done {
			idx = append(idx, i)
		}
	}
	return idx
}

// stagerCrashError marks an injected test crash: the drain stops on the spot,
// with nothing cleaned up, exactly as a killed process would.
type stagerCrashError struct{ err error }

func (c *stagerCrashError) Error() string { return "stager crash point: " + c.err.Error() }
func (c *stagerCrashError) Unwrap() error { return c.err }

func isStagerCrash(err error) bool {
	var c *stagerCrashError
	return errors.As(err, &c)
}

type failureBaselineKey struct{}

// withFailureBaseline records the health failure count at the start of a
// drain pass.
func withFailureBaseline(ctx context.Context, n int64) context.Context {
	return context.WithValue(ctx, failureBaselineKey{}, n)
}

// storeFailedSince reports whether an availability failure was reported after
// the pass in ctx started.
func (e *stagerEngine) storeFailedSince(ctx context.Context) bool {
	n, ok := ctx.Value(failureBaselineKey{}).(int64)
	return ok && e.health.failureCount() > n
}

func (e *stagerEngine) crashPoint(point string) error {
	if e.hooks.crash == nil {
		return nil
	}
	if err := e.hooks.crash(point); err != nil {
		return &stagerCrashError{err: err}
	}
	return nil
}

// sanitizeStagerPodID keeps [A-Za-z0-9-] so the id can sit inside an object
// name between "_" separators; anything else becomes '-'.
func sanitizeStagerPodID(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 63 {
		out = strings.Trim(out[:63], "-")
	}
	if out == "" {
		return "ingest"
	}
	return out
}

// defaultStagerPodID is the pod name (HOSTNAME in Kubernetes), else the host
// name.
func defaultStagerPodID() string {
	if h := strings.TrimSpace(os.Getenv("HOSTNAME")); h != "" {
		return h
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "ingest"
}

func (e *stagerEngine) newJobID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on supported platforms; fall back to time.
		return fmt.Sprintf("%s-%x", e.opts.PodID, time.Now().UnixNano())
	}
	return e.opts.PodID + "-" + hex.EncodeToString(b[:])
}

func (e *stagerEngine) outDir(jobID string) string {
	return filepath.Join(e.stageDir, stagerOutDirPrefix+e.spec.kind+"_"+jobID)
}

func outputFileName(partition string) string {
	return "p_" + strings.ReplaceAll(partition, "/", "") + ".parquet"
}

func (e *stagerEngine) outputPath(j *stagerJob, partition string) string {
	return filepath.Join(e.outDir(j.ID), outputFileName(partition))
}

func (e *stagerEngine) jobFilePath(jobID string) string {
	return filepath.Join(e.stageDir, stagerJobFilePrefix+jobID+".json")
}

// persistJob writes the job file durably: temp file, fsync, rename, directory
// fsync. Unlike a stage file it is never removed on a failed directory fsync:
// a job file that may or may not survive a crash is still the truth for this
// process, and removing it would hand its sources to a new job (new keys).
func (e *stagerEngine) persistJob(j *stagerJob) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	dir := filepath.Dir(j.file)
	tmp := filepath.Join(dir, "."+filepath.Base(j.file)+".tmp")
	if err := writeFileSynced(tmp, data); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, j.file); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

// loadJobsLocked reads this stager's persisted jobs. A job file that cannot be
// parsed is moved to the quarantine dir (logged): its sources then count as
// unjobbed again, which risks a duplicate of whatever it had uploaded but
// never wedges the stager. Caller holds e.mu.
func (e *stagerEngine) loadJobsLocked() ([]*stagerJob, error) {
	entries, err := os.ReadDir(e.stageDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s stager: read stage dir: %w", e.spec.kind, err)
	}
	var jobs []*stagerJob
	for _, en := range entries {
		name := en.Name()
		if en.IsDir() || !strings.HasPrefix(name, stagerJobFilePrefix) || !strings.HasSuffix(name, ".json") {
			continue
		}
		path := filepath.Join(e.stageDir, name)
		j, err := readStagerJob(path)
		if err != nil {
			e.logger.Error().Err(err).Str("job_file", name).
				Msg(e.spec.logName + ": unreadable job file; quarantining it (its sources will be merged again)")
			_ = quarantineSource(e.stageDir, path, "unreadable job file", e.logger)
			continue
		}
		if j.Kind != e.spec.kind {
			continue // the other stager's job in a shared directory
		}
		j.dir = e.stageDir
		j.file = path
		jobs = append(jobs, j)
	}
	jobs = append(jobs, e.loadImportedJobsLocked()...)
	sort.Slice(jobs, func(a, b int) bool {
		if jobs[a].CreatedUnixNano != jobs[b].CreatedUnixNano {
			return jobs[a].CreatedUnixNano < jobs[b].CreatedUnixNano
		}
		return jobs[a].ID < jobs[b].ID
	})
	return jobs, nil
}

func readStagerJob(path string) (*stagerJob, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var j stagerJob
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, err
	}
	if j.Version != stagerJobVersion || j.ID == "" || j.Kind == "" {
		return nil, fmt.Errorf("job file %s: unsupported version %d or missing id/kind", filepath.Base(path), j.Version)
	}
	return &j, nil
}

// jobSourceSetLocked lists the sources owned by persisted jobs, which the scan
// must never hand to a new job. Caller holds e.mu.
func (e *stagerEngine) jobSourceSetLocked() (map[string]bool, error) {
	jobs, err := e.loadJobsLocked()
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool)
	for _, j := range jobs {
		if j.dir != e.stageDir {
			continue
		}
		for _, s := range j.Sources {
			set[s] = true
		}
	}
	return set, nil
}

// stagerJobFailure is the failure record of one persisted job since this
// process started: the current streak of failed attempts.
type stagerJobFailure struct {
	consecutive int
	since       time.Time // first failure of the streak
	upload      bool      // the last failure was a PUT, not a re-merge
}

// errStagerUploadFailed marks a job failure that happened at a PUT (as opposed
// to a re-merge of its sources).
var errStagerUploadFailed = errors.New("stager job upload failed")

// recordJobFailure extends the job's failure streak.
func (e *stagerEngine) recordJobFailure(jobID string, err error, now time.Time) {
	e.failMu.Lock()
	defer e.failMu.Unlock()
	if e.jobFailures == nil {
		e.jobFailures = make(map[string]*stagerJobFailure)
	}
	f := e.jobFailures[jobID]
	if f == nil {
		f = &stagerJobFailure{since: now}
		e.jobFailures[jobID] = f
	}
	f.consecutive++
	f.upload = errors.Is(err, errStagerUploadFailed)
}

func (e *stagerEngine) clearJobFailure(jobID string) {
	e.failMu.Lock()
	defer e.failMu.Unlock()
	delete(e.jobFailures, jobID)
}

// jobFailedBefore reports a job that already failed in an earlier attempt.
func (e *stagerEngine) jobFailedBefore(jobID string) bool {
	e.failMu.Lock()
	defer e.failMu.Unlock()
	f := e.jobFailures[jobID]
	return f != nil && f.consecutive > 0
}

// pruneJobFailures forgets jobs that no longer exist (handed off, or their job
// file was quarantined), so the failing-job gauges do not report ghosts.
func (e *stagerEngine) pruneJobFailures(jobs []*stagerJob) {
	live := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		live[j.ID] = true
	}
	e.failMu.Lock()
	defer e.failMu.Unlock()
	for id := range e.jobFailures {
		if !live[id] {
			delete(e.jobFailures, id)
		}
	}
}

// failingJobStats is the number of jobs with a failure streak and the age of
// the oldest streak.
func (e *stagerEngine) failingJobStats(now time.Time) (int, time.Duration) {
	e.failMu.Lock()
	defer e.failMu.Unlock()
	var oldest time.Duration
	for _, f := range e.jobFailures {
		if age := now.Sub(f.since); age > oldest {
			oldest = age
		}
	}
	return len(e.jobFailures), oldest
}

// orderJobsForResume keeps the jobs oldest first but moves those that failed
// before to the back: a job that keeps failing (or hangs until its timeout)
// must not run ahead of every other job in every pass.
func (e *stagerEngine) orderJobsForResume(jobs []*stagerJob) {
	failed := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		failed[j.ID] = e.jobFailedBefore(j.ID)
	}
	sort.SliceStable(jobs, func(a, b int) bool { return !failed[jobs[a].ID] && failed[jobs[b].ID] })
}

// reportUploadFailure tells the health tracker about a failed PUT, unless the
// job already failed in an earlier attempt and the store answers a probe now.
// Then the failure is the job's own (a key- or size-specific error that the
// tiny probe object does not hit): marking the store unavailable would end
// every pass of both stagers on this one job, and the prober would declare a
// recovery and the same job would end the next pass again, forever.
func (e *stagerEngine) reportUploadFailure(ctx context.Context, j *stagerJob, err error) {
	if !isS3AvailabilityError(err) {
		return // says nothing about the store; ReportFailure would ignore it
	}
	if e.jobFailedBefore(j.ID) {
		if perr := e.health.Probe(ctx); perr == nil {
			e.logger.Warn().Err(err).Str("job", j.ID).
				Msg(e.spec.logName + ": upload failed again while the object store answers its probe; failure of this job only, other work continues")
			return
		}
	}
	e.health.ReportFailure(err)
}

// resumeJobs finishes persisted jobs, oldest first (jobs that failed before
// last), before any new job is built. A job that fails stays pending for the
// next pass.
func (e *stagerEngine) resumeJobs(ctx context.Context) error {
	e.mu.Lock()
	jobs, err := e.loadJobsLocked()
	e.mu.Unlock()
	if err != nil {
		return err
	}
	e.pruneJobFailures(jobs)
	e.orderJobsForResume(jobs)
	for _, j := range jobs {
		if ctx.Err() != nil || e.storeFailedSince(ctx) {
			return nil
		}
		e.logger.Info().Str("job", j.ID).Int("pending_outputs", len(j.pending())).
			Msg(e.spec.logName + ": resuming merge job with its recorded keys")
		metrics.Get().IncStagerJobsResumed()
		jctx, cancel := context.WithTimeout(ctx, stagerJobTimeout)
		err := e.processJob(jctx, j)
		cancel()
		if err != nil {
			if isStagerCrash(err) {
				return err
			}
			e.logger.Error().Err(err).Str("job", j.ID).
				Msg(e.spec.logName + ": resumed job failed; it stays pending with the same keys")
		}
	}
	return nil
}

// runBatch builds one job from a claimed batch and runs it.
func (e *stagerEngine) runBatch(ctx context.Context, batch *stagerBatch) error {
	defer e.release(batch.files)
	ctx, cancel := context.WithTimeout(ctx, stagerJobTimeout)
	defer cancel()
	j, err := e.createJob(ctx, batch)
	if err != nil || j == nil {
		return err
	}
	return e.processJob(ctx, j)
}

// createJob merges the batch locally and persists the job file. Returns nil,
// nil when nothing was left to merge (every source quarantined).
func (e *stagerEngine) createJob(ctx context.Context, batch *stagerBatch) (*stagerJob, error) {
	paths := make([]string, len(batch.files))
	sizes := make(map[string]int64, len(batch.files))
	for i, f := range batch.files {
		paths[i] = f.path
		sizes[f.path] = f.size
	}
	if e.hooks.onJob != nil {
		e.hooks.onJob(batch.database, batch.measurement, paths, batch.bytes)
	}
	// Drop sources that cannot possibly be parquet before DuckDB ever sees
	// them. A single unreadable file used to fail the whole group's merge
	// forever, because the retry re-submitted the same poisoned list.
	paths = quarantineStructurallyBad(e.stageDir, paths, e.logger)
	if len(paths) == 0 {
		return nil, nil
	}

	created := time.Now().UTC()
	id := e.newJobID()
	outDir := e.outDir(id)
	var outputs []mergedOutput
	merged, err := execMergeWithQuarantine(ctx, e.stageDir, paths, e.logger,
		func(ctx context.Context, attempt []string) error {
			var mergeErr error
			outputs, mergeErr = e.runMerge(ctx, batch.measurement, attempt, outDir)
			return mergeErr
		})
	if err != nil {
		_ = os.RemoveAll(outDir)
		return nil, err
	}
	if len(merged) == 0 {
		_ = os.RemoveAll(outDir)
		return nil, nil
	}
	if err := e.crashPoint(stagerPointMerged); err != nil {
		return nil, err
	}

	j := &stagerJob{
		Version:         stagerJobVersion,
		ID:              id,
		Kind:            e.spec.kind,
		Pod:             e.opts.PodID,
		Database:        batch.database,
		Measurement:     batch.measurement,
		CreatedUnixNano: created.UnixNano(),
		dir:             e.stageDir,
		file:            e.jobFilePath(id),
	}
	for _, p := range merged {
		j.Sources = append(j.Sources, filepath.Base(p))
		j.SourceBytes += sizes[p]
	}
	for _, o := range outputs {
		j.Outputs = append(j.Outputs, stagerJobOutput{
			Partition: o.partition,
			Key:       e.spec.jobKey(batch.database, batch.measurement, o.partition, id, created),
			Size:      o.size,
		})
	}
	if err := e.persistJob(j); err != nil {
		_ = os.RemoveAll(outDir)
		_ = os.Remove(j.file) // a rename that did happen must not claim the sources
		return nil, fmt.Errorf("persist job %s: %w", id, err)
	}
	if err := e.crashPoint(stagerPointJobPersisted); err != nil {
		return nil, err
	}
	return j, nil
}

// processJob uploads a persisted job's pending outputs and, once all are done,
// removes its sources and state.
func (e *stagerEngine) processJob(ctx context.Context, j *stagerJob) error {
	if pending := j.pending(); len(pending) > 0 {
		if !e.outputsReady(j, pending) {
			if err := e.remergeJob(ctx, j); err != nil {
				if !errors.Is(err, context.Canceled) {
					e.recordJobFailure(j.ID, err, time.Now())
				}
				return fmt.Errorf("job %s: %w", j.ID, err)
			}
		}
		if err := e.uploadPending(ctx, j, pending); err != nil {
			// A pass stopped by another job's failure, a crash point or a
			// shutdown is not a failure of this job.
			if !errors.Is(err, errStagerPassInterrupted) && !isStagerCrash(err) && !errors.Is(err, context.Canceled) {
				e.recordJobFailure(j.ID, fmt.Errorf("%w: %w", errStagerUploadFailed, err), time.Now())
			}
			return fmt.Errorf("job %s: %w", j.ID, err)
		}
		// Never finish (delete the sources of) a job with an output that is
		// not confirmed in the object store.
		if left := j.pending(); len(left) > 0 {
			return fmt.Errorf("job %s: %d output(s) not uploaded: %w", j.ID, len(left), errStagerPassInterrupted)
		}
		if err := e.crashPoint(stagerPointUploaded); err != nil {
			return err
		}
	}
	if err := e.finishJob(j); err != nil {
		return err
	}
	e.clearJobFailure(j.ID)
	return nil
}

// outputsReady reports whether every pending output's local file is present
// with the recorded size.
func (e *stagerEngine) outputsReady(j *stagerJob, pending []int) bool {
	for _, i := range pending {
		fi, err := os.Stat(e.outputPath(j, j.Outputs[i].Partition))
		if err != nil || fi.Size() != j.Outputs[i].Size {
			return false
		}
	}
	return true
}

// remergeJob rebuilds a job's local outputs from its sources (they were lost,
// or the job came from another pod without them). The merge of the same
// sources yields the same rows per partition, so the pending outputs are
// uploaded to their recorded keys with the same content. A partition set that
// differs from the recorded one refuses the job rather than drop or add rows.
func (e *stagerEngine) remergeJob(ctx context.Context, j *stagerJob) error {
	paths := make([]string, len(j.Sources))
	for i, s := range j.Sources {
		p := filepath.Join(j.dir, s)
		if _, err := os.Stat(p); err != nil {
			return fmt.Errorf("re-merge: source %s: %w", s, err)
		}
		paths[i] = p
	}
	tmp := e.outDir(j.ID) + ".remerge"
	outs, err := e.runMerge(ctx, j.Measurement, paths, tmp)
	if err != nil {
		_ = os.RemoveAll(tmp)
		return fmt.Errorf("re-merge: %w", err)
	}
	got := make(map[string]mergedOutput, len(outs))
	for _, o := range outs {
		got[o.partition] = o
	}
	recorded := make(map[string]bool, len(j.Outputs))
	for _, o := range j.Outputs {
		recorded[o.Partition] = true
		if _, ok := got[o.Partition]; !ok && !o.Done {
			_ = os.RemoveAll(tmp)
			return fmt.Errorf("re-merge produced no partition %s for a pending output", o.Partition)
		}
	}
	for p := range got {
		if !recorded[p] {
			_ = os.RemoveAll(tmp)
			return fmt.Errorf("re-merge produced partition %s that the job never recorded", p)
		}
	}
	out := e.outDir(j.ID)
	if err := os.RemoveAll(out); err != nil {
		return err
	}
	if err := os.Rename(tmp, out); err != nil {
		return fmt.Errorf("re-merge: install outputs: %w", err)
	}
	if err := syncDir(e.stageDir); err != nil {
		return err
	}
	for i := range j.Outputs {
		if o, ok := got[j.Outputs[i].Partition]; ok && !j.Outputs[i].Done {
			j.Outputs[i].Size = o.size
		}
	}
	return e.persistJob(j)
}

// uploadPending PUTs the pending outputs with bounded concurrency.
func (e *stagerEngine) uploadPending(ctx context.Context, j *stagerJob, pending []int) error {
	var (
		wg       sync.WaitGroup
		jobMu    sync.Mutex
		errMu    sync.Mutex
		firstErr error
	)
	// Stop launching PUTs once the store reported unavailable during this
	// pass: the remaining outputs would only fail too.
	stop := func() bool {
		errMu.Lock()
		defer errMu.Unlock()
		return isStagerCrash(firstErr) || ctx.Err() != nil || e.storeFailedSince(ctx)
	}
	sem := make(chan struct{}, e.opts.UploadConcurrency)
	for _, i := range pending {
		sem <- struct{}{}
		if stop() {
			<-sem
			break
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := e.uploadOutput(ctx, j, i, &jobMu); err != nil {
				errMu.Lock()
				if firstErr == nil || isStagerCrash(err) {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if firstErr == nil && ctx.Err() != nil {
		firstErr = ctx.Err()
	}
	if firstErr == nil {
		// The loop may have stopped launching PUTs without any of its own
		// failing: the shared health tracker went unhealthy because the OTHER
		// stager failed. Outputs that were never attempted are not done.
		for _, i := range pending {
			if !j.Outputs[i].Done {
				firstErr = errStagerPassInterrupted
				break
			}
		}
	}
	return firstErr
}

// errStagerPassInterrupted: a job's upload stopped before every output was
// attempted (the object store was reported unavailable). The job stays
// pending with its keys.
var errStagerPassInterrupted = errors.New("upload pass stopped: object store reported unavailable; job stays pending")

// uploadOutput PUTs one output to its recorded key and persists its status.
func (e *stagerEngine) uploadOutput(ctx context.Context, j *stagerJob, i int, jobMu *sync.Mutex) error {
	jobMu.Lock()
	out := j.Outputs[i]
	jobMu.Unlock()
	putErr := e.putFile(ctx, out.Key, e.outputPath(j, out.Partition))
	if putErr != nil {
		if !e.objectPresent(out.Key, out.Size) {
			metrics.Get().IncStagerUploadFailures(e.spec.kind)
			if ctx.Err() == nil {
				e.reportUploadFailure(ctx, j, putErr)
			}
			return fmt.Errorf("upload %s: %w", out.Key, putErr)
		}
		metrics.Get().IncStagerHeadDedup(e.spec.kind)
		e.logger.Warn().Err(putErr).Str("storage_path", out.Key).Str("job", j.ID).
			Msg(e.spec.logName + ": upload returned an error but the object exists - treating as uploaded")
	}
	jobMu.Lock()
	j.Outputs[i].Done = true
	err := e.persistJob(j)
	jobMu.Unlock()
	if err != nil {
		// The object is uploaded but its status may not be on disk: a resume
		// PUTs the same key again, which overwrites, never duplicates.
		return fmt.Errorf("persist status of %s: %w", out.Key, err)
	}
	return e.crashPoint(stagerPointOutputDone)
}

func (e *stagerEngine) putFile(ctx context.Context, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	return e.storage.WriteReader(ctx, key, f, fi.Size())
}

// objectPresent is the HEAD after a failed PUT. It goes to the unwrapped
// backend, past the circuit breaker the failed upload may just have opened,
// with its own timeout: the PUT error is often the caller's deadline. An
// object of a different size than the local output (where the backend reports
// sizes) is not ours to trust and is overwritten by the retry.
func (e *stagerEngine) objectPresent(key string, size int64) bool {
	ctx, cancel := context.WithTimeout(context.Background(), stagerUploadExistsTimeout)
	defer cancel()
	base := storage.Unwrap(e.storage)
	ok, err := base.Exists(ctx, key)
	if err != nil || !ok {
		return false
	}
	if size > 0 {
		if n, err := base.StatFile(ctx, key); err == nil && n >= 0 && n != size {
			e.logger.Warn().Str("storage_path", key).Int64("object_size", n).Int64("local_size", size).
				Msg(e.spec.logName + ": object present with another size; uploading it again")
			return false
		}
	}
	return true
}

// finishJob removes a completed job: sources first, then the job file, then
// the local outputs. Each step tolerates a previous attempt having done it.
func (e *stagerEngine) finishJob(j *stagerJob) error {
	// Imported units are tombstoned BEFORE their local copy goes, so a late
	// re-send of them from the source pod is acknowledged, not re-uploaded.
	if j.imported {
		if err := e.tombstone(stagerJobFilePrefix + j.ID); err != nil {
			return fmt.Errorf("job %s: tombstone: %w", j.ID, err)
		}
	} else {
		for _, s := range j.Sources {
			if e.isImportedSourceName(s) {
				if err := e.tombstone(s); err != nil {
					return fmt.Errorf("job %s: tombstone %s: %w", j.ID, s, err)
				}
			}
		}
	}
	for _, s := range j.Sources {
		if err := os.Remove(filepath.Join(j.dir, s)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("job %s: delete source %s: %w", j.ID, s, err)
		}
		if err := e.crashPoint(stagerPointSourceDeleted); err != nil {
			return err
		}
	}
	if err := syncDir(j.dir); err != nil {
		return fmt.Errorf("job %s: fsync source dir: %w", j.ID, err)
	}
	if err := os.Remove(j.file); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("job %s: delete job file: %w", j.ID, err)
	}
	if err := syncDir(filepath.Dir(j.file)); err != nil {
		return fmt.Errorf("job %s: fsync job dir: %w", j.ID, err)
	}
	if j.imported {
		if err := os.RemoveAll(j.dir); err != nil {
			return fmt.Errorf("job %s: remove imported job dir: %w", j.ID, err)
		}
		if err := syncDir(e.stageDir); err != nil {
			return fmt.Errorf("job %s: fsync stage dir: %w", j.ID, err)
		}
	}
	_ = os.RemoveAll(e.outDir(j.ID))
	e.logger.Info().
		Str("job", j.ID).
		Str("database", j.Database).
		Str("measurement", j.Measurement).
		Int("staged_files", len(j.Sources)).
		Int("uploaded_files", len(j.Outputs)).
		Msg(e.spec.logName + " merged + uploaded")
	return nil
}
