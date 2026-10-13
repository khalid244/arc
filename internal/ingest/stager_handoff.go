package ingest

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Stager handoff (pod deletion without a PVC).
//
// A stager's staged data lives only on its pod's emptyDir until uploaded. When
// the pod shuts down during an object-store outage, the bounded final drain
// cannot upload it and the pod's deletion would lose it. With
// ingest.stager_handoff_url set (a Service URL: a terminating pod is removed
// from the Service endpoints, so it reaches a live peer), the stager hands
// each remaining UNIT to the peer's POST /api/v1/internal/stager/import:
//
//   - a job unit: a persisted merge job (job file with its recorded keys and
//     per-output status) plus its sources;
//   - a sources unit: a batch of unjobbed staged files.
//
// A unit is deleted locally only after a 2xx. The peer (with
// ingest.stager_import_enabled) stores a unit atomically and idempotently:
//
//   - a job lands as one directory rename, importjob_<job id>/ holding the job
//     file and its sources, and is resumed with the recorded keys, so outputs
//     already uploaded are skipped and the rest go to the same keys;
//   - a source file lands under a name carrying the source pod
//     (<db>__<m>__<kind>_stage_<pod>-<seq>.parquet) and is merged like any
//     other staged file.
//
// A unit that already exists, or that the peer already uploaded (a tombstone
// under .imported/ records it for a day), is acknowledged without being
// stored again, so a retry after a lost response never duplicates rows.

const (
	// StagerImportPath is the peer endpoint (registered only with
	// ingest.stager_import_enabled).
	StagerImportPath = "/api/v1/internal/stager/import"
	// StagerKindHeader routes a unit to the peer's fresh or late stager.
	StagerKindHeader = "X-Arc-Stager-Kind"

	stagerImportJobPrefix = "importjob_"
	stagerImportTmpPrefix = ".import_"
	stagerTombstoneDir    = ".imported"
	stagerTombstoneTTL    = 24 * time.Hour
	stagerUnitManifest    = "unit.json"
	stagerUnitVersion     = 1
	stagerUnitJob         = "job"
	stagerUnitSources     = "sources"

	stagerHandoffMaxFiles    = 200
	stagerHandoffMaxBytes    = 64 << 20
	stagerHandoffAttempts    = 3
	stagerUnitManifestMaxLen = 16 << 20
)

var (
	// ErrStagerImportInvalid: the unit is malformed (HTTP 400). The sender
	// keeps it.
	ErrStagerImportInvalid = errors.New("invalid stager import unit")
	// ErrStagerImportFromSelf: the unit came from this very pod (HTTP 409);
	// storing it here would delete it with the pod.
	ErrStagerImportFromSelf = errors.New("stager import unit comes from this pod")
	// ErrStagerClosing: this stager is shutting down (HTTP 503).
	ErrStagerClosing = errors.New("stager is shutting down")
)

// StagerImportResult reports what an import stored.
type StagerImportResult struct {
	Stored  int `json:"stored"`
	Skipped int `json:"skipped"`
}

// StagerImportStatus maps an ImportUnit error to its HTTP status.
func StagerImportStatus(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, ErrStagerImportInvalid):
		return http.StatusBadRequest
	case errors.Is(err, ErrStagerImportFromSelf):
		return http.StatusConflict
	case errors.Is(err, ErrStagerClosing):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// stagerUnit is the unit.json manifest, the first entry of a unit's tar
// stream.
type stagerUnit struct {
	Version   int        `json:"version"`
	Kind      string     `json:"kind"`
	SourcePod string     `json:"source_pod"`
	Type      string     `json:"type"`
	Job       *stagerJob `json:"job,omitempty"`
	Files     []string   `json:"files"`
}

type stagerUnitFile struct {
	name  string
	path  string // read from disk when set
	data  []byte // used when path is empty
	mtime time.Time
}

func invalidUnit(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrStagerImportInvalid, fmt.Sprintf(format, args...))
}

// writeStagerUnit writes a unit as a tar stream: unit.json, then the files.
func writeStagerUnit(w io.Writer, u stagerUnit, files []stagerUnitFile) error {
	tw := tar.NewWriter(w)
	manifest, err := json.Marshal(u)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: stagerUnitManifest, Mode: 0600, Size: int64(len(manifest)), ModTime: time.Now()}); err != nil {
		return err
	}
	if _, err := tw.Write(manifest); err != nil {
		return err
	}
	for _, f := range files {
		var r io.Reader
		size := int64(len(f.data))
		if f.path != "" {
			fh, err := os.Open(f.path)
			if err != nil {
				return err
			}
			fi, err := fh.Stat()
			if err != nil {
				fh.Close()
				return err
			}
			size = fi.Size()
			r = fh
			defer fh.Close()
		} else {
			r = bytes.NewReader(f.data)
		}
		if err := tw.WriteHeader(&tar.Header{Name: f.name, Mode: 0600, Size: size, ModTime: f.mtime, Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		if _, err := io.CopyN(tw, r, size); err != nil {
			return err
		}
	}
	return tw.Close()
}

// validImportName reports whether name is one of this stager's staged
// source names: <db>__<m>__<marker><rest>.parquet, a plain file name.
func (e *stagerEngine) validImportName(name string) bool {
	if name == "" || strings.HasPrefix(name, ".") || strings.ContainsAny(name, "/\\") ||
		strings.Contains(name, "..") || !strings.HasSuffix(name, ".parquet") {
		return false
	}
	parts := strings.SplitN(name, "__", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || !strings.HasPrefix(parts[2], e.spec.marker) {
		return false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(parts[2], e.spec.marker), ".parquet")
	return rest != "" && sanitizeStagerPodID(rest) == rest
}

// importedSourceName puts the source pod in front of the sequence:
// <db>__<m>__<marker><pod>-<rest>.parquet.
func (e *stagerEngine) importedSourceName(name, pod string) string {
	parts := strings.SplitN(name, "__", 3)
	rest := strings.TrimPrefix(parts[2], e.spec.marker)
	return parts[0] + "__" + parts[1] + "__" + e.spec.marker + pod + "-" + rest
}

// isImportedSourceName reports a source a peer handed over (its sequence part
// carries a pod id, not just digits).
func (e *stagerEngine) isImportedSourceName(name string) bool {
	parts := strings.SplitN(name, "__", 3)
	if len(parts) != 3 {
		return false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(parts[2], e.spec.marker), ".parquet")
	for _, r := range rest {
		if r < '0' || r > '9' {
			return true
		}
	}
	return false
}

func (e *stagerEngine) tombstonePath(name string) string {
	return filepath.Join(e.stageDir, stagerTombstoneDir, name)
}

func (e *stagerEngine) tombstoned(name string) bool {
	_, err := os.Stat(e.tombstonePath(name))
	return err == nil
}

// tombstone records that an imported unit was uploaded, before its local copy
// is deleted, so a late re-send of it is acknowledged without a second upload.
func (e *stagerEngine) tombstone(name string) error {
	dir := filepath.Join(e.stageDir, stagerTombstoneDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err := writeFileSynced(filepath.Join(dir, name), nil); err != nil {
		return err
	}
	return syncDir(dir)
}

// expireTombstones drops tombstones older than stagerTombstoneTTL.
func (e *stagerEngine) expireTombstones() {
	dir := filepath.Join(e.stageDir, stagerTombstoneDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-stagerTombstoneTTL)
	for _, en := range entries {
		if info, err := en.Info(); err == nil && info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(dir, en.Name()))
		}
	}
}

func (e *stagerEngine) importJobDir(jobID string) string {
	return filepath.Join(e.stageDir, stagerImportJobPrefix+jobID)
}

// jobKnownLocked reports a job this stager holds or already uploaded.
func (e *stagerEngine) jobKnownLocked(jobID string) bool {
	if _, err := os.Stat(e.importJobDir(jobID)); err == nil {
		return true
	}
	if _, err := os.Stat(e.jobFilePath(jobID)); err == nil {
		return true
	}
	return e.tombstoned(stagerJobFilePrefix + jobID)
}

// kick asks the drain loop for a pass now (after an import).
func (e *stagerEngine) kick() {
	select {
	case e.kickCh <- struct{}{}:
	default:
	}
}

// ImportUnit stores a unit handed over by a peer pod (see the package notes
// above). It is safe to call concurrently with the drain loop.
func (e *stagerEngine) ImportUnit(r io.Reader) (StagerImportResult, error) {
	if e.closing.Load() {
		return StagerImportResult{}, ErrStagerClosing
	}
	tr := tar.NewReader(r)
	hdr, err := tr.Next()
	if err != nil {
		return StagerImportResult{}, invalidUnit("read unit manifest: %v", err)
	}
	if hdr.Name != stagerUnitManifest || hdr.Size > stagerUnitManifestMaxLen {
		return StagerImportResult{}, invalidUnit("first entry is %q, want %s", hdr.Name, stagerUnitManifest)
	}
	raw, err := io.ReadAll(io.LimitReader(tr, stagerUnitManifestMaxLen))
	if err != nil {
		return StagerImportResult{}, invalidUnit("read unit manifest: %v", err)
	}
	var u stagerUnit
	if err := json.Unmarshal(raw, &u); err != nil {
		return StagerImportResult{}, invalidUnit("parse unit manifest: %v", err)
	}
	if u.Version != stagerUnitVersion || u.Kind != e.spec.kind {
		return StagerImportResult{}, invalidUnit("unit version %d kind %q, this stager is %q", u.Version, u.Kind, e.spec.kind)
	}
	pod := sanitizeStagerPodID(u.SourcePod)
	if strings.TrimSpace(u.SourcePod) == "" || pod != u.SourcePod {
		return StagerImportResult{}, invalidUnit("bad source pod %q", u.SourcePod)
	}
	if pod == e.opts.PodID {
		return StagerImportResult{}, fmt.Errorf("%w: %s", ErrStagerImportFromSelf, pod)
	}
	if len(u.Files) == 0 {
		return StagerImportResult{}, invalidUnit("unit lists no files")
	}
	seen := make(map[string]bool, len(u.Files))
	for _, f := range u.Files {
		if !e.validImportName(f) || seen[f] {
			return StagerImportResult{}, invalidUnit("bad or repeated file name %q", f)
		}
		seen[f] = true
	}
	switch u.Type {
	case stagerUnitSources:
		return e.importSources(tr, u)
	case stagerUnitJob:
		return e.importJob(tr, u)
	default:
		return StagerImportResult{}, invalidUnit("unknown unit type %q", u.Type)
	}
}

type receivedFile struct {
	name  string
	tmp   string
	mtime time.Time
}

// receiveFiles writes the unit's file entries into dir (each fsynced) and
// checks that exactly the listed files arrived, complete.
func receiveFiles(tr *tar.Reader, dir string, want []string) ([]receivedFile, error) {
	expected := make(map[string]bool, len(want))
	for _, n := range want {
		expected[n] = true
	}
	var got []receivedFile
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, invalidUnit("read unit: %v", err)
		}
		if hdr.Typeflag != tar.TypeReg || !expected[hdr.Name] {
			return nil, invalidUnit("unexpected entry %q", hdr.Name)
		}
		delete(expected, hdr.Name)
		tmp := filepath.Join(dir, hdr.Name)
		f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return nil, err
		}
		n, copyErr := io.Copy(f, tr)
		syncErr := f.Sync()
		closeErr := f.Close()
		if copyErr != nil {
			return nil, invalidUnit("read %s: %v", hdr.Name, copyErr)
		}
		if n != hdr.Size {
			return nil, invalidUnit("%s: %d of %d bytes", hdr.Name, n, hdr.Size)
		}
		if syncErr != nil {
			return nil, syncErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		mtime := hdr.ModTime
		if mtime.IsZero() {
			mtime = time.Now()
		}
		if err := os.Chtimes(tmp, mtime, mtime); err != nil {
			return nil, err
		}
		got = append(got, receivedFile{name: hdr.Name, tmp: tmp, mtime: mtime})
	}
	if len(expected) > 0 {
		return nil, invalidUnit("unit is missing %d listed file(s)", len(expected))
	}
	return got, syncDir(dir)
}

func (e *stagerEngine) newImportTmpDir() (string, error) {
	var b [8]byte
	_, _ = rand.Read(b[:])
	dir := filepath.Join(e.stageDir, stagerImportTmpPrefix+hex.EncodeToString(b[:])+".tmp")
	return dir, os.MkdirAll(dir, 0700)
}

// importSources stores unjobbed source files under source-pod names. A name
// that is already here, or that this stager already uploaded, is skipped.
func (e *stagerEngine) importSources(tr *tar.Reader, u stagerUnit) (StagerImportResult, error) {
	var res StagerImportResult
	tmpDir, err := e.newImportTmpDir()
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(tmpDir)
	files, err := receiveFiles(tr, tmpDir, u.Files)
	if err != nil {
		return res, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, f := range files {
		target := e.importedSourceName(f.name, u.SourcePod)
		final := filepath.Join(e.stageDir, target)
		if _, err := os.Stat(final); err == nil || e.tombstoned(target) {
			res.Skipped++
			continue
		}
		if err := os.Rename(f.tmp, final); err != nil {
			return res, fmt.Errorf("store imported %s: %w", target, err)
		}
		res.Stored++
	}
	if err := syncDir(e.stageDir); err != nil {
		return res, err
	}
	if res.Stored > 0 {
		e.kick()
	}
	return res, nil
}

// importJob stores a merge job with its sources as one directory rename.
func (e *stagerEngine) importJob(tr *tar.Reader, u stagerUnit) (StagerImportResult, error) {
	var res StagerImportResult
	j := u.Job
	if j == nil || j.Version != stagerJobVersion || j.Kind != e.spec.kind || j.ID == "" ||
		sanitizeStagerPodID(j.ID) != j.ID || len(j.Outputs) == 0 {
		return res, invalidUnit("bad job in unit")
	}
	if len(j.Sources) != len(u.Files) {
		return res, invalidUnit("job lists %d sources, unit carries %d files", len(j.Sources), len(u.Files))
	}
	listed := make(map[string]bool, len(u.Files))
	for _, f := range u.Files {
		listed[f] = true
	}
	for _, s := range j.Sources {
		if !listed[s] {
			return res, invalidUnit("job source %q is not in the unit", s)
		}
	}
	for _, o := range j.Outputs {
		if o.Key == "" || o.Partition == "" {
			return res, invalidUnit("job output without key or partition")
		}
	}
	e.mu.Lock()
	known := e.jobKnownLocked(j.ID)
	e.mu.Unlock()
	if known {
		_, _ = io.Copy(io.Discard, tr)
		res.Skipped = 1
		return res, nil
	}
	tmpDir, err := e.newImportTmpDir()
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(tmpDir)
	if _, err := receiveFiles(tr, tmpDir, u.Files); err != nil {
		return res, err
	}
	data, err := json.Marshal(j)
	if err != nil {
		return res, err
	}
	if err := writeFileSynced(filepath.Join(tmpDir, "job.json"), data); err != nil {
		return res, err
	}
	if err := syncDir(tmpDir); err != nil {
		return res, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.jobKnownLocked(j.ID) {
		res.Skipped = 1
		return res, nil
	}
	if err := os.Rename(tmpDir, e.importJobDir(j.ID)); err != nil {
		return res, fmt.Errorf("store imported job %s: %w", j.ID, err)
	}
	if err := syncDir(e.stageDir); err != nil {
		return res, err
	}
	res.Stored = 1
	e.kick()
	return res, nil
}

// loadImportedJobsLocked reads importjob_<id>/job.json. Caller holds e.mu.
func (e *stagerEngine) loadImportedJobsLocked() []*stagerJob {
	entries, err := os.ReadDir(e.stageDir)
	if err != nil {
		return nil
	}
	var jobs []*stagerJob
	for _, en := range entries {
		if !en.IsDir() || !strings.HasPrefix(en.Name(), stagerImportJobPrefix) {
			continue
		}
		dir := filepath.Join(e.stageDir, en.Name())
		path := filepath.Join(dir, "job.json")
		j, err := readStagerJob(path)
		if err != nil {
			if !os.IsNotExist(errors.Unwrap(err)) && !os.IsNotExist(err) {
				e.logger.Error().Err(err).Str("dir", en.Name()).Msg(e.spec.logName + ": unreadable imported job")
			}
			continue
		}
		if j.Kind != e.spec.kind || stagerImportJobPrefix+j.ID != en.Name() {
			continue
		}
		j.dir = dir
		j.file = path
		j.imported = true
		jobs = append(jobs, j)
	}
	return jobs
}

// countImportedJobSources counts the sources held in imported job dirs.
func (e *stagerEngine) importedJobSourceStats() (files int, bytes int64, oldest time.Time) {
	entries, err := os.ReadDir(e.stageDir)
	if err != nil {
		return 0, 0, time.Time{}
	}
	for _, en := range entries {
		if !en.IsDir() || !strings.HasPrefix(en.Name(), stagerImportJobPrefix) {
			continue
		}
		sub, err := os.ReadDir(filepath.Join(e.stageDir, en.Name()))
		if err != nil {
			continue
		}
		for _, s := range sub {
			if s.IsDir() || !strings.HasSuffix(s.Name(), ".parquet") {
				continue
			}
			info, err := s.Info()
			if err != nil {
				continue
			}
			files++
			bytes += info.Size()
			if oldest.IsZero() || info.ModTime().Before(oldest) {
				oldest = info.ModTime()
			}
		}
	}
	return files, bytes, oldest
}

// removeImportLeftovers deletes incomplete imports (the sender got no 2xx and
// still holds them) and imported job dirs whose upload had already finished.
func (e *stagerEngine) removeImportLeftovers() {
	entries, err := os.ReadDir(e.stageDir)
	if err != nil {
		return
	}
	for _, en := range entries {
		name := en.Name()
		path := filepath.Join(e.stageDir, name)
		switch {
		case en.IsDir() && strings.HasPrefix(name, stagerImportTmpPrefix) && strings.HasSuffix(name, ".tmp"):
			_ = os.RemoveAll(path)
		case en.IsDir() && strings.HasPrefix(name, stagerImportJobPrefix):
			id := strings.TrimPrefix(name, stagerImportJobPrefix)
			if _, err := os.Stat(filepath.Join(path, "job.json")); err != nil && e.tombstoned(stagerJobFilePrefix+id) {
				_ = os.RemoveAll(path) // finished; the crash hit its removal
			}
		}
	}
	e.expireTombstones()
}

// handoff hands every remaining unit to the peer until ctx ends. Units are
// deleted locally only after the peer answered 2xx.
func (e *stagerEngine) handoff(ctx context.Context) {
	url := strings.TrimRight(e.opts.HandoffURL, "/") + StagerImportPath
	client := &http.Client{}
	start := time.Now()
	handed, failedUnits := 0, 0

	e.mu.Lock()
	jobs, err := e.loadJobsLocked()
	e.mu.Unlock()
	if err != nil {
		e.logger.Error().Err(err).Msg(e.spec.logName + ": handoff cannot list jobs")
		return
	}
	for _, j := range jobs {
		if ctx.Err() != nil {
			break
		}
		if len(j.pending()) == 0 {
			// Uploaded, only the local cleanup was missing: no peer needed.
			if err := e.finishJob(j); err != nil {
				e.logger.Error().Err(err).Str("job", j.ID).Msg(e.spec.logName + ": cleanup of a completed job failed")
			}
			continue
		}
		u := stagerUnit{Version: stagerUnitVersion, Kind: e.spec.kind, SourcePod: e.opts.PodID, Type: stagerUnitJob, Job: j, Files: j.Sources}
		files := make([]stagerUnitFile, 0, len(j.Sources))
		for _, s := range j.Sources {
			p := filepath.Join(j.dir, s)
			fi, err := os.Stat(p)
			if err != nil {
				e.logger.Error().Err(err).Str("job", j.ID).Msg(e.spec.logName + ": job source missing; cannot hand the job over")
				files = nil
				break
			}
			files = append(files, stagerUnitFile{name: s, path: p, mtime: fi.ModTime()})
		}
		if files == nil {
			failedUnits++
			continue
		}
		if err := e.sendUnit(ctx, client, url, u, files); err != nil {
			e.logger.Error().Err(err).Str("job", j.ID).Str("peer", e.opts.HandoffURL).
				Msg(e.spec.logName + ": handoff of a merge job failed; it stays on this pod")
			if errors.Is(err, ErrStagerImportInvalid) {
				failedUnits++
				continue
			}
			return
		}
		e.dropHandedOff(j)
		handed++
	}

	for ctx.Err() == nil {
		batch, err := e.handoffBatch()
		if err != nil || len(batch) == 0 {
			break
		}
		names := make([]string, len(batch))
		for i, f := range batch {
			names[i] = f.name
		}
		u := stagerUnit{Version: stagerUnitVersion, Kind: e.spec.kind, SourcePod: e.opts.PodID, Type: stagerUnitSources, Files: names}
		if err := e.sendUnit(ctx, client, url, u, batch); err != nil {
			e.logger.Error().Err(err).Int("files", len(batch)).Str("peer", e.opts.HandoffURL).
				Msg(e.spec.logName + ": handoff of staged files failed; they stay on this pod")
			break
		}
		for _, f := range batch {
			if err := os.Remove(f.path); err != nil && !os.IsNotExist(err) {
				e.logger.Warn().Err(err).Str("path", f.path).Msg(e.spec.logName + ": delete of a handed-off file failed")
			}
		}
		_ = syncDir(e.stageDir)
		handed++
	}
	e.logger.Info().Int("units_handed_off", handed).Int("units_refused", failedUnits).
		Dur("took", time.Since(start)).Str("peer", e.opts.HandoffURL).
		Msg(e.spec.logName + ": shutdown handoff finished")
}

// handoffBatch takes the oldest unjobbed sources, at most
// stagerHandoffMaxFiles files / stagerHandoffMaxBytes bytes.
func (e *stagerEngine) handoffBatch() ([]stagerUnitFile, error) {
	e.mu.Lock()
	groups, err := e.listStaged()
	e.mu.Unlock()
	if err != nil {
		return nil, err
	}
	var all []stagedFile
	for _, g := range groups {
		all = append(all, g.files...)
	}
	sort.Slice(all, func(i, j int) bool { return olderStaged(all[i], all[j]) })
	var batch []stagerUnitFile
	var size int64
	for _, f := range all {
		if len(batch) >= stagerHandoffMaxFiles || (len(batch) > 0 && size+f.size > stagerHandoffMaxBytes) {
			break
		}
		batch = append(batch, stagerUnitFile{name: filepath.Base(f.path), path: f.path, mtime: f.mtime})
		size += f.size
	}
	return batch, nil
}

// dropHandedOff deletes a job the peer acknowledged.
func (e *stagerEngine) dropHandedOff(j *stagerJob) {
	if j.imported {
		_ = os.RemoveAll(j.dir)
	} else {
		for _, s := range j.Sources {
			_ = os.Remove(filepath.Join(j.dir, s))
		}
		_ = syncDir(j.dir)
		_ = os.Remove(j.file)
	}
	_ = syncDir(e.stageDir)
	_ = os.RemoveAll(e.outDir(j.ID))
}

// sendUnit POSTs a unit, retrying a few times (the Service may route the
// first attempt to a pod that is going away as well).
func (e *stagerEngine) sendUnit(ctx context.Context, client *http.Client, url string, u stagerUnit, files []stagerUnitFile) error {
	var lastErr error
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < stagerHandoffAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("handoff: %w (last error: %v)", ctx.Err(), lastErr)
			case <-time.After(backoff):
			}
			backoff *= 2
		}
		status, err := e.postUnit(ctx, client, url, u, files)
		if err == nil && status/100 == 2 {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("peer answered HTTP %d", status)
			if status == http.StatusBadRequest {
				return fmt.Errorf("%w: %v", ErrStagerImportInvalid, err)
			}
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	return lastErr
}

func (e *stagerEngine) postUnit(ctx context.Context, client *http.Client, url string, u stagerUnit, files []stagerUnitFile) (int, error) {
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(writeStagerUnit(pw, u, files))
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, pr)
	if err != nil {
		pr.Close()
		return 0, err
	}
	req.Header.Set("Content-Type", "application/x-tar")
	req.Header.Set(StagerKindHeader, e.spec.kind)
	if e.opts.HandoffToken != "" {
		req.Header.Set("Authorization", "Bearer "+e.opts.HandoffToken)
	}
	resp, err := client.Do(req)
	pr.Close()
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	return resp.StatusCode, nil
}
