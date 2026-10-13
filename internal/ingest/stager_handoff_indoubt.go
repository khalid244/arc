package ingest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// In-doubt handoff units.
//
// A unit is "in doubt" when it was sent to the peer completely and no answer
// came back: the shutdown budget ended while the answer was pending, the
// connection was reset, the process was killed mid-request. The peer may hold
// it, so it must not be deleted here; but if the container restarts on the
// same emptyDir, uploading it from here as well would store its rows twice.
//
// Before a unit is sent, a marker naming it is written to the stage dir
// (.handoff_<kind>_<id>.json: temp file, fsync, rename, directory fsync): its
// type, its files (a sources unit) or job id (a job unit), the peer URL and
// the source pod. The peer keys an imported source by that pod, the file name
// and a hash of the file's bytes, and a job by its id, so the marker plus the
// files on disk pin the identity the peer de-duplicates on. Once the outcome
// is known the marker goes:
//
//   - a 2xx: the marker is renamed to .handoff_<kind>_<id>.acked, the unit is
//     deleted, then the marker (a crash in between finishes the deletion on
//     the next start);
//   - an answer refusing the unit, or a request that never left completely
//     (the peer imports only a complete unit): the marker is removed and the
//     unit is ordinary local work again;
//   - no answer to a complete request: the marker stays, the unit is in doubt.
//
// An in-doubt unit is never merged into a new job, resumed or handed off as
// part of another unit. On the next start it is offered to the peer again
// before anything else touches it (at a shutdown, the handoff offers it
// first). The peer acknowledges a unit it holds or already uploaded without
// storing it again (identity check, .imported tombstones) and stores one it
// never got; either way its 2xx proves the rows are safe there and the unit is
// deleted here. A refusal of the re-offer (400/413), or a peer that cannot be
// reached for HandoffReofferWindow, sends the unit to the local upload: if the
// peer did hold it after all its rows are uploaded twice, which is logged and
// counted (arc_stager_handoff_in_doubt_fallbacks_total{kind}).
//
// The re-offer goes to the URL the unit was sent to. Behind a Service with
// more than two ingest pods it may reach a pod other than the one that stored
// the unit, which would store it again: in-doubt detection is exact with one
// peer (or a peer URL naming one pod).

const (
	stagerInDoubtPrefix  = ".handoff_"
	stagerInDoubtSuffix  = ".json"
	stagerAckedSuffix    = ".acked"
	stagerInDoubtVersion = 1

	defaultHandoffReofferWindow = 60 * time.Second
	reofferMinBackoff           = 200 * time.Millisecond
	reofferMaxBackoff           = 5 * time.Second
)

// stagerInDoubtUnit is the marker of one handoff unit.
type stagerInDoubtUnit struct {
	Version int    `json:"version"`
	Kind    string `json:"kind"`
	Type    string `json:"type"` // stagerUnitJob | stagerUnitSources
	// PeerURL is ingest.stager_handoff_url when the unit was sent: the re-offer
	// goes there even if the setting changed since.
	PeerURL string `json:"peer_url"`
	// SourcePod is the pod the peer stores the unit's sources under; the
	// re-offer sends it again so the peer derives the same identity.
	SourcePod string `json:"source_pod"`
	JobID     string `json:"job_id,omitempty"`
	// JobImported: the job lives in importjob_<id>/ (this pod imported it).
	JobImported bool `json:"job_imported,omitempty"`
	// Files are the unit's files: stage-dir names of a sources unit, the job's
	// sources of a job unit.
	Files           []string `json:"files"`
	CreatedUnixNano int64    `json:"created_unix_nano"`

	name string // marker file name without suffix: .handoff_<kind>_<id>
}

func (e *stagerEngine) inDoubtNamePrefix() string {
	return stagerInDoubtPrefix + e.spec.kind + "_"
}

func (e *stagerEngine) inDoubtPath(rec *stagerInDoubtUnit, suffix string) string {
	return filepath.Join(e.stageDir, rec.name+suffix)
}

func (rec *stagerInDoubtUnit) importURL(fallback string) string {
	base := rec.PeerURL
	if base == "" {
		base = fallback
	}
	return strings.TrimRight(base, "/") + StagerImportPath
}

// newInDoubtUnit describes a unit about to be sent to peerURL.
func (e *stagerEngine) newInDoubtUnit(peerURL string, u stagerUnit, job *stagerJob) *stagerInDoubtUnit {
	var b [8]byte
	_, _ = rand.Read(b[:])
	rec := &stagerInDoubtUnit{
		Version:         stagerInDoubtVersion,
		Kind:            e.spec.kind,
		Type:            u.Type,
		PeerURL:         peerURL,
		SourcePod:       u.SourcePod,
		Files:           append([]string(nil), u.Files...),
		CreatedUnixNano: time.Now().UnixNano(),
		name:            e.inDoubtNamePrefix() + hex.EncodeToString(b[:]),
	}
	if job != nil {
		rec.JobID = job.ID
		rec.JobImported = job.imported
	}
	return rec
}

// recordInDoubt writes the marker durably and indexes the unit, so nothing in
// this process touches it until its outcome is settled.
func (e *stagerEngine) recordInDoubt(rec *stagerInDoubtUnit) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	path := e.inDoubtPath(rec, stagerInDoubtSuffix)
	tmp := path + ".tmp"
	if err := writeFileSynced(tmp, data); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := syncDir(e.stageDir); err != nil {
		return err
	}
	e.mu.Lock()
	e.indexInDoubtLocked(rec)
	e.mu.Unlock()
	return nil
}

func (e *stagerEngine) indexInDoubtLocked(rec *stagerInDoubtUnit) {
	if e.inDoubt == nil {
		e.inDoubt = make(map[string]*stagerInDoubtUnit)
		e.inDoubtFiles = make(map[string]bool)
		e.inDoubtJobs = make(map[string]bool)
	}
	e.inDoubt[rec.name] = rec
	if rec.Type == stagerUnitJob {
		e.inDoubtJobs[rec.JobID] = true
		return
	}
	for _, f := range rec.Files {
		e.inDoubtFiles[f] = true
	}
}

func (e *stagerEngine) unindexInDoubtLocked(rec *stagerInDoubtUnit) {
	delete(e.inDoubt, rec.name)
	if rec.Type == stagerUnitJob {
		delete(e.inDoubtJobs, rec.JobID)
		return
	}
	for _, f := range rec.Files {
		delete(e.inDoubtFiles, f)
	}
}

// inDoubtJobLocked reports a job that is part of an in-doubt unit.
func (e *stagerEngine) inDoubtJobLocked(id string) bool { return e.inDoubtJobs[id] }

// inDoubtSnapshot lists the in-doubt units, oldest first.
func (e *stagerEngine) inDoubtSnapshot() []*stagerInDoubtUnit {
	e.mu.Lock()
	units := make([]*stagerInDoubtUnit, 0, len(e.inDoubt))
	for _, rec := range e.inDoubt {
		units = append(units, rec)
	}
	e.mu.Unlock()
	sort.Slice(units, func(i, j int) bool {
		if units[i].CreatedUnixNano != units[j].CreatedUnixNano {
			return units[i].CreatedUnixNano < units[j].CreatedUnixNano
		}
		return units[i].name < units[j].name
	})
	return units
}

// inDoubtCount is the number of units still in doubt.
func (e *stagerEngine) inDoubtCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.inDoubt)
}

// clearInDoubt removes the marker of a unit whose outcome is known and that
// stays here: it is ordinary local work again.
func (e *stagerEngine) clearInDoubt(rec *stagerInDoubtUnit) {
	if err := os.Remove(e.inDoubtPath(rec, stagerInDoubtSuffix)); err != nil && !os.IsNotExist(err) {
		// A marker left behind is re-offered on the next start, which the peer
		// answers for what it holds: harmless.
		e.logger.Warn().Err(err).Str("marker", rec.name).Msg(e.spec.logName + ": cannot remove a settled handoff marker")
	}
	_ = syncDir(e.stageDir)
	e.mu.Lock()
	e.unindexInDoubtLocked(rec)
	e.mu.Unlock()
}

// settleAcked deletes a unit the peer acknowledged: marker renamed to .acked,
// unit deleted, marker removed. The 2xx is the proof the peer holds or
// uploaded the unit, so the deletion goes ahead even if the rename fails.
func (e *stagerEngine) settleAcked(rec *stagerInDoubtUnit) {
	acked := e.inDoubtPath(rec, stagerAckedSuffix)
	if err := os.Rename(e.inDoubtPath(rec, stagerInDoubtSuffix), acked); err != nil {
		e.logger.Warn().Err(err).Str("marker", rec.name).Msg(e.spec.logName + ": cannot record the peer's acknowledgement; deleting the unit anyway")
	} else {
		_ = syncDir(e.stageDir)
	}
	e.dropUnitFiles(rec)
	_ = os.Remove(acked)
	_ = syncDir(e.stageDir)
	e.mu.Lock()
	e.unindexInDoubtLocked(rec)
	e.mu.Unlock()
}

// dropUnitFiles deletes a unit the peer holds. Each step tolerates an earlier
// attempt having done it (it also finishes a deletion a crash interrupted).
func (e *stagerEngine) dropUnitFiles(rec *stagerInDoubtUnit) {
	if rec.Type == stagerUnitJob {
		if rec.JobImported {
			_ = os.RemoveAll(e.importJobDir(rec.JobID))
		} else {
			// Sources first, then the job file: a job file never outlives
			// fewer than all of its sources unnoticed.
			for _, s := range rec.Files {
				_ = os.Remove(filepath.Join(e.stageDir, s))
			}
			_ = syncDir(e.stageDir)
			_ = os.Remove(e.jobFilePath(rec.JobID))
		}
		_ = syncDir(e.stageDir)
		_ = os.RemoveAll(e.outDir(rec.JobID))
		return
	}
	for _, f := range rec.Files {
		if err := os.Remove(filepath.Join(e.stageDir, f)); err != nil && !os.IsNotExist(err) {
			e.logger.Warn().Err(err).Str("file", f).Msg(e.spec.logName + ": delete of a handed-off file failed")
		}
	}
	_ = syncDir(e.stageDir)
}

// fallbackInDoubt gives up on the peer: the unit is uploaded from here.
func (e *stagerEngine) fallbackInDoubt(rec *stagerInDoubtUnit, why error) {
	metrics.Get().IncStagerHandoffInDoubtFallback(e.spec.kind)
	e.logger.Error().Err(why).Str("unit", rec.name).Str("type", rec.Type).Str("job", rec.JobID).
		Int("files", len(rec.Files)).Str("peer", rec.PeerURL).
		Msg(e.spec.logName + ": in-doubt handoff unit not confirmed by the peer; uploading it from this pod. " +
			"If the peer did store it at the last shutdown, its rows are uploaded twice")
	e.clearInDoubt(rec)
}

// loadInDoubt runs at construction, before any drain: it finishes deletions a
// crash interrupted (.acked markers), drops half-written markers (their unit
// was never sent: the marker is durable before the request starts), and
// indexes the units still in doubt.
func (e *stagerEngine) loadInDoubt() {
	entries, err := os.ReadDir(e.stageDir)
	if err != nil {
		return
	}
	prefix := e.inDoubtNamePrefix()
	for _, en := range entries {
		name := en.Name()
		if en.IsDir() || !strings.HasPrefix(name, prefix) {
			continue
		}
		path := filepath.Join(e.stageDir, name)
		switch {
		case strings.HasSuffix(name, stagerInDoubtSuffix+".tmp"):
			_ = os.Remove(path)
		case strings.HasSuffix(name, stagerAckedSuffix):
			if rec, err := e.readInDoubt(path, strings.TrimSuffix(name, stagerAckedSuffix)); err == nil {
				e.dropUnitFiles(rec)
			} else {
				e.logger.Error().Err(err).Str("marker", name).Msg(e.spec.logName + ": unreadable acknowledged handoff marker; its unit stays here")
			}
			_ = os.Remove(path)
		case strings.HasSuffix(name, stagerInDoubtSuffix):
			rec, err := e.readInDoubt(path, strings.TrimSuffix(name, stagerInDoubtSuffix))
			if err != nil {
				metrics.Get().IncStagerHandoffInDoubtFallback(e.spec.kind)
				e.logger.Error().Err(err).Str("marker", name).
					Msg(e.spec.logName + ": unreadable in-doubt handoff marker; quarantining it, its files are uploaded from this pod (rows the peer may hold are uploaded twice)")
				_ = quarantineSource(e.stageDir, path, "unreadable handoff marker", e.logger)
				continue
			}
			e.mu.Lock()
			e.indexInDoubtLocked(rec)
			e.mu.Unlock()
		}
	}
	_ = syncDir(e.stageDir)
	if n := e.inDoubtCount(); n > 0 {
		e.logger.Warn().Int("units", n).
			Msg(e.spec.logName + ": handoff units whose outcome was unknown at the last shutdown; offering them to the peer again before they are drained here")
	}
}

func (e *stagerEngine) readInDoubt(path, name string) (*stagerInDoubtUnit, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rec stagerInDoubtUnit
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, err
	}
	if rec.Version != stagerInDoubtVersion || rec.Kind != e.spec.kind ||
		(rec.Type != stagerUnitJob && rec.Type != stagerUnitSources) || (rec.Type == stagerUnitJob && !validStagerJobID(rec.JobID)) {
		return nil, fmt.Errorf("handoff marker %s: unsupported version %d, kind %q or type %q", filepath.Base(path), rec.Version, rec.Kind, rec.Type)
	}
	for _, f := range rec.Files {
		if f == "" || strings.ContainsAny(f, "/\\") || strings.Contains(f, "..") {
			return nil, fmt.Errorf("handoff marker %s: bad file name %q", filepath.Base(path), f)
		}
	}
	rec.name = name
	return &rec, nil
}

// inDoubtPayload rebuilds an in-doubt unit from disk exactly as it was sent:
// same source pod, same files (so the peer derives the same identities), or
// the same job. An empty file list means nothing of it is left here.
func (e *stagerEngine) inDoubtPayload(rec *stagerInDoubtUnit) (stagerUnit, []stagerUnitFile, error) {
	u := stagerUnit{Version: stagerUnitVersion, Kind: e.spec.kind, SourcePod: rec.SourcePod, Type: rec.Type}
	if rec.Type == stagerUnitSources {
		var files []stagerUnitFile
		for _, n := range rec.Files {
			p := filepath.Join(e.stageDir, n)
			fi, err := os.Stat(p)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return u, nil, err
			}
			u.Files = append(u.Files, n)
			files = append(files, stagerUnitFile{name: n, path: p, mtime: fi.ModTime()})
		}
		return u, files, nil
	}
	e.mu.Lock()
	jobs, err := e.loadJobsLocked()
	e.mu.Unlock()
	if err != nil {
		return u, nil, err
	}
	for _, j := range jobs {
		if j.ID != rec.JobID {
			continue
		}
		files := make([]stagerUnitFile, 0, len(j.Sources))
		for _, s := range j.Sources {
			p := filepath.Join(j.dir, s)
			fi, err := os.Stat(p)
			if err != nil {
				return u, nil, fmt.Errorf("in-doubt job %s: source %s: %w", j.ID, s, err)
			}
			files = append(files, stagerUnitFile{name: s, path: p, mtime: fi.ModTime()})
		}
		u.Job, u.Files = j, j.Sources
		return u, files, nil
	}
	return u, nil, nil
}

// offerOutcome is the result of offering an in-doubt unit to the peer again.
type offerOutcome int

const (
	offerAcked   offerOutcome = iota // 2xx: deleted here
	offerRetry                       // no answer, or one that may change (5xx, 409 from this pod, 401/404/429...)
	offerRefused                     // 400/413: the peer refuses the unit as it is
	offerBroken                      // the unit cannot be rebuilt from disk
	offerGone                        // nothing of it is left here
)

// offerInDoubt sends an in-doubt unit once and settles it on a 2xx.
func (e *stagerEngine) offerInDoubt(ctx context.Context, client *http.Client, rec *stagerInDoubtUnit) (offerOutcome, error) {
	u, files, err := e.inDoubtPayload(rec)
	if err != nil {
		return offerBroken, err
	}
	if len(files) == 0 {
		e.clearInDoubt(rec)
		return offerGone, nil
	}
	status, _, err := e.postUnit(ctx, client, rec.importURL(e.opts.HandoffURL), u, files)
	if err != nil {
		return offerRetry, err
	}
	if status/100 == 2 {
		e.settleAcked(rec)
		e.logger.Info().Str("unit", rec.name).Str("type", rec.Type).Str("job", rec.JobID).Int("files", len(files)).Str("peer", rec.PeerURL).
			Msg(e.spec.logName + ": the peer acknowledged an in-doubt handoff unit; deleted from this pod")
		return offerAcked, nil
	}
	serr := &handoffStatusError{status: status}
	if status == http.StatusBadRequest || status == http.StatusRequestEntityTooLarge {
		return offerRefused, serr
	}
	return offerRetry, serr
}

// reofferInDoubt runs once after a start with in-doubt units: each is offered
// to the peer until it answers, for at most HandoffReofferWindow without any
// unit acknowledged; then the rest is uploaded from here. Close stops it, and
// the shutdown handoff offers what is left.
func (e *stagerEngine) reofferInDoubt(ctx context.Context) {
	defer close(e.reofferDone)
	window := e.opts.HandoffReofferWindow
	client := &http.Client{}
	lastProgress := time.Now()
	fellBack := false
	defer func() {
		if fellBack {
			e.kick()
		}
	}()
	for _, rec := range e.inDoubtSnapshot() {
		backoff := reofferMinBackoff
		for {
			if ctx.Err() != nil {
				return
			}
			actx, cancel := context.WithTimeout(ctx, window)
			outcome, err := e.offerInDoubt(actx, client, rec)
			cancel()
			if outcome == offerRetry && ctx.Err() != nil {
				return // closing: the unit stays in doubt for the shutdown handoff
			}
			switch outcome {
			case offerAcked:
				lastProgress = time.Now()
			case offerRefused, offerBroken:
				e.fallbackInDoubt(rec, err)
				fellBack = true
			case offerRetry:
				elapsed := time.Since(lastProgress)
				if elapsed >= window {
					e.fallbackInDoubt(rec, fmt.Errorf("peer gave no acknowledgement for %v: %w", window, err))
					fellBack = true
					break
				}
				e.logger.Warn().Err(err).Str("unit", rec.name).Str("peer", rec.PeerURL).
					Msg(e.spec.logName + ": re-offer of an in-doubt handoff unit failed; retrying")
				select {
				case <-ctx.Done():
					return
				case <-time.After(min(backoff, window-elapsed)):
				}
				backoff = min(backoff*2, reofferMaxBackoff)
				continue
			}
			break
		}
	}
}

// handoffInDoubt offers the units still in doubt to the peer at a shutdown,
// before any other unit. It returns an error when the peer cannot be reached
// or the budget ended (the handoff stops; the units stay in doubt).
func (e *stagerEngine) handoffInDoubt(ctx context.Context, client *http.Client) (handed, refused int, err error) {
	for _, rec := range e.inDoubtSnapshot() {
		if ctx.Err() != nil {
			return handed, refused, ctx.Err()
		}
		outcome, err := e.offerInDoubt(ctx, client, rec)
		switch outcome {
		case offerAcked:
			handed++
		case offerRefused, offerBroken:
			e.fallbackInDoubt(rec, err)
			refused++
		case offerRetry:
			if !isUnitRefusal(err) {
				return handed, refused, err
			}
			refused++ // answered, but not acknowledged: it stays in doubt
		}
	}
	return handed, refused, nil
}
