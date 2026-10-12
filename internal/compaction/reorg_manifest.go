package compaction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// ReorgManifestBasePath is the storage prefix where reorganizer manifests
// live. Kept under _arc/ alongside the compaction manifests so operators
// have one place to look for "what's in-flight or stuck right now".
const ReorgManifestBasePath = "_arc/reorg"

// ReorgManifestMaxAge mirrors ManifestMaxAge — anything older than this on
// disk almost certainly indicates a deeper problem (e.g. backend write
// repeatedly failing) and is logged loudly during recovery.
const ReorgManifestMaxAge = 7 * 24 * time.Hour

// ReorgManifestMinRecoveryAge bounds when recovery may declare a manifest
// orphaned. Younger manifests are assumed to belong to an in-flight
// reorganizer run and are left alone — the same defense the compactor uses
// to avoid stepping on its own toes during a slow upload.
const ReorgManifestMinRecoveryAge = 5 * time.Minute

// ReorgRollbackDeadlineMargin is how long past the writer's recorded run
// deadline recovery waits before it rolls an incomplete chunk back. Every
// storage call the writer makes runs under that deadline, so once it has
// passed the writer can no longer upload, mark the manifest uploaded or delete
// sources; the margin covers clock skew between pods and a request that was
// already on the wire when the deadline fired.
const ReorgRollbackDeadlineMargin = 5 * time.Minute

// ReorgUndatedRollbackMinAge is the rollback gate for a manifest that records
// no writer deadline (written by an older binary, or by a run whose context
// had none). It exceeds the longest run Arc starts: the cron run's
// reorg.cycle_timeout (30m by default, raised to 2-3h for catch-up) and the
// manual API trigger's 4h.
const ReorgUndatedRollbackMinAge = 6 * time.Hour

// ErrReorgManifestUnparseable marks a reorg manifest whose bytes were read but
// do not decode. Like ErrManifestUnparseable on the compaction side it is kept
// apart from a read failure on purpose: a read failure is transient and the
// manifest must be retained, while an undecodable body will never decode on a
// later pass and is parked instead (see parkUnparseable).
var ErrReorgManifestUnparseable = errors.New("reorg manifest cannot be parsed")

// ReorgManifestStatus mirrors the compactor's two-phase commit:
//
//	pending  → outputs may or may not be uploaded; recovery probes each.
//	uploaded → every output was uploaded; recovery re-verifies them, then
//	           completes the source delete + manifest delete.
type ReorgManifestStatus string

const (
	ReorgStatusPending  ReorgManifestStatus = "pending"
	ReorgStatusUploaded ReorgManifestStatus = "uploaded"
)

// PlannedReorgOutput is one file the reorganizer wrote to local scratch and
// intends to upload to <Path>. Size is the local file size, used by recovery
// to detect partial uploads (S3 PUT may return success on a truncated body
// when the connection drops mid-request).
type PlannedReorgOutput struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// ReorgManifest tracks one bucket's worth of reorg work for crash recovery.
// The bucket is identified by (Database, LateName, BucketHour); the JobID
// disambiguates concurrent attempts if recovery races a new run.
type ReorgManifest struct {
	JobID          string               `json:"job_id"`
	Database       string               `json:"database"`
	Measurement    string               `json:"measurement"`
	LateName       string               `json:"late_name"`
	BucketHour     time.Time            `json:"bucket_hour"`
	SourceFiles    []string             `json:"source_files"`
	PlannedOutputs []PlannedReorgOutput `json:"planned_outputs"`
	Status         ReorgManifestStatus  `json:"status"`
	CreatedAt      time.Time            `json:"created_at"`
	UpdatedAt      time.Time            `json:"updated_at"`
	// Deadline is the writer run's context deadline. No storage call of
	// that run can succeed after it, which is what lets recovery delete the
	// outputs of an incomplete chunk without racing a writer that is still
	// uploading (see rollbackAllowedAt). Zero for manifests written by older
	// binaries or by a run without a deadline.
	Deadline time.Time `json:"deadline,omitempty"`
}

// rollbackAllowedAt is the earliest time recovery may roll back an incomplete
// chunk, i.e. delete its outputs. Before it, the writer may still be live and
// about to mark the chunk uploaded and delete its sources; deleting outputs
// then would lose those rows.
func rollbackAllowedAt(manifest *ReorgManifest) time.Time {
	if !manifest.Deadline.IsZero() {
		return manifest.Deadline.Add(ReorgRollbackDeadlineMargin)
	}
	return manifest.UpdatedAt.Add(ReorgUndatedRollbackMinAge)
}

// ReorgManifestManager is the storage-backed manifest layer for the
// reorganizer. Same shape as ManifestManager in manifest.go; kept separate
// because the compaction manifest carries an OutputUploaded flag used by
// query-time filters that don't apply here (events_late isn't queried).
type ReorgManifestManager struct {
	backend storage.Backend
	logger  zerolog.Logger
	mu      sync.Mutex
}

// NewReorgManifestManager constructs a manifest manager bound to a storage
// backend. Holds no state beyond the backend reference; thread-safe.
func NewReorgManifestManager(backend storage.Backend, logger zerolog.Logger) *ReorgManifestManager {
	return &ReorgManifestManager{
		backend: backend,
		logger:  logger.With().Str("component", "reorg-manifest").Logger(),
	}
}

// path returns the canonical manifest object key for a given manifest.
// Format: _arc/reorg/<lateName>_<bucketYYYYMMDDHH>_<jobID>.json
// The structure mirrors compaction/ManifestManager.GenerateManifestPath.
func (m *ReorgManifestManager) path(manifest *ReorgManifest) string {
	hourStr := manifest.BucketHour.UTC().Format("2006010215")
	return filepath.Join(ReorgManifestBasePath,
		fmt.Sprintf("%s_%s_%s.json", manifest.LateName, hourStr, manifest.JobID))
}

// Write serializes a manifest to storage atomically (one Backend.Write call).
// Returns the storage key it was written to so the caller can pass that key
// back to Update / Delete without re-deriving it.
func (m *ReorgManifestManager) Write(ctx context.Context, manifest *ReorgManifest) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if manifest.CreatedAt.IsZero() {
		manifest.CreatedAt = time.Now().UTC()
	}
	manifest.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal reorg manifest: %w", err)
	}
	key := m.path(manifest)
	if err := m.backend.Write(ctx, key, data); err != nil {
		return "", fmt.Errorf("write reorg manifest %s: %w", key, err)
	}
	return key, nil
}

// MarkUploaded flips Status to "uploaded" and re-writes the manifest.
// Called after every PlannedOutput has been successfully uploaded — past
// this point recovery only has to retry the source-delete step.
func (m *ReorgManifestManager) MarkUploaded(ctx context.Context, key string) error {
	manifest, err := m.Read(ctx, key)
	if err != nil {
		return err
	}
	if manifest.Status == ReorgStatusUploaded {
		return nil
	}
	manifest.Status = ReorgStatusUploaded
	manifest.UpdatedAt = time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal reorg manifest: %w", err)
	}
	if err := m.backend.Write(ctx, key, data); err != nil {
		return fmt.Errorf("rewrite reorg manifest %s: %w", key, err)
	}
	return nil
}

// Delete removes a manifest object. Idempotent at the call site — callers
// invoke this after the source-delete step succeeds, completing the cycle.
func (m *ReorgManifestManager) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.backend.Delete(ctx, key)
}

// Read fetches and decodes one manifest by storage key. A body that was read
// but does not decode wraps ErrReorgManifestUnparseable; a classified
// not-found satisfies objectGone; anything else is a read failure the caller
// must treat as transient.
func (m *ReorgManifestManager) Read(ctx context.Context, key string) (*ReorgManifest, error) {
	data, err := m.backend.Read(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("read reorg manifest %s: %w", key, err)
	}
	var manifest ReorgManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("unmarshal reorg manifest %s: %w: %w", key, ErrReorgManifestUnparseable, err)
	}
	return &manifest, nil
}

// List returns every reorg manifest key currently in storage.
//
// A listing failure is returned, never reported as an empty list. "No
// manifests" tells the drain that nothing is in flight; "the listing failed"
// means it cannot know, and proceeding would re-drain the sources of a chunk
// whose outputs are already uploaded (duplicates). A missing prefix is not an
// error here: the backends report it as an empty listing.
func (m *ReorgManifestManager) List(ctx context.Context) ([]string, error) {
	objects, err := m.backend.List(ctx, ReorgManifestBasePath+"/")
	if err != nil {
		return nil, fmt.Errorf("list reorg manifests: %w", err)
	}
	out := make([]string, 0, len(objects))
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".json") {
			out = append(out, obj)
		}
	}
	return out, nil
}

// SourceFilesInManifests returns the union of SourceFiles across every reorg
// manifest currently in storage. The reorganizer consults it before bucketing
// the sidecar so it never re-reads a source file that an in-flight or
// recovering manifest already owns — the same guard the compactor gets from
// ManifestManager.GetFilesInManifests via filterCandidateFiles. Recovery is
// age-gated (ReorgManifestMinRecoveryAge); without this filter a manifest
// whose sources are uploaded-but-not-yet-deleted could be skipped by recovery
// AND re-read in the same cycle, producing a duplicate set of target files.
//
// It fails CLOSED: a manifest that cannot be read (transient error) or decoded
// (not parked yet) may own sources the drain is about to re-read, so the whole
// lookup errors and the caller skips the cycle. Only a classified not-found is
// skipped: that manifest is gone (its run finished between list and read) or
// is a Ceph phantom listing entry, and holds nothing back.
func (m *ReorgManifestManager) SourceFilesInManifests(ctx context.Context) (map[string]struct{}, error) {
	manifests, err := m.readAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("reorg source-file filter: %w", err)
	}
	out := make(map[string]struct{})
	for _, manifest := range manifests {
		for _, f := range manifest.SourceFiles {
			out[f] = struct{}{}
		}
	}
	return out, nil
}

// PlannedOutputsInManifests returns the union of PlannedOutputs across every
// reorg manifest in storage, pending or uploaded. The compaction candidate
// filter excludes them: while a manifest exists its chunk can still be rolled
// back (outputs deleted, sources re-drained), so compacting one of its
// outputs into a daily file would leave those rows twice. Fails closed like
// SourceFilesInManifests.
func (m *ReorgManifestManager) PlannedOutputsInManifests(ctx context.Context) (map[string]struct{}, error) {
	manifests, err := m.readAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("reorg planned-output filter: %w", err)
	}
	out := make(map[string]struct{})
	for _, manifest := range manifests {
		for _, o := range manifest.PlannedOutputs {
			out[o.Path] = struct{}{}
		}
	}
	return out, nil
}

// readAll lists and decodes every manifest, failing closed: an unreadable or
// undecodable manifest is an error, and only a classified not-found (gone
// between list and read, or a phantom listing entry) is skipped.
func (m *ReorgManifestManager) readAll(ctx context.Context) ([]*ReorgManifest, error) {
	keys, err := m.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*ReorgManifest, 0, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		manifest, err := m.Read(ctx, key)
		if err != nil {
			if objectGone(err) {
				continue
			}
			return nil, err
		}
		out = append(out, manifest)
	}
	return out, nil
}

// RecoverOrphanedReorgManifests is the startup / pre-cycle hook that drives
// recovery for every orphaned manifest. Strategy mirrors the compactor:
//
//   - Skip manifests younger than ReorgManifestMinRecoveryAge so an in-flight
//     run on another goroutine / pod is not mistaken for an orphan.
//   - Loudly log manifests older than ReorgManifestMaxAge but still process
//     them — the operator needs to know they exist.
//
// Returns the number of manifests successfully recovered. The error is
// non-nil only when the pass itself could not run (the listing failed, or ctx
// ended): the caller must then skip its cycle. A single manifest that cannot
// be recovered is logged and left in place for the next pass; the drain's
// source filter keeps its sources out of the cycle meanwhile.
func (m *ReorgManifestManager) RecoverOrphanedReorgManifests(ctx context.Context) (int, error) {
	manifests, err := m.List(ctx)
	if err != nil {
		return 0, err
	}
	if len(manifests) == 0 {
		return 0, nil
	}
	m.logger.Info().Int("count", len(manifests)).Msg("Reorg recovery: scanning manifests")

	var recovered int
	for _, key := range manifests {
		select {
		case <-ctx.Done():
			return recovered, ctx.Err()
		default:
		}
		done, err := m.recoverOne(ctx, key)
		if err != nil {
			if ctx.Err() != nil {
				return recovered, ctx.Err()
			}
			m.logger.Error().Err(err).Str("manifest", key).Msg("Reorg recovery: manifest failed; leaving in place")
			continue
		}
		if done {
			recovered++
		}
	}
	if recovered > 0 {
		metrics.Get().IncReorgManifestsRecovered(int64(recovered))
	}
	m.logger.Info().Int("recovered", recovered).Int("total", len(manifests)).Msg("Reorg recovery: complete")
	return recovered, nil
}

// recoverOne handles a single manifest. The two-phase commit means three
// possible states the manifest can be in when recovery sees it:
//
//  1. Status=uploaded and every planned output is live with the right size —
//     finish the source delete and remove the manifest.
//  2. Status=pending and every planned output is live with the right size —
//     we crashed between the last successful upload and MarkUploaded; treat
//     as case 1 and complete the work.
//  3. Any other state — the chunk is incomplete: some output is missing or
//     wrong-sized, so some rows exist ONLY in the sources. Roll it back
//     (rollback): delete EVERY output of this attempt, verify they are gone,
//     then delete the manifest. The sources stay in <m>_late/ and the next
//     drain re-processes them under a new jobID, so each row ends up in
//     exactly one live output. The sources are never deleted on this path.
//
// A manifest is only ever removed from the work set on proof: a classified
// not-found (nothing to do), a body that was read but does not decode (parked
// and counted, never deleted), or a completed recovery. Any other read error
// is transient and keeps it. done reports whether recovery work completed.
func (m *ReorgManifestManager) recoverOne(ctx context.Context, key string) (done bool, err error) {
	manifest, err := m.Read(ctx, key)
	if err != nil {
		switch {
		case objectGone(err):
			// Gone between the listing and the read (its run or another
			// recovery completed it), or a Ceph phantom listing entry.
			m.logger.Debug().Err(err).Str("manifest", key).Msg("Reorg recovery: listed manifest no longer exists; skipping")
			return false, nil
		case errors.Is(err, ErrReorgManifestUnparseable):
			return false, m.parkUnparseable(ctx, key, err)
		default:
			return false, fmt.Errorf("read (transient, manifest kept): %w", err)
		}
	}

	age := time.Since(manifest.UpdatedAt)
	if age < ReorgManifestMinRecoveryAge {
		m.logger.Debug().Str("manifest", key).Dur("age", age).Msg("Reorg recovery: manifest too young, skipping")
		return false, nil
	}
	if age > ReorgManifestMaxAge {
		m.logger.Warn().
			Str("manifest", key).
			Dur("age", age).
			Time("updated_at", manifest.UpdatedAt).
			Msg("Reorg recovery: stale manifest (>7d) — investigate the root cause")
	}

	// Both statuses re-verify the outputs before any source is deleted:
	// the sources are the only other copy of these rows.
	allGood, err := m.probeOutputs(ctx, manifest)
	if err != nil {
		return false, err
	}
	if allGood {
		if manifest.Status != ReorgStatusUploaded {
			m.logger.Info().Str("manifest", key).Msg("Reorg recovery: outputs intact, completing delete")
			if err := m.MarkUploaded(ctx, key); err != nil {
				return false, err
			}
		}
		// Source delete and manifest delete are idempotent, so a repeated
		// recovery of the same manifest is safe.
		return true, m.finishDelete(ctx, key, manifest)
	}
	if manifest.Status == ReorgStatusUploaded {
		m.logger.Error().
			Str("manifest", key).
			Msg("Reorg recovery: manifest is marked uploaded but not every planned output is live with its recorded size; NOT deleting its sources (their rows exist nowhere else) and rolling the chunk back instead")
	}
	return m.rollback(ctx, key, manifest)
}

// rollback undoes an incomplete chunk without ever deleting a source:
//
//  1. Wait until rollbackAllowedAt: before it the writer may still be live,
//     and deleting outputs it is about to commit to would lose rows.
//  2. Delete EVERY planned output. They are all jobID-bound, so none of them
//     can belong to another attempt; a complete, correctly-sized output must
//     go too, otherwise the re-drain writes its rows a second time.
//  3. Verify each one is gone with a HEAD. A classified not-found (which is
//     also what a Ceph phantom listing entry answers) counts as gone; a
//     failed or positive check keeps the manifest for the next pass.
//  4. Only then delete the manifest, which releases the sources to the
//     drain.
//
// A failure at any step keeps the manifest, so the sources stay held back
// and nothing is re-drained next to a live output.
func (m *ReorgManifestManager) rollback(ctx context.Context, key string, manifest *ReorgManifest) (bool, error) {
	if allowedAt := rollbackAllowedAt(manifest); time.Now().Before(allowedAt) {
		m.logger.Info().
			Str("manifest", key).
			Time("rollback_after", allowedAt).
			Msg("Reorg recovery: chunk is incomplete; waiting until its writer can no longer be uploading before rolling it back (its sources stay held back meanwhile)")
		return false, nil
	}
	m.logger.Warn().
		Str("manifest", key).
		Str("job_id", manifest.JobID).
		Int("planned_outputs", len(manifest.PlannedOutputs)).
		Int("sources", len(manifest.SourceFiles)).
		Msg("Reorg recovery: incomplete chunk, deleting every output of this attempt and re-queueing its sources")
	for _, o := range manifest.PlannedOutputs {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if err := m.backend.Delete(ctx, o.Path); err != nil && !objectGone(err) {
			return false, fmt.Errorf("rollback %s: delete output %s: %w", key, o.Path, err)
		}
	}
	for _, o := range manifest.PlannedOutputs {
		exists, err := m.backend.Exists(ctx, o.Path)
		if err != nil {
			return false, fmt.Errorf("rollback %s: verify output %s is gone: %w", key, o.Path, err)
		}
		if exists {
			return false, fmt.Errorf("rollback %s: output %s still exists after its delete; keeping the manifest so its sources are not re-drained next to it", key, o.Path)
		}
	}
	if err := m.Delete(ctx, key); err != nil {
		return false, fmt.Errorf("rollback %s: delete manifest: %w", key, err)
	}
	metrics.Get().IncReorgManifestsRolledBack(1)
	return true, nil
}

// parkUnparseable moves a manifest whose body does not decode out of the
// ".json" work set, keeping its raw bytes under the quarantine suffix so an
// operator can still see which bucket and job it named (the key encodes the
// late measurement, bucket hour and jobID). It is parked rather than deleted:
// the manifest's sources and outputs are unknowable, and the parked copy plus
// the counter are the only record that the sidecar's next drain may duplicate
// rows of that chunk. Write first, delete second: a crash between the two
// leaves both and the next pass re-parks idempotently. Counted only after
// both landed, so a park that fails transiently (and is retried) counts
// nothing.
func (m *ReorgManifestManager) parkUnparseable(ctx context.Context, key string, cause error) error {
	parked, err := quarantinePathFor(key)
	if err != nil {
		return fmt.Errorf("park reorg manifest %s: %w", key, err)
	}
	raw, err := m.backend.Read(ctx, key)
	if err != nil {
		return fmt.Errorf("park reorg manifest %s: read: %w", key, err)
	}
	if err := m.backend.Write(ctx, parked, raw); err != nil {
		return fmt.Errorf("park reorg manifest %s: write %s: %w", key, parked, err)
	}
	if err := m.Delete(ctx, key); err != nil {
		return fmt.Errorf("park reorg manifest %s: remove original after parking: %w", key, err)
	}
	metrics.Get().IncReorgManifestParkedUnparseable()
	m.logger.Error().
		Err(cause).
		Str("manifest", key).
		Str("parked_to", parked).
		Msg("Reorg manifest cannot be parsed; parked so it no longer blocks the drain. Its sources are no longer held back: if its chunk had uploaded outputs, that late measurement's target days may now hold duplicate rows")
	return nil
}

// probeOutputs reports whether every PlannedOutput is live with its recorded
// size. With an ObjectLister the size is checked against the exact key (a
// prefix listing can also return a longer sibling key); without one recovery
// can only check existence, which is logged as degraded.
func (m *ReorgManifestManager) probeOutputs(ctx context.Context, manifest *ReorgManifest) (bool, error) {
	objectLister, ok := m.backend.(storage.ObjectLister)
	if !ok {
		m.logger.Warn().Msg("Reorg recovery: backend lacks ObjectLister, falling back to existence-only checks")
		for _, o := range manifest.PlannedOutputs {
			exists, err := m.backend.Exists(ctx, o.Path)
			if err != nil {
				return false, err
			}
			if !exists {
				return false, nil
			}
		}
		return true, nil
	}

	for _, o := range manifest.PlannedOutputs {
		objects, err := objectLister.ListObjects(ctx, o.Path)
		if err != nil {
			return false, err
		}
		size, found := outputObjectSize(objects, o.Path)
		if !found || size != o.Size {
			return false, nil
		}
	}
	return true, nil
}

// finishDelete is the tail of a successful recovery: delete the source
// files (if any are still present) and then the manifest itself. Idempotent
// — running it twice is harmless. Returns nil on success; an error from
// here leaves the manifest in place for the next cycle to retry.
func (m *ReorgManifestManager) finishDelete(ctx context.Context, key string, manifest *ReorgManifest) error {
	if len(manifest.SourceFiles) > 0 {
		if err := m.backend.DeleteBatch(ctx, manifest.SourceFiles); err != nil {
			return fmt.Errorf("recover delete sources: %w", err)
		}
	}
	return m.Delete(ctx, key)
}
