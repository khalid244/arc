package compaction

import (
	"context"
	"encoding/json"
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

// ReorgManifestStatus mirrors the compactor's two-phase commit:
//
//	pending  → outputs may or may not be uploaded; recovery probes each.
//	uploaded → every output is on storage; recovery only completes the
//	           source delete + manifest delete.
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

// Read fetches and decodes one manifest by storage key.
func (m *ReorgManifestManager) Read(ctx context.Context, key string) (*ReorgManifest, error) {
	data, err := m.backend.Read(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("read reorg manifest %s: %w", key, err)
	}
	var manifest ReorgManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("unmarshal reorg manifest %s: %w", key, err)
	}
	return &manifest, nil
}

// List returns every reorg manifest key currently in storage.
func (m *ReorgManifestManager) List(ctx context.Context) ([]string, error) {
	objects, err := m.backend.List(ctx, ReorgManifestBasePath+"/")
	if err != nil {
		return []string{}, nil // treat "doesn't exist" as empty
	}
	out := make([]string, 0, len(objects))
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".json") {
			out = append(out, obj)
		}
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
// Returns the number of manifests successfully recovered.
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
		if err := m.recoverOne(ctx, key); err != nil {
			m.logger.Error().Err(err).Str("manifest", key).Msg("Reorg recovery: manifest failed; leaving in place")
			continue
		}
		recovered++
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
//  1. Status=uploaded — every output is on storage; just finish the source
//     delete and remove the manifest.
//  2. Status=pending and every planned output exists with the right size —
//     we crashed between the last successful upload and MarkUploaded; treat
//     as case 1 and complete the work.
//  3. Status=pending and one or more outputs are missing or partial — we
//     crashed mid-upload. Delete the partially-uploaded outputs, delete
//     the manifest. Source files remain in events_late/, so the next reorg
//     cycle re-processes them from scratch.
func (m *ReorgManifestManager) recoverOne(ctx context.Context, key string) error {
	manifest, err := m.Read(ctx, key)
	if err != nil {
		// Unreadable manifests are suspicious; nuke and move on. Source
		// files in events_late/ stay put either way.
		m.logger.Warn().Err(err).Str("manifest", key).Msg("Reorg recovery: unreadable manifest, deleting")
		return m.Delete(ctx, key)
	}

	age := time.Since(manifest.UpdatedAt)
	if age < ReorgManifestMinRecoveryAge {
		m.logger.Debug().Str("manifest", key).Dur("age", age).Msg("Reorg recovery: manifest too young, skipping")
		return nil
	}
	if age > ReorgManifestMaxAge {
		m.logger.Warn().
			Str("manifest", key).
			Dur("age", age).
			Time("updated_at", manifest.UpdatedAt).
			Msg("Reorg recovery: stale manifest (>7d) — investigate the root cause")
	}

	// Status=uploaded: outputs are confirmed live. Re-run source delete and
	// manifest delete. Both are idempotent so re-recovery is safe.
	if manifest.Status == ReorgStatusUploaded {
		return m.finishDelete(ctx, key, manifest)
	}

	// Status=pending: probe each planned output.
	allGood, partial, err := m.probeOutputs(ctx, manifest)
	if err != nil {
		return err
	}
	if allGood {
		m.logger.Info().Str("manifest", key).Msg("Reorg recovery: outputs intact, completing delete")
		if err := m.MarkUploaded(ctx, key); err != nil {
			return err
		}
		return m.finishDelete(ctx, key, manifest)
	}

	// Roll back: delete the partials, drop the manifest, leave the source
	// files for the next cycle to reprocess. Any output that turned out to
	// be a complete & correct-sized leftover from an earlier successful
	// attempt is preserved — we only delete genuine partials.
	m.logger.Warn().
		Str("manifest", key).
		Int("partials", len(partial)).
		Msg("Reorg recovery: partial upload detected, rolling back and re-queueing source files")
	for _, p := range partial {
		if err := m.backend.Delete(ctx, p); err != nil {
			m.logger.Warn().Err(err).Str("path", p).Msg("Reorg recovery: failed to delete partial output (leak; daily-dedup will fold)")
		}
	}
	metrics.Get().IncReorgManifestsRolledBack(1)
	return m.Delete(ctx, key)
}

// probeOutputs classifies each PlannedOutput as:
//   - present and correctly sized (treated as success)
//   - present but wrong size (partial — needs deletion)
//   - missing (needs nothing; the rest of the bucket also failed)
//
// Returns (allGood, partials, err) where allGood is true iff every output
// is present and correctly sized.
func (m *ReorgManifestManager) probeOutputs(ctx context.Context, manifest *ReorgManifest) (bool, []string, error) {
	objectLister, ok := m.backend.(storage.ObjectLister)
	if !ok {
		// Without size info we cannot distinguish "complete" from "partial",
		// so we have to fall back to existence-only and hope. Logged so the
		// operator knows recovery is degraded.
		m.logger.Warn().Msg("Reorg recovery: backend lacks ObjectLister, falling back to existence-only checks")
		allPresent := true
		for _, o := range manifest.PlannedOutputs {
			exists, err := m.backend.Exists(ctx, o.Path)
			if err != nil {
				return false, nil, err
			}
			if !exists {
				allPresent = false
				break
			}
		}
		return allPresent, nil, nil
	}

	allGood := true
	var partials []string
	for _, o := range manifest.PlannedOutputs {
		objects, err := objectLister.ListObjects(ctx, o.Path)
		if err != nil {
			return false, nil, err
		}
		if len(objects) == 0 {
			allGood = false
			continue
		}
		if objects[0].Size != o.Size {
			allGood = false
			partials = append(partials, o.Path)
		}
	}
	return allGood, partials, nil
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
