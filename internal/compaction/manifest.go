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

// ManifestStatus represents the state of a compaction manifest
type ManifestStatus string

const (
	// ManifestStatusPending indicates the compaction is in progress
	ManifestStatusPending ManifestStatus = "pending"
)

// ManifestBasePath is the base directory for storing compaction manifests.
const ManifestBasePath = "_arc/compaction"

// ManifestMaxAge is the maximum age for manifests before they're considered stale.
// Manifests older than this are deleted during recovery - they likely indicate
// a deeper problem that requires investigation.
const ManifestMaxAge = 7 * 24 * time.Hour // 7 days

// ManifestMinRecoveryAge is the minimum age a manifest must reach before
// recovery may delete it as orphaned. Younger manifests are assumed to
// belong to an in-flight upload on this or a peer node and are left alone.
const ManifestMinRecoveryAge = 30 * time.Minute

// Manifest tracks the state of a compaction operation for crash recovery.
// If a pod crashes after uploading the compacted file but before deleting
// source files, the manifest allows recovery to complete the deletion.
type Manifest struct {
	// Output file information
	OutputPath string `json:"output_path"` // Full storage path of compacted file
	OutputSize int64  `json:"output_size"` // Expected size of output file (for validation)

	// OutputUploaded is set to true once the compacted output has been
	// successfully written to storage. Query-time exclusion only hides
	// InputFiles when this is true, so concurrent queries see source rows
	// until the output is readable and only then switch to seeing the
	// compacted file. False during the upload window.
	OutputUploaded bool `json:"output_uploaded,omitempty"`

	// Input files that were compacted
	InputFiles []string `json:"input_files"`

	// Metadata
	Database      string         `json:"database"`
	Measurement   string         `json:"measurement"`
	PartitionPath string         `json:"partition_path"`
	Tier          string         `json:"tier"`
	Status        ManifestStatus `json:"status"`
	CreatedAt     time.Time      `json:"created_at"`
	JobID         string         `json:"job_id"`
}

// ManifestManager handles reading, writing, and recovering from compaction manifests
type ManifestManager struct {
	backend storage.Backend
	logger  zerolog.Logger
	mu      sync.Mutex

	// Cache of manifest paths to input files for quick lookup during candidate
	// filtering. Used by GetFilesInManifests (candidate scanner). Key:
	// manifest path, Value: set of input file paths.
	manifestCache     map[string]map[string]struct{}
	manifestCacheMu   sync.RWMutex
	manifestCacheTime time.Time
	cacheTTL          time.Duration

	// Cache of input files whose compacted output has been uploaded. Used
	// by GetInputFilesForUploadedOutputs (query-time exclusion filter).
	// Without this cache, every query on every query pod did 1 LIST + N
	// GETs against S3 — under load that runs the manifest prefix into
	// rate-limit territory the same way the compactor's per-file deletes
	// used to. Lifetime is bounded by cacheTTL AND by explicit invalidation
	// on every WriteManifest / MarkOutputUploaded / DeleteManifest, so the
	// OutputUploaded visibility transition is immediate within a single pod;
	// cross-pod visibility is bounded by cacheTTL.
	uploadedFilesCache     map[string]struct{}
	uploadedFilesCacheTime time.Time
}

// NewManifestManager creates a new manifest manager
func NewManifestManager(backend storage.Backend, logger zerolog.Logger) *ManifestManager {
	return &ManifestManager{
		backend:       backend,
		logger:        logger.With().Str("component", "manifest-manager").Logger(),
		manifestCache: make(map[string]map[string]struct{}),
		// 10s balances cross-pod visibility staleness vs query-path S3 load.
		// Single-pod transitions are immediate via explicit invalidateCache
		// in WriteManifest / MarkOutputUploaded / DeleteManifest.
		cacheTTL: 10 * time.Second,
	}
}

// GenerateManifestPath generates a unique manifest path for a compaction job
func (m *ManifestManager) GenerateManifestPath(tier, database, partitionPath, jobID string) string {
	// Path format: _compaction_state/{tier}/{database}/{partition_path_sanitized}_{jobID}.json
	sanitizedPartition := strings.ReplaceAll(partitionPath, "/", "_")
	return filepath.Join(ManifestBasePath, tier, database, fmt.Sprintf("%s_%s.json", sanitizedPartition, jobID))
}

// WriteManifest writes a manifest to storage
func (m *ManifestManager) WriteManifest(ctx context.Context, manifest *Manifest) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	manifestPath := m.GenerateManifestPath(manifest.Tier, manifest.Database, manifest.PartitionPath, manifest.JobID)

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal manifest: %w", err)
	}

	if err := m.backend.Write(ctx, manifestPath, data); err != nil {
		return "", fmt.Errorf("failed to write manifest to %s: %w", manifestPath, err)
	}

	m.logger.Debug().
		Str("path", manifestPath).
		Str("output", manifest.OutputPath).
		Int("input_count", len(manifest.InputFiles)).
		Msg("Wrote compaction manifest")

	// Invalidate cache
	m.invalidateCache()

	return manifestPath, nil
}

// MarkOutputUploaded re-writes the manifest with OutputUploaded=true.
// Called by the job after the compacted file has been successfully written
// to storage so the query-time exclusion filter can switch from "sources
// visible, output absent" to "sources hidden, output visible".
func (m *ManifestManager) MarkOutputUploaded(ctx context.Context, manifestPath string) error {
	manifest, err := m.ReadManifest(ctx, manifestPath)
	if err != nil {
		return fmt.Errorf("failed to read manifest for upload marking: %w", err)
	}
	if manifest.OutputUploaded {
		return nil
	}
	manifest.OutputUploaded = true

	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}
	if err := m.backend.Write(ctx, manifestPath, data); err != nil {
		return fmt.Errorf("failed to re-write manifest %s: %w", manifestPath, err)
	}

	m.invalidateCache()
	return nil
}

// DeleteManifest removes a manifest from storage
func (m *ManifestManager) DeleteManifest(ctx context.Context, manifestPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.backend.Delete(ctx, manifestPath); err != nil {
		return fmt.Errorf("failed to delete manifest %s: %w", manifestPath, err)
	}

	m.logger.Debug().Str("path", manifestPath).Msg("Deleted compaction manifest")

	// Invalidate cache
	m.invalidateCache()

	return nil
}

// ReadManifest reads a manifest from storage
func (m *ManifestManager) ReadManifest(ctx context.Context, manifestPath string) (*Manifest, error) {
	data, err := m.backend.Read(ctx, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest %s: %w", manifestPath, err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest %s: %w", manifestPath, err)
	}

	return &manifest, nil
}

// ListManifests lists all manifest files in storage
func (m *ManifestManager) ListManifests(ctx context.Context) ([]string, error) {
	objects, err := m.backend.List(ctx, ManifestBasePath+"/")
	if err != nil {
		// If the directory doesn't exist, return empty list
		return []string{}, nil
	}

	var manifests []string
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".json") {
			manifests = append(manifests, obj)
		}
	}

	return manifests, nil
}

// RecoverOrphanedManifests finds and processes orphaned manifests from interrupted compactions.
// Returns the number of manifests recovered and any error encountered.
func (m *ManifestManager) RecoverOrphanedManifests(ctx context.Context) (int, error) {
	manifests, err := m.ListManifests(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to list manifests: %w", err)
	}

	if len(manifests) == 0 {
		return 0, nil
	}

	m.logger.Info().Int("count", len(manifests)).Msg("Found orphaned manifests, starting recovery")

	var recovered int
	for _, manifestPath := range manifests {
		select {
		case <-ctx.Done():
			return recovered, ctx.Err()
		default:
		}

		if err := m.recoverManifest(ctx, manifestPath); err != nil {
			m.logger.Error().Err(err).Str("manifest", manifestPath).Msg("Failed to recover manifest")
			continue
		}
		recovered++
	}

	m.logger.Info().Int("recovered", recovered).Int("total", len(manifests)).Msg("Manifest recovery complete")

	// Track recovery metrics
	if recovered > 0 {
		metrics.Get().IncCompactionManifestsRecovered(int64(recovered))
	}

	return recovered, nil
}

// recoverManifest processes a single orphaned manifest
func (m *ManifestManager) recoverManifest(ctx context.Context, manifestPath string) error {
	manifest, err := m.ReadManifest(ctx, manifestPath)
	if err != nil {
		// If we can't read the manifest, delete it and let compaction retry
		m.logger.Warn().Err(err).Str("manifest", manifestPath).Msg("Cannot read manifest, deleting")
		return m.DeleteManifest(ctx, manifestPath)
	}

	// Check for stale manifests - older than ManifestMaxAge likely indicate a deeper problem
	manifestAge := time.Since(manifest.CreatedAt)
	isStale := manifestAge > ManifestMaxAge
	if isStale {
		m.logger.Warn().
			Str("manifest", manifestPath).
			Dur("age", manifestAge).
			Time("created_at", manifest.CreatedAt).
			Msg("Processing stale manifest (older than 7 days) - investigate root cause")
	}

	// Skip recently-written manifests so an in-flight upload on another
	// node (or a slow upload on this node after a leader flap) is not
	// mistaken for an orphan. The compaction job re-runs its own delete
	// path on success; only treat manifests as truly orphaned once they
	// have aged past the upload window.
	if manifestAge < ManifestMinRecoveryAge {
		m.logger.Debug().
			Str("manifest", manifestPath).
			Dur("age", manifestAge).
			Msg("Skipping manifest younger than recovery min-age, likely in-flight")
		return nil
	}

	m.logger.Info().
		Str("manifest", manifestPath).
		Str("output", manifest.OutputPath).
		Int("inputs", len(manifest.InputFiles)).
		Msg("Processing orphaned manifest")

	// Check if output file exists
	exists, err := m.backend.Exists(ctx, manifest.OutputPath)
	if err != nil {
		return fmt.Errorf("failed to check output file existence: %w", err)
	}

	if !exists {
		// Output file doesn't exist - compaction was interrupted before upload completed
		// Delete manifest and let compaction retry
		m.logger.Info().
			Str("manifest", manifestPath).
			Str("output", manifest.OutputPath).
			Msg("Output file missing, deleting manifest for retry")
		return m.DeleteManifest(ctx, manifestPath)
	}

	// Output file exists - require size validation before deleting sources.
	// Without a working size check we cannot distinguish a complete upload
	// from a truncated/partial one, so fail safe: leave the manifest in
	// place and let a later cycle retry once the backend recovers.
	objectLister, ok := m.backend.(storage.ObjectLister)
	if !ok {
		m.logger.Warn().
			Str("manifest", manifestPath).
			Msg("Backend does not implement ObjectLister, cannot validate output size; leaving manifest for later retry")
		return fmt.Errorf("cannot validate output size: backend lacks ObjectLister")
	}
	objects, err := objectLister.ListObjects(ctx, manifest.OutputPath)
	if err != nil {
		return fmt.Errorf("failed to list output for size validation: %w", err)
	}
	if len(objects) == 0 {
		// Exists returned true but ListObjects returned nothing — inconsistent
		// view, treat as not-yet-validated and retry later.
		return fmt.Errorf("output exists check disagrees with list, deferring recovery")
	}
	actualSize := objects[0].Size
	if actualSize != manifest.OutputSize {
		// Size mismatch - partial upload, delete output and manifest for retry
		m.logger.Warn().
			Str("manifest", manifestPath).
			Int64("expected_size", manifest.OutputSize).
			Int64("actual_size", actualSize).
			Msg("Output file size mismatch, deleting for retry")

		if err := m.backend.Delete(ctx, manifest.OutputPath); err != nil {
			m.logger.Warn().Err(err).Str("output", manifest.OutputPath).Msg("Failed to delete partial output")
		}
		return m.DeleteManifest(ctx, manifestPath)
	}

	// Output file exists and is valid - complete the deletion of input files
	m.logger.Info().
		Str("manifest", manifestPath).
		Int("inputs", len(manifest.InputFiles)).
		Msg("Output file valid, completing input file deletion")

	if batchDeleter, ok := m.backend.(storage.BatchDeleter); ok {
		if err := batchDeleter.DeleteBatch(ctx, manifest.InputFiles); err != nil {
			m.logger.Warn().Err(err).
				Int("total", len(manifest.InputFiles)).
				Msg("BatchDelete failed during recovery, keeping manifest for retry")
			return fmt.Errorf("batch delete failed: %w", err)
		}
	} else {
		var deleteErrors int
		for _, inputFile := range manifest.InputFiles {
			if err := m.backend.Delete(ctx, inputFile); err != nil {
				exists, checkErr := m.backend.Exists(ctx, inputFile)
				if checkErr == nil && !exists {
					continue
				}
				m.logger.Warn().Err(err).Str("file", inputFile).Msg("Failed to delete input file during recovery")
				deleteErrors++
			}
		}
		if deleteErrors > 0 {
			m.logger.Warn().
				Int("errors", deleteErrors).
				Int("total", len(manifest.InputFiles)).
				Msg("Some input files could not be deleted during recovery, keeping manifest for retry")
			return fmt.Errorf("failed to delete %d of %d input files", deleteErrors, len(manifest.InputFiles))
		}
	}

	// All input files deleted — safe to remove manifest
	return m.DeleteManifest(ctx, manifestPath)
}

// GetFilesInManifests returns a set of all input files currently tracked by manifests.
// This is used to exclude files from compaction candidate scans.
func (m *ManifestManager) GetFilesInManifests(ctx context.Context) (map[string]struct{}, error) {
	m.manifestCacheMu.RLock()
	if time.Since(m.manifestCacheTime) < m.cacheTTL && len(m.manifestCache) > 0 {
		// Return cached result
		result := make(map[string]struct{})
		for _, files := range m.manifestCache {
			for f := range files {
				result[f] = struct{}{}
			}
		}
		m.manifestCacheMu.RUnlock()
		return result, nil
	}
	m.manifestCacheMu.RUnlock()

	// Rebuild cache
	m.manifestCacheMu.Lock()
	defer m.manifestCacheMu.Unlock()

	// Double-check after acquiring write lock
	if time.Since(m.manifestCacheTime) < m.cacheTTL && len(m.manifestCache) > 0 {
		result := make(map[string]struct{})
		for _, files := range m.manifestCache {
			for f := range files {
				result[f] = struct{}{}
			}
		}
		return result, nil
	}

	manifests, err := m.ListManifests(ctx)
	if err != nil {
		return nil, err
	}

	newCache := make(map[string]map[string]struct{})
	result := make(map[string]struct{})

	for _, manifestPath := range manifests {
		manifest, err := m.ReadManifest(ctx, manifestPath)
		if err != nil {
			m.logger.Warn().Err(err).Str("manifest", manifestPath).Msg("Failed to read manifest for cache")
			continue
		}

		files := make(map[string]struct{})
		for _, f := range manifest.InputFiles {
			files[f] = struct{}{}
			result[f] = struct{}{}
		}
		// Also add output file to prevent re-compaction
		result[manifest.OutputPath] = struct{}{}
		newCache[manifestPath] = files
	}

	m.manifestCache = newCache
	m.manifestCacheTime = time.Now()

	return result, nil
}

// GetInputFilesForUploadedOutputs returns the set of input file paths from
// manifests whose OutputUploaded flag is true. Used by the query-time
// exclusion filter so source files are hidden only after their compacted
// output is readable. During the upload window (manifest exists, output
// not yet uploaded), the inputs remain visible to queries — preventing
// both duplicate-row and missing-row windows.
//
// Result is cached behind cacheTTL and invalidated explicitly on every
// WriteManifest / MarkOutputUploaded / DeleteManifest. Without this
// cache the function would do 1 LIST + N GETs per query call — a critical
// regression on hot query pods.
func (m *ManifestManager) GetInputFilesForUploadedOutputs(ctx context.Context) (map[string]struct{}, error) {
	m.manifestCacheMu.RLock()
	if time.Since(m.uploadedFilesCacheTime) < m.cacheTTL && m.uploadedFilesCache != nil {
		// Copy under the read lock so callers can mutate freely.
		out := make(map[string]struct{}, len(m.uploadedFilesCache))
		for k := range m.uploadedFilesCache {
			out[k] = struct{}{}
		}
		m.manifestCacheMu.RUnlock()
		return out, nil
	}
	m.manifestCacheMu.RUnlock()

	manifests, err := m.ListManifests(ctx)
	if err != nil {
		return nil, err
	}

	result := make(map[string]struct{})
	for _, manifestPath := range manifests {
		manifest, err := m.ReadManifest(ctx, manifestPath)
		if err != nil {
			m.logger.Warn().Err(err).Str("manifest", manifestPath).Msg("Failed to read manifest for query exclusion")
			continue
		}
		if !manifest.OutputUploaded {
			continue
		}
		for _, f := range manifest.InputFiles {
			result[f] = struct{}{}
		}
	}

	// Populate cache. We re-take the write lock here rather than holding it
	// across the (potentially slow) S3 round trips above; a concurrent
	// invalidate between the RUnlock and Lock is benign — we'd just
	// overwrite a freshly-cleared cache with identical-or-newer data, and
	// the next caller will refresh against TTL anyway.
	m.manifestCacheMu.Lock()
	m.uploadedFilesCache = make(map[string]struct{}, len(result))
	for k := range result {
		m.uploadedFilesCache[k] = struct{}{}
	}
	m.uploadedFilesCacheTime = time.Now()
	m.manifestCacheMu.Unlock()

	// Return a copy so the caller can mutate without racing the cache.
	out := make(map[string]struct{}, len(result))
	for k := range result {
		out[k] = struct{}{}
	}
	return out, nil
}

// invalidateCache clears both manifest caches: the candidate-scanner cache
// (manifestCache) and the query-time uploaded-files cache (uploadedFilesCache).
// Called from WriteManifest, MarkOutputUploaded, and DeleteManifest so any
// state change is immediately visible to the next query / scan on this pod.
func (m *ManifestManager) invalidateCache() {
	m.manifestCacheMu.Lock()
	defer m.manifestCacheMu.Unlock()
	m.manifestCache = make(map[string]map[string]struct{})
	m.manifestCacheTime = time.Time{}
	m.uploadedFilesCache = nil
	m.uploadedFilesCacheTime = time.Time{}
}

// IsFileInManifest checks if a file is tracked by any manifest
func (m *ManifestManager) IsFileInManifest(ctx context.Context, filePath string) (bool, error) {
	files, err := m.GetFilesInManifests(ctx)
	if err != nil {
		return false, err
	}
	_, exists := files[filePath]
	return exists, nil
}
