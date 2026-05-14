package rollup

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// ManifestBasePath is the prefix under which per-rollup manifest directories live.
const ManifestBasePath = "_arc/rollup"

// ManifestMaxAge is how old a manifest can be before recovery logs loudly.
const ManifestMaxAge = 7 * 24 * time.Hour

// WindowManifest records a pending window build for crash recovery.
// Written before COPY+upload; deleted after the watermark advances.
type WindowManifest struct {
	RollupName  string    `json:"rollup_name"`
	StoragePath string    `json:"storage_path"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	OutputKey   string    `json:"output_key"` // storage key for the parquet file
	CreatedAt   time.Time `json:"created_at"`
}

// manifestKey returns the storage key for a window manifest.
// Format: _arc/rollup/<storage_path>/_manifests/<start_iso>_<end_iso>.json
func manifestKey(storagePath string, windowStart, windowEnd time.Time) string {
	start := windowStart.UTC().Format("2006-01-02T15-04-05Z")
	end := windowEnd.UTC().Format("2006-01-02T15-04-05Z")
	return fmt.Sprintf("%s/%s/_manifests/%s_%s.json", ManifestBasePath, storagePath, start, end)
}

// manifestPrefix returns the listing prefix for all manifests of a rollup.
func manifestPrefix(storagePath string) string {
	return fmt.Sprintf("%s/%s/_manifests/", ManifestBasePath, storagePath)
}

// ManifestStore reads, writes, and deletes per-window manifests for one rollup.
type ManifestStore struct {
	backend storage.Backend
	logger  zerolog.Logger
}

// NewManifestStore creates a ManifestStore backed by the given storage backend.
func NewManifestStore(backend storage.Backend, logger zerolog.Logger) *ManifestStore {
	return &ManifestStore{
		backend: backend,
		logger:  logger.With().Str("component", "rollup-manifest").Logger(),
	}
}

// Write persists a WindowManifest before the build begins.
func (s *ManifestStore) Write(ctx context.Context, m WindowManifest) error {
	if m.StoragePath == "" {
		return fmt.Errorf("manifest.StoragePath is required")
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	key := manifestKey(m.StoragePath, m.WindowStart, m.WindowEnd)
	if err := s.backend.Write(ctx, key, data); err != nil {
		return fmt.Errorf("write manifest %s: %w", key, err)
	}
	s.logger.Debug().Str("key", key).Msg("wrote window manifest")
	return nil
}

// Delete removes the manifest after a successful build. Best-effort; not-found is OK.
func (s *ManifestStore) Delete(ctx context.Context, storagePath string, windowStart, windowEnd time.Time) error {
	key := manifestKey(storagePath, windowStart, windowEnd)
	if err := s.backend.Delete(ctx, key); err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete manifest %s: %w", key, err)
	}
	s.logger.Debug().Str("key", key).Msg("deleted window manifest")
	return nil
}

// List returns all manifest keys for the variant at storagePath.
func (s *ManifestStore) List(ctx context.Context, storagePath string) ([]string, error) {
	prefix := manifestPrefix(storagePath)
	keys, err := s.backend.List(ctx, prefix)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list manifests for %s: %w", storagePath, err)
	}
	var out []string
	for _, k := range keys {
		if strings.HasSuffix(k, ".json") {
			out = append(out, k)
		}
	}
	return out, nil
}

// Read parses a manifest from storage by its key.
func (s *ManifestStore) Read(ctx context.Context, key string) (WindowManifest, error) {
	data, err := s.backend.Read(ctx, key)
	if err != nil {
		return WindowManifest{}, fmt.Errorf("read manifest %s: %w", key, err)
	}
	var m WindowManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return WindowManifest{}, fmt.Errorf("decode manifest %s: %w", key, err)
	}
	return m, nil
}

// Recover scans all manifests at storagePath and resolves each orphan:
//
//   - Output parquet exists → advance watermark (if not already advanced), delete manifest.
//   - Output parquet missing → delete manifest (scheduler will retry the window naturally).
//   - Manifest older than ManifestMaxAge → log loudly, then proceed with one of the above.
//
// bucketInterval is the live spec's bucket interval, used when recovery has
// to write a brand-new watermark (no prior watermark file exists). Without
// it, the recovered watermark would persist BucketInterval=0 and downstream
// readers of the watermark file would see bad data on first-build crashes.
//
// Recover is idempotent: calling it twice produces the same outcome.
func Recover(ctx context.Context, storagePath string, bucketInterval time.Duration, store *ManifestStore, wmStore WMReadWriter, logger zerolog.Logger) error {
	keys, err := store.List(ctx, storagePath)
	if err != nil {
		return fmt.Errorf("recover list manifests: %w", err)
	}
	if len(keys) == 0 {
		return nil
	}

	logger.Info().Str("storage_path", storagePath).Int("count", len(keys)).Msg("found orphaned rollup manifests, recovering")

	for _, key := range keys {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := recoverManifest(ctx, key, bucketInterval, store, wmStore, logger); err != nil {
			logger.Error().Err(err).Str("manifest", key).Msg("failed to recover rollup manifest")
		}
	}
	return nil
}

func recoverManifest(ctx context.Context, key string, bucketInterval time.Duration, store *ManifestStore, wmStore WMReadWriter, logger zerolog.Logger) error {
	m, err := store.Read(ctx, key)
	if err != nil {
		// Unreadable manifest — delete and let the scheduler retry the window.
		logger.Warn().Err(err).Str("key", key).Msg("cannot read manifest, deleting")
		return store.backend.Delete(ctx, key)
	}

	age := time.Since(m.CreatedAt)
	if age > ManifestMaxAge {
		logger.Warn().
			Str("rollup", m.RollupName).
			Str("key", key).
			Dur("age", age).
			Time("created_at", m.CreatedAt).
			Msg("manifest is older than 7 days — investigate root cause")
	}

	logger.Info().
		Str("rollup", m.RollupName).
		Time("window_start", m.WindowStart).
		Time("window_end", m.WindowEnd).
		Str("output_key", m.OutputKey).
		Msg("processing orphaned rollup manifest")

	// Check whether the parquet was successfully written.
	exists, err := store.backend.Exists(ctx, m.OutputKey)
	if err != nil {
		return fmt.Errorf("check output existence: %w", err)
	}

	if !exists {
		// Build never completed — drop manifest, scheduler will rebuild.
		logger.Info().Str("rollup", m.RollupName).Str("output_key", m.OutputKey).
			Msg("output parquet missing, deleting manifest for retry")
		return store.backend.Delete(ctx, key)
	}

	// Build completed (parquet exists) but watermark or manifest were not updated.
	// Advance watermark if needed.
	wm, err := wmStore.Get(ctx, m.StoragePath)
	if err != nil {
		return fmt.Errorf("read watermark during recovery: %w", err)
	}

	if wm.Watermark.IsZero() || m.WindowEnd.After(wm.Watermark) {
		// Preserve the existing watermark's BucketInterval when present;
		// otherwise (first-build crash, no prior file) use the live spec's
		// bucketInterval so we don't persist a 0 in the recovered record.
		bi := wm.BucketInterval
		if bi == 0 {
			bi = bucketInterval
		}
		recovered := Watermark{
			Rollup:               m.RollupName,
			StoragePath:          m.StoragePath,
			BucketInterval:       bi,
			Watermark:            m.WindowEnd,
			LastBuildCompletedAt: time.Now().UTC(),
			LastBuildWindowStart: m.WindowStart,
			LastBuildWindowEnd:   m.WindowEnd,
		}
		if err := wmStore.Put(ctx, recovered); err != nil {
			return fmt.Errorf("advance watermark during recovery: %w", err)
		}
		logger.Info().Str("rollup", m.RollupName).Time("watermark", m.WindowEnd).Msg("advanced watermark during recovery")
	}

	// Manifest is now safe to remove.
	if err := store.backend.Delete(ctx, key); err != nil && !isNotFound(err) {
		return fmt.Errorf("delete manifest after recovery: %w", err)
	}
	logger.Info().Str("rollup", m.RollupName).Str("key", key).Msg("manifest recovered and deleted")
	return nil
}
