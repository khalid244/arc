package rollup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Watermark records how fresh a rollup is. Stored as one JSON object per rollup
// at _arc/rollup/<storage_path>/watermark.json on the storage backend.
//
// Rollup is the human-readable variant name (e.g. "default__events__1d") used
// only in logs and JSON output. StoragePath is the slash-separated path under
// _arc/rollup/ that anchors the watermark and parquet files for this variant
// (e.g. "default/events/all/1d"); Put uses it as the storage key.
type Watermark struct {
	Rollup               string        `json:"rollup"`
	StoragePath          string        `json:"storage_path"`
	BucketInterval       time.Duration `json:"bucket_interval_ns"`
	Watermark            time.Time     `json:"watermark"`
	LastBuildCompletedAt time.Time     `json:"last_build_completed_at"`
	LastBuildWindowStart time.Time     `json:"last_build_window_start"`
	LastBuildWindowEnd   time.Time     `json:"last_build_window_end"`
	SpecFingerprint      string        `json:"spec_fingerprint,omitempty"`
}

// IsZero reports whether the watermark is the zero value (no successful builds
// yet OR no watermark file exists).
func (w Watermark) IsZero() bool {
	return w.Watermark.IsZero() && w.Rollup == "" && w.StoragePath == ""
}

// WatermarkStore reads and writes per-rollup watermark files via a storage backend.
type WatermarkStore struct {
	backend storage.Backend
}

func NewWatermarkStore(backend storage.Backend) *WatermarkStore {
	return &WatermarkStore{backend: backend}
}

// watermarkKey takes a variant's storage path (e.g. "default/events/all/1d")
// and returns its watermark.json key under _arc/rollup/.
func watermarkKey(storagePath string) string {
	return fmt.Sprintf("_arc/rollup/%s/watermark.json", storagePath)
}

// Get returns the watermark stored at _arc/rollup/<storagePath>/watermark.json,
// or a zero Watermark if no file exists yet (first-ever build of this variant,
// or post-disaster recovery).
func (s *WatermarkStore) Get(ctx context.Context, storagePath string) (Watermark, error) {
	key := watermarkKey(storagePath)
	exists, err := s.backend.Exists(ctx, key)
	if err == nil && !exists {
		return Watermark{}, nil
	}

	data, err := s.backend.Read(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return Watermark{}, nil
		}
		return Watermark{}, fmt.Errorf("get watermark for %q: %w", storagePath, err)
	}
	var w Watermark
	if err := json.Unmarshal(data, &w); err != nil {
		return Watermark{}, fmt.Errorf("decode watermark for %q: %w", storagePath, err)
	}
	return w, nil
}

// Put writes the watermark for a rollup, overwriting any existing file. Backend
// writes are atomic at the object level (S3 PUT is atomic; local file is a
// rename-after-write — see backend implementations).
func (s *WatermarkStore) Put(ctx context.Context, w Watermark) error {
	if w.StoragePath == "" {
		return errors.New("watermark.StoragePath is required")
	}
	data, err := json.MarshalIndent(w, "", "  ")
	if err != nil {
		return fmt.Errorf("encode watermark: %w", err)
	}
	if err := s.backend.Write(ctx, watermarkKey(w.StoragePath), data); err != nil {
		return fmt.Errorf("put watermark for %q: %w", w.StoragePath, err)
	}
	return nil
}

// isNotFound is liberal: different backends signal missing keys differently,
// and we'd rather treat ambiguous errors as "missing" (which causes a
// recompute) than as "real error" (which blocks builds).
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "file not found") ||
		strings.Contains(msg, "no such file") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "nosuchkey") ||
		strings.Contains(msg, "404")
}
