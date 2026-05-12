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
// at _arc/rollups/<name>/watermark.json on the storage backend.
//
// SpecFingerprint pins the shape of the spec that produced this watermark
// (KeepDimensions, Aggregations, BucketColumn, BucketInterval — see
// RollupSpec.Fingerprint). The scheduler compares the current spec's
// fingerprint against this stored value on startup and resets the
// watermark (forcing a rebuild) when they don't match — so a TOML change
// that alters spec shape (e.g. raising dim_cardinality_max, adding a
// keep_columns entry) doesn't leave the variant with a mix of old- and
// new-shape parquet for queries to read.
type Watermark struct {
	Rollup               string        `json:"rollup"`
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
	return w.Watermark.IsZero() && w.Rollup == ""
}

// WatermarkStore reads and writes per-rollup watermark files via a storage backend.
type WatermarkStore struct {
	backend storage.Backend
}

func NewWatermarkStore(backend storage.Backend) *WatermarkStore {
	return &WatermarkStore{backend: backend}
}

func watermarkKey(rollupName string) string {
	return fmt.Sprintf("_arc/rollups/%s/watermark.json", rollupName)
}

// Get returns the watermark for the rollup, or a zero Watermark if no file
// exists yet (first-ever build of this rollup, or post-disaster recovery).
func (s *WatermarkStore) Get(ctx context.Context, rollupName string) (Watermark, error) {
	key := watermarkKey(rollupName)
	exists, err := s.backend.Exists(ctx, key)
	if err == nil && !exists {
		return Watermark{}, nil
	}

	data, err := s.backend.Read(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return Watermark{}, nil
		}
		return Watermark{}, fmt.Errorf("get watermark for %q: %w", rollupName, err)
	}
	var w Watermark
	if err := json.Unmarshal(data, &w); err != nil {
		return Watermark{}, fmt.Errorf("decode watermark for %q: %w", rollupName, err)
	}
	return w, nil
}

// Put writes the watermark for a rollup, overwriting any existing file. Backend
// writes are atomic at the object level (S3 PUT is atomic; local file is a
// rename-after-write — see backend implementations).
func (s *WatermarkStore) Put(ctx context.Context, w Watermark) error {
	if w.Rollup == "" {
		return errors.New("watermark.Rollup is required")
	}
	data, err := json.MarshalIndent(w, "", "  ")
	if err != nil {
		return fmt.Errorf("encode watermark: %w", err)
	}
	if err := s.backend.Write(ctx, watermarkKey(w.Rollup), data); err != nil {
		return fmt.Errorf("put watermark for %q: %w", w.Rollup, err)
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
