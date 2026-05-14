package rollup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// persistedSpecsKey is the storage key for the cached inference output.
// One file per cluster — a small JSON blob that lets every pod restart
// skip schema inference and reuse the previous run's specs (so all of
// their fingerprints stay stable across restarts).
const persistedSpecsKey = "_arc/rollup/.specs/inferred.json"

// PersistedSpecs is the cache file written after a successful inference run.
// On subsequent restarts, if (a) the rollup config block hasn't changed and
// (b) every table's column-shape fingerprint still matches, we load the
// Specs verbatim and skip both DESCRIBE and COUNT(DISTINCT) entirely. That
// removes the restart-time non-determinism that made every deploy reset
// every spec's watermark.
type PersistedSpecs struct {
	Specs              []RollupSpec      `json:"specs"`
	SchemaFingerprints map[string]string `json:"schema_fingerprints"` // "db.table" -> sha
	ConfigFingerprint  string            `json:"config_fingerprint"`
	InferredAt         time.Time         `json:"inferred_at"`
	// Note: we deliberately do NOT store cardinality counts here. Tracking
	// them would defeat the purpose — they shift with live ingest and are
	// what caused the original drift. We only re-infer when the column SET
	// changes (add/remove/type change) or the operator changes the TOML.
}

// LoadPersistedSpecs returns the cached spec file, or nil (no error) if it
// doesn't exist yet — first-ever run, or after an explicit invalidation.
func LoadPersistedSpecs(ctx context.Context, backend storage.Backend) (*PersistedSpecs, error) {
	exists, err := backend.Exists(ctx, persistedSpecsKey)
	if err == nil && !exists {
		return nil, nil
	}
	data, err := backend.Read(ctx, persistedSpecsKey)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read persisted specs: %w", err)
	}
	var ps PersistedSpecs
	if err := json.Unmarshal(data, &ps); err != nil {
		return nil, fmt.Errorf("decode persisted specs: %w", err)
	}
	return &ps, nil
}

// SavePersistedSpecs writes (or overwrites) the cache. Atomic at the object
// level on S3/MinIO; safe under concurrent writes (last-writer-wins, but in
// practice only one rollup pod runs inference per cluster).
func SavePersistedSpecs(ctx context.Context, backend storage.Backend, ps *PersistedSpecs) error {
	if ps == nil {
		return fmt.Errorf("SavePersistedSpecs: nil")
	}
	data, err := json.MarshalIndent(ps, "", "  ")
	if err != nil {
		return fmt.Errorf("encode persisted specs: %w", err)
	}
	if err := backend.Write(ctx, persistedSpecsKey, data); err != nil {
		return fmt.Errorf("write persisted specs: %w", err)
	}
	return nil
}

// DeletePersistedSpecs invalidates the cache. The next pod start will then
// re-run inference. Operators call this when they want to re-pick up a new
// data shape (e.g. after a major schema migration that should reclassify
// dimensions).
func DeletePersistedSpecs(ctx context.Context, backend storage.Backend) error {
	if err := backend.Delete(ctx, persistedSpecsKey); err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete persisted specs: %w", err)
	}
	return nil
}

// Fingerprint returns a stable hash of the parts of Config that affect spec
// inference output. Changes here force re-inference so the operator's intent
// can take effect. Cardinality knobs and per-table hints are included; the
// Enabled bool isn't (toggling enabled doesn't change the inferred shape).
func (c Config) Fingerprint() string {
	h := sha256.New()
	fmt.Fprintf(h, "dim_max=%d\n", c.DimCardinalityMax)
	fmt.Fprintf(h, "sketch_max=%d\n", c.SketchCardinalityMax)
	keys := make([]string, 0, len(c.Tables))
	for k := range c.Tables {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		tc := c.Tables[k]
		fmt.Fprintf(h, "table=%s\n", strings.ToLower(k))
		fmt.Fprintf(h, "  sketch=%s\n", joinSorted(tc.SketchColumns))
		fmt.Fprintf(h, "  ignore=%s\n", joinSorted(tc.IgnoreColumns))
		fmt.Fprintf(h, "  quantile=%s\n", joinSorted(tc.QuantileColumns))
		fmt.Fprintf(h, "  keep=%s\n", joinSorted(tc.KeepColumns))
		fmt.Fprintf(h, "  time_column=%s\n", tc.TimeColumn)
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// SchemaFingerprint hashes a column list. Stable under reordering: we sort
// by column name first. Only (name, type) is included — cardinality changes
// with live ingest and is exactly the noise we want to ignore.
func SchemaFingerprint(cols []ColumnStats) string {
	if len(cols) == 0 {
		return ""
	}
	sorted := make([]ColumnStats, len(cols))
	copy(sorted, cols)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	h := sha256.New()
	for _, c := range sorted {
		fmt.Fprintf(h, "%s:%s\n", c.Name, c.Type)
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

func joinSorted(in []string) string {
	out := make([]string, len(in))
	copy(out, in)
	sort.Strings(out)
	return strings.Join(out, ",")
}
