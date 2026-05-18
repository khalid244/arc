package tiered

import (
	"encoding/json"
	"fmt"
	"time"
)

// Manifest is the source of truth for which parquet files belong to which
// (tier, variant) for a given table. Builders append entries; readers fetch
// the manifest and open exactly those files (no S3 LIST).
type Manifest struct {
	Table      string          `json:"table"`
	Generation int64           `json:"generation"`
	Entries    []ManifestEntry `json:"entries"`
	Watermarks map[string]time.Time `json:"watermarks"` // key: "<tier>.<variant>"
}

// ManifestEntry is one parquet file's entry in the manifest.
type ManifestEntry struct {
	Tier           string    `json:"tier"`
	Variant        string    `json:"variant"`
	Path           string    `json:"path"`
	BucketLo       time.Time `json:"bucket_lo"`
	BucketHi       time.Time `json:"bucket_hi"`
	SchemaHash     string    `json:"schema_hash"`
	BuilderVersion string    `json:"builder_version"`
	Obsolete       bool      `json:"obsolete,omitempty"`
	WrittenAt      time.Time `json:"written_at"`
}

// FilesForTierVariant returns paths of non-obsolete entries matching the
// (tier, variant) pair.
func (m *Manifest) FilesForTierVariant(tier, variant string) []string {
	var out []string
	for _, e := range m.Entries {
		if e.Tier == tier && e.Variant == variant && !e.Obsolete {
			out = append(out, e.Path)
		}
	}
	return out
}

// EarliestBucketLo returns the smallest BucketLo across non-obsolete entries
// for (tier, variant). The second return is false when no entries exist.
// Used by the router to detect coverage gaps at the start of the query range.
func (m *Manifest) EarliestBucketLo(tier, variant string) (time.Time, bool) {
	var out time.Time
	found := false
	for _, e := range m.Entries {
		if e.Tier != tier || e.Variant != variant || e.Obsolete {
			continue
		}
		if !found || e.BucketLo.Before(out) {
			out = e.BucketLo
			found = true
		}
	}
	return out, found
}

// FilesForTierVariantWindow returns paths of non-obsolete entries for
// (tier, variant) that overlap the half-open window [lo, hi).
// An entry overlaps when entry.BucketLo < hi AND entry.BucketHi > lo.
func (m *Manifest) FilesForTierVariantWindow(tier, variant string, lo, hi time.Time) []string {
	var out []string
	for _, e := range m.Entries {
		if e.Tier != tier || e.Variant != variant || e.Obsolete {
			continue
		}
		if e.BucketLo.Before(hi) && e.BucketHi.After(lo) {
			out = append(out, e.Path)
		}
	}
	return out
}

// Add appends an entry and bumps generation.
func (m *Manifest) Add(e ManifestEntry) {
	e.WrittenAt = time.Now().UTC()
	m.Entries = append(m.Entries, e)
	m.Generation++
}

// Watermark returns the highest bucket fully built for (tier, variant), or
// zero time if none.
func (m *Manifest) Watermark(tier, variant string) time.Time {
	if m.Watermarks == nil {
		return time.Time{}
	}
	return m.Watermarks[tier+"."+variant]
}

// SetWatermark records the highest sealed bucket for (tier, variant).
func (m *Manifest) SetWatermark(tier, variant string, t time.Time) {
	if m.Watermarks == nil {
		m.Watermarks = make(map[string]time.Time)
	}
	m.Watermarks[tier+"."+variant] = t
	m.Generation++
}

// JSON serializes the manifest with stable field ordering.
func (m *Manifest) JSON() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// FromJSON parses a manifest from bytes.
func ManifestFromJSON(b []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("manifest unmarshal: %w", err)
	}
	return &m, nil
}
