package precalc

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// Spec is the classifier output for a single table. Persisted as spec.json
// alongside the precalc tier data on storage. Builder reads it to know what
// variants to produce; router reads it to know which dims/values it can target.
type Spec struct {
	Table          string             `json:"table"`
	TZ             string             `json:"tz"`
	TimeColumn     string             `json:"time_column"`
	Dims           map[string]DimSpec `json:"dims"`
	BuilderVersion string             `json:"builder_version"`
	CoverageThreshold float64         `json:"coverage_threshold"`
}

// DimSpec describes how one column is treated by the precalc system.
type DimSpec struct {
	Role       string   `json:"role"`        // "Dim", "PerDim", "Sketch", "Drop"
	KeptValues []string `json:"kept_values,omitempty"`
	// Effective cardinality after _OTHER_ collapse. Only set for Dim/PerDim.
	EffectiveCard int `json:"effective_card,omitempty"`
}

// SchemaHash returns a deterministic SHA-256 of the spec contents. Builder
// writes this into each parquet's KV-metadata; readers refuse files whose
// schema hash doesn't match the current spec.
//
// Hash is invariant under map iteration order and kept-value list order.
func (s *Spec) SchemaHash() (string, error) {
	canonical := struct {
		Table          string             `json:"t"`
		TZ             string             `json:"z"`
		TimeColumn     string             `json:"tc"`
		Dims           map[string]DimSpec `json:"d"`
		BuilderVersion string             `json:"v"`
		CoverageThreshold float64         `json:"ct"`
	}{
		Table: s.Table, TZ: s.TZ, TimeColumn: s.TimeColumn,
		BuilderVersion: s.BuilderVersion, CoverageThreshold: s.CoverageThreshold,
		Dims: make(map[string]DimSpec, len(s.Dims)),
	}
	for k, v := range s.Dims {
		sorted := append([]string(nil), v.KeptValues...)
		sort.Strings(sorted)
		canonical.Dims[k] = DimSpec{Role: v.Role, KeptValues: sorted, EffectiveCard: v.EffectiveCard}
	}
	b, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal canonical spec: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
