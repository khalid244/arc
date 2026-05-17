package tiered

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/basekick-labs/arc/internal/storage"
)

// SpecStore persists Spec objects to a storage backend under SpecPath(table).
// The on-disk form is pretty-printed JSON for human readability — specs change
// rarely (only on classifier re-runs) and are operator-inspected.
type SpecStore struct {
	backend storage.Backend
}

// NewSpecStore wraps a storage backend with spec read/write semantics.
func NewSpecStore(backend storage.Backend) *SpecStore {
	return &SpecStore{backend: backend}
}

// Put writes the spec to SpecPath(table). Overwrites any previous spec.
func (s *SpecStore) Put(ctx context.Context, table string, spec Spec) error {
	b, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}
	return s.backend.Write(ctx, SpecPath(table), b)
}

// Get reads the spec at SpecPath(table). Returns an error if missing or
// malformed.
func (s *SpecStore) Get(ctx context.Context, table string) (Spec, error) {
	b, err := s.backend.Read(ctx, SpecPath(table))
	if err != nil {
		return Spec{}, fmt.Errorf("get spec %s: %w", table, err)
	}
	var spec Spec
	if err := json.Unmarshal(b, &spec); err != nil {
		return Spec{}, fmt.Errorf("unmarshal spec %s: %w", table, err)
	}
	return spec, nil
}
