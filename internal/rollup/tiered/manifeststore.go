package tiered

import (
	"context"
	"errors"
	"fmt"

	"github.com/basekick-labs/arc/internal/storage"
)

// ErrManifestStale is returned by ManifestStore.Put when the on-disk
// manifest's Generation is at least as high as the proposed Put's
// Generation. Caller should re-read the manifest, re-apply their change,
// and retry.
var ErrManifestStale = errors.New("manifest generation is stale")

// ManifestStore persists Manifest objects with optimistic-concurrency
// semantics: Put rejects writes whose Generation isn't strictly greater
// than the on-disk Generation. This prevents two builders racing to
// publish from clobbering each other.
type ManifestStore struct {
	backend storage.Backend
}

func NewManifestStore(backend storage.Backend) *ManifestStore {
	return &ManifestStore{backend: backend}
}

// Get reads the manifest at ManifestPath(table). Returns an error if
// missing — callers wanting "either fetch or start fresh" semantics
// should check the error and create an empty Manifest on miss.
func (s *ManifestStore) Get(ctx context.Context, table string) (*Manifest, error) {
	b, err := s.backend.Read(ctx, ManifestPath(table))
	if err != nil {
		return nil, fmt.Errorf("get manifest %s: %w", table, err)
	}
	return ManifestFromJSON(b)
}

// Put writes the manifest only if its Generation is strictly greater
// than any existing on-disk Generation. Returns ErrManifestStale on
// conflict.
//
// Note: this is best-effort optimistic concurrency at the application
// layer. On S3 backends, a true compare-and-swap requires the backend
// to support conditional PUT (ETag) — this implementation reads then
// writes, so a tight race between two builders can both pass the
// generation check. Production v2 should upgrade to backend-level CAS.
func (s *ManifestStore) Put(ctx context.Context, table string, m *Manifest) error {
	if existing, err := s.Get(ctx, table); err == nil {
		if existing.Generation >= m.Generation {
			return ErrManifestStale
		}
	}
	// Either no existing manifest (Get errored) or existing is older.
	b, err := m.JSON()
	if err != nil {
		return fmt.Errorf("marshal manifest %s: %w", table, err)
	}
	return s.backend.Write(ctx, ManifestPath(table), b)
}
