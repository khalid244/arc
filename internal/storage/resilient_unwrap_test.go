package storage

import (
	"testing"

	"github.com/rs/zerolog"
)

// TestResilientBackend_Unwrap verifies the wrapper exposes the backend it
// wraps, so callers (e.g. compaction credential extraction) can reach concrete
// backend types through the resilience layer.
func TestResilientBackend_Unwrap(t *testing.T) {
	inner, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	r := NewResilientBackend(inner, nil, zerolog.Nop())
	if r.Unwrap() != inner {
		t.Fatalf("Unwrap() did not return the wrapped backend")
	}
}
