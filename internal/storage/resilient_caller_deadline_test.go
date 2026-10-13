package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// hangingBackend answers nothing until the caller gives up, like a slow but
// healthy object store asked for many objects under one short deadline.
type hangingBackend struct {
	*LocalBackend
}

func (h *hangingBackend) Read(ctx context.Context, _ string) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// A caller's own deadline or cancel says nothing about the store's health.
// Counting those as failures opened the breaker in production the moment a
// bounded startup load (hundreds of manifest reads under one deadline) timed
// out, and every query after it was refused with 503 until the pod rolled back.
func TestResilientBackend_CallerDeadlineDoesNotTripBreaker(t *testing.T) {
	local, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	cfg := fastResilientConfig()
	cfg.MaxFailures = 5
	r := NewResilientBackend(&hangingBackend{LocalBackend: local}, cfg, zerolog.Nop())

	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, err := r.Read(ctx, "a/b.json")
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("read %d: err = %v, want the caller's DeadlineExceeded", i, err)
		}
	}
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := r.Read(ctx, "a/b.json"); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled read %d: err = %v, want context.Canceled", i, err)
		}
	}
	if r.IsCircuitOpen() {
		t.Fatal("caller deadlines/cancels opened the circuit breaker")
	}
}

// The store's own failures still count: a breaker that never opens on a real
// outage would make every caller wait out its full timeout.
func TestResilientBackend_StoreFailuresStillTripBreaker(t *testing.T) {
	f := newFlaky(t)
	f.readFailures = 1 << 30
	cfg := fastResilientConfig()
	cfg.MaxFailures = 5
	cfg.MaxRetries = 0
	r := NewResilientBackend(f, cfg, zerolog.Nop())
	for i := 0; i < 10; i++ {
		_ = r.ReadTo(context.Background(), "a/b.parquet", &discardWriter{})
	}
	if !r.IsCircuitOpen() {
		t.Fatal("store failures with a live caller context must open the breaker")
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
