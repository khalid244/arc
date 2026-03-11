package ingest

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// slowStorageBackend simulates slow S3 that causes queue buildup.
type slowStorageBackend struct {
	mu           sync.Mutex
	delay        time.Duration
	writeCount   atomic.Int32
	writtenPaths []string
}

func newSlowStorage(delay time.Duration) *slowStorageBackend {
	return &slowStorageBackend{delay: delay}
}

func (s *slowStorageBackend) Write(ctx context.Context, path string, data []byte) error {
	s.writeCount.Add(1)
	select {
	case <-time.After(s.delay):
		s.mu.Lock()
		s.writtenPaths = append(s.writtenPaths, path)
		s.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *slowStorageBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	data, _ := io.ReadAll(r)
	return s.Write(ctx, path, data)
}
func (s *slowStorageBackend) Read(ctx context.Context, path string) ([]byte, error)     { return nil, nil }
func (s *slowStorageBackend) ReadTo(ctx context.Context, path string, w io.Writer) error { return nil }
func (s *slowStorageBackend) List(ctx context.Context, prefix string) ([]string, error)  { return nil, nil }
func (s *slowStorageBackend) Delete(ctx context.Context, path string) error              { return nil }
func (s *slowStorageBackend) Exists(ctx context.Context, path string) (bool, error)      { return false, nil }
func (s *slowStorageBackend) Close() error                                               { return nil }
func (s *slowStorageBackend) Type() string                                               { return "mock-slow" }
func (s *slowStorageBackend) ConfigJSON() string                                         { return "{}" }

// selectiveFailStorageBackend fails writes whose path contains failMeasurement.
type selectiveFailStorageBackend struct {
	mu              sync.Mutex
	failMeasurement string
	failCount       atomic.Int32
	successCount    atomic.Int32
}

func newSelectiveFailStorage(failMeasurement string) *selectiveFailStorageBackend {
	return &selectiveFailStorageBackend{failMeasurement: failMeasurement}
}

func (s *selectiveFailStorageBackend) Write(ctx context.Context, path string, data []byte) error {
	s.mu.Lock()
	fail := s.failMeasurement
	s.mu.Unlock()

	if fail != "" && strings.Contains(path, fail) {
		s.failCount.Add(1)
		return fmt.Errorf("simulated S3 failure for %s", fail)
	}
	s.successCount.Add(1)
	return nil
}

func (s *selectiveFailStorageBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	data, _ := io.ReadAll(r)
	return s.Write(ctx, path, data)
}
func (s *selectiveFailStorageBackend) Read(ctx context.Context, path string) ([]byte, error)     { return nil, nil }
func (s *selectiveFailStorageBackend) ReadTo(ctx context.Context, path string, w io.Writer) error { return nil }
func (s *selectiveFailStorageBackend) List(ctx context.Context, prefix string) ([]string, error)  { return nil, nil }
func (s *selectiveFailStorageBackend) Delete(ctx context.Context, path string) error              { return nil }
func (s *selectiveFailStorageBackend) Exists(ctx context.Context, path string) (bool, error)      { return false, nil }
func (s *selectiveFailStorageBackend) Close() error                                               { return nil }
func (s *selectiveFailStorageBackend) Type() string                                               { return "mock-selective-fail" }
func (s *selectiveFailStorageBackend) ConfigJSON() string                                         { return "{}" }

// =============================================================================
// Bug 1: Flush Queue Full → Data Lost on Close
//
// When the flush queue is full, data is evicted from memory (arrow_writer.go:1299).
// Close() only flushes in-memory buffers — the evicted data is gone.
// All sent records must be persisted after Close().
// =============================================================================

func TestFlushQueueFull_AllRecordsMustPersist(t *testing.T) {
	logger := zerolog.Nop()
	store := newSlowStorage(2 * time.Second)

	cfg := &config.IngestConfig{
		MaxBufferSize:       1000,
		MaxBufferAgeMS:      60000,
		Compression:         "snappy",
		UseDictionary:       true,
		WriteStatistics:     true,
		DataPageVersion:     "2.0",
		FlushWorkers:        2,
		FlushQueueSize:      3,
		ShardCount:          1,
		FlushTimeoutSeconds: 30,
	}

	buf := NewArrowBuffer(cfg, store, logger)

	totalRecordsSent := 0
	for i := 0; i < 20; i++ {
		cols := makeColumns(1100)
		buf.WriteColumnarDirect(context.Background(), "db", "sensor", cols)
		totalRecordsSent += 1100
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify queue-full actually occurred (precondition)
	stats := buf.GetStats()
	errors, _ := stats["total_errors"].(int64)
	if errors == 0 {
		t.Fatal("Precondition failed: expected queue-full errors but got 0")
	}
	t.Logf("Precondition met: %d queue-full drops occurred", errors)

	buf.Close()
	time.Sleep(3 * time.Second)

	statsAfter := buf.GetStats()
	recordsWritten, _ := statsAfter["total_records_written"].(int64)

	if recordsWritten < int64(totalRecordsSent) {
		lost := int64(totalRecordsSent) - recordsWritten
		pct := float64(lost) / float64(totalRecordsSent) * 100
		t.Errorf("Data loss: %d/%d records lost (%.1f%%) — queue-full data evicted from memory was never persisted",
			lost, totalRecordsSent, pct)
	}
}

// =============================================================================
// Bug 2: pendingFlushFailures Counter Drain Race
//
// pendingFlushFailures is a single global counter. Successful flushes from
// any measurement decrement it, even if a different measurement caused the
// failure. HasFlushFailure() must remain true as long as any measurement
// has unresolved flush failures.
// =============================================================================

func TestCounterDrain_HasFlushFailureMustRemainTrue(t *testing.T) {
	logger := zerolog.Nop()
	store := newSelectiveFailStorage("failing_metric")

	cfg := &config.IngestConfig{
		MaxBufferSize:       500,
		MaxBufferAgeMS:      60000,
		Compression:         "snappy",
		UseDictionary:       true,
		WriteStatistics:     true,
		DataPageVersion:     "2.0",
		FlushWorkers:        4,
		FlushQueueSize:      20,
		ShardCount:          4,
		FlushTimeoutSeconds: 30,
	}

	buf := NewArrowBuffer(cfg, store, logger)

	// Trigger a flush failure for "failing_metric"
	buf.WriteColumnarDirect(context.Background(), "db", "failing_metric", makeColumns(600))
	time.Sleep(500 * time.Millisecond)

	if !buf.HasFlushFailure() {
		t.Fatal("Precondition failed: expected HasFlushFailure()=true after failed flush")
	}

	// Now flush 10 healthy measurements — none of these fix the failing_metric problem
	for i := 0; i < 10; i++ {
		buf.WriteColumnarDirect(context.Background(), "db", fmt.Sprintf("healthy_%d", i), makeColumns(600))
	}
	time.Sleep(2 * time.Second)

	// "failing_metric" never succeeded. Storage confirms it was never written.
	if store.failCount.Load() == 0 {
		t.Fatal("Precondition failed: expected storage failures for failing_metric")
	}
	if store.successCount.Load() == 0 {
		t.Fatal("Precondition failed: expected storage successes for healthy metrics")
	}
	t.Logf("Storage: %d failures (failing_metric), %d successes (healthy_*)",
		store.failCount.Load(), store.successCount.Load())

	// HasFlushFailure must still be true — failing_metric data was never persisted.
	// If it's false, WAL maintenance will purge WAL files while failing_metric
	// data exists only in the buffer + WAL.
	if !buf.HasFlushFailure() {
		t.Errorf("HasFlushFailure()=false but 'failing_metric' never persisted — "+
			"WAL purge would destroy the only durable copy")
	}

	buf.Close()
}

func TestCounterDrain_MultipleMeasurements(t *testing.T) {
	logger := zerolog.Nop()
	store := newSelectiveFailStorage("failing_metric")

	cfg := &config.IngestConfig{
		MaxBufferSize:       500,
		MaxBufferAgeMS:      60000,
		Compression:         "snappy",
		UseDictionary:       true,
		WriteStatistics:     true,
		DataPageVersion:     "2.0",
		FlushWorkers:        8,
		FlushQueueSize:      50,
		ShardCount:          4,
		FlushTimeoutSeconds: 30,
	}

	buf := NewArrowBuffer(cfg, store, logger)

	// Create multiple failures
	for i := 0; i < 5; i++ {
		buf.WriteColumnarDirect(context.Background(), "db", "failing_metric", makeColumns(600))
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)

	if !buf.HasFlushFailure() {
		t.Fatal("Precondition failed: expected failures to register")
	}
	t.Log("After 5 failing flushes: HasFlushFailure()=true")

	// Fire 20 healthy flushes from different measurements
	for i := 0; i < 20; i++ {
		buf.WriteColumnarDirect(context.Background(), "db", fmt.Sprintf("healthy_%d", i), makeColumns(600))
	}
	time.Sleep(2 * time.Second)

	t.Logf("After 20 healthy flushes: HasFlushFailure()=%v, failures=%d, successes=%d",
		buf.HasFlushFailure(), store.failCount.Load(), store.successCount.Load())

	if !buf.HasFlushFailure() {
		t.Errorf("HasFlushFailure()=false after %d healthy flushes drained the counter, "+
			"but failing_metric never persisted (%d storage failures, 0 successes for it)",
			store.successCount.Load(), store.failCount.Load())
	}

	buf.Close()
}

// =============================================================================
// Bug 2 variant: concurrent flushes for the same buffer key.
//
// Two workers can flush the same buffer key concurrently:
//   Worker A: flush fails → markFlushFailure("key") → restoreToBuffer
//   Worker B: flush succeeds → clearFlushFailure("key")
//
// With a simple set (map[string]struct{}), Worker B's success would remove
// the key entirely, masking Worker A's failure. Per-key counting prevents this:
// mark increments to 1, mark increments to 2 (if two fail), clear decrements.
// =============================================================================

func TestCounterDrain_ConcurrentFlushSameKey(t *testing.T) {
	logger := zerolog.Nop()

	// This backend fails the first N writes, then succeeds.
	// With 2 concurrent flushes for the same key, flush A fails, flush B succeeds.
	store := &firstNFailStorageBackend{failCount: 1}

	cfg := &config.IngestConfig{
		MaxBufferSize:       500,
		MaxBufferAgeMS:      60000,
		Compression:         "snappy",
		UseDictionary:       true,
		WriteStatistics:     true,
		DataPageVersion:     "2.0",
		FlushWorkers:        4,
		FlushQueueSize:      20,
		ShardCount:          1, // single shard to maximize same-key concurrency
		FlushTimeoutSeconds: 30,
	}

	buf := NewArrowBuffer(cfg, store, logger)

	// Write two batches rapidly for the same measurement — both trigger flushes
	buf.WriteColumnarDirect(context.Background(), "db", "metric_x", makeColumns(600))
	buf.WriteColumnarDirect(context.Background(), "db", "metric_x", makeColumns(600))
	time.Sleep(2 * time.Second)

	failures := store.failures.Load()
	successes := store.successes.Load()
	t.Logf("Storage: %d failures, %d successes, HasFlushFailure=%v",
		failures, successes, buf.HasFlushFailure())

	// If at least one flush failed and the data was restored, HasFlushFailure must
	// remain true until that restored data is successfully flushed.
	if failures > 0 && successes > 0 && !buf.HasFlushFailure() {
		t.Errorf("HasFlushFailure()=false despite concurrent flush failure — "+
			"Worker B's success masked Worker A's failure for the same key")
	}

	buf.Close()
}

// firstNFailStorageBackend fails the first N writes, then succeeds.
type firstNFailStorageBackend struct {
	mu        sync.Mutex
	failCount int
	attempted int
	failures  atomic.Int32
	successes atomic.Int32
}

func (s *firstNFailStorageBackend) Write(ctx context.Context, path string, data []byte) error {
	s.mu.Lock()
	s.attempted++
	shouldFail := s.attempted <= s.failCount
	s.mu.Unlock()

	if shouldFail {
		s.failures.Add(1)
		return fmt.Errorf("simulated failure #%d", s.attempted)
	}
	s.successes.Add(1)
	return nil
}

func (s *firstNFailStorageBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	data, _ := io.ReadAll(r)
	return s.Write(ctx, path, data)
}
func (s *firstNFailStorageBackend) Read(ctx context.Context, path string) ([]byte, error)     { return nil, nil }
func (s *firstNFailStorageBackend) ReadTo(ctx context.Context, path string, w io.Writer) error { return nil }
func (s *firstNFailStorageBackend) List(ctx context.Context, prefix string) ([]string, error)  { return nil, nil }
func (s *firstNFailStorageBackend) Delete(ctx context.Context, path string) error              { return nil }
func (s *firstNFailStorageBackend) Exists(ctx context.Context, path string) (bool, error)      { return false, nil }
func (s *firstNFailStorageBackend) Close() error                                               { return nil }
func (s *firstNFailStorageBackend) Type() string                                               { return "mock-first-n-fail" }
func (s *firstNFailStorageBackend) ConfigJSON() string                                         { return "{}" }
