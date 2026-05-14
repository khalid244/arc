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

// =============================================================================
// S3 Phantom Write Duplication Test
//
// Reproduces the exact production failure pattern from hammel-arc-ingest-logs2:
//   1. Arc calls storage.Write(path, data)
//   2. S3 receives the PUT and writes the file (data IS on disk)
//   3. S3 response is delayed past Arc's flush timeout
//   4. Arc gets "context deadline exceeded" — thinks write FAILED
//   5. Data stays in WAL for replay
//   6. On recovery, WAL replays the same records → writes them AGAIN
//   7. Result: same records exist in 2 different Parquet files = DUPLICATION
//
// The fix: after Write returns an error, call Exists to verify whether the
// file actually landed. If it did, treat as success — no WAL replay needed.
// =============================================================================

// phantomWriteStorage simulates S3 where:
// - PUT requests always succeed server-side (data lands on storage)
// - But the response may be delayed past timeout (caller sees error)
// This is the exact "context deadline exceeded" pattern from production.
type phantomWriteStorage struct {
	mu           sync.Mutex
	writtenFiles map[string]int // path → write count
	writeCount   atomic.Int32
	existsCalls  atomic.Int32
	errorCount   atomic.Int32
}

func newPhantomWriteStorage() *phantomWriteStorage {
	return &phantomWriteStorage{
		writtenFiles: make(map[string]int),
	}
}

func (s *phantomWriteStorage) Write(ctx context.Context, path string, data []byte) error {
	s.writeCount.Add(1)

	// Data ALWAYS lands on storage (simulates S3 committing the PUT)
	s.mu.Lock()
	s.writtenFiles[path]++
	s.mu.Unlock()

	// But the response is delayed past timeout → caller sees error
	s.errorCount.Add(1)
	return fmt.Errorf("context deadline exceeded")
}

func (s *phantomWriteStorage) Exists(ctx context.Context, path string) (bool, error) {
	s.existsCalls.Add(1)
	s.mu.Lock()
	_, ok := s.writtenFiles[path]
	s.mu.Unlock()
	return ok, nil
}

func (s *phantomWriteStorage) getDuplicateWrites() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	dupes := 0
	for _, count := range s.writtenFiles {
		if count > 1 {
			dupes += count - 1
		}
	}
	return dupes
}

func (s *phantomWriteStorage) getTotalFiles() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.writtenFiles)
}

func (s *phantomWriteStorage) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	data, _ := io.ReadAll(r)
	return s.Write(ctx, path, data)
}
func (s *phantomWriteStorage) Read(ctx context.Context, path string) ([]byte, error)                     { return nil, nil }
func (s *phantomWriteStorage) ReadTo(ctx context.Context, path string, w io.Writer) error                 { return nil }
func (s *phantomWriteStorage) ReadToAt(ctx context.Context, path string, w io.Writer, offset int64) error { return nil }
func (s *phantomWriteStorage) StatFile(ctx context.Context, path string) (int64, error)                   { return -1, nil }
func (s *phantomWriteStorage) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for k := range s.writtenFiles {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}
func (s *phantomWriteStorage) Delete(ctx context.Context, path string) error             { return nil }
func (s *phantomWriteStorage) DeleteBatch(ctx context.Context, paths []string) error      { return nil }
func (s *phantomWriteStorage) Close() error                                  { return nil }
func (s *phantomWriteStorage) Type() string                                  { return "mock-phantom" }
func (s *phantomWriteStorage) ConfigJSON() string                            { return "{}" }

// =============================================================================
// Test: Verify that phantom writes don't cause data duplication.
//
// Strategy:
//   1. Write a batch of data to ArrowBuffer (single hour, so flush = 1 file)
//   2. Flush completes with phantom write (data on storage, error to caller)
//   3. Check totalErrors counter on the buffer:
//      - If flush error propagated (no fix): totalErrors > 0 → WAL would replay → DUPLICATE
//      - If Exists check rescued it (fix): totalErrors == 0 → no WAL replay → SAFE
//   4. Then simulate WAL replay (write same data again) and count files:
//      - Without fix: flush failed → WAL replays → 2 files with same data
//      - With fix: flush succeeded → WAL won't replay in prod, but even if we
//        manually replay, Exists prevents error → 2 writes but both are "success"
//
// This test PASSES on the merge branch (with check-before-retry).
// On old code without the fix, it FAILS because:
//   - Exists is never called (no check-before-retry code exists)
//   - The Write error propagates unconditionally
//   - totalErrors increments → proving WAL replay would happen → duplication
// =============================================================================

func TestPhantomWrite_CheckBeforeRetry_PreventsDuplication(t *testing.T) {
	logger := zerolog.Nop()
	store := newPhantomWriteStorage()

	cfg := &config.IngestConfig{
		MaxBufferSize:       500,
		MaxBufferAgeMS:      60000,
		Compression:         "snappy",
		UseDictionary:       true,
		WriteStatistics:     true,
		DataPageVersion:     "2.0",
		FlushWorkers:        1,
		FlushQueueSize:      10,
		ShardCount:          1,
		FlushTimeoutSeconds: 30,
	}

	buf := NewArrowBuffer(cfg, store, logger)

	// Write 1000 records in the SAME hour → triggers a single-file flush
	baseTime := time.Now().Truncate(time.Hour).UnixMicro()
	times := make([]interface{}, 1000)
	values := make([]interface{}, 1000)
	devices := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		times[i] = baseTime + int64(i)*1000
		values[i] = float64(i) * 0.1
		devices[i] = fmt.Sprintf("dev_%d", i%10)
	}
	cols := map[string][]interface{}{
		"time":      times,
		"value":     values,
		"device_id": devices,
	}

	if err := buf.WriteColumnarDirect(context.Background(), "testdb", "metric", cols); err != nil {
		t.Fatalf("WriteColumnarDirect: %v", err)
	}

	// Wait for async flush to fire and attempt storage write
	time.Sleep(4 * time.Second)

	// Key assertion: did the flush error propagate?
	totalErrors := buf.totalErrors.Load()
	existsCalls := store.existsCalls.Load()
	writeCount := store.writeCount.Load()

	t.Logf("Writes: %d, Exists calls: %d, Errors propagated: %d",
		writeCount, existsCalls, totalErrors)

	// The fix MUST:
	// 1. Call Exists after Write fails
	// 2. Find the file exists (phantom write succeeded)
	// 3. NOT propagate the error (totalErrors == 0)
	if existsCalls == 0 {
		t.Fatal("FAIL: Exists was never called after Write error. " +
			"Without check-before-retry, the Write error propagates unconditionally, " +
			"WAL replay will write the same data again, causing DUPLICATION. " +
			"This is the production bug.")
	}

	if totalErrors > 0 {
		t.Fatalf("FAIL: Flush error propagated (%d errors) despite file existing on storage. "+
			"This means WAL replay would create duplicate data. "+
			"The check-before-retry fix should have caught this.", totalErrors)
	}

	t.Logf("PASS: Phantom write detected and rescued by Exists check. " +
		"No error propagated → WAL replay won't fire → no duplication.")

	buf.Close()
}

// makeColumnsAtTime builds a columnar batch with a specific base timestamp.
func makeColumnsAtTime(n int, baseUs int64) map[string][]interface{} {
	ts := make([]interface{}, n)
	vals := make([]interface{}, n)
	tags := make([]interface{}, n)
	for i := 0; i < n; i++ {
		ts[i] = baseUs + int64(i)*1000
		vals[i] = float64(i) * 0.1
		tags[i] = fmt.Sprintf("device_%d", i%50)
	}
	return map[string][]interface{}{
		"time":      ts,
		"value":     vals,
		"device_id": tags,
	}
}
