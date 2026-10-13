package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// TestArrowBuffer_SchemaChurnExceededErrorContract pins that rotating column
// sets against one (database, measurement) are never rejected.
//
// Many writers rotate through distinct schemas (a unique column name each),
// with a slow storage backend. This used to drive the schema-evolution flush
// loop to its cap and fail writes with ErrSchemaChurnExceeded (HTTP 503); a
// schema change now hands the open buffer to a flush worker in the same
// critical section (rotateBufferLocked), so every write must succeed and the
// churn counter must stay 0.
func TestArrowBuffer_SchemaChurnExceededErrorContract(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping schema-churn contract test in -short mode")
	}

	cfg := &config.IngestConfig{
		MaxBufferSize:   25,
		MaxBufferAgeMS:  1000,
		Compression:     "snappy",
		FlushWorkers:    1, // single worker so flushes serialize and we can saturate
		FlushQueueSize:  4,
		ShardCount:      1, // single shard so all writers contend on the same lock
		DataPageVersion: "2.0",
	}
	storage := newHangingStorage(0, 10*time.Millisecond) // wider I/O window
	buf := NewArrowBuffer(cfg, storage, zerolog.Nop())
	defer buf.Close()

	const numWriters = 16
	const writesPerWriter = 200
	const numSchemas = 12 // more than the old loop's cap (8), which made it reachable

	// Each writer rotates through `numSchemas` distinct schemas, choosing
	// based on (writerID, iteration) to maximise cross-writer collision.
	// Each schema has a unique column name so the signature differs.
	makeRotatingSchema := func(schemaIdx, n int) map[string][]interface{} {
		ts := make([]interface{}, n)
		v := make([]interface{}, n)
		base := time.Now().UnixMicro()
		for i := 0; i < n; i++ {
			ts[i] = base + int64(i)
			v[i] = float64(i)
		}
		colName := fmt.Sprintf("col_%d", schemaIdx)
		return map[string][]interface{}{"time": ts, colName: v}
	}

	beforeCount := buf.totalSchemaChurnExceeded.Load()

	var (
		wg             sync.WaitGroup
		churnErrCount  atomic.Int64
		otherErrCount  atomic.Int64
		successesCount atomic.Int64
	)
	wg.Add(numWriters)
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("writer %d panicked: %v", writerID, r)
				}
			}()
			for j := 0; j < writesPerWriter; j++ {
				schemaIdx := (writerID + j) % numSchemas
				err := buf.WriteColumnarDirect(context.Background(), "testdb", "churn",
					makeRotatingSchema(schemaIdx, 5))
				switch {
				case err == nil:
					successesCount.Add(1)
				case errors.Is(err, ErrSchemaChurnExceeded):
					churnErrCount.Add(1)
				default:
					// Any other error is a contract violation: the only
					// failure mode under this workload is schema churn.
					t.Errorf("writer %d iter %d returned unexpected error type: %v", writerID, j, err)
					otherErrCount.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	afterCount := buf.totalSchemaChurnExceeded.Load()
	counterDelta := afterCount - beforeCount

	t.Logf("churn workload result: successes=%d, churn_errors=%d, counter_delta=%d, other_errors=%d",
		successesCount.Load(), churnErrCount.Load(), counterDelta, otherErrCount.Load())

	if churnErrCount.Load() != 0 || counterDelta != 0 {
		t.Errorf("rotating column sets were rejected: %d ErrSchemaChurnExceeded, counter delta %d",
			churnErrCount.Load(), counterDelta)
	}
}

// TestArrowBuffer_ErrSchemaChurnExceeded_IsExported asserts the
// sentinel is exported and stable so external callers (LP/TLE
// handlers) can errors.Is it without an internal-package reach.
func TestArrowBuffer_ErrSchemaChurnExceeded_IsExported(t *testing.T) {
	if ErrSchemaChurnExceeded == nil {
		t.Fatal("ErrSchemaChurnExceeded must be a non-nil exported sentinel")
	}
	wrapped := fmt.Errorf("write rejected: %w", ErrSchemaChurnExceeded)
	if !errors.Is(wrapped, ErrSchemaChurnExceeded) {
		t.Fatal("ErrSchemaChurnExceeded must be unwrappable via errors.Is")
	}
}
