package ingest

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Posthog-shaped ingest (hammel-posthog-api src/write_buffer.rs): every batch
// carries time, uuid, event, distinct_id, created_at, seq and the UNION of the
// property columns its events happen to have. Properties are sparse, so two
// batches almost never carry the same column set, and several producer
// replicas write the same (database, measurement) concurrently.
//
// On 11feca96 every batch whose column set differed from the open buffer's
// flushed that buffer, synchronously, inside the writer — and the flush
// releases the shard lock for its I/O, so the next writer installed yet
// another column set and the first writer flushed again, up to
// schemaEvolutionMaxIters times, then got ErrSchemaChurnExceeded (HTTP 503).
// Production: "Schema-evolution loop hit max iterations ... buffer_key=
// posthog/lifecycle max_iters=8" after 13 s, and the producer, which does not
// retry, dropped the batch.

// slowCountingStorage is a local backend whose writes take `delay`, like an
// object store or a fsynced stage, and which counts them.
type slowCountingStorage struct {
	storage.Backend
	delay  time.Duration
	writes atomic.Int64
}

func (s *slowCountingStorage) Write(ctx context.Context, path string, data []byte) error {
	s.writes.Add(1)
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.Backend.Write(ctx, path, data)
}

func (s *slowCountingStorage) WriteReader(ctx context.Context, path string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return s.Write(ctx, path, data)
}

// posthogPropertyPool is the set of sparse property columns batches draw from.
func posthogPropertyPool(n int) []string {
	pool := make([]string, n)
	for i := range pool {
		pool[i] = fmt.Sprintf("prop_%03d", i)
	}
	return pool
}

// posthogSparseColumns builds one producer batch: the fixed columns plus a
// random subset of the property pool, every property a string ("" when the
// event lacks it, as the producer sends).
func posthogSparseColumns(rnd *rand.Rand, pool []string, rows int, uuidPrefix string) map[string][]interface{} {
	now := time.Now().UnixMicro()
	// Rows within the current hour (up to 30 s old), so one flush is one file.
	span := min(int64(30_000_000), now-time.Now().Truncate(time.Hour).UnixMicro())
	cols := map[string][]interface{}{
		"time": make([]interface{}, rows), "uuid": make([]interface{}, rows),
		"event": make([]interface{}, rows), "distinct_id": make([]interface{}, rows),
		"created_at": make([]interface{}, rows), "seq": make([]interface{}, rows),
	}
	for i := 0; i < rows; i++ {
		cols["time"][i] = now - rnd.Int63n(max(span, 1))
		cols["uuid"][i] = fmt.Sprintf("%s-%06d", uuidPrefix, i)
		cols["event"][i] = fmt.Sprintf("event_%d", rnd.Intn(20))
		cols["distinct_id"][i] = fmt.Sprintf("user-%d", rnd.Intn(50_000))
		cols["created_at"][i] = time.UnixMicro(now).UTC().Format(time.RFC3339)
		cols["seq"][i] = int64(i)
	}
	k := 10 + rnd.Intn(70)
	for _, j := range rnd.Perm(len(pool))[:k] {
		vals := make([]interface{}, rows)
		for i := range vals {
			if rnd.Intn(3) == 0 {
				vals[i] = ""
			} else {
				vals[i] = fmt.Sprintf("v%d", rnd.Intn(1000))
			}
		}
		cols[pool[j]] = vals
	}
	return cols
}

// storedUUIDCounts reads every Parquet file under root and counts each uuid.
func storedUUIDCounts(t *testing.T, root string) (map[string]int, int) {
	t.Helper()
	counts := map[string]int{}
	files := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		files++
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		rdr, err := file.NewParquetReader(f)
		if err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		defer rdr.Close()
		fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		if err != nil {
			return err
		}
		tbl, err := fr.ReadTable(context.Background())
		if err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		defer tbl.Release()
		idx := tbl.Schema().FieldIndices("uuid")
		if len(idx) != 1 {
			return fmt.Errorf("%s: no uuid column", p)
		}
		for _, chunk := range tbl.Column(idx[0]).Data().Chunks() {
			a, ok := chunk.(*array.String)
			if !ok {
				return fmt.Errorf("%s: uuid column is %T", p, chunk)
			}
			for i := 0; i < a.Len(); i++ {
				counts[a.Value(i)]++
			}
		}
		return nil
	})
	require.NoError(t, err)
	return counts, files
}

func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

type sparseFixture struct {
	buf     *ArrowBuffer
	store   *slowCountingStorage
	dir     string
	wal     *wal.Writer
	decoder *MessagePackDecoder
}

func newSparseFixture(t *testing.T, delay time.Duration, maxBuffer int) *sparseFixture {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	require.NoError(t, err)
	store := &slowCountingStorage{Backend: backend, delay: delay}
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(t.TempDir(), "wal"), SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	buf := NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize:       maxBuffer,
		MaxBufferAgeMS:      3_600_000, // no age flush during the test
		Compression:         "snappy",
		ShardCount:          32,
		FlushWorkers:        16,
		FlushQueueSize:      1500,
		FlushTimeoutSeconds: 30,
		DataPageVersion:     "2.0",
	}, store, zerolog.Nop())
	buf.SetWAL(w)
	dec := NewMessagePackDecoder(zerolog.Nop())
	dec.SetTypedDecodeEnabled(true)
	return &sparseFixture{buf: buf, store: store, dir: dir, wal: w, decoder: dec}
}

type sparseRun struct {
	lat                  []time.Duration // sorted
	errs                 []error
	written              int64
	storageWritesInLoad  int64
	rotations, inline    int64
	storedDistinct, dups int
	walPending           int
}

// runSparseWriters drives `writers` goroutines, each writing writesPerWriter
// posthog-shaped batches to posthog/lifecycle (pausing `pace` between them),
// then closes the buffer and reads back what was stored.
func runSparseWriters(t *testing.T, path string, writers, writesPerWriter, rows int, storageDelay, pace time.Duration) sparseRun {
	t.Helper()
	fx := newSparseFixture(t, storageDelay, 10_000_000)
	pool := posthogPropertyPool(300)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		r       sparseRun
		written atomic.Int64
	)
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(w)))
			<-start
			time.Sleep(time.Duration(rnd.Int63n(int64(pace) + 1))) // writers are not phase-locked
			for j := 0; j < writesPerWriter; j++ {
				cols := posthogSparseColumns(rnd, pool, rows, fmt.Sprintf("w%02d-b%03d", w, j))
				var err error
				t0 := time.Now()
				if path == "msgpack_typed" {
					payload, merr := msgpack.Marshal(map[string]interface{}{"m": "lifecycle", "columns": cols})
					if merr != nil {
						t.Error(merr)
						return
					}
					records, derr := fx.decoder.Decode(payload)
					if derr != nil {
						t.Error(derr)
						return
					}
					t0 = time.Now()
					err = fx.buf.Write(context.Background(), "posthog", records)
				} else {
					err = fx.buf.WriteColumnarDirect(context.Background(), "posthog", "lifecycle", cols)
				}
				d := time.Since(t0)
				mu.Lock()
				r.lat = append(r.lat, d)
				if err != nil {
					r.errs = append(r.errs, err)
				} else {
					written.Add(int64(rows))
				}
				mu.Unlock()
				time.Sleep(pace)
			}
		}(w)
	}
	close(start)
	wg.Wait()
	r.storageWritesInLoad = fx.store.writes.Load()
	stats := fx.buf.GetStats()
	r.rotations, _ = stats["total_schema_rotations"].(int64)
	r.inline, _ = stats["total_schema_rotations_sync"].(int64)
	sort.Slice(r.lat, func(i, j int) bool { return r.lat[i] < r.lat[j] })

	require.NoError(t, fx.buf.Close())
	counts, files := storedUUIDCounts(t, fx.dir)
	for _, c := range counts {
		if c > 1 {
			r.dups++
		}
	}
	r.storedDistinct = len(counts)
	r.written = written.Load()
	r.walPending = fx.wal.PendingUnflushedCount()
	t.Logf("%d writes: p50=%v p99=%v max=%v errors=%d; schema rotations %d (inline %d); storage writes during load %d; "+
		"stored %d files, %d distinct uuids, %d duplicated", len(r.lat), percentile(r.lat, 0.5), percentile(r.lat, 0.99),
		r.lat[len(r.lat)-1], len(r.errs), r.rotations, r.inline, r.storageWritesInLoad, files, len(counts), r.dups)
	return r
}

func (r sparseRun) requireIntact(t *testing.T, want int64) {
	t.Helper()
	if len(r.errs) > 0 {
		t.Errorf("%d of %d writes failed; first: %v", len(r.errs), len(r.lat), r.errs[0])
	}
	if r.written != want {
		t.Errorf("acknowledged %d rows, want %d", r.written, want)
	}
	if int64(r.storedDistinct) != r.written || r.dups != 0 {
		t.Errorf("stored %d distinct uuids (%d duplicated), acknowledged %d rows", r.storedDistinct, r.dups, r.written)
	}
	if r.walPending != 0 {
		t.Errorf("%d WAL identities still pending after a clean Close: a flush lost their checkpoint", r.walPending)
	}
}

// TestArrowBuffer_SparseSchemaConcurrentWriters is the regression for the
// production schema-churn rejections: 24 concurrent writers to ONE
// (database, measurement), each batch with a random subset of a 300-column
// pool of sparse string columns, at a rate the flush workers can keep up with.
//
// Required: zero errors; every row stored exactly once; every WAL identity
// checkpointed after Close; and no write waits on storage I/O — p99 below one
// storage write. On the base every write flushed the previous buffer
// synchronously, often several times in a loop (p99 ~3 storage writes here;
// in production 13 s and then ErrSchemaChurnExceeded).
func TestArrowBuffer_SparseSchemaConcurrentWriters(t *testing.T) {
	if testing.Short() {
		t.Skip("concurrency regression; skipped in -short mode")
	}
	const storageDelay = 100 * time.Millisecond
	for _, path := range []string{"msgpack_typed", "columnar_untyped"} {
		t.Run(path, func(t *testing.T) {
			// 24 writers x 1 batch / 250 ms = ~96 schema changes/s; 16 workers
			// at 100 ms flush ~160/s.
			r := runSparseWriters(t, path, 24, 15, 300, storageDelay, 250*time.Millisecond)
			r.requireIntact(t, 24*15*300)
			if p99 := percentile(r.lat, 0.99); p99 >= storageDelay {
				t.Errorf("p99 write latency %v >= one storage write (%v): writes waited on flush I/O", p99, storageDelay)
			}
		})
	}
}

// TestArrowBuffer_SparseSchemaOverload_BackpressureNotRejection: the same
// writers with no pause, far more schema changes than the flush workers can
// write. Writers are then slowed — each flushes the buffer it displaced
// itself, once — but never rejected and never looped. The latency bound
// allows one flush plus the WAL fsync queue of 24 writers (the base looped
// through several flushes: 535 ms - 1.1 s max in this setup).
func TestArrowBuffer_SparseSchemaOverload_BackpressureNotRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("concurrency regression; skipped in -short mode")
	}
	const storageDelay = 100 * time.Millisecond
	r := runSparseWriters(t, "msgpack_typed", 24, 15, 300, storageDelay, 0)
	r.requireIntact(t, 24*15*300)
	if r.inline == 0 {
		t.Error("no writer flushed inline: the overload did not exercise the backpressure path")
	}
	if max := r.lat[len(r.lat)-1]; max >= 4*storageDelay {
		t.Errorf("slowest write took %v: a write waited on more than one flush (one storage write is %v)", max, storageDelay)
	}
}

// TestArrowBuffer_TypeConflictWritersNeverRejected: writers that send the same
// column with two different types (the signature includes the type, so every
// alternation is a schema change). They must not be rejected either, and no
// write may wait on more than one flush: the old buffer is handed off in the
// same critical section that installs the new batch, never in a loop.
func TestArrowBuffer_TypeConflictWritersNeverRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("concurrency regression; skipped in -short mode")
	}
	const (
		writers         = 16
		writesPerWriter = 20
		rows            = 50
		storageDelay    = 50 * time.Millisecond
	)
	fx := newSparseFixture(t, storageDelay, 10_000_000)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		lat     []time.Duration
		errs    []error
		written atomic.Int64
	)
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for j := 0; j < writesPerWriter; j++ {
				now := time.Now().UnixMicro()
				cols := map[string][]interface{}{"time": make([]interface{}, rows), "uuid": make([]interface{}, rows),
					"x": make([]interface{}, rows)}
				for i := 0; i < rows; i++ {
					cols["time"][i] = now + int64(i)
					cols["uuid"][i] = fmt.Sprintf("w%02d-b%03d-%03d", w, j, i)
					if w%2 == 0 {
						cols["x"][i] = int64(i) // even writers: x is an integer
					} else {
						cols["x"][i] = fmt.Sprint(i) // odd writers: x is a string
					}
				}
				t0 := time.Now()
				err := fx.buf.WriteColumnarDirect(context.Background(), "db", "conflict", cols)
				d := time.Since(t0)
				mu.Lock()
				lat = append(lat, d)
				if err != nil {
					errs = append(errs, err)
				} else {
					written.Add(rows)
				}
				mu.Unlock()
			}
		}(w)
	}
	close(start)
	wg.Wait()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	t.Logf("%d writes: p50=%v p99=%v max=%v errors=%d", len(lat), percentile(lat, 0.5), percentile(lat, 0.99), lat[len(lat)-1], len(errs))

	require.NoError(t, fx.buf.Close())
	counts, files := storedUUIDCounts(t, fx.dir)
	dups := 0
	for _, c := range counts {
		if c > 1 {
			dups++
		}
	}
	t.Logf("stored: %d files, %d distinct uuids, %d duplicated", files, len(counts), dups)
	if len(errs) > 0 {
		t.Errorf("%d of %d writes failed; first: %v", len(errs), len(lat), errs[0])
	}
	if max := lat[len(lat)-1]; max >= 4*storageDelay {
		t.Errorf("slowest write took %v: a write waited on several flushes (one storage write is %v)", max, storageDelay)
	}
	if len(counts) != int(written.Load()) || dups != 0 {
		t.Errorf("stored %d distinct uuids (%d duplicated), acknowledged %d rows", len(counts), dups, written.Load())
	}
	if n := fx.wal.PendingUnflushedCount(); n != 0 {
		t.Errorf("%d WAL identities still pending after a clean Close", n)
	}
}
