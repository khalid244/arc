package ingest

// Data-safety regression tests for the ingest flush path after the upstream
// merge (upstream failure model + the fork's Late/FreshStager routing).
//
// Tests A-G started life as review repro tests written against the pre-merge
// fork (internal review of 3879d488, IW-01..IW-15), where each one FAILED.
// They are adapted here to the merged APIs: WriteColumnarDirectWithWAL is gone
// (replay goes through WriteColumnarDirectReplay), restoreToBuffer is gone
// (a failed flush is dropped and WAL recovery re-flushes it), and the shutdown
// purge gates on CloseFlushedCleanly plus the stagers' Pending/Close.
//
// The remaining tests pin the merge's own invariants: a WAL identity is
// checkpointed only after its rows are durably staged (or written), a failed
// stage leaves the identity un-checkpointed, and a multi-hour flush that fails
// half-way takes back what it staged.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

// ---- storage mocks ---------------------------------------------------------

// flakyStore fails the first failN Write/WriteReader calls, then succeeds.
// With phantom set, a "failed" call still stores the object: an S3 PUT that
// committed server-side but timed out client-side. With down set, every call
// fails (outage), including deletes.
type flakyStore struct {
	*slowStorageBackend
	mu      sync.Mutex
	failN   int32
	calls   atomic.Int32
	phantom bool
	objects map[string]int64 // key -> size
	down    atomic.Bool
	// failKeyContains, when set, fails only uploads whose key contains it.
	failKeyContains string
	// failDeletes fails every Delete (an outage that only hit deletes, or the
	// one that failed an upload).
	failDeletes atomic.Bool
}

func newFlakyStore(failN int32, phantom bool) *flakyStore {
	return &flakyStore{slowStorageBackend: newSlowStorage(0), failN: failN, phantom: phantom, objects: map[string]int64{}}
}

func (f *flakyStore) put(path string, n int64) error {
	if f.down.Load() {
		return errors.New("simulated S3 outage: connection reset")
	}
	f.mu.Lock()
	failKey := f.failKeyContains
	f.mu.Unlock()
	if failKey != "" && strings.Contains(path, failKey) {
		return errors.New("simulated S3 failure for one partition")
	}
	c := f.calls.Add(1)
	if c <= f.failN {
		if f.phantom {
			f.mu.Lock()
			f.objects[path] = n
			f.mu.Unlock()
		}
		return errors.New("simulated S3 timeout: context deadline exceeded")
	}
	f.mu.Lock()
	f.objects[path] = n
	f.mu.Unlock()
	return nil
}

func (f *flakyStore) Write(ctx context.Context, path string, data []byte) error {
	return f.put(path, int64(len(data)))
}

func (f *flakyStore) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	b, _ := io.ReadAll(r)
	return f.put(path, int64(len(b)))
}

func (f *flakyStore) Exists(ctx context.Context, path string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[path]
	return ok, nil
}

func (f *flakyStore) Delete(ctx context.Context, path string) error {
	if f.down.Load() || f.failDeletes.Load() {
		return errors.New("simulated S3 outage")
	}
	f.mu.Lock()
	delete(f.objects, path)
	f.mu.Unlock()
	return nil
}

func (f *flakyStore) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.objects))
	for k := range f.objects {
		out = append(out, k)
	}
	return out
}

func reviewCfg() *config.IngestConfig {
	return &config.IngestConfig{
		MaxBufferSize:       1000,
		MaxBufferAgeMS:      600000,
		Compression:         "snappy",
		DataPageVersion:     "2.0",
		FlushWorkers:        1,
		FlushQueueSize:      10,
		ShardCount:          1,
		FlushTimeoutSeconds: 30,
	}
}

func newTestFreshStager(t *testing.T, store *flakyStore, dir string) *FreshStager {
	t.Helper()
	fs, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

func newTestLateStager(t *testing.T, store *flakyStore, dir string) *LateStager {
	t.Helper()
	ls, err := NewLateStager(&LateStagerConfig{Storage: store, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	return ls
}

// breakStageDir replaces a stager's directory with a regular file, so every
// later Stage fails at MkdirAll: a stand-in for a dead or full stage volume.
func breakStageDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
}

// ---- A-G: review repro tests, adapted --------------------------------------

// A. Close() must flush tasks still waiting in flushQueue (upstream #1017),
// or at least report the close as unclean so the shutdown WAL purge keeps the
// WAL. Pre-merge, Close dropped them and the purge deleted their only copy.
func TestStagerSafety_CloseFlushesQueuedFlushTasks(t *testing.T) {
	store := newSlowStorage(2 * time.Second)
	buf := NewArrowBuffer(reviewCfg(), store, zerolog.Nop())
	sent := 0
	for i := 0; i < 4; i++ {
		if err := buf.WriteColumnarDirect(context.Background(), "db", "m", makeColumns(1100)); err != nil {
			t.Fatal(err)
		}
		sent += 1100
	}
	time.Sleep(100 * time.Millisecond) // worker picks up task 1; tasks 2-4 wait in the queue
	_ = buf.Close()
	written := buf.GetStats()["total_records_written"].(int64)
	t.Logf("sent=%d written=%d CloseFlushedCleanly=%v", sent, written, buf.CloseFlushedCleanly())
	if written < int64(sent) && buf.CloseFlushedCleanly() {
		t.Errorf("LOSS: %d rows dropped at Close while CloseFlushedCleanly()=true -> shutdown wal-purge would delete their only copy", int64(sent)-written)
	}
	if written != int64(sent) {
		t.Errorf("Close left %d of %d rows unwritten with healthy (slow) storage", int64(sent)-written, sent)
	}
}

// B. A stager Close during an S3 outage must not look like a clean drain:
// the staged rows were already checkpointed out of the WAL. Close reports
// ErrStagedNotUploaded and Pending stays non-zero, which is what the shutdown
// wal-purge gates on. (Surviving pod deletion of the stage volume itself is
// the later spool/handoff work; see TestStagerSafety_SpoolSurvivesPodDeletion.)
func TestStagerSafety_FreshStagerCloseDuringOutageIsReported(t *testing.T) {
	tmp := t.TempDir()
	store := newFlakyStore(0, false)
	store.down.Store(true)
	cfg := reviewCfg()
	cfg.MaxBufferSize = 100000
	buf := NewArrowBuffer(cfg, store, zerolog.Nop())
	fs := newTestFreshStager(t, store, filepath.Join(tmp, "fresh"))
	fs.SetCloseBudget(2 * time.Second)
	buf.SetFreshStager(fs)
	if err := buf.WriteColumnarDirect(context.Background(), "db", "m", makeColumns(500)); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	_ = buf.Close()
	closeErr := fs.Close()
	pending, err := fs.Pending()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("stager.Close()=%v pending=%d CloseFlushedCleanly=%v", closeErr, pending, buf.CloseFlushedCleanly())
	if pending == 0 {
		t.Fatal("precondition: the outage should have left the staged file in place")
	}
	if !errors.Is(closeErr, ErrStagedNotUploaded) {
		t.Errorf("stager Close returned %v with %d staged file(s) left; want ErrStagedNotUploaded so shutdown keeps the WAL and alerts", closeErr, pending)
	}
}

// B (late). Same contract for the LateStager.
func TestStagerSafety_LateStagerCloseDuringOutageIsReported(t *testing.T) {
	store := newFlakyStore(0, false)
	store.down.Store(true)
	ls := newTestLateStager(t, store, filepath.Join(t.TempDir(), "late"))
	ls.SetCloseBudget(2 * time.Second)
	if err := ls.Stage("db", "m", makeParquet(t, "late-%d", 3, map[string]struct{}{})); err != nil {
		t.Fatal(err)
	}
	closeErr := ls.Close()
	if !errors.Is(closeErr, ErrStagedNotUploaded) {
		t.Errorf("LateStager.Close() = %v during an outage with a staged file; want ErrStagedNotUploaded", closeErr)
	}
	if n, _ := ls.Pending(); n != 1 {
		t.Errorf("Pending() = %d, want 1", n)
	}
}

// B (healthy). With storage up, Close drains and reports clean.
func TestStagerSafety_StagerCloseHealthyIsClean(t *testing.T) {
	store := newFlakyStore(0, false)
	fs := newTestFreshStager(t, store, filepath.Join(t.TempDir(), "fresh"))
	if err := fs.Stage("db", "m", makeParquet(t, "ok-%d", 3, map[string]struct{}{})); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close() with healthy storage = %v, want nil", err)
	}
	if n, _ := fs.Pending(); n != 0 {
		t.Errorf("Pending() after a healthy Close = %d, want 0", n)
	}
	if len(store.keys()) == 0 {
		t.Error("nothing was uploaded")
	}
}

// C. A PUT that committed server-side but returned an error must not be
// re-uploaded under a NEW key by the next flush (HEAD-on-error).
func TestStagerSafety_FreshStagerPhantomPutNoDuplicate(t *testing.T) {
	tmp := t.TempDir()
	store := newFlakyStore(1, true) // first PUT commits but reports failure
	cfg := reviewCfg()
	cfg.MaxBufferSize = 100000
	buf := NewArrowBuffer(cfg, store, zerolog.Nop())
	defer buf.Close()
	fs := newTestFreshStager(t, store, filepath.Join(tmp, "fresh"))
	defer fs.Close()
	buf.SetFreshStager(fs)
	if err := buf.WriteColumnarDirect(context.Background(), "db", "m", makeColumns(500)); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = fs.Flush(context.Background()) // phantom PUT
	time.Sleep(5 * time.Millisecond)
	_ = fs.Flush(context.Background()) // would retry under a new key
	keys := store.keys()
	t.Logf("objects on S3: %v", keys)
	if len(keys) != 1 {
		t.Errorf("DUPLICATE/LOSS: the same 500 rows are %d objects, want 1", len(keys))
	}
	if n, _ := fs.Pending(); n != 0 {
		t.Errorf("Pending() = %d after the object was confirmed present, want 0", n)
	}
}

// C (late). Same HEAD-on-error for the LateStager upload.
func TestStagerSafety_LateStagerPhantomPutNoDuplicate(t *testing.T) {
	store := newFlakyStore(1, true)
	ls := newTestLateStager(t, store, filepath.Join(t.TempDir(), "late"))
	defer ls.Close()
	if err := ls.Stage("db", "m", makeParquet(t, "late-%d", 3, map[string]struct{}{})); err != nil {
		t.Fatal(err)
	}
	_ = ls.Flush(context.Background())
	time.Sleep(5 * time.Millisecond)
	_ = ls.Flush(context.Background())
	if keys := store.keys(); len(keys) != 1 {
		t.Errorf("late rows uploaded as %d objects, want 1: %v", len(keys), keys)
	}
}

// C (partial). A multi-partition FreshStager upload that fails half-way during
// an outage: the rollback deletes fail too, and the retry uploads every
// partition again under new keys.
//
// A merge job records its fixed object keys before the first PUT and resumes
// with them, so the retry uploads only the partition that did not land.
func TestStagerSafety_FreshStagerPartialUploadNoDuplicate(t *testing.T) {
	store := newFlakyStore(0, false)
	store.failDeletes.Store(true) // the outage that failed the PUT fails deletes too
	// makeParquet spreads rows over now, now-1h and now-2h: three partitions,
	// uploaded oldest hour first. The newest one fails.
	last := time.Now().UTC()
	store.failKeyContains = fmt.Sprintf("/%04d/%02d/%02d/%02d/", last.Year(), int(last.Month()), last.Day(), last.Hour())
	fs := newTestFreshStager(t, store, filepath.Join(t.TempDir(), "fresh"))
	defer fs.Close()
	if err := fs.Stage("db", "m", makeParquet(t, "p-%d", 30, map[string]struct{}{})); err != nil {
		t.Fatal(err)
	}
	_ = fs.Flush(context.Background()) // 2 of 3 partitions land, the third fails
	first := store.keys()
	if len(first) != 2 {
		t.Fatalf("precondition: expected 2 of 3 partitions uploaded, got %d: %v", len(first), first)
	}
	if n, _ := fs.Pending(); n != 1 {
		t.Fatalf("the source must stay staged after a partial upload, Pending()=%d", n)
	}

	store.mu.Lock()
	store.failKeyContains = ""
	store.mu.Unlock()
	_ = fs.Flush(context.Background()) // retry
	keys := store.keys()
	t.Logf("objects after retry: %v", keys)
	if len(keys) != 3 {
		t.Errorf("DUPLICATE: 3 partitions are %d objects after the retry, want 3", len(keys))
	}
	for _, k := range first {
		found := false
		for _, k2 := range keys {
			found = found || k == k2
		}
		if !found {
			t.Errorf("object %s from the first attempt is gone: the retry must keep the job's keys", k)
		}
	}
	if n, _ := fs.Pending(); n != 0 {
		t.Errorf("Pending() = %d after the job completed, want 0", n)
	}
}

// B/IW-01 (pod deletion). The stage volume is an emptyDir: rows checkpointed
// out of the WAL and staged locally would be lost with the pod when S3 is
// down at shutdown. With ingest.stager_handoff_url the stager hands them to a
// live peer, which uploads them once the store is back.
func TestStagerSafety_SpoolSurvivesPodDeletion(t *testing.T) {
	store := newFlakyStore(0, false)
	store.down.Store(true)
	tmp := t.TempDir()
	peer, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: filepath.Join(tmp, "peer"), FlushAge: time.Hour,
		Logger: zerolog.Nop(), Options: StagerOptions{PodID: "ingest-b"}})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	srv := newImportPeer(t, map[string]*stagerEngine{"fresh": peer.stagerEngine})

	cfg := reviewCfg()
	cfg.MaxBufferSize = 100000
	buf := NewArrowBuffer(cfg, store, zerolog.Nop())
	podDir := filepath.Join(tmp, "pod-a")
	fs, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: podDir, FlushAge: time.Hour,
		Logger: zerolog.Nop(), Options: StagerOptions{PodID: "ingest-a", HandoffURL: srv.srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	fs.SetCloseBudget(5 * time.Second)
	buf.SetFreshStager(fs)
	if err := buf.WriteColumnarDirect(context.Background(), "db", "m", makeColumns(500)); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = buf.Close()
	if err := fs.Close(); err != nil {
		t.Fatalf("Close during the outage = %v; the staged rows should have gone to the peer", err)
	}
	if err := os.RemoveAll(podDir); err != nil { // the pod and its emptyDir are deleted
		t.Fatal(err)
	}
	if n, _ := peer.Pending(); n != 1 {
		t.Fatalf("peer holds %d staged files, want the 1 handed over", n)
	}
	// The store recovers: the peer drains on its own (its import-triggered
	// drain hit the outage, so its prober brings it back), no restart.
	store.down.Store(false)
	deadline := time.Now().Add(20 * time.Second)
	for {
		if n, _ := peer.Pending(); n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("peer did not upload the handed-over rows within 20s of the store recovering")
		}
		time.Sleep(50 * time.Millisecond)
	}
	var objects []string
	for _, k := range store.keys() {
		if !strings.HasPrefix(k, "_arc/") {
			objects = append(objects, k)
		}
	}
	if len(objects) != 1 {
		t.Fatalf("the 500 rows are %d objects, want exactly 1: %v", len(objects), objects)
	}
}

// D. After a failed flush, a later write whose column changes type must not
// panic in mergeBatches. Pre-merge, restoreToBuffer dropped the schema
// signature and the drifted batch was merged into the restored one.
func TestStagerSafety_FailedFlushThenTypeDriftNoPanic(t *testing.T) {
	store := newFlakyStore(1, false) // first write fails, then OK
	cfg := reviewCfg()
	cfg.MaxBufferSize = 100000
	buf := NewArrowBuffer(cfg, store, zerolog.Nop())
	defer buf.Close()
	now := time.Now().UnixMicro()
	ints := map[string][]interface{}{"time": {now, now + 1}, "v": {int64(1), int64(2)}}
	if err := buf.WriteColumnarDirect(context.Background(), "db", "m", ints); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err == nil {
		t.Fatal("precondition: expected first flush to fail")
	}
	if !buf.HasFlushFailure() {
		t.Error("a failed flush must arm WAL recovery (HasFlushFailure)")
	}
	floats := map[string][]interface{}{"time": {now + 2, now + 3}, "v": {1.5, 2.5}}
	if err := buf.WriteColumnarDirect(context.Background(), "db", "m", floats); err != nil {
		t.Fatal(err)
	}
	var panicked interface{}
	func() {
		defer func() { panicked = recover() }()
		if err := buf.FlushAll(context.Background()); err != nil {
			t.Errorf("second flush: %v", err)
		}
	}()
	if panicked != nil {
		t.Errorf("PANIC in flush after a failed flush + type drift (process crash in a worker): %v", panicked)
	}
}

// E. WAL replay of a seconds-precision payload must be normalized to µs, not
// land in a 1970 partition. Pre-merge the crash-replay path skipped #590.
func TestStagerSafety_ReplayNormalizesTimestamps(t *testing.T) {
	store := newFlakyStore(0, false)
	cfg := reviewCfg()
	cfg.MaxBufferSize = 100000
	buf := NewArrowBuffer(cfg, store, zerolog.Nop())
	defer buf.Close()
	sec := time.Now().Unix()
	cols := map[string][]interface{}{"time": {sec, sec + 1}, "v": {1.0, 2.0}}
	if err := buf.WriteColumnarDirectReplay(context.Background(), "db", "m", cols, ""); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	keys := store.keys()
	if len(keys) != 1 {
		t.Fatalf("want 1 object, got %v", keys)
	}
	for _, k := range keys {
		if strings.Contains(k, "/1970/") {
			t.Errorf("replayed second-precision rows landed in a 1970 partition: %s", k)
		}
	}
}

// F. A replayed entry without a time column (the live decoder generated it,
// the WAL kept the raw bytes) must flush, not poison the buffer forever.
func TestStagerSafety_ReplayMissingTimeFlushes(t *testing.T) {
	store := newFlakyStore(0, false)
	cfg := reviewCfg()
	cfg.MaxBufferSize = 100000
	buf := NewArrowBuffer(cfg, store, zerolog.Nop())
	defer buf.Close()
	cols := map[string][]interface{}{"v": {1.0, 2.0}, "host": {"a", "b"}}
	if err := buf.WriteColumnarDirectReplay(context.Background(), "db", "m", cols, ""); err != nil {
		t.Fatalf("replay rejected: %v", err)
	}
	for i := 0; i < 3; i++ {
		_ = buf.FlushAll(context.Background())
	}
	if buf.HasFlushFailure() || len(store.keys()) != 1 {
		t.Errorf("replayed entry without time did not flush cleanly: objects=%d HasFlushFailure=%v", len(store.keys()), buf.HasFlushFailure())
	}
}

// recoverInto replays every non-active WAL file in dir into buf, the way
// cmd/arc's startup recovery does (tracked rows and columnar entries inherit
// their WAL identity), and returns the stats.
func recoverInto(t *testing.T, dir string, active string, buf *ArrowBuffer) *wal.RecoveryStats {
	t.Helper()
	replayRows := func(ctx context.Context, records []map[string]interface{}, identity string) error {
		if len(records) == 0 {
			return nil
		}
		db, m, cols, err := WALRowsToColumns(records)
		if err != nil {
			return err
		}
		return buf.WriteColumnarDirectReplay(ctx, db, m, cols, identity)
	}
	rec := wal.NewRecovery(dir, zerolog.Nop())
	stats, err := rec.RecoverWithOptions(context.Background(),
		func(ctx context.Context, records []map[string]interface{}) error { return replayRows(ctx, records, "") },
		&wal.RecoveryOptions{
			SkipActiveFile:     active,
			TrackedRowCallback: replayRows,
			ColumnarCallback:   buf.WriteColumnarDirectReplay,
			BeforeDelete:       buf.NewRecoveryFlushBarrier(),
		})
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

// G. After a crash, startup recovery must not replay rows that were already
// flushed (upstream checkpoints #998/#1048). Pre-merge it replayed the whole
// previously-active WAL file.
func TestStagerSafety_CrashReplaySkipsFlushedRows(t *testing.T) {
	dir := t.TempDir()
	w1, err := wal.NewWriter(&wal.WriterConfig{WALDir: dir, SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	store1 := newFlakyStore(0, false)
	cfg := reviewCfg()
	cfg.MaxBufferSize = 100000
	buf1 := NewArrowBuffer(cfg, store1, zerolog.Nop())
	buf1.SetWAL(w1)
	if err := buf1.WriteColumnarDirect(context.Background(), "db", "m", makeColumns(500)); err != nil {
		t.Fatal(err)
	}
	if err := buf1.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store1.keys()) != 1 {
		t.Fatalf("precondition: expected 1 flushed object, got %d", len(store1.keys()))
	}
	waitWALPersisted(t, w1)

	// "crash": no Close, no purge. New process:
	w2, err := wal.NewWriter(&wal.WriterConfig{WALDir: dir, SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	store2 := newFlakyStore(0, false)
	buf2 := NewArrowBuffer(cfg, store2, zerolog.Nop())
	defer buf2.Close()
	buf2.SetWAL(w2)
	stats := recoverInto(t, dir, w2.CurrentFile(), buf2)
	_ = buf2.FlushAll(context.Background())
	written := buf2.GetStats()["total_records_written"].(int64)
	t.Logf("recovered entries=%d files=%d re-flushed rows=%d objects=%d", stats.RecoveredEntries, stats.RecoveredFiles, written, len(store2.keys()))
	if written != 0 || len(store2.keys()) != 0 {
		t.Errorf("DUPLICATE on crash: %d already-flushed rows replayed into %d new object(s)", written, len(store2.keys()))
	}
}

// G (staged). The merge's central invariant: a staged flush checkpoints its
// WAL identities (the staged file is fsynced and is the durable copy), so a
// crash replays nothing, and the surviving staged file is uploaded exactly
// once by the next process's stager.
func TestStagerSafety_CrashReplaySkipsStagedRows(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	stageDir := filepath.Join(dir, "fresh")
	w1, err := wal.NewWriter(&wal.WriterConfig{WALDir: walDir, SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	store := newFlakyStore(0, false)
	store.down.Store(true) // S3 is down: the rows exist only in the stage dir
	cfg := reviewCfg()
	cfg.MaxBufferSize = 100000
	buf1 := NewArrowBuffer(cfg, store, zerolog.Nop())
	buf1.SetWAL(w1)
	fs1 := newTestFreshStager(t, store, stageDir)
	buf1.SetFreshStager(fs1)
	if err := buf1.WriteColumnarDirect(context.Background(), "db", "m", makeColumns(500)); err != nil {
		t.Fatal(err)
	}
	if err := buf1.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, _ := fs1.Pending(); n != 1 {
		t.Fatalf("precondition: expected 1 staged file, got %d", n)
	}
	if buf1.HasFlushFailure() {
		t.Fatal("a successful stage must not arm WAL recovery")
	}
	waitWALPersisted(t, w1)

	// "crash": no Close of the buffer, stager or WAL. New process, same volumes.
	w2, err := wal.NewWriter(&wal.WriterConfig{WALDir: walDir, SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	buf2 := NewArrowBuffer(cfg, store, zerolog.Nop())
	defer buf2.Close()
	buf2.SetWAL(w2)
	fs2 := newTestFreshStager(t, store, stageDir)
	defer fs2.Close()
	buf2.SetFreshStager(fs2)

	stats := recoverInto(t, walDir, w2.CurrentFile(), buf2)
	_ = buf2.FlushAll(context.Background())
	if written := buf2.GetStats()["total_records_written"].(int64); written != 0 {
		t.Errorf("DUPLICATE on crash: %d staged rows replayed from the WAL (recovered entries=%d)", written, stats.RecoveredEntries)
	}
	if n, _ := fs2.Pending(); n != 1 {
		t.Errorf("staged file count after recovery = %d, want the 1 surviving file", n)
	}

	store.down.Store(false) // S3 back: the new process uploads the survivor once
	if err := fs2.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if keys := store.keys(); len(keys) != 1 {
		t.Errorf("surviving staged file uploaded as %d objects, want 1: %v", len(keys), keys)
	}
}

// waitWALPersisted waits until the writer's background loop has written and
// synced everything handed to it, including flush checkpoints.
func waitWALPersisted(t *testing.T, w *wal.Writer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for w.PendingUnflushedCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("WAL still has %d unflushed tracked entries", w.PendingUnflushedCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Checkpoints are queued to the same writer loop; give it a sync cycle.
	time.Sleep(300 * time.Millisecond)
}

// ---- checkpoint-at-stage invariants ----------------------------------------

// A staged flush (single hour, fresh) checkpoints the batch's WAL identity.
func TestStagerSafety_StagedFlushCheckpointsWAL(t *testing.T) {
	buf, walWriter := newCheckpointBuffer(1000)
	defer buf.Close()
	fs := newTestFreshStager(t, newFlakyStore(0, false), filepath.Join(t.TempDir(), "fresh"))
	defer fs.Close()
	buf.SetFreshStager(fs)

	if err := buf.writeColumnarInternal(context.Background(), "default", checkpointTestRecord(), false, ""); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, _ := fs.Pending(); n != 1 {
		t.Fatalf("expected the flush to be staged, pending=%d", n)
	}
	handedOut, flushed := walWriter.counts()
	if handedOut != 1 || flushed != 1 {
		t.Errorf("identities handed out=%d checkpointed=%d; a durable stage must checkpoint", handedOut, flushed)
	}
}

// A failed stage must NOT checkpoint: the WAL is then the only copy.
func TestStagerSafety_FailedStageDoesNotCheckpoint(t *testing.T) {
	buf, walWriter := newCheckpointBuffer(1000)
	defer buf.Close()
	stageDir := filepath.Join(t.TempDir(), "fresh")
	fs := newTestFreshStager(t, newFlakyStore(0, false), stageDir)
	defer fs.Close()
	buf.SetFreshStager(fs)
	breakStageDir(t, stageDir)

	if err := buf.writeColumnarInternal(context.Background(), "default", checkpointTestRecord(), false, ""); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err == nil {
		t.Fatal("precondition: the stage should have failed")
	}
	if _, flushed := walWriter.counts(); flushed != 0 {
		t.Errorf("a failed stage checkpointed %d identities; the WAL would be purged with the rows nowhere else", flushed)
	}
	if !buf.HasFlushFailure() {
		t.Error("a failed stage must arm WAL recovery (HasFlushFailure)")
	}
}

// A multi-hour flush that stages its late group and then fails to stage the
// fresh group must take the late file back: the batch is not checkpointed, so
// WAL recovery re-flushes ALL of it, and a staged late file left behind would
// be uploaded a second time.
func TestStagerSafety_MultiHourPartialStageRollsBack(t *testing.T) {
	buf, walWriter := newCheckpointBuffer(1000)
	defer buf.Close()
	buf.config.LateWindowSeconds = 3600
	buf.config.FutureSkewSeconds = 300
	tmp := t.TempDir()
	store := newFlakyStore(0, false)
	ls := newTestLateStager(t, store, filepath.Join(tmp, "late"))
	defer ls.Close()
	freshDir := filepath.Join(tmp, "fresh")
	fs := newTestFreshStager(t, store, freshDir)
	defer fs.Close()
	buf.SetLateStager(ls)
	buf.SetFreshStager(fs)
	breakStageDir(t, freshDir)

	now := time.Now().UTC()
	rec := checkpointTestRecord()
	rec.Columns = map[string][]interface{}{
		"time":  {now.Add(-5 * time.Hour).UnixMicro(), now.UnixMicro()},
		"value": {1.0, 2.0},
	}
	if err := buf.writeColumnarInternal(context.Background(), "default", rec, false, ""); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err == nil {
		t.Fatal("precondition: the fresh stage should have failed")
	}
	if n, _ := ls.Pending(); n != 0 {
		t.Errorf("late stager still holds %d file(s) from the failed flush; WAL recovery will stage the same rows again", n)
	}
	if _, flushed := walWriter.counts(); flushed != 0 {
		t.Errorf("a partially staged flush checkpointed %d identities", flushed)
	}
	if !buf.HasFlushFailure() {
		t.Error("a partially staged flush must arm WAL recovery (HasFlushFailure)")
	}
}

// A multi-hour flush with both stagers healthy stages one late file and one
// fresh file and checkpoints once.
func TestStagerSafety_MultiHourStagedCommitsBothGroups(t *testing.T) {
	buf, walWriter := newCheckpointBuffer(1000)
	defer buf.Close()
	buf.config.LateWindowSeconds = 3600
	buf.config.FutureSkewSeconds = 300
	tmp := t.TempDir()
	store := newFlakyStore(0, false)
	ls := newTestLateStager(t, store, filepath.Join(tmp, "late"))
	defer ls.Close()
	fs := newTestFreshStager(t, store, filepath.Join(tmp, "fresh"))
	defer fs.Close()
	buf.SetLateStager(ls)
	buf.SetFreshStager(fs)

	now := time.Now().UTC()
	rec := checkpointTestRecord()
	rec.Columns = map[string][]interface{}{
		"time":  {now.Add(-5 * time.Hour).UnixMicro(), now.Add(-6 * time.Hour).UnixMicro(), now.UnixMicro()},
		"value": {1.0, 2.0, 3.0},
	}
	if err := buf.writeColumnarInternal(context.Background(), "default", rec, false, ""); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	lateN, _ := ls.Pending()
	freshN, _ := fs.Pending()
	if lateN != 1 || freshN != 1 {
		t.Errorf("staged late=%d fresh=%d, want 1 and 1", lateN, freshN)
	}
	if _, flushed := walWriter.counts(); flushed != 1 {
		t.Errorf("checkpointed %d identities, want 1", flushed)
	}
	if got := buf.GetStats()["total_records_written"].(int64); got != 3 {
		t.Errorf("total_records_written = %d, want 3", got)
	}
}

// ---- inline exists-check (fork) --------------------------------------------

// An inline write whose PUT committed but returned an error is treated as
// written (HEAD-on-error), so the batch is checkpointed and not replayed into
// a second object.
func TestStagerSafety_InlinePhantomPutIsWritten(t *testing.T) {
	store := newFlakyStore(1, true)
	cfg := reviewCfg()
	cfg.MaxBufferSize = 100000
	buf := NewArrowBuffer(cfg, store, zerolog.Nop())
	defer buf.Close()
	if err := buf.WriteColumnarDirect(context.Background(), "db", "m", makeColumns(10)); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatalf("phantom PUT treated as failure: %v", err)
	}
	if buf.HasFlushFailure() {
		t.Error("phantom PUT armed WAL recovery; replay would write the rows a second time")
	}
	if keys := store.keys(); len(keys) != 1 {
		t.Errorf("objects = %v, want exactly 1", keys)
	}
}

// ---- partitionWriteContext --------------------------------------------------

// A partition write gets a fresh budget when the flush's own deadline has
// passed, but still stops on an explicit cancel and on flushParent's cancel.
func TestStagerSafety_PartitionWriteContext(t *testing.T) {
	buf := NewArrowBuffer(reviewCfg(), newFlakyStore(0, false), zerolog.Nop())
	defer buf.Close()

	expired, cancelExpired := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancelExpired()
	<-expired.Done()
	part, cancel := buf.partitionWriteContext(expired)
	if err := part.Err(); err != nil {
		t.Errorf("partition context inherited the expired flush deadline: %v", err)
	}
	cancel()

	parent, cancelParent := context.WithCancel(context.Background())
	part, cancel = buf.partitionWriteContext(parent)
	defer cancel()
	cancelParent()
	select {
	case <-part.Done():
	case <-time.After(time.Second):
		t.Error("partition context ignored an explicit cancel of its parent")
	}
}

// Sanity: the staged file name keeps the db/measurement grouping the stager
// flush relies on.
func TestStagerSafety_StageFileNameGroups(t *testing.T) {
	fs := newTestFreshStager(t, newFlakyStore(0, false), filepath.Join(t.TempDir(), "fresh"))
	defer fs.Close()
	path, err := fs.stageFile("db", "m", makeParquet(t, "x-%d", 1, map[string]struct{}{}))
	if err != nil {
		t.Fatal(err)
	}
	if base := filepath.Base(path); !strings.HasPrefix(base, fmt.Sprintf("%s__%s__fresh_stage_", "db", "m")) {
		t.Errorf("unexpected staged name %q", base)
	}
}

// The FreshStager files merged rows under extract(hour FROM time). Without a
// UTC session the extraction followed the host's time zone, so on a non-UTC
// host every staged row was uploaded to the wrong hour (or day) partition.
func TestStagerSafety_FreshStagerPartitionsInUTC(t *testing.T) {
	store := newFlakyStore(0, false)
	cfg := reviewCfg()
	cfg.MaxBufferSize = 100000
	buf := NewArrowBuffer(cfg, store, zerolog.Nop())
	defer buf.Close()
	fs := newTestFreshStager(t, store, filepath.Join(t.TempDir(), "fresh"))
	defer fs.Close()
	buf.SetFreshStager(fs)

	at := time.Date(2026, 1, 2, 3, 30, 0, 0, time.UTC)
	cols := map[string][]interface{}{
		"time":  {at.UnixMicro(), at.Add(time.Minute).UnixMicro()},
		"value": {1.0, 2.0},
	}
	if err := buf.WriteColumnarDirect(context.Background(), "db", "m", cols); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fs.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	keys := store.keys()
	if len(keys) != 1 || !strings.HasPrefix(keys[0], "db/m/2026/01/02/03/") {
		t.Errorf("staged rows at %s uploaded to %v, want one object under db/m/2026/01/02/03/", at, keys)
	}
}

// Valid measurement names (^[a-zA-Z][a-zA-Z0-9_-]*$) with "__" or a trailing
// underscore used to be staged under an irreversible name: "a__b" uploaded as
// "a_b", and "m_" produced "db__m___fresh_stage_N", which the "__" split never
// matched, so the file was never uploaded (and its rows are checkpointed out of
// the WAL once staged).
func TestStagerSafety_StageNamesRoundTrip(t *testing.T) {
	for _, tc := range []struct{ db, m string }{
		{"db", "a__b"},
		{"db", "m_"},
		{"db_", "m"},
		{"my-db", "x___y_"},
	} {
		t.Run(tc.db+"/"+tc.m, func(t *testing.T) {
			if got := decodeStagerSegment(encodeStagerSegment(tc.m)); got != tc.m {
				t.Fatalf("round trip %q -> %q", tc.m, got)
			}
			store := newFlakyStore(0, false)
			fs := newTestFreshStager(t, store, filepath.Join(t.TempDir(), "fresh"))
			ls := newTestLateStager(t, store, filepath.Join(t.TempDir(), "late"))
			data := makeParquet(t, "rt-%d", 1, map[string]struct{}{})
			if err := fs.Stage(tc.db, tc.m, data); err != nil {
				t.Fatal(err)
			}
			if err := ls.Stage(tc.db, tc.m, data); err != nil {
				t.Fatal(err)
			}
			if n, _ := fs.Pending(); n != 1 {
				t.Errorf("fresh Pending() = %d, want 1 (file not recognised as staged)", n)
			}
			if err := fs.Close(); err != nil {
				t.Errorf("fresh Close: %v", err)
			}
			if err := ls.Close(); err != nil {
				t.Errorf("late Close: %v", err)
			}
			var fresh, late int
			for _, k := range store.keys() {
				switch {
				case strings.HasPrefix(k, tc.db+"/"+tc.m+"/"):
					fresh++
				case strings.HasPrefix(k, tc.db+"/"+tc.m+LateSuffix+"/"):
					late++
				default:
					t.Errorf("object uploaded under the wrong measurement: %s", k)
				}
			}
			if fresh == 0 || late != 1 {
				t.Errorf("uploads fresh=%d late=%d, want >=1 and 1: %v", fresh, late, store.keys())
			}
		})
	}
}

// Files staged by a pre-merge binary (plain segments, no '%') still group
// under their original names.
func TestStagerSafety_LegacyStageNamesStillUpload(t *testing.T) {
	store := newFlakyStore(0, false)
	dir := filepath.Join(t.TempDir(), "fresh")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data := makeParquet(t, "legacy-%d", 1, map[string]struct{}{})
	if err := os.WriteFile(filepath.Join(dir, "posthog__app_events__fresh_stage_7.parquet"), data, 0600); err != nil {
		t.Fatal(err)
	}
	fs := newTestFreshStager(t, store, dir)
	if err := fs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	keys := store.keys()
	if len(keys) == 0 {
		t.Fatal("legacy staged file was not uploaded")
	}
	for _, k := range keys {
		if !strings.HasPrefix(k, "posthog/app_events/") {
			t.Errorf("legacy staged file uploaded to %s, want posthog/app_events/...", k)
		}
	}
}
