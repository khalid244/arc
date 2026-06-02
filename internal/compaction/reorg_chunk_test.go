package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestReorg_ChunkedDrain_MonotonicProgressOnDownloadFailure is the regression
// test for the prod death-spiral: processBucket used to download EVERY source
// file in a bucket before committing anything, so a single transient Ceph RGW
// 504 (or the cycle deadline) failed the entire bucket pre-manifest. The big
// 05-30 storm buckets (90-113K tiny files) therefore never drained — every
// cycle re-read them from scratch and re-failed.
//
// With chunked draining (each MaxFilesPerBatch slice is an independent,
// atomically-committed unit) a download failure forfeits ONLY the in-flight
// chunk; every other chunk still commits, so the bucket makes monotonic forward
// progress and a clean retry finishes it losslessly.
//
// Setup: 6 single-row late files (one distinct midday event-day each),
// MaxFilesPerBatch=2 => 3 chunks. failOnReadN=3 fails a download in the SECOND
// chunk. We assert exactly that chunk's 2 sources survive (4 drained), no
// manifest leaks (the failure is pre-manifest), then a disarmed re-run drains
// the rest with no loss and no duplication.
func TestReorg_ChunkedDrain_MonotonicProgressOnDownloadFailure(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)

	local, err := storage.NewLocalBackend(tmp, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	fb := &faultBackend{LocalBackend: local, failOnReadN: 3}

	lateDir := filepath.Join(tmp, "posthog", "events_late")
	if err := os.MkdirAll(lateDir, 0o755); err != nil {
		t.Fatal(err)
	}

	const numFiles = 6
	db, _ := sql.Open("duckdb", "")
	for f := 0; f < numFiles; f++ {
		// One midday row at a distinct day per file (TZ-safe day boundary), and
		// lexically-sortable names so List() yields deterministic chunk
		// membership: chunk0={_0,_1}, chunk1={_2,_3}, chunk2={_4,_5}.
		writeLateParquet(t, db, lateDir, fmt.Sprintf("events_late_20260520_030000_%d.parquet", f), fmt.Sprintf(`
			SELECT TIMESTAMP '2026-04-01 12:00:00' + (%d * INTERVAL 1 DAY) AS time, ('h'||%d)::VARCHAR AS host, %d::BIGINT AS value`,
			f, f, f))
	}
	db.Close()

	reorg := newReorg(fb, filepath.Join(tmp, "scratch"), logger)
	reorg.MaxFilesPerBatch = 2 // 3 chunks of 2 files each
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)

	// Run 1: the 3rd download (inside chunk 1) fails. Run() logs per-bucket and
	// returns nil; assert on side effects.
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run (run1) unexpected top-level error: %v", err)
	}

	// Monotonic progress: only the failed chunk's 2 sources remain. The other
	// two chunks committed (their sources deleted). On the OLD all-or-nothing
	// code this would be 6 (whole bucket aborted) — the RED assertion.
	if remaining := countParquetFlat(t, lateDir); remaining != 2 {
		t.Fatalf("chunked drain broken: expected 2 source files remaining (one failed chunk), got %d", remaining)
	}

	// Pre-manifest failure: no leaked manifest from the failed chunk, and the
	// committed chunks cleaned theirs up.
	if keys, _ := reorg.ManifestManager.List(ctx); len(keys) != 0 {
		t.Errorf("expected 0 manifests after run1 (download fail is pre-manifest), got %d (%v)", len(keys), keys)
	}

	eventsDir := filepath.Join(tmp, "posthog", "events")
	total1, perDay1 := outputRowsByDay(t, eventsDir)
	if total1 != 4 {
		t.Errorf("expected 4 rows drained by the two committed chunks, got %d", total1)
	}
	for k, v := range perDay1 {
		if v != 1 {
			t.Errorf("duplicate in committed chunk: day %s has %d rows (want 1)", k, v)
		}
	}

	// Run 2: disarm the fault. The remaining chunk drains; total is lossless and
	// duplicate-free across the whole bucket.
	fb.failOnReadN = 0
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run (run2) error: %v", err)
	}
	if remaining := countParquetFlat(t, lateDir); remaining != 0 {
		t.Errorf("after clean retry, expected events_late drained to 0, got %d", remaining)
	}
	total2, perDay2 := outputRowsByDay(t, eventsDir)
	if total2 != numFiles {
		t.Errorf("not lossless: expected %d total rows, got %d", numFiles, total2)
	}
	if len(perDay2) != numFiles {
		t.Errorf("expected %d distinct day partitions, got %d", numFiles, len(perDay2))
	}
	for k, v := range perDay2 {
		if v != 1 {
			t.Errorf("DUPLICATE after retry: day %s has %d rows (want 1)", k, v)
		}
	}
}

// TestReorg_ChunkedDrain_MultiChunkLossless verifies the happy-path chunking:
// a bucket larger than MaxFilesPerBatch is split into several chunks that each
// commit independently, and the union is lossless with one output per event-day.
func TestReorg_ChunkedDrain_MultiChunkLossless(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, _ := storage.NewLocalBackend(tmp, logger)

	lateDir := filepath.Join(tmp, "posthog", "events_late")
	_ = os.MkdirAll(lateDir, 0o755)

	const numFiles = 10
	db, _ := sql.Open("duckdb", "")
	for f := 0; f < numFiles; f++ {
		writeLateParquet(t, db, lateDir, fmt.Sprintf("events_late_20260520_030000_%d.parquet", f), fmt.Sprintf(`
			SELECT TIMESTAMP '2026-04-01 12:00:00' + (%d * INTERVAL 1 DAY) AS time, ('h'||%d)::VARCHAR AS host, %d::BIGINT AS value`,
			f, f, f))
	}
	db.Close()

	reorg := newReorg(backend, filepath.Join(tmp, "scratch"), logger)
	reorg.MaxFilesPerBatch = 3 // 10 files -> chunks of 3,3,3,1
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run: %v", err)
	}

	if remaining := countParquetFlat(t, lateDir); remaining != 0 {
		t.Errorf("expected fully drained, %d remain", remaining)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	total, perDay := outputRowsByDay(t, eventsDir)
	if total != numFiles {
		t.Errorf("expected %d rows, got %d", numFiles, total)
	}
	if len(perDay) != numFiles {
		t.Errorf("expected %d day partitions, got %d", numFiles, len(perDay))
	}
	for k, v := range perDay {
		if v != 1 {
			t.Errorf("day %s has %d rows, want 1 (chunk boundary caused dup?)", k, v)
		}
	}
}
