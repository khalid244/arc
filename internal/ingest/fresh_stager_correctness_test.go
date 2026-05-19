package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

// TestFreshStager_NoLossNoDup verifies per-record-id correctness:
//   - every unique row_id that the writers send appears in output
//   - no row_id appears twice in output
//
// Strategy: each writer emits records that include a unique "row_id" string
// column (writer_idx * 10^9 + counter). After all writes + final flush,
// we scan all output parquets, collect every row_id, and assert:
//   - count(row_ids) == count(unique row_ids)  -> no duplication
//   - set(row_ids) == set(expected_row_ids)    -> no loss
func TestFreshStager_NoLossNoDup(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.WarnLevel)

	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	cfg := &config.IngestConfig{
		MaxBufferSize:   600000,
		MaxBufferAgeMS:  30000,
		FlushWorkers:    16,
		FlushQueueSize:  1500,
		ShardCount:      32,
		Compression:     "zstd",
		DataPageVersion: "2.0",
	}
	buf := NewArrowBuffer(cfg, store, logger)
	defer buf.Close()

	fs, err := NewFreshStager(&FreshStagerConfig{
		Storage:  store,
		StageDir: filepath.Join(tmpDir, "_fresh_stager"),
		FlushAge: 2 * time.Second,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	buf.SetFreshStager(fs)

	// Build expected set as we write.
	expected := make(map[string]struct{})
	var mu sync.Mutex
	add := func(id string) {
		mu.Lock()
		expected[id] = struct{}{}
		mu.Unlock()
	}

	// 3 writers, mixed schemas, 4-hour spread.
	const writers = 3
	const recordsPerWriter = 6000
	const batchSize = 50
	const numSchemas = 10

	now := time.Now().UTC()
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			counter := int64(0)
			for written := 0; written < recordsPerWriter; written += batchSize {
				sIdx := (wid + written/batchSize) % numSchemas
				cols := makeColumnSet(sIdx)

				data := make(map[string][]interface{}, len(cols)+2)
				// time column
				times := make([]interface{}, batchSize)
				rowids := make([]interface{}, batchSize)
				for i := 0; i < batchSize; i++ {
					counter++
					rid := fmt.Sprintf("w%d-%d", wid, counter)
					add(rid)
					rowids[i] = rid
					times[i] = now.Add(-time.Duration(i%4) * time.Hour).UnixMicro()
				}
				data["time"] = times
				data["row_id"] = rowids
				for _, c := range cols {
					colvals := make([]interface{}, batchSize)
					for i := range colvals {
						colvals[i] = "v"
					}
					data[c] = colvals
				}
				rec := &models.ColumnarRecord{Measurement: "events", Columns: data}
				if err := buf.writeColumnar(context.Background(), "posthog", rec); err != nil {
					t.Errorf("writeColumnar: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Drain stager
	time.Sleep(3 * time.Second)
	if err := fs.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	rowIDs, err := collectAllRowIDs(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("expected unique row_ids: %d", len(expected))
	t.Logf("collected row_ids:       %d", len(rowIDs))

	// Distinct count
	uniqueCollected := make(map[string]int, len(rowIDs))
	for _, id := range rowIDs {
		uniqueCollected[id]++
	}
	dup := 0
	for _, c := range uniqueCollected {
		if c > 1 {
			dup++
		}
	}
	missing := 0
	for id := range expected {
		if _, ok := uniqueCollected[id]; !ok {
			missing++
		}
	}
	extra := 0
	for id := range uniqueCollected {
		if _, ok := expected[id]; !ok {
			extra++
		}
	}

	t.Logf("unique row_ids in output:   %d", len(uniqueCollected))
	t.Logf("row_ids appearing >1x:      %d  (DUPLICATION)", dup)
	t.Logf("expected ids missing:       %d  (LOSS)", missing)
	t.Logf("output ids never written:   %d  (PHANTOM)", extra)

	if dup > 0 {
		t.Errorf("DUPLICATION detected: %d row_ids appear more than once", dup)
	}
	if missing > 0 {
		t.Errorf("DATA LOSS detected: %d expected row_ids are missing", missing)
	}
	if extra > 0 {
		t.Errorf("PHANTOM data: %d output row_ids were never written", extra)
	}
}

// TestFreshStager_CrashRestartIdempotent simulates a pod restart between
// stage and upload: stage files, close stager WITHOUT flushing, reopen,
// flush, count rows. No duplicates, no loss.
func TestFreshStager_CrashRestartIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.WarnLevel)
	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	stageDir := filepath.Join(tmpDir, "_fresh_stager")
	if err := os.MkdirAll(stageDir, 0700); err != nil {
		t.Fatal(err)
	}

	// Simulate "before crash" staged files by writing directly to the
	// stager directory using the filename convention. This bypasses the
	// running-stager's clean-shutdown flush which would otherwise drain
	// everything and defeat the test.
	const groups = 5
	const rowsPerGroup = 100
	expected := make(map[string]struct{})
	for g := 0; g < groups; g++ {
		parquet := makeParquet(t, fmt.Sprintf("group-%d-row-%%d", g), rowsPerGroup, expected)
		name := fmt.Sprintf("posthog__events__fresh_stage_%d.parquet", g+1)
		if err := os.WriteFile(filepath.Join(stageDir, name), parquet, 0600); err != nil {
			t.Fatal(err)
		}
	}

	// Verify staged files still on disk before the restarted stager runs.
	entries, _ := os.ReadDir(stageDir)
	stagedCount := 0
	for _, e := range entries {
		if strings.Contains(e.Name(), "fresh_stage_") {
			stagedCount++
		}
	}
	if stagedCount != groups {
		t.Fatalf("expected %d staged files pre-restart, found %d", groups, stagedCount)
	}

	// Stager #2: restart, flush, verify.
	fs2, err := NewFreshStager(&FreshStagerConfig{
		Storage: store, StageDir: stageDir,
		FlushAge: 1 * time.Hour,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs2.Close()
	if err := fs2.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	rowIDs, err := collectAllRowIDs(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after restart: expected=%d  collected=%d", len(expected), len(rowIDs))

	uniq := make(map[string]int)
	for _, id := range rowIDs {
		uniq[id]++
	}
	dups := 0
	for _, c := range uniq {
		if c > 1 {
			dups++
		}
	}
	missing := 0
	for id := range expected {
		if _, ok := uniq[id]; !ok {
			missing++
		}
	}
	if dups != 0 || missing != 0 {
		t.Errorf("crash/restart: dups=%d missing=%d", dups, missing)
	}
}

// TestFreshStager_DoubleFlushIdempotent calls Flush() twice in succession
// with no Stage between. Second call should be a no-op (no extra files, no
// new uploads). Catches a regression where the stager would re-upload the
// same merge output.
func TestFreshStager_DoubleFlushIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.WarnLevel)
	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	fs, err := NewFreshStager(&FreshStagerConfig{
		Storage: store, StageDir: filepath.Join(tmpDir, "_fresh_stager"),
		FlushAge: 1 * time.Hour, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	expected := make(map[string]struct{})
	for g := 0; g < 3; g++ {
		p := makeParquet(t, fmt.Sprintf("g%d-%%d", g), 50, expected)
		if err := fs.Stage("posthog", "events", p); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := countOutputParquets(t, tmpDir)

	// Call Flush again — should find nothing staged and do nothing.
	if err := fs.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := countOutputParquets(t, tmpDir)

	if first != second {
		t.Errorf("double-flush not idempotent: first=%d  second=%d", first, second)
	}

	// Also verify no duplication of row_ids across flushes.
	rowIDs, _ := collectAllRowIDs(tmpDir)
	uniq := make(map[string]int)
	for _, id := range rowIDs {
		uniq[id]++
	}
	dups := 0
	for _, c := range uniq {
		if c > 1 {
			dups++
		}
	}
	if dups != 0 {
		t.Errorf("double-flush: %d row_ids appear >1x", dups)
	}
	if len(uniq) != len(expected) {
		t.Errorf("double-flush: expected=%d  got=%d", len(expected), len(uniq))
	}
}

// ---- helpers ----

// makeColumnSet returns a column set (excluding "time" and "row_id") for schema idx.
func makeColumnSet(idx int) []string {
	cols := []string{}
	n := 3 + (idx % 5)
	for i := 0; i < n; i++ {
		cols = append(cols, fmt.Sprintf("prop_%02d_%02d", idx, i))
	}
	return cols
}

// makeParquet writes a small parquet directly via DuckDB so we can stage it.
// Each row's row_id is filled from the pattern with sequential ids; the
// expected set is grown.
func makeParquet(t *testing.T, idPattern string, n int, expected map[string]struct{}) []byte {
	t.Helper()
	tmpFile := filepath.Join(t.TempDir(), fmt.Sprintf("stage_%d.parquet", time.Now().UnixNano()))

	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	var values []string
	now := time.Now().UTC()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf(idPattern, i)
		expected[id] = struct{}{}
		// Use a TIMESTAMP literal slightly shifted so multi-hour partitioning is exercised.
		ts := now.Add(-time.Duration(i%3) * time.Hour).Format("2006-01-02 15:04:05.000000")
		values = append(values, fmt.Sprintf("(TIMESTAMP '%s', '%s')", ts, id))
	}
	q := fmt.Sprintf(`
		COPY (
			SELECT * FROM (VALUES %s) AS t(time, row_id)
		) TO '%s' (FORMAT PARQUET);
	`, strings.Join(values, ","), tmpFile)
	if _, err := d.Exec(q); err != nil {
		t.Fatalf("makeParquet: %v", err)
	}
	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// collectAllRowIDs reads every parquet file under tmpDir/posthog/events/...
// (excluding any _fresh_stager/ scratch) and returns the row_id of each row.
func collectAllRowIDs(tmpDir string) ([]string, error) {
	var paths []string
	_ = filepath.Walk(tmpDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(p, ".parquet") {
			return nil
		}
		if strings.Contains(p, "_fresh_stager") {
			return nil
		}
		paths = append(paths, p)
		return nil
	})
	if len(paths) == 0 {
		return nil, nil
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	defer d.Close()
	// Build a DuckDB array literal.
	var b strings.Builder
	b.WriteByte('[')
	for i, p := range paths {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('\'')
		b.WriteString(strings.ReplaceAll(strings.ReplaceAll(p, "\\", "\\\\"), "'", "''"))
		b.WriteByte('\'')
	}
	b.WriteByte(']')
	q := fmt.Sprintf(`SELECT row_id FROM read_parquet(%s, union_by_name=true) WHERE row_id IS NOT NULL`, b.String())
	rows, err := d.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// TestFreshStager_TypeCollision exercises what happens when two staged
// parquets carry a column of the SAME NAME but DIFFERENT TYPES (int vs
// string). Two outcomes are acceptable:
//   (a) DuckDB's union_by_name fails → the staged source files MUST remain
//       on disk for a future flush retry, NOT silently deleted (no loss).
//   (b) DuckDB's union_by_name succeeds via implicit cast → no loss, no dup.
// Either outcome is data-safe. The test asserts no source files vanish if
// the merge errors.
func TestFreshStager_TypeCollision(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.WarnLevel)
	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stageDir := filepath.Join(tmpDir, "_fresh_stager")
	if err := os.MkdirAll(stageDir, 0700); err != nil {
		t.Fatal(err)
	}

	// File A: column x is INTEGER
	intParquet := makeOneColParquet(t, "x", "INTEGER", []string{"1", "2", "3"})
	if err := os.WriteFile(filepath.Join(stageDir, "posthog__events__fresh_stage_1.parquet"), intParquet, 0600); err != nil {
		t.Fatal(err)
	}
	// File B: column x is VARCHAR
	strParquet := makeOneColParquet(t, "x", "VARCHAR", []string{"'a'", "'b'", "'c'"})
	if err := os.WriteFile(filepath.Join(stageDir, "posthog__events__fresh_stage_2.parquet"), strParquet, 0600); err != nil {
		t.Fatal(err)
	}

	fs, err := NewFreshStager(&FreshStagerConfig{
		Storage: store, StageDir: stageDir, FlushAge: 1 * time.Hour, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	flushErr := fs.Flush(context.Background())
	t.Logf("Flush err: %v", flushErr)

	// Check what's left in stageDir.
	entries, _ := os.ReadDir(stageDir)
	stagedLeft := 0
	for _, e := range entries {
		if strings.Contains(e.Name(), "fresh_stage_") {
			stagedLeft++
		}
	}
	t.Logf("staged files remaining after Flush attempt: %d", stagedLeft)

	if flushErr != nil {
		// Merge failed → sources MUST still be there (next flush retries).
		if stagedLeft != 2 {
			t.Errorf("merge failed but %d source files remain (expected 2)", stagedLeft)
		}
		t.Logf("OK: merge errored on type collision, sources preserved for retry")
	} else {
		// Merge succeeded → both files merged + uploaded + deleted.
		if stagedLeft != 0 {
			t.Errorf("merge succeeded but %d source files remain (expected 0)", stagedLeft)
		}
		// 3 INT + 3 VARCHAR = 6 rows total (DuckDB casts somehow).
		rowCount := 0
		_ = filepath.Walk(tmpDir, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".parquet") || strings.Contains(p, "_fresh_stager") {
				return nil
			}
			if n, err := countRowsInParquet(p); err == nil {
				rowCount += n
			}
			return nil
		})
		t.Logf("OK: merge succeeded via union; output rows=%d (expected 6)", rowCount)
		if rowCount != 6 {
			t.Errorf("merge succeeded but rows=%d, expected 6 (no loss/dup)", rowCount)
		}
	}
}

// faultyBackend wraps a real Backend; fails subsequent WriteReader calls
// after `failAfter` successful ones. All other methods delegate.
type faultyBackend struct {
	storage.Backend
	mu          sync.Mutex
	writeCount  int
	failAfter   int
	failedCalls int
}

func newWriteReaderFaultBackend(inner storage.Backend, failAfter int) *faultyBackend {
	return &faultyBackend{Backend: inner, failAfter: failAfter}
}

func (f *faultyBackend) setFailAfter(n int) {
	f.mu.Lock()
	f.failAfter = n
	f.mu.Unlock()
}

func (f *faultyBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	f.mu.Lock()
	f.writeCount++
	count := f.writeCount
	after := f.failAfter
	f.mu.Unlock()
	if count > after {
		f.mu.Lock()
		f.failedCalls++
		f.mu.Unlock()
		return fmt.Errorf("simulated upload failure (call=%d)", count)
	}
	return f.Backend.WriteReader(ctx, path, r, size)
}

// TestFreshStager_MidWalkUploadFailure simulates the partition-output
// walk failing after the first file uploads but before the second. Sources
// must remain staged so the next Flush can retry without data loss.
func TestFreshStager_MidWalkUploadFailure(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.WarnLevel)
	real, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer real.Close()

	faulty := newWriteReaderFaultBackend(real, 1) // succeed first call, fail subsequent
	stageDir := filepath.Join(tmpDir, "_fresh_stager")
	if err := os.MkdirAll(stageDir, 0700); err != nil {
		t.Fatal(err)
	}

	// Stage multiple files spanning enough hours to produce >1 partition output.
	expected := make(map[string]struct{})
	for g := 0; g < 4; g++ {
		p := makeParquet(t, fmt.Sprintf("g%d-%%d", g), 50, expected)
		name := fmt.Sprintf("posthog__events__fresh_stage_%d.parquet", g+1)
		if err := os.WriteFile(filepath.Join(stageDir, name), p, 0600); err != nil {
			t.Fatal(err)
		}
	}

	fs, err := NewFreshStager(&FreshStagerConfig{
		Storage: faulty, StageDir: stageDir,
		FlushAge: 1 * time.Hour, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	flushErr := fs.Flush(context.Background())
	t.Logf("first Flush err: %v", flushErr)
	t.Logf("faulty backend WriteReader: total=%d failed=%d", faulty.writeCount, faulty.failedCalls)

	// Sources MUST remain on disk because the walk errored.
	entries, _ := os.ReadDir(stageDir)
	stagedAfter := 0
	for _, e := range entries {
		if strings.Contains(e.Name(), "fresh_stage_") {
			stagedAfter++
		}
	}
	if stagedAfter != 4 {
		t.Errorf("expected 4 source files retained after walk failure, got %d", stagedAfter)
	}

	// Now retry with the SAME faulty backend repaired (failAfter very large).
	faulty.setFailAfter(1000000)
	if err := fs.Flush(context.Background()); err != nil {
		t.Fatalf("retry flush after repair: %v", err)
	}

	// Sources should be gone now (deleted after successful upload).
	entries2, _ := os.ReadDir(stageDir)
	stagedAfter2 := 0
	for _, e := range entries2 {
		if strings.Contains(e.Name(), "fresh_stage_") {
			stagedAfter2++
		}
	}
	if stagedAfter2 != 0 {
		t.Errorf("after retry success, %d source files still on disk", stagedAfter2)
	}

	// Verify final data: all expected row_ids present, no dups.
	rowIDs, _ := collectAllRowIDs(tmpDir)
	uniq := make(map[string]int)
	for _, id := range rowIDs {
		uniq[id]++
	}
	dups := 0
	for _, c := range uniq {
		if c > 1 {
			dups++
		}
	}
	t.Logf("after retry: rows=%d unique=%d expected=%d dups=%d", len(rowIDs), len(uniq), len(expected), dups)
	if dups > 0 {
		t.Errorf("DUPLICATION after walk-failure retry: %d ids appear >1x", dups)
	}
	missing := 0
	for id := range expected {
		if _, ok := uniq[id]; !ok {
			missing++
		}
	}
	if missing > 0 {
		t.Errorf("LOSS after walk-failure retry: %d expected ids missing", missing)
	}
}

func makeOneColParquet(t *testing.T, colName, sqlType string, values []string) []byte {
	t.Helper()
	tmpFile := filepath.Join(t.TempDir(), fmt.Sprintf("typecol_%d.parquet", time.Now().UnixNano()))
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	// Build VALUES tuples with one time column and the typed column.
	now := time.Now().UTC().Format("2006-01-02 15:04:05.000000")
	var vs []string
	for _, v := range values {
		vs = append(vs, fmt.Sprintf("(TIMESTAMP '%s', %s)", now, v))
	}
	q := fmt.Sprintf(`
		COPY (
			SELECT t AS time, x::%s AS %s FROM (VALUES %s) AS t(t,x)
		) TO '%s' (FORMAT PARQUET);
	`, sqlType, colName, strings.Join(vs, ","), tmpFile)
	if _, err := d.Exec(q); err != nil {
		t.Fatalf("makeOneColParquet: %v", err)
	}
	data, _ := os.ReadFile(tmpFile)
	return data
}

func countOutputParquets(t *testing.T, tmpDir string) int {
	t.Helper()
	c := 0
	_ = filepath.Walk(tmpDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(p, ".parquet") {
			return nil
		}
		if strings.Contains(p, "_fresh_stager") {
			return nil
		}
		c++
		return nil
	})
	return c
}
