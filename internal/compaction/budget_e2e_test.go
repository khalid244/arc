package compaction

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	_ "github.com/duckdb/duckdb-go/v2"
)

// mkParquetDir ensures the parent directory for outPath exists before
// writeFixtureParquet writes the file. DuckDB's COPY ... TO does not create
// missing parents.
func mkParquetDir(t *testing.T, outPath string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(outPath), err)
	}
}

// TestEnrichCandidateFileSizes_E2E_LocalBackend exercises the full input-budget
// path end-to-end:
//  1. Write real parquet files of varying sizes into a real LocalBackend.
//  2. Build a Candidate from the listed files.
//  3. Call enrichCandidateFileSizes — verify FileSizes match actual file sizes.
//  4. Call SplitCandidateByBudget with a small budget — verify each batch's
//     sum-of-sizes respects the budget, that no file is duplicated or lost,
//     and that file order within the candidate is preserved across batches.
func TestEnrichCandidateFileSizes_E2E_LocalBackend(t *testing.T) {
	tmp := t.TempDir()
	backend, err := storage.NewLocalBackend(tmp, zerolog.Nop())
	if err != nil {
		t.Fatalf("new local backend: %v", err)
	}

	db := openDuckDBForCompactionTest(t)
	partition := "test_db/test_meas/2026/05/19/12"

	// Each file gets a different row count so its on-disk size differs.
	// Files: f1=10 rows (tiny), f2=200 rows, f3=2000 rows, f4=20 rows, f5=500 rows.
	rowCounts := []int{10, 200, 2000, 20, 500}
	files := make([]string, len(rowCounts))
	for i, n := range rowCounts {
		rows := make([]fixtureRow, n)
		for j := 0; j < n; j++ {
			rows[j] = fixtureRow{
				ts:    fakeTime{year: 2026, month: 5, day: 19, hour: 12, min: j % 60},
				host:  fmt.Sprintf("h%d-%d", i, j),
				value: float64(i*1000 + j),
			}
		}
		rel := filepath.Join(partition, fmt.Sprintf("f%d.parquet", i+1))
		abs := filepath.Join(tmp, rel)
		mkParquetDir(t, abs)
		writeFixtureParquet(t, db, abs, rows)
		files[i] = rel
	}

	// Sanity: backend.List should see all 5 files.
	listed, err := backend.List(context.Background(), partition+"/")
	if err != nil {
		t.Fatalf("backend list: %v", err)
	}
	if len(listed) != len(files) {
		t.Fatalf("expected backend to list %d files, got %d", len(files), len(listed))
	}

	// Read actual sizes through StatFile so the test's "ground truth" comes
	// from the backend itself (avoids relying on os.Stat directly).
	actualSizes := make(map[string]int64, len(files))
	for _, f := range files {
		sz, err := backend.StatFile(context.Background(), f)
		if err != nil {
			t.Fatalf("stat %s: %v", f, err)
		}
		if sz <= 0 {
			t.Fatalf("expected positive size for %s, got %d", f, sz)
		}
		actualSizes[f] = sz
	}

	candidate := Candidate{
		Database:      "test_db",
		Measurement:   "test_meas",
		PartitionPath: partition,
		Files:         append([]string(nil), files...),
		FileCount:     len(files),
		Tier:          "hourly",
		PartitionTime: time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC),
	}

	// --- Enrich ---
	enrichCandidateFileSizes(context.Background(), backend, &candidate)
	if candidate.FileSizes == nil {
		t.Fatalf("enrichCandidateFileSizes did not populate FileSizes (backend may not implement ObjectLister)")
	}
	if len(candidate.FileSizes) != len(candidate.Files) {
		t.Fatalf("FileSizes length %d != Files length %d", len(candidate.FileSizes), len(candidate.Files))
	}
	for i, f := range candidate.Files {
		if candidate.FileSizes[i] != actualSizes[f] {
			t.Errorf("file %s: FileSizes[%d]=%d, actual=%d", f, i, candidate.FileSizes[i], actualSizes[f])
		}
	}

	// --- Pick a budget that forces splitting ---
	// Use ~60% of the largest file's size to force multiple batches but still
	// be reachable by smaller files (no skip-all scenario).
	var maxSize int64
	for _, sz := range candidate.FileSizes {
		if sz > maxSize {
			maxSize = sz
		}
	}
	budget := maxSize + 1 // ensure largest file fits alone, then forces a split after it
	t.Logf("file sizes: %v, budget=%d", candidate.FileSizes, budget)

	batches := SplitCandidateByBudget(candidate, 0 /* no count cap */, budget)
	if len(batches) == 0 {
		t.Fatal("expected at least 1 batch, got 0")
	}

	// --- Verify each batch respects the budget ---
	for i, b := range batches {
		var sum int64
		for _, sz := range b.FileSizes {
			sum += sz
		}
		if sum > budget {
			t.Errorf("batch %d sum=%d exceeds budget=%d", i, sum, budget)
		}
		if len(b.Files) != len(b.FileSizes) {
			t.Errorf("batch %d Files/FileSizes length mismatch: %d vs %d", i, len(b.Files), len(b.FileSizes))
		}
		if b.PartitionPath != candidate.PartitionPath {
			t.Errorf("batch %d PartitionPath drifted: %q vs %q", i, b.PartitionPath, candidate.PartitionPath)
		}
		t.Logf("batch %d: %d files, %d bytes", i, len(b.Files), sum)
	}

	// --- Verify no file lost and no duplicate, order preserved across batches ---
	flat := make([]string, 0, len(candidate.Files))
	for _, b := range batches {
		flat = append(flat, b.Files...)
	}
	if len(flat) != len(candidate.Files) {
		t.Errorf("expected %d files across all batches, got %d", len(candidate.Files), len(flat))
	}
	for i, f := range flat {
		if i >= len(candidate.Files) || f != candidate.Files[i] {
			t.Errorf("file order changed at position %d: got %q, want %q", i, f, candidate.Files[i])
		}
	}
	seen := map[string]bool{}
	for _, f := range flat {
		if seen[f] {
			t.Errorf("file %q appears in multiple batches", f)
		}
		seen[f] = true
	}
}

// TestEnrichCandidateFileSizes_SkipsOversize_E2E confirms that when a file
// individually exceeds the budget, it is skipped (not packed alone in a batch
// that would breach the budget).
func TestEnrichCandidateFileSizes_SkipsOversize_E2E(t *testing.T) {
	tmp := t.TempDir()
	backend, err := storage.NewLocalBackend(tmp, zerolog.Nop())
	if err != nil {
		t.Fatalf("new local backend: %v", err)
	}
	db := openDuckDBForCompactionTest(t)

	partition := "db/m/2026/05/19/13"
	// One small file + one larger file.
	small := filepath.Join(partition, "small.parquet")
	big := filepath.Join(partition, "big.parquet")
	smallRows := make([]fixtureRow, 5)
	for i := range smallRows {
		smallRows[i] = fixtureRow{ts: fakeTime{year: 2026, month: 5, day: 19, hour: 13, min: i}, host: "h", value: 1}
	}
	smallAbs := filepath.Join(tmp, small)
	mkParquetDir(t, smallAbs)
	writeFixtureParquet(t, db, smallAbs, smallRows)

	bigRows := make([]fixtureRow, 5000)
	for i := range bigRows {
		bigRows[i] = fixtureRow{ts: fakeTime{year: 2026, month: 5, day: 19, hour: 13, min: i % 60}, host: fmt.Sprintf("h%d", i), value: float64(i)}
	}
	bigAbs := filepath.Join(tmp, big)
	mkParquetDir(t, bigAbs)
	writeFixtureParquet(t, db, bigAbs, bigRows)

	smallSize, _ := backend.StatFile(context.Background(), small)
	bigSize, _ := backend.StatFile(context.Background(), big)
	if bigSize <= smallSize {
		t.Fatalf("test setup invariant: big must be larger than small (small=%d big=%d)", smallSize, bigSize)
	}

	candidate := Candidate{
		PartitionPath: partition,
		Files:         []string{small, big},
		FileCount:     2,
	}
	enrichCandidateFileSizes(context.Background(), backend, &candidate)
	if candidate.FileSizes == nil {
		t.Fatal("FileSizes not populated")
	}

	// Budget between small and big — big should be skipped.
	budget := (smallSize + bigSize) / 2
	if budget <= smallSize || budget >= bigSize {
		t.Fatalf("budget invariant: want smallSize(%d) < budget(%d) < bigSize(%d)", smallSize, budget, bigSize)
	}

	batches := SplitCandidateByBudget(candidate, 0, budget)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch (small only, big skipped), got %d", len(batches))
	}
	if len(batches[0].Files) != 1 || batches[0].Files[0] != small {
		t.Errorf("expected batch to contain only %q, got %v", small, batches[0].Files)
	}
}

// TestEnrichCandidateFileSizes_NoOversize_FitsBatches_E2E confirms the common
// case: when budget is comfortably larger than any single file but smaller
// than the total, files pack across multiple batches and none are skipped.
func TestEnrichCandidateFileSizes_NoOversize_FitsBatches_E2E(t *testing.T) {
	tmp := t.TempDir()
	backend, err := storage.NewLocalBackend(tmp, zerolog.Nop())
	if err != nil {
		t.Fatalf("new local backend: %v", err)
	}
	db := openDuckDBForCompactionTest(t)

	partition := "db/m/2026/05/19/14"
	const n = 6
	files := make([]string, n)
	for i := 0; i < n; i++ {
		rel := filepath.Join(partition, fmt.Sprintf("p%d.parquet", i))
		rows := make([]fixtureRow, 100)
		for j := range rows {
			rows[j] = fixtureRow{ts: fakeTime{year: 2026, month: 5, day: 19, hour: 14, min: j % 60}, host: fmt.Sprintf("h%d-%d", i, j), value: float64(i*100 + j)}
		}
		abs := filepath.Join(tmp, rel)
		mkParquetDir(t, abs)
		writeFixtureParquet(t, db, abs, rows)
		files[i] = rel
	}

	candidate := Candidate{
		PartitionPath: partition,
		Files:         files,
		FileCount:     n,
	}
	enrichCandidateFileSizes(context.Background(), backend, &candidate)
	if candidate.FileSizes == nil {
		t.Fatal("FileSizes not populated")
	}

	var total int64
	var maxSz int64
	for _, sz := range candidate.FileSizes {
		total += sz
		if sz > maxSz {
			maxSz = sz
		}
	}
	// Budget = ~half of total: forces a split but no file is oversize.
	budget := total / 2
	if budget <= maxSz {
		t.Skipf("test setup: budget %d not > maxSz %d (files too similar in size for split scenario)", budget, maxSz)
	}

	batches := SplitCandidateByBudget(candidate, 0, budget)
	if len(batches) < 2 {
		t.Fatalf("expected ≥2 batches with budget=%d total=%d, got %d", budget, total, len(batches))
	}

	// All files must be present.
	flat := map[string]bool{}
	for _, b := range batches {
		for _, f := range b.Files {
			if flat[f] {
				t.Errorf("file %q duplicated across batches", f)
			}
			flat[f] = true
		}
	}
	if len(flat) != n {
		t.Errorf("expected %d unique files across batches, got %d", n, len(flat))
	}
}
