package compaction

import (
	"reflect"
	"sort"
	"testing"
)

// Input-size-budget splitter: given a Candidate whose Files have known
// sizes, split into batches where each batch's sum-of-sizes ≤ budget.
//
// This is the replacement for the FILE_SIZE_BYTES output-side approach.
// Bounds disk peak per Job by bounding the inputs the Job has to download
// and merge, rather than splitting the OUTPUT into multiple files.

// ---- Greedy packing ----

func TestSplitCandidateByBudget_AccumulatesUntilBudget(t *testing.T) {
	// 5 files totalling 2.5 GB; budget 2 GB.
	// Greedy (in-order) packing: take files until next one would exceed budget.
	// f1(800M) + f2(800M) = 1.6G; f3(800M) → would be 2.4G, new batch.
	// f3(800M) + f4(500M) = 1.3G; f5(100M) → 1.4G fits.
	files := []string{"f1", "f2", "f3", "f4", "f5"}
	sizes := []int64{800_000_000, 800_000_000, 800_000_000, 500_000_000, 100_000_000}
	c := Candidate{Files: files, FileSizes: sizes}

	batches := SplitCandidateByBudget(c, 0 /* no file-count cap */, 2_000_000_000)

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	// Every batch's sum ≤ budget.
	for i, b := range batches {
		var sum int64
		for _, s := range b.FileSizes {
			sum += s
		}
		if sum > 2_000_000_000 {
			t.Errorf("batch %d sum %d exceeds budget", i, sum)
		}
		if len(b.Files) != len(b.FileSizes) {
			t.Errorf("batch %d Files/FileSizes length mismatch: %d vs %d", i, len(b.Files), len(b.FileSizes))
		}
	}

	// All files present across batches (no loss).
	var allFiles []string
	for _, b := range batches {
		allFiles = append(allFiles, b.Files...)
	}
	sort.Strings(allFiles)
	expectedSorted := append([]string{}, files...)
	sort.Strings(expectedSorted)
	if !reflect.DeepEqual(allFiles, expectedSorted) {
		t.Errorf("union of batch files != original files\n  got:  %v\n  want: %v", allFiles, expectedSorted)
	}
}

func TestSplitCandidateByBudget_AllFilesFit_SingleBatch(t *testing.T) {
	files := []string{"a", "b", "c"}
	sizes := []int64{100_000_000, 100_000_000, 100_000_000} // 300 MB total
	c := Candidate{Files: files, FileSizes: sizes}

	batches := SplitCandidateByBudget(c, 0, 1_000_000_000) // 1 GB budget

	if len(batches) != 1 {
		t.Fatalf("expected 1 batch (total ≪ budget), got %d", len(batches))
	}
	if len(batches[0].Files) != 3 {
		t.Errorf("expected all 3 files in single batch, got %d", len(batches[0].Files))
	}
}

func TestSplitCandidateByBudget_SkipsFilesLargerThanBudget(t *testing.T) {
	// Two large already-at-or-over budget files + one small. Only the small
	// one should land in a batch; oversize files are skipped (the existing
	// _compacted-suffix filter would catch them at candidate scan, but the
	// splitter shouldn't trust that and includes a defense-in-depth check).
	files := []string{"big1", "big2", "small"}
	sizes := []int64{3_000_000_000, 5_000_000_000, 100_000_000}
	c := Candidate{Files: files, FileSizes: sizes}

	batches := SplitCandidateByBudget(c, 0, 2_000_000_000) // 2 GB budget

	if len(batches) != 1 {
		t.Fatalf("expected 1 batch (only small fits), got %d", len(batches))
	}
	if len(batches[0].Files) != 1 || batches[0].Files[0] != "small" {
		t.Errorf("expected only 'small' in batch, got %v", batches[0].Files)
	}
}

func TestSplitCandidateByBudget_MaxFilesPerBatchHonored(t *testing.T) {
	// 10 tiny files, budget large enough to fit them all by size, but
	// max files per batch = 4. Expect 3 batches: 4 + 4 + 2.
	files := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	sizes := []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	c := Candidate{Files: files, FileSizes: sizes}

	batches := SplitCandidateByBudget(c, 4 /* max files per batch */, 1_000_000_000)

	if len(batches) != 3 {
		t.Fatalf("expected 3 batches (4+4+2), got %d", len(batches))
	}
	if len(batches[0].Files) != 4 || len(batches[1].Files) != 4 || len(batches[2].Files) != 2 {
		t.Errorf("expected sizes [4,4,2], got [%d,%d,%d]",
			len(batches[0].Files), len(batches[1].Files), len(batches[2].Files))
	}
}

func TestSplitCandidateByBudget_NoBudget_FallbackToCountOnly(t *testing.T) {
	// budget = 0 means "no size-based splitting". Behavior should match
	// existing SplitCandidateIntoBatches by count alone.
	files := []string{"a", "b", "c", "d", "e"}
	sizes := []int64{99, 99, 99, 99, 99}
	c := Candidate{Files: files, FileSizes: sizes}

	batches := SplitCandidateByBudget(c, 2 /* max-files */, 0 /* no budget */)

	if len(batches) != 3 {
		t.Fatalf("expected 3 batches (2+2+1), got %d", len(batches))
	}
}

func TestSplitCandidateByBudget_PreservesPartitionMetadata(t *testing.T) {
	files := []string{"a", "b"}
	sizes := []int64{1, 1}
	c := Candidate{
		Database:      "posthog",
		Measurement:   "events",
		PartitionPath: "posthog/events/2026/05/19",
		Files:         files,
		FileSizes:     sizes,
		Tier:          "daily",
	}

	batches := SplitCandidateByBudget(c, 1 /* force 2 batches */, 0)

	for i, b := range batches {
		if b.Database != "posthog" || b.Measurement != "events" || b.Tier != "daily" {
			t.Errorf("batch %d: metadata not preserved %+v", i, b)
		}
		if b.BatchNumber != i+1 || b.TotalBatches != 2 {
			t.Errorf("batch %d: BatchNumber=%d TotalBatches=%d (want %d/2)", i, b.BatchNumber, b.TotalBatches, i+1)
		}
	}
}

func TestSplitCandidateByBudget_EmptyCandidate_NoOp(t *testing.T) {
	c := Candidate{Files: []string{}, FileSizes: []int64{}}
	batches := SplitCandidateByBudget(c, 4, 1_000_000_000)
	if len(batches) != 0 {
		t.Errorf("expected 0 batches for empty candidate, got %d", len(batches))
	}
}

func TestSplitCandidateByBudget_NilFileSizes_FallbackToCountOnly(t *testing.T) {
	// FileSizes nil: cannot enforce budget; fall back to count-based splitting.
	c := Candidate{
		Files:     []string{"a", "b", "c", "d", "e"},
		FileSizes: nil,
	}
	batches := SplitCandidateByBudget(c, 2, 2_000_000_000)
	if len(batches) != 3 {
		t.Fatalf("expected 3 batches when FileSizes is nil (count-only), got %d", len(batches))
	}
}
