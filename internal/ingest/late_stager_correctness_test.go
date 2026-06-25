package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestLateStager_MergesToOneObjectNoLoss is the regression test for the
// _late/ small-file flood: each Stage() used to become its own S3 object
// (one per schema-change flush). The stager must merge all staged parquets
// for a (db, measurement) into a SINGLE events_late/ object per flush window
// with no rows lost or duplicated, then delete the staged sources.
func TestLateStager_MergesToOneObjectNoLoss(t *testing.T) {
	tmpDir := t.TempDir()
	stageDir := t.TempDir() // outside the storage root so the row-collector ignores it
	logger := zerolog.New(os.Stdout).Level(zerolog.WarnLevel)

	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ls, err := NewLateStager(&LateStagerConfig{
		Storage:  store,
		StageDir: stageDir,
		FlushAge: 1 * time.Hour, // long; the test drives Flush() manually
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Close()

	expected := make(map[string]struct{})
	const staged = 8
	for i := 0; i < staged; i++ {
		p := makeParquet(t, fmt.Sprintf("late-%d-%%d", i), 50, expected)
		if err := ls.Stage("posthog", "events", p); err != nil {
			t.Fatal(err)
		}
	}

	if err := ls.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 8 staged files collapse to exactly one merged object, under <db>/<m>_late/.
	out := outputParquetPaths(tmpDir)
	if len(out) != 1 {
		t.Fatalf("expected 1 merged object, got %d: %v", len(out), out)
	}
	if want := filepath.Join("posthog", "events_late"); !strings.Contains(out[0], want) {
		t.Errorf("merged object %q not under %q", out[0], want)
	}
	assertNoLossNoDup(t, tmpDir, expected)
	if n := countLateStaged(stageDir); n != 0 {
		t.Errorf("expected staged sources deleted after upload, %d remain", n)
	}
}

// TestLateStager_UploadFailureKeepsStagedForRetry asserts the durability
// claim: if the merged-file upload fails, the staged sources stay on disk
// for the next flush instead of vanishing (no silent loss), and the retry
// drains them cleanly with no duplication.
func TestLateStager_UploadFailureKeepsStagedForRetry(t *testing.T) {
	tmpDir := t.TempDir()
	stageDir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.WarnLevel)

	real, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer real.Close()
	faulty := newWriteReaderFaultBackend(real, 0) // fail the very first upload

	ls, err := NewLateStager(&LateStagerConfig{
		Storage:  faulty,
		StageDir: stageDir,
		FlushAge: 1 * time.Hour,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Close()

	expected := make(map[string]struct{})
	const staged = 4
	for i := 0; i < staged; i++ {
		p := makeParquet(t, fmt.Sprintf("g%d-%%d", i), 50, expected)
		if err := ls.Stage("posthog", "events", p); err != nil {
			t.Fatal(err)
		}
	}

	// Upload fails → Flush keeps every source, writes no output (no loss).
	_ = ls.Flush(context.Background())
	if n := countLateStaged(stageDir); n != staged {
		t.Errorf("expected %d staged sources retained after upload failure, got %d", staged, n)
	}
	if out := outputParquetPaths(tmpDir); len(out) != 0 {
		t.Errorf("expected no output after failed upload, got %v", out)
	}

	// Repair the backend and retry → success, sources deleted, no loss/dup.
	faulty.setFailAfter(1 << 30)
	if err := ls.Flush(context.Background()); err != nil {
		t.Fatalf("retry flush after repair: %v", err)
	}
	if n := countLateStaged(stageDir); n != 0 {
		t.Errorf("after retry success, %d staged sources still on disk", n)
	}
	assertNoLossNoDup(t, tmpDir, expected)
}

// outputParquetPaths returns every .parquet written under the storage root.
// The stager scratch dir is a sibling temp dir, so only merged outputs appear.
func outputParquetPaths(root string) []string {
	var out []string
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return nil
		}
		out = append(out, p)
		return nil
	})
	return out
}

// countLateStaged counts un-merged staged source files left in the stage dir.
func countLateStaged(stageDir string) int {
	entries, _ := os.ReadDir(stageDir)
	n := 0
	for _, e := range entries {
		if strings.Contains(e.Name(), "late_stage_") {
			n++
		}
	}
	return n
}

// assertNoLossNoDup reads every row_id under the storage root and checks the
// set matches exactly what was staged: no duplicates, no missing, no phantoms.
func assertNoLossNoDup(t *testing.T, tmpDir string, expected map[string]struct{}) {
	t.Helper()
	ids, err := collectAllRowIDs(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	uniq := make(map[string]int, len(ids))
	for _, id := range ids {
		uniq[id]++
	}
	dup, missing, extra := 0, 0, 0
	for _, c := range uniq {
		if c > 1 {
			dup++
		}
	}
	for id := range expected {
		if _, ok := uniq[id]; !ok {
			missing++
		}
	}
	for id := range uniq {
		if _, ok := expected[id]; !ok {
			extra++
		}
	}
	if dup > 0 {
		t.Errorf("DUPLICATION: %d row_ids appear more than once", dup)
	}
	if missing > 0 {
		t.Errorf("DATA LOSS: %d expected row_ids missing", missing)
	}
	if extra > 0 {
		t.Errorf("PHANTOM: %d output row_ids were never staged", extra)
	}
}
