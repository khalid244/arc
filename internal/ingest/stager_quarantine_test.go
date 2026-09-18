package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// These tests cover the 2026-09-17 production incident: a node ENOSPC burst
// left a zero-length parquet in the fresh-stager directory. Every subsequent
// merge failed on that one file ("too small to be a Parquet file"), and
// because the retry re-submits the WHOLE source list, the group could never
// make progress. One ingest pod published zero rows for ~11 hours while
// 1154 perfectly good staged files piled up behind the poisoned one.
//
// Contract: an unreadable source must cost exactly itself. It is moved to a
// quarantine directory (never deleted — it may be forensic evidence) and the
// remaining sources merge and upload normally.

// writeRawStageFile drops bytes straight into the stage dir under a name the
// stager will pick up, bypassing Stage() so we can plant malformed content.
func writeRawStageFile(t *testing.T, stageDir, db, measurement, marker string, seq int, data []byte) string {
	t.Helper()
	if err := os.MkdirAll(stageDir, 0700); err != nil {
		t.Fatal(err)
	}
	name := db + "__" + measurement + "__" + marker + itoa(seq) + ".parquet"
	p := filepath.Join(stageDir, name)
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// stageDirEntries returns the .parquet file names sitting directly in dir.
func stageDirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

// quarantinedNames returns the file names under the stager's quarantine dir.
func quarantinedNames(t *testing.T, stageDir string) []string {
	t.Helper()
	qdir := filepath.Join(stageDir, quarantineDirName)
	entries, err := os.ReadDir(qdir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

// corruptBodyParquet has valid PAR1 magic at both ends but a garbage body, so
// a cheap magic-byte check accepts it and only DuckDB rejects it. This is the
// case that forces a post-merge-failure probe rather than a pre-merge scan.
func corruptBodyParquet() []byte {
	b := make([]byte, 0, 1024)
	b = append(b, []byte("PAR1")...)
	b = append(b, make([]byte, 1016)...)
	b = append(b, []byte("PAR1")...)
	return b
}

func newFreshStagerForQuarantine(t *testing.T) (*FreshStager, string, string) {
	t.Helper()
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.ErrorLevel)
	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	stageDir := filepath.Join(tmpDir, "_fresh_stager")
	fs, err := NewFreshStager(&FreshStagerConfig{
		Storage: store,
		// Long enough that the background loop never races our manual Flush.
		StageDir: stageDir,
		FlushAge: time.Hour,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	return fs, tmpDir, stageDir
}

// TestFreshStager_QuarantinesZeroByteSource is the direct reproduction of the
// production incident: one zero-length file must not block the other sources.
func TestFreshStager_QuarantinesZeroByteSource(t *testing.T) {
	fs, tmpDir, stageDir := newFreshStagerForQuarantine(t)

	expected := map[string]struct{}{}
	if err := fs.Stage("default", "downloads", makeParquet(t, "good-a-%d", 5, expected)); err != nil {
		t.Fatal(err)
	}
	if err := fs.Stage("default", "downloads", makeParquet(t, "good-b-%d", 5, expected)); err != nil {
		t.Fatal(err)
	}
	// The poisoned file, exactly as ENOSPC left it.
	writeRawStageFile(t, stageDir, "default", "downloads", "fresh_stage_", 999999, nil)

	if err := fs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	got, err := collectAllRowIDs(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(expected) {
		t.Fatalf("uploaded %d rows, want %d — the good sources did not survive the corrupt one", len(got), len(expected))
	}
	for _, id := range got {
		if _, ok := expected[id]; !ok {
			t.Fatalf("unexpected row_id %q in output", id)
		}
	}

	if left := stageDirEntries(t, stageDir); len(left) != 0 {
		t.Fatalf("stage dir still holds %v; a successful merge must clear its sources", left)
	}
	q := quarantinedNames(t, stageDir)
	if len(q) != 1 {
		t.Fatalf("quarantined %v, want exactly the one corrupt file", q)
	}
	if !strings.Contains(q[0], "fresh_stage_999999") {
		t.Fatalf("quarantined %q, want the corrupt source", q[0])
	}
}

// TestFreshStager_QuarantinesTruncatedSource covers a partially-written file
// (ENOSPC mid-write), which has bytes but no valid trailing magic.
func TestFreshStager_QuarantinesTruncatedSource(t *testing.T) {
	fs, tmpDir, stageDir := newFreshStagerForQuarantine(t)

	expected := map[string]struct{}{}
	good := makeParquet(t, "good-%d", 4, expected)
	if err := fs.Stage("default", "downloads", good); err != nil {
		t.Fatal(err)
	}
	// First 64 bytes of a real parquet: head magic present, tail magic gone.
	writeRawStageFile(t, stageDir, "default", "downloads", "fresh_stage_", 999998, good[:64])

	if err := fs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	got, err := collectAllRowIDs(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(expected) {
		t.Fatalf("uploaded %d rows, want %d", len(got), len(expected))
	}
	if q := quarantinedNames(t, stageDir); len(q) != 1 {
		t.Fatalf("quarantined %v, want exactly the truncated file", q)
	}
}

// TestFreshStager_QuarantinesCorruptBodyAfterMergeFailure covers the file that
// passes a magic-byte check but that DuckDB cannot read. The merge must not be
// abandoned: the offending source is identified, quarantined, and the rest
// uploaded in the same flush.
func TestFreshStager_QuarantinesCorruptBodyAfterMergeFailure(t *testing.T) {
	fs, tmpDir, stageDir := newFreshStagerForQuarantine(t)

	expected := map[string]struct{}{}
	if err := fs.Stage("default", "downloads", makeParquet(t, "good-%d", 6, expected)); err != nil {
		t.Fatal(err)
	}
	writeRawStageFile(t, stageDir, "default", "downloads", "fresh_stage_", 999997, corruptBodyParquet())

	if err := fs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	got, err := collectAllRowIDs(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(expected) {
		t.Fatalf("uploaded %d rows, want %d — a DuckDB-unreadable source wedged the whole group", len(got), len(expected))
	}
	if q := quarantinedNames(t, stageDir); len(q) != 1 {
		t.Fatalf("quarantined %v, want exactly the corrupt-body file", q)
	}
}

// TestFreshStager_AllSourcesCorruptDoesNotWedge asserts the degenerate case
// terminates: with nothing readable, the group is drained to quarantine rather
// than retried forever.
func TestFreshStager_AllSourcesCorruptDoesNotWedge(t *testing.T) {
	fs, _, stageDir := newFreshStagerForQuarantine(t)

	writeRawStageFile(t, stageDir, "default", "downloads", "fresh_stage_", 1, nil)
	writeRawStageFile(t, stageDir, "default", "downloads", "fresh_stage_", 2, corruptBodyParquet())

	if err := fs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	if left := stageDirEntries(t, stageDir); len(left) != 0 {
		t.Fatalf("stage dir still holds %v — the group would retry the same poison forever", left)
	}
	if q := quarantinedNames(t, stageDir); len(q) != 2 {
		t.Fatalf("quarantined %v, want both corrupt files", q)
	}
}

// TestFreshStager_QuarantinePreservesBytes asserts quarantine is a move, not a
// delete: the original content must remain on disk for forensics.
func TestFreshStager_QuarantinePreservesBytes(t *testing.T) {
	fs, _, stageDir := newFreshStagerForQuarantine(t)

	body := corruptBodyParquet()
	writeRawStageFile(t, stageDir, "default", "downloads", "fresh_stage_", 4242, body)

	if err := fs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	q := quarantinedNames(t, stageDir)
	if len(q) != 1 {
		t.Fatalf("quarantined %v, want one file", q)
	}
	got, err := os.ReadFile(filepath.Join(stageDir, quarantineDirName, q[0]))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(body) {
		t.Fatalf("quarantined file is %d bytes, want %d — quarantine must preserve the evidence", len(got), len(body))
	}
}

// TestFreshStager_HealthySourcesAreNeverQuarantined guards against the fix
// eating good data: a flush with only valid sources must quarantine nothing.
func TestFreshStager_HealthySourcesAreNeverQuarantined(t *testing.T) {
	fs, tmpDir, stageDir := newFreshStagerForQuarantine(t)

	expected := map[string]struct{}{}
	if err := fs.Stage("default", "downloads", makeParquet(t, "ok-a-%d", 3, expected)); err != nil {
		t.Fatal(err)
	}
	if err := fs.Stage("default", "downloads", makeParquet(t, "ok-b-%d", 3, expected)); err != nil {
		t.Fatal(err)
	}

	if err := fs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	got, err := collectAllRowIDs(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(expected) {
		t.Fatalf("uploaded %d rows, want %d", len(got), len(expected))
	}
	if q := quarantinedNames(t, stageDir); len(q) != 0 {
		t.Fatalf("quarantined %v from a healthy flush", q)
	}
}

// TestLateStager_QuarantinesZeroByteSource mirrors the fresh-path contract on
// the late path. In the same incident the LateStager was wedged on
// posthog__events__late_stage_269162.parquet with 37,796 files behind it.
func TestLateStager_QuarantinesZeroByteSource(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.ErrorLevel)
	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	stageDir := filepath.Join(tmpDir, "_late_stager")
	ls, err := NewLateStager(&LateStagerConfig{
		Storage:  store,
		StageDir: stageDir,
		FlushAge: time.Hour,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Close()

	expected := map[string]struct{}{}
	if err := ls.Stage("posthog", "events", makeParquet(t, "late-%d", 5, expected)); err != nil {
		t.Fatal(err)
	}
	writeRawStageFile(t, stageDir, "posthog", "events", "late_stage_", 269162, nil)

	if err := ls.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	got, err := collectAllRowIDs(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(expected) {
		t.Fatalf("uploaded %d rows, want %d — the good late sources did not survive", len(got), len(expected))
	}
	if left := stageDirEntries(t, stageDir); len(left) != 0 {
		t.Fatalf("stage dir still holds %v", left)
	}
	if q := quarantinedNames(t, stageDir); len(q) != 1 {
		t.Fatalf("quarantined %v, want exactly the corrupt file", q)
	}
}

// --- Regression tests from adversarial review -------------------------------

// TestFreshStager_ExpiredContextDoesNotQuarantine is the blocker case: a dead
// context makes DuckDB fail on EVERY source, which must not be read as "every
// source is corrupt". Quarantining a healthy backlog is worse than the wedge
// this fix exists to prevent — the wedge leaves the files where the next flush
// finds them, quarantine moves them somewhere nothing ever looks.
func TestFreshStager_ExpiredContextDoesNotQuarantine(t *testing.T) {
	fs, _, stageDir := newFreshStagerForQuarantine(t)

	expected := map[string]struct{}{}
	for i := 0; i < 3; i++ {
		if err := fs.Stage("default", "downloads", makeParquet(t, "ctx-%d", 3, expected)); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // dead before the merge ever runs

	_ = fs.Flush(ctx)

	if q := quarantinedNames(t, stageDir); len(q) != 0 {
		t.Fatalf("quarantined %d healthy sources because the context expired: %v", len(q), q)
	}
	if left := stageDirEntries(t, stageDir); len(left) != 3 {
		t.Fatalf("stage dir holds %d files, want all 3 preserved for the next flush: %v", len(left), left)
	}
}

// TestFreshStager_UnreadableFileIsNotQuarantined separates "we could not check
// this file" from "this file is bad". An EIO or EMFILE — exactly what a node
// under DiskPressure produces — must leave the source in place, not condemn it.
func TestFreshStager_UnreadableFileIsNotQuarantined(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not deny access")
	}
	fs, _, stageDir := newFreshStagerForQuarantine(t)

	expected := map[string]struct{}{}
	good := makeParquet(t, "perm-%d", 3, expected)
	p := writeRawStageFile(t, stageDir, "default", "downloads", "fresh_stage_", 77, good)
	if err := os.Chmod(p, 0000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(p, 0600) })

	_ = fs.Flush(context.Background())

	if q := quarantinedNames(t, stageDir); len(q) != 0 {
		t.Fatalf("quarantined %v — a valid file we merely could not open is not corrupt", q)
	}
}

// TestFreshStager_SeqSeededFromExistingStageDir guards the collision that lets
// a quarantine rename a file out from under a concurrent Stage(): after a
// container restart the in-memory seq restarted at 1 and overwrote surviving
// staged files of the same name.
func TestFreshStager_SeqSeededFromExistingStageDir(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.ErrorLevel)
	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	stageDir := filepath.Join(tmpDir, "_fresh_stager")
	// seq 1 is exactly what a restarted counter hands out first.
	survivor := writeRawStageFile(t, stageDir, "default", "downloads", "fresh_stage_", 1, []byte("survivor"))

	fs, err := NewFreshStager(&FreshStagerConfig{
		Storage: store, StageDir: stageDir, FlushAge: time.Hour, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	expected := map[string]struct{}{}
	if err := fs.Stage("default", "downloads", makeParquet(t, "seq-%d", 2, expected)); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(survivor)
	if err != nil {
		t.Fatalf("surviving staged file was clobbered: %v", err)
	}
	if string(got) != "survivor" {
		t.Fatalf("surviving staged file was overwritten by a restarted seq counter")
	}
}

// TestProbeUnreadable_OnlyParseErrorsMeanCorrupt pins layer 2 to the same
// contract as layer 1. DuckDB fails for two very different reasons: the file
// is not parquet (Invalid Input Error), or we could not read it at all
// (IO Error). Only the first is a verdict about the DATA. Treating the second
// as corruption means a systemic condition — fd exhaustion, an EIO storm on
// the emptyDir, memory pressure — condemns every healthy source at once,
// which is the exact weather that produces corrupt files in the first place.
func TestProbeUnreadable_OnlyParseErrorsMeanCorrupt(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not deny access")
	}
	dir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.ErrorLevel)
	expected := map[string]struct{}{}

	healthy := filepath.Join(dir, "healthy.parquet")
	if err := os.WriteFile(healthy, makeParquet(t, "probe-%d", 2, expected), 0600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty.parquet")
	if err := os.WriteFile(empty, nil, 0600); err != nil {
		t.Fatal(err)
	}
	badBody := filepath.Join(dir, "badbody.parquet")
	if err := os.WriteFile(badBody, corruptBodyParquet(), 0600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing.parquet")
	noPerm := filepath.Join(dir, "noperm.parquet")
	if err := os.WriteFile(noPerm, makeParquet(t, "perm2-%d", 2, expected), 0000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(noPerm, 0600) })

	corrupt, unknown, err := probeUnreadable(context.Background(),
		[]string{healthy, empty, badBody, missing, noPerm}, logger)
	if err != nil {
		t.Fatalf("probe returned error: %v", err)
	}

	inList := func(l []string, p string) bool {
		for _, x := range l {
			if x == p {
				return true
			}
		}
		return false
	}
	if !inList(corrupt, empty) || !inList(corrupt, badBody) {
		t.Fatalf("corrupt=%v, want the empty and bad-body files", corrupt)
	}
	if inList(corrupt, healthy) {
		t.Fatalf("healthy file classified corrupt")
	}
	if inList(corrupt, missing) {
		t.Fatalf("a missing file was classified corrupt; it is an IO error, not a verdict on the data")
	}
	if inList(corrupt, noPerm) {
		t.Fatalf("an unreadable-but-healthy file was classified corrupt — this is the systemic-failure data-loss path")
	}
	if !inList(unknown, missing) || !inList(unknown, noPerm) {
		t.Fatalf("unknown=%v, want the missing and unreadable files reported as undecidable", unknown)
	}
}

// TestExecMergeWithQuarantine_UndecidableSourceIsExcludedNotQuarantined covers
// the middle ground: a source DuckDB could not read, but not because it is
// corrupt. Quarantining it would lose good rows; leaving it in the retry list
// would re-wedge the whole group on every flush. It must be dropped from this
// merge only, and left on disk for the next one.
func TestExecMergeWithQuarantine_UndecidableSourceIsExcludedNotQuarantined(t *testing.T) {
	dir := t.TempDir()
	logger := zerolog.New(os.Stdout).Level(zerolog.ErrorLevel)
	expected := map[string]struct{}{}

	healthy := filepath.Join(dir, "healthy.parquet")
	if err := os.WriteFile(healthy, makeParquet(t, "excl-%d", 2, expected), 0600); err != nil {
		t.Fatal(err)
	}
	undecidable := filepath.Join(dir, "gone.parquet") // never created: IO Error, not a parse error

	var calls [][]string
	exec := func(_ context.Context, p []string) error {
		calls = append(calls, append([]string(nil), p...))
		if len(calls) == 1 {
			return fmt.Errorf("duckdb merge: boom")
		}
		return nil
	}

	survivors, err := execMergeWithQuarantine(context.Background(), dir,
		[]string{healthy, undecidable}, logger, exec)
	if err != nil {
		t.Fatalf("merge returned error: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("exec called %d times, want a retry with the survivors", len(calls))
	}
	if len(calls[1]) != 1 || calls[1][0] != healthy {
		t.Fatalf("retry ran with %v, want only the healthy source — an unreadable source re-wedges the group", calls[1])
	}
	if len(survivors) != 1 || survivors[0] != healthy {
		t.Fatalf("survivors=%v, want just the healthy source", survivors)
	}
	if q := quarantinedNames(t, dir); len(q) != 0 {
		t.Fatalf("quarantined %v — an unreadable source is not a corrupt one", q)
	}
}
