package compaction

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// CS-3: recovering a chunk whose upload did not complete must never leave the
// failed attempt's outputs live next to a re-drain of the same sources, and
// must never delete sources whose rows are not all in live outputs.

// rewriteManifestFields applies mutate to the manifest's raw JSON object and
// writes it back without touching any other field (no UpdatedAt refresh).
func rewriteManifestFields(t *testing.T, b *hookBackend, key string, mutate func(m map[string]any)) {
	t.Helper()
	ctx := context.Background()
	raw, err := b.LocalBackend.Read(ctx, key)
	if err != nil {
		t.Fatalf("read manifest %s: %v", key, err)
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	mutate(obj)
	out, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.LocalBackend.Write(ctx, key, out); err != nil {
		t.Fatal(err)
	}
}

func shiftTimeField(t *testing.T, obj map[string]any, field string, by time.Duration) {
	t.Helper()
	s, ok := obj[field].(string)
	if !ok || s == "" {
		t.Fatalf("manifest has no %q field (got %v)", field, obj[field])
	}
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("parse %s: %v", field, err)
	}
	obj[field] = ts.Add(by).UTC().Format(time.RFC3339Nano)
}

// newFailedUploadRig drains one bucket whose upload fails on the 2nd output, so
// some outputs of the failed jobID are live, the manifest is pending and the
// source is untouched.
func newFailedUploadRig(t *testing.T, ctx context.Context, days int) (*hookBackend, *Reorganizer, string, string, string) {
	t.Helper()
	t.Setenv("TZ", "UTC")
	b, tmp := newHookBackend(t)
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	if err := os.MkdirAll(lateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	makeBucketManyDays(t, db, lateDir, "events_late_20260520_030000_0.parquet", days)
	db.Close()
	scratch := filepath.Join(tmp, "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	r := newReorg(b, scratch, zerolog.New(io.Discard))
	r.DownloadWorkers = 1 // deterministic: outputs before the failing one land first
	var n int
	b.set(func(b *hookBackend) {
		b.writeRHook = func(_ context.Context, path string) error {
			if !strings.Contains(path, reorgFileMarker) {
				return nil
			}
			n++
			if n == 2 {
				return errors.New("injected: PUT 504 Gateway Timeout")
			}
			return nil
		}
	})
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run1: %v", err)
	}
	b.set(func(b *hookBackend) { b.writeRHook = nil })
	keys, err := r.ManifestManager.List(context.Background())
	if err != nil || len(keys) != 1 {
		t.Fatalf("want exactly one pending manifest after the failed upload, got %v (%v)", keys, err)
	}
	if n := countParquetFlat(t, lateDir); n != 1 {
		t.Fatalf("source must survive a failed upload, got %d", n)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	if total, _ := outputRowsByDay(t, eventsDir); total == 0 {
		t.Fatal("setup: the failed attempt must have left at least one live output")
	}
	return b, r, lateDir, eventsDir, keys[0]
}

// The writer records its run deadline; until that deadline (plus a margin) has
// passed, recovery must not roll the chunk back, because the writer may still
// be uploading. Once it has, the rollback removes every output of the failed
// attempt and the re-drain yields exactly one copy of each row.
func TestReorgRecovery_RollbackWaitsForWriterDeadline(t *testing.T) {
	const days = 5
	runCtx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	b, r, lateDir, eventsDir, key := newFailedUploadRig(t, runCtx, days)

	beforeTotal, _ := outputRowsByDay(t, eventsDir)
	// Past the 5-minute orphan age, but the writer's deadline is still ahead.
	rewriteManifestFields(t, b, key, func(m map[string]any) {
		shiftTimeField(t, m, "updated_at", -30*time.Minute)
		shiftTimeField(t, m, "created_at", -30*time.Minute)
		if _, ok := m["deadline"]; !ok {
			t.Fatal("the manifest must record the writer's run deadline")
		}
	})
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("run2: %v", err)
	}
	if !objectExists(t, b, key) {
		t.Fatal("recovery rolled back a chunk whose writer may still be live (deadline not passed)")
	}
	if total, _ := outputRowsByDay(t, eventsDir); total != beforeTotal {
		t.Fatalf("outputs changed while the writer's deadline was ahead: %d -> %d rows", beforeTotal, total)
	}
	if n := countParquetFlat(t, lateDir); n != 1 {
		t.Fatalf("source must stay owned by the pending manifest, got %d", n)
	}

	// The writer's deadline is now well past: roll back and re-drain.
	rewriteManifestFields(t, b, key, func(m map[string]any) {
		shiftTimeField(t, m, "deadline", -2*time.Hour)
	})
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("run3: %v", err)
	}
	assertNoDuplicateDays(t, eventsDir, days)
	if n := countParquetFlat(t, lateDir); n != 0 {
		t.Errorf("sources must be drained after rollback + re-drain, %d remain", n)
	}
	if left, _ := r.ManifestManager.List(context.Background()); len(left) != 0 {
		t.Errorf("no manifest should remain, got %v", left)
	}
}

// An "uploaded" manifest whose outputs are not all live must not delete its
// sources: those rows exist nowhere else. Recovery rolls the chunk back instead
// and the re-drain restores every row exactly once.
func TestReorgRecovery_UploadedWithMissingOutputKeepsSources(t *testing.T) {
	const days = 4
	b, r, lateDir, eventsDir := setupUploadedNotDeleted(t, days)
	ctx := context.Background()
	keys, err := r.ManifestManager.List(ctx)
	if err != nil || len(keys) != 1 {
		t.Fatalf("setup: %v %v", keys, err)
	}
	man, err := r.ManifestManager.Read(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	// One output vanishes (operator delete, lost object, ...).
	if err := b.LocalBackend.Delete(ctx, man.PlannedOutputs[0].Path); err != nil {
		t.Fatal(err)
	}
	rewriteManifestFields(t, b, keys[0], func(m map[string]any) {
		shiftTimeField(t, m, "updated_at", -24*time.Hour)
		shiftTimeField(t, m, "created_at", -24*time.Hour)
	})
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run2: %v", err)
	}
	total, perDay := outputRowsByDay(t, eventsDir)
	if len(perDay) != days {
		t.Fatalf("DATA LOSS: %d of %d event-days present after recovery (rows=%d); "+
			"sources were deleted although an output was missing", len(perDay), days, total)
	}
	assertNoDuplicateDays(t, eventsDir, days)
	if n := countParquetFlat(t, lateDir); n != 0 {
		t.Errorf("after rollback + re-drain the sources should be drained, %d remain", n)
	}
}

// Daily/hourly compaction must not consume a reorg output while a reorg
// manifest still names it: that chunk may yet be rolled back (its outputs
// deleted and its sources re-drained), and a consumed output would then exist
// twice.
func TestCompactionFilter_ExcludesOutputsOfInFlightReorgManifests(t *testing.T) {
	m, backend, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	rmm := NewReorgManifestManager(backend, zerolog.Nop())
	owned := "posthog/events/2026/04/01/events_reorg_123_0_0.parquet"
	free := "posthog/events/2026/04/01/events_reorg_999_0_0.parquet"
	daily := "posthog/events/2026/04/01/events_20260402_080000_abcd1234_daily.parquet"
	if _, err := rmm.Write(ctx, &ReorgManifest{
		JobID: "123_0", Database: "posthog", Measurement: "events", LateName: "events_late",
		SourceFiles:    []string{"posthog/events_late/events_late_20260520_030000_0.parquet"},
		PlannedOutputs: []PlannedReorgOutput{{Path: owned, Size: 100}},
		Status:         ReorgStatusPending,
	}); err != nil {
		t.Fatal(err)
	}
	cand := Candidate{
		Database: "posthog", Measurement: "events", PartitionPath: "posthog/events/2026/04/01",
		Files: []string{daily, owned, free}, FileCount: 3, Tier: "daily",
	}
	got, ok, err := m.filterCandidateFilesWithError(ctx, cand)
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if !ok {
		t.Fatal("the candidate still has compactable files")
	}
	for _, f := range got.Files {
		if f == owned {
			t.Fatalf("DUPLICATION HAZARD: output %s of an in-flight reorg manifest was handed to compaction", owned)
		}
	}
	if got.FileCount != 2 {
		t.Errorf("FileCount = %d, want 2 (%v)", got.FileCount, got.Files)
	}
}

func TestCompactionFilter_FailsClosedOnUnreadableReorgManifest(t *testing.T) {
	b, _ := newHookBackend(t)
	m := NewManager(&ManagerConfig{
		StorageBackend: b,
		LockManager:    NewLockManager(),
		MinAgeHours:    1,
		MinFiles:       2,
		TempDirectory:  t.TempDir(),
		Logger:         zerolog.Nop(),
	})
	ctx := context.Background()
	rmm := NewReorgManifestManager(b, zerolog.Nop())
	key, err := rmm.Write(ctx, &ReorgManifest{JobID: "1_0", LateName: "events_late", Status: ReorgStatusPending,
		PlannedOutputs: []PlannedReorgOutput{{Path: "posthog/events/2026/04/01/events_reorg_1_0_0.parquet", Size: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	b.set(func(b *hookBackend) {
		b.readHook = func(path string) error {
			if path == key {
				return transientReadErrors()["timeout"]
			}
			return nil
		}
	})
	cand := Candidate{
		Database: "posthog", Measurement: "events", PartitionPath: "posthog/events/2026/04/01",
		Files: []string{
			"posthog/events/2026/04/01/events_20260402_080000_abcd1234_daily.parquet",
			"posthog/events/2026/04/01/events_reorg_1_0_0.parquet",
		},
		FileCount: 2, Tier: "daily",
	}
	if _, ok, err := m.filterCandidateFilesWithError(ctx, cand); ok || err == nil {
		t.Fatalf("an unreadable reorg manifest may own this partition's reorg outputs: want skip+error, got ok=%v err=%v", ok, err)
	}
}
