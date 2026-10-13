package compaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/circuitbreaker"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// CS-5: a transient S3 read error must never delete a manifest, a failed
// manifest listing must never read as "no manifests", and the reorg drain must
// skip its cycle rather than re-drain sources an unreadable manifest may own.
// Every one of these used to produce duplicate rows on a Ceph flap.

// transientReadErrors are the shapes a Hetzner Ceph flap produces. None of them
// proves the manifest is gone. The 503 carries "0404" inside its hex
// RequestID: a classifier that matches a bare "404" would call it not-found.
func transientReadErrors() map[string]error {
	return map[string]error{
		"503_with_404_in_request_id": errors.New("failed to read from S3: operation error S3: GetObject, " +
			"https response error StatusCode: 503, RequestID: tx00000a3f2e0404b1c2d3e-0068f0a1b2-1a2b3c-fsn1, " +
			"HostID: 1a2b3c-fsn1-prod, api error SlowDown: Please reduce your request rate."),
		"timeout":          fmt.Errorf("storage read failed after 3 retries: %w", context.DeadlineExceeded),
		"connection_reset": errors.New("failed to read S3 object body: read tcp 10.0.0.7:41822->49.12.0.1:443: read: connection reset by peer"),
		"circuit_open":     fmt.Errorf("storage read: %w", circuitbreaker.ErrCircuitOpen),
	}
}

func newHookBackend(t *testing.T) (*hookBackend, string) {
	t.Helper()
	tmp := t.TempDir()
	local, err := storage.NewLocalBackend(tmp, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	return &hookBackend{LocalBackend: local}, tmp
}

// writeAgedReorgManifest stores a manifest whose UpdatedAt/CreatedAt are `age`
// in the past, so recovery treats it as eligible.
func writeAgedReorgManifest(t *testing.T, b storage.Backend, mm *ReorgManifestManager, man *ReorgManifest, age time.Duration) string {
	t.Helper()
	ctx := context.Background()
	key, err := mm.Write(ctx, man)
	if err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	man.UpdatedAt = time.Now().UTC().Add(-age)
	man.CreatedAt = man.UpdatedAt
	if err := writeRawManifest(ctx, b, key, man); err != nil {
		t.Fatalf("age manifest: %v", err)
	}
	return key
}

func objectExists(t *testing.T, b *hookBackend, key string) bool {
	t.Helper()
	ok, err := b.LocalBackend.Exists(context.Background(), key)
	if err != nil {
		t.Fatalf("exists %s: %v", key, err)
	}
	return ok
}

func TestReorgManifestList_PropagatesStorageErrors(t *testing.T) {
	b, _ := newHookBackend(t)
	sentinel := errors.New("SlowDown: please reduce your request rate")
	b.set(func(b *hookBackend) {
		b.listHook = func(prefix string) error {
			if strings.HasPrefix(prefix, ReorgManifestBasePath) {
				return sentinel
			}
			return nil
		}
	})
	mm := NewReorgManifestManager(b, zerolog.Nop())

	keys, err := mm.List(context.Background())
	if err == nil {
		t.Fatalf("a failed manifest listing must be an error, got nil with %d keys "+
			"(an empty list tells the drain that nothing is in flight)", len(keys))
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("the storage error should be wrapped, got %v", err)
	}
	if _, err := mm.SourceFilesInManifests(context.Background()); err == nil {
		t.Error("SourceFilesInManifests must fail when the listing fails")
	}
	if _, err := mm.RecoverOrphanedReorgManifests(context.Background()); err == nil {
		t.Error("recovery must report a failed listing instead of 'nothing to recover'")
	}
}

func TestReorgManifestList_EmptyStorageIsNotAnError(t *testing.T) {
	b, _ := newHookBackend(t)
	mm := NewReorgManifestManager(b, zerolog.Nop())
	keys, err := mm.List(context.Background())
	if err != nil {
		t.Fatalf("a fresh install with no reorg manifests must not error: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected no manifests, got %v", keys)
	}
}

func TestReorgRecovery_TransientReadKeepsManifest(t *testing.T) {
	for name, readErr := range transientReadErrors() {
		t.Run(name, func(t *testing.T) {
			b, _ := newHookBackend(t)
			mm := NewReorgManifestManager(b, zerolog.Nop())
			key := writeAgedReorgManifest(t, b, mm, &ReorgManifest{
				JobID: "j1", Database: "posthog", Measurement: "events", LateName: "events_late",
				BucketHour:  time.Date(2026, 5, 20, 3, 0, 0, 0, time.UTC),
				SourceFiles: []string{"posthog/events_late/events_late_20260520_030000_0.parquet"},
				PlannedOutputs: []PlannedReorgOutput{
					{Path: "posthog/events/2026/04/01/events_reorg_j1_0.parquet", Size: 10},
				},
				Status: ReorgStatusUploaded,
			}, time.Hour)

			b.set(func(b *hookBackend) {
				b.readHook = func(path string) error {
					if path == key {
						return readErr
					}
					return nil
				}
			})
			recovered, err := mm.RecoverOrphanedReorgManifests(context.Background())
			if err != nil {
				t.Fatalf("a per-manifest read failure must not fail the whole pass: %v", err)
			}
			if recovered != 0 {
				t.Errorf("recovered = %d, want 0 (nothing was recovered)", recovered)
			}
			if !objectExists(t, b, key) {
				t.Fatalf("DUPLICATION HAZARD: a transient read error (%v) deleted the manifest", readErr)
			}
		})
	}
}

// A manifest that is listed but whose GET answers a classified not-found is
// gone (its run finished between list and read) or a Ceph phantom listing
// entry. Either way there is nothing to recover and nothing to fail on.
func TestReorgRecovery_NotFoundIsSkipped(t *testing.T) {
	b, _ := newHookBackend(t)
	mm := NewReorgManifestManager(b, zerolog.Nop())
	key := writeAgedReorgManifest(t, b, mm, &ReorgManifest{
		JobID: "j1", Database: "posthog", Measurement: "events", LateName: "events_late",
		Status: ReorgStatusPending,
	}, time.Hour)
	b.set(func(b *hookBackend) {
		b.readHook = func(path string) error {
			if path == key {
				return fmt.Errorf("%w: %s", storage.ErrNotFound, path)
			}
			return nil
		}
	})
	recovered, err := mm.RecoverOrphanedReorgManifests(context.Background())
	if err != nil {
		t.Fatalf("not-found must not fail the pass: %v", err)
	}
	if recovered != 0 {
		t.Errorf("recovered = %d, want 0", recovered)
	}
	files, err := mm.SourceFilesInManifests(context.Background())
	if err != nil {
		t.Fatalf("a manifest that is gone holds nothing back; got error %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected no owned sources, got %v", files)
	}
}

func reorgParkedCount() int64 {
	v, _ := metrics.Get().Snapshot()["reorg_manifests_parked_unparseable_total"].(int64)
	return v
}

// An undecodable manifest is parked (copied under the quarantine suffix, then
// removed from the .json work set) and counted, never silently deleted.
func TestReorgRecovery_UnparseableIsParkedAndCounted(t *testing.T) {
	b, _ := newHookBackend(t)
	mm := NewReorgManifestManager(b, zerolog.Nop())
	ctx := context.Background()
	key := ReorgManifestBasePath + "/events_late_2026052003_garbage.json"
	if err := b.Write(ctx, key, []byte("{corrupt")); err != nil {
		t.Fatal(err)
	}

	// The candidate filter must not treat it as "owns nothing": it fails closed.
	if _, err := mm.SourceFilesInManifests(ctx); err == nil {
		t.Fatal("an unparseable manifest names sources nobody can see; the drain filter must fail closed")
	}

	before := reorgParkedCount()
	recovered, err := mm.RecoverOrphanedReorgManifests(ctx)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if recovered != 0 {
		t.Errorf("parking is not recovery: recovered = %d", recovered)
	}
	if objectExists(t, b, key) {
		t.Fatal("the unparseable manifest must leave the .json work set")
	}
	parked, err := quarantinePathFor(key)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := b.LocalBackend.Read(ctx, parked)
	if err != nil {
		t.Fatalf("the parked copy must survive at %s: %v", parked, err)
	}
	if string(raw) != "{corrupt" {
		t.Errorf("parked bytes = %q, want the original bytes", raw)
	}
	if got := reorgParkedCount(); got != before+1 {
		t.Errorf("reorg_manifests_parked_unparseable_total = %d, want %d", got, before+1)
	}
	// Parked manifests are invisible to the filter and to the next pass.
	if _, err := mm.SourceFilesInManifests(ctx); err != nil {
		t.Fatalf("after parking the filter must succeed: %v", err)
	}
	if _, err := mm.RecoverOrphanedReorgManifests(ctx); err != nil {
		t.Fatal(err)
	}
	if got := reorgParkedCount(); got != before+1 {
		t.Errorf("nothing left to park must count nothing: %d", got)
	}
}

func TestReorgSourceFilesInManifests_FailsClosedOnTransientRead(t *testing.T) {
	for name, readErr := range transientReadErrors() {
		t.Run(name, func(t *testing.T) {
			b, _ := newHookBackend(t)
			mm := NewReorgManifestManager(b, zerolog.Nop())
			ctx := context.Background()
			if _, err := mm.Write(ctx, &ReorgManifest{JobID: "ok", LateName: "events_late",
				SourceFiles: []string{"posthog/events_late/a.parquet"}}); err != nil {
				t.Fatal(err)
			}
			bad, err := mm.Write(ctx, &ReorgManifest{JobID: "flaky", LateName: "events_late",
				SourceFiles: []string{"posthog/events_late/b.parquet"}})
			if err != nil {
				t.Fatal(err)
			}
			b.set(func(b *hookBackend) {
				b.readHook = func(path string) error {
					if path == bad {
						return readErr
					}
					return nil
				}
			})
			files, err := mm.SourceFilesInManifests(ctx)
			if err == nil {
				t.Fatalf("DUPLICATION HAZARD: an unreadable manifest was skipped; its sources %v "+
					"would be re-drained (got %v)", []string{"posthog/events_late/b.parquet"}, files)
			}
		})
	}
}

// setupUploadedNotDeleted runs one drain whose source delete fails, leaving a
// young "uploaded" manifest that owns a still-present source and live outputs.
func setupUploadedNotDeleted(t *testing.T, days int) (*hookBackend, *Reorganizer, string, string) {
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
	b.set(func(b *hookBackend) {
		b.deleteBHook = func(paths []string) error { return errors.New("injected DeleteBatch failure") }
	})
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("run1: %v", err)
	}
	b.set(func(b *hookBackend) { b.deleteBHook = nil })
	if n := countParquetFlat(t, lateDir); n != 1 {
		t.Fatalf("setup: source must survive the failed delete, got %d", n)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	if total, _ := outputRowsByDay(t, eventsDir); total != int64(days) {
		t.Fatalf("setup: outputs hold %d rows, want %d", total, days)
	}
	return b, r, lateDir, eventsDir
}

func assertNoDuplicateDays(t *testing.T, eventsDir string, days int) {
	t.Helper()
	total, perDay := outputRowsByDay(t, eventsDir)
	if total != int64(days) || len(perDay) != days {
		t.Errorf("got %d rows over %d days, want exactly %d over %d (duplicates or loss)", total, len(perDay), days, days)
	}
	for k, v := range perDay {
		if v != 1 {
			t.Errorf("DUPLICATE: day %s has %d rows, want 1", k, v)
		}
	}
}

// A manifest LIST failure must skip the drain cycle: proceeding "as if no
// manifests" re-reads the sources an uploaded manifest still owns.
func TestReorgRun_ManifestListErrorSkipsCycle(t *testing.T) {
	const days = 3
	b, r, lateDir, eventsDir := setupUploadedNotDeleted(t, days)

	b.set(func(b *hookBackend) {
		b.listHook = func(prefix string) error {
			if strings.HasPrefix(prefix, ReorgManifestBasePath) {
				return errors.New("operation error S3: ListObjectsV2, StatusCode: 503, SlowDown")
			}
			return nil
		}
	})
	_ = r.Run(context.Background())
	assertNoDuplicateDays(t, eventsDir, days)
	if n := countParquetFlat(t, lateDir); n != 1 {
		t.Errorf("source must stay put while manifests are unreadable, got %d", n)
	}
}

// A transient manifest READ failure must neither delete the manifest (recovery)
// nor let the drain re-read its sources (filter). Once storage recovers, the
// next pass completes the chunk without a duplicate.
func TestReorgRun_ManifestTransientReadDoesNotReDrain(t *testing.T) {
	const days = 3
	b, r, lateDir, eventsDir := setupUploadedNotDeleted(t, days)
	ctx := context.Background()

	keys, err := r.ManifestManager.List(ctx)
	if err != nil || len(keys) != 1 {
		t.Fatalf("setup: want 1 manifest, got %v (%v)", keys, err)
	}
	// Age it so recovery is allowed to act on it.
	man, err := r.ManifestManager.Read(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	man.UpdatedAt = man.UpdatedAt.Add(-2 * ReorgManifestMinRecoveryAge)
	man.CreatedAt = man.UpdatedAt
	if err := writeRawManifest(ctx, b, keys[0], man); err != nil {
		t.Fatal(err)
	}

	b.set(func(b *hookBackend) {
		b.readHook = func(path string) error {
			if strings.HasPrefix(path, ReorgManifestBasePath) {
				return transientReadErrors()["503_with_404_in_request_id"]
			}
			return nil
		}
	})
	_ = r.Run(ctx)
	if !objectExists(t, b, keys[0]) {
		t.Fatal("DUPLICATION HAZARD: recovery deleted the manifest on a transient read error")
	}
	assertNoDuplicateDays(t, eventsDir, days)

	// Storage recovers: recovery completes the uploaded chunk.
	b.set(func(b *hookBackend) { b.readHook = nil })
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run3: %v", err)
	}
	assertNoDuplicateDays(t, eventsDir, days)
	if n := countParquetFlat(t, lateDir); n != 0 {
		t.Errorf("recovery must finish the source delete, %d remain", n)
	}
	if left, _ := r.ManifestManager.List(ctx); len(left) != 0 {
		t.Errorf("manifest must be gone after completion, got %v", left)
	}
}

// Compaction side (CS-5a, fixed in the merge): a transient read error keeps an
// aged compaction manifest; only a parse failure parks it.
func TestCompactionRecovery_TransientReadKeepsManifest(t *testing.T) {
	for name, readErr := range transientReadErrors() {
		t.Run(name, func(t *testing.T) {
			b, _ := newHookBackend(t)
			mm := NewManifestManager(b, zerolog.Nop())
			ctx := context.Background()
			key, err := mm.WriteManifest(ctx, &Manifest{
				OutputPath:    "db/m/2026/05/20/03/m_compacted.parquet",
				OutputSize:    10,
				InputFiles:    []string{"db/m/2026/05/20/03/a.parquet"},
				Database:      "db",
				Measurement:   "m",
				PartitionPath: "db/m/2026/05/20/03",
				Tier:          "hourly",
				CreatedAt:     time.Now().UTC().Add(-2 * ManifestMinRecoveryAge),
				JobID:         "job1",
			})
			if err != nil {
				t.Fatal(err)
			}
			b.set(func(b *hookBackend) {
				b.readHook = func(path string) error {
					if path == key {
						return readErr
					}
					return nil
				}
			})
			if _, err := mm.RecoverOrphanedManifests(ctx, nil, nil); err == nil {
				t.Error("a manifest left unrecovered should be reported")
			}
			if !objectExists(t, b, key) {
				t.Fatalf("a transient read error (%v) deleted the compaction manifest", readErr)
			}
			if _, err := mm.GetFilesInManifests(ctx); err == nil {
				t.Error("GetFilesInManifests must fail closed on an unreadable manifest")
			}
		})
	}
}
