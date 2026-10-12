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

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// CS-10: Hetzner Ceph can leave a phantom index entry: LIST shows the key while
// GET and HEAD answer 404. One such key in <m>_late/ used to fail its whole
// chunk on every cycle, stranding up to max_files_per_batch late files forever.

func reorgPhantomCount() int64 {
	v, _ := metrics.Get().Snapshot()["reorg_phantom_keys_total"].(int64)
	return v
}

// phantomRig writes three one-row late files (distinct event days) into one
// closed bucket. The middle one is the key the test makes misbehave.
func phantomRig(t *testing.T) (*hookBackend, *Reorganizer, string, string, string) {
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
	for i, name := range []string{
		"events_late_20260520_030000_000000001.parquet",
		"events_late_20260520_030100_000000002.parquet",
		"events_late_20260520_030200_000000003.parquet",
	} {
		writeLateParquet(t, db, lateDir, name, fmt.Sprintf(
			`SELECT TIMESTAMP '2026-04-0%d 12:00:00' AS time, 'h%d'::VARCHAR AS host, %d::BIGINT AS value`, i+1, i, i))
	}
	db.Close()
	scratch := filepath.Join(tmp, "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	r := newReorg(b, scratch, zerolog.New(io.Discard))
	odd := "posthog/events_late/events_late_20260520_030100_000000002.parquet"
	return b, r, lateDir, filepath.Join(tmp, "posthog", "events"), odd
}

func notFoundErr(path string) error {
	return fmt.Errorf("storage read: %w: %s: operation error S3: GetObject, StatusCode: 404, api error NoSuchKey", storage.ErrNotFound, path)
}

func TestReorg_PhantomKeySkippedChunkStillDrains(t *testing.T) {
	b, r, lateDir, eventsDir, phantom := phantomRig(t)
	b.set(func(b *hookBackend) {
		b.readToHook = func(path string) error {
			if path == phantom {
				return notFoundErr(path)
			}
			return nil
		}
		b.existsHook = func(path string) (bool, error, bool) {
			if path == phantom {
				return false, nil, true
			}
			return false, nil, false
		}
	})
	before := reorgPhantomCount()
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	total, perDay := outputRowsByDay(t, eventsDir)
	if total != 2 || len(perDay) != 2 {
		t.Fatalf("STALL: the two real files must drain past the phantom key; got %d rows over %d days", total, len(perDay))
	}
	if got := reorgPhantomCount(); got != before+1 {
		t.Errorf("reorg_phantom_keys_total = %d, want %d", got, before+1)
	}
	// The real sources are drained; the phantom listing entry is not touched.
	entries, _ := os.ReadDir(lateDir)
	var left []string
	for _, e := range entries {
		left = append(left, e.Name())
	}
	if len(left) != 1 || !strings.HasSuffix(phantom, left[0]) {
		t.Errorf("only the phantom key should remain listed, got %v", left)
	}
	if keys, _ := r.ManifestManager.List(context.Background()); len(keys) != 0 {
		t.Errorf("the chunk must commit fully, leftover manifests %v", keys)
	}
}

// A GET 404 is not proof on its own: when HEAD says the object exists (or
// cannot answer), the key is never skipped and the chunk keeps its old
// all-or-nothing behaviour.
func TestReorg_Get404ButObjectExistsIsNotSkipped(t *testing.T) {
	for name, head := range map[string]func(string) (bool, error, bool){
		"head_says_exists": func(string) (bool, error, bool) { return true, nil, true },
		"head_fails":       func(string) (bool, error, bool) { return false, errors.New("HeadObject: StatusCode: 503"), true },
	} {
		t.Run(name, func(t *testing.T) {
			b, r, lateDir, eventsDir, odd := phantomRig(t)
			b.set(func(b *hookBackend) {
				b.readToHook = func(path string) error {
					if path == odd {
						return notFoundErr(path)
					}
					return nil
				}
				b.existsHook = func(path string) (bool, error, bool) {
					if path == odd {
						return head(path)
					}
					return false, nil, false
				}
			})
			before := reorgPhantomCount()
			_ = r.Run(context.Background())
			if total, _ := outputRowsByDay(t, eventsDir); total != 0 {
				t.Fatalf("a key that may exist was skipped: %d rows drained without it", total)
			}
			if n := countParquetFlat(t, lateDir); n != 3 {
				t.Fatalf("all sources must stay for the retry, got %d", n)
			}
			if got := reorgPhantomCount(); got != before {
				t.Errorf("phantom counter moved for a key that was not proven gone: %d -> %d", before, got)
			}
		})
	}
}

// A transient GET failure is never treated as a phantom.
func TestReorg_TransientDownloadErrorIsNotAPhantom(t *testing.T) {
	b, r, lateDir, eventsDir, odd := phantomRig(t)
	b.set(func(b *hookBackend) {
		b.readToHook = func(path string) error {
			if path == odd {
				return transientReadErrors()["503_with_404_in_request_id"]
			}
			return nil
		}
		b.existsHook = func(path string) (bool, error, bool) {
			if path == odd {
				return false, nil, true
			}
			return false, nil, false
		}
	})
	_ = r.Run(context.Background())
	if total, _ := outputRowsByDay(t, eventsDir); total != 0 {
		t.Fatalf("a transient GET error must fail the chunk, %d rows drained", total)
	}
	if n := countParquetFlat(t, lateDir); n != 3 {
		t.Fatalf("all sources must stay, got %d", n)
	}
}

// A chunk made only of phantom keys is a no-op, not a crash.
func TestReorg_AllPhantomChunkIsNoop(t *testing.T) {
	b, r, lateDir, eventsDir, _ := phantomRig(t)
	b.set(func(b *hookBackend) {
		b.readToHook = func(path string) error { return notFoundErr(path) }
		b.existsHook = func(path string) (bool, error, bool) {
			if strings.Contains(path, "events_late/") {
				return false, nil, true
			}
			return false, nil, false
		}
	})
	before := reorgPhantomCount()
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if total, _ := outputRowsByDay(t, eventsDir); total != 0 {
		t.Fatalf("nothing can be drained, got %d rows", total)
	}
	if n := countParquetFlat(t, lateDir); n != 3 {
		t.Fatalf("phantom keys are never deleted, got %d left", n)
	}
	if got := reorgPhantomCount(); got != before+3 {
		t.Errorf("reorg_phantom_keys_total = %d, want %d", got, before+3)
	}
	if keys, _ := r.ManifestManager.List(context.Background()); len(keys) != 0 {
		t.Errorf("no manifest for an empty chunk, got %v", keys)
	}
}
