package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// REVIEW-2: CS-10 skips a proven phantom key inside its chunk, but bucket
// selection (oldest first, capped at max_buckets_per_run) runs before any GET.
// A phantom is never purged, so a phantom-only hour bucket stays the oldest and
// took a slot on every cycle while draining nothing. With more phantom-only
// buckets than max_buckets_per_run, newer real late files were never drained.

// phantomListing adds listing-only keys (Ceph phantom index entries: LIST
// shows them, GET and HEAD answer not-found) to the events_late listing of a
// real local backend, and records every GET and HEAD the drain makes.
type phantomListing struct {
	*hookBackend

	mu       sync.Mutex
	phantoms map[string]bool
	gets     map[string]int
	heads    map[string]int
}

const phantomSidecarPrefix = "posthog/events_late/"

func newPhantomListing(t *testing.T) (*phantomListing, string) {
	t.Helper()
	t.Setenv("TZ", "UTC")
	hb, tmp := newHookBackend(t)
	p := &phantomListing{hookBackend: hb, phantoms: map[string]bool{}, gets: map[string]int{}, heads: map[string]int{}}
	hb.set(func(h *hookBackend) {
		h.readToHook = func(path string) error {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.gets[path]++
			if p.phantoms[path] {
				return notFoundErr(path)
			}
			return nil
		}
		h.existsHook = func(path string) (bool, error, bool) {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.heads[path]++
			if p.phantoms[path] {
				return false, nil, true
			}
			return false, nil, false
		}
		h.deleteBHook = func(paths []string) error {
			p.mu.Lock()
			defer p.mu.Unlock()
			for _, k := range paths {
				if p.phantoms[k] {
					return fmt.Errorf("test: the drain tried to delete phantom key %s", k)
				}
			}
			return nil
		}
	})
	if err := os.MkdirAll(filepath.Join(tmp, "posthog", "events_late"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p, tmp
}

func (p *phantomListing) List(ctx context.Context, prefix string) ([]string, error) {
	out, err := p.hookBackend.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if prefix == phantomSidecarPrefix {
		p.mu.Lock()
		for k := range p.phantoms {
			out = append(out, k)
		}
		p.mu.Unlock()
		sort.Strings(out)
	}
	return out, nil
}

func (p *phantomListing) addPhantoms(keys ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, k := range keys {
		p.phantoms[k] = true
	}
}

// phantomGetsHeads returns how many GETs and HEADs the drain sent for the
// listed phantom keys since the last reset, and resets the counts.
func (p *phantomListing) phantomGetsHeads(keys []string) (gets, heads int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, k := range keys {
		gets += p.gets[k]
		heads += p.heads[k]
	}
	p.gets = map[string]int{}
	p.heads = map[string]int{}
	return gets, heads
}

// phantomOnlyBuckets returns n phantom keys, one per hour of 2026-05-01, all
// older than the real files the tests write on 2026-05-20.
func phantomOnlyBuckets(n int) []string {
	keys := make([]string, 0, n)
	for h := 0; h < n; h++ {
		keys = append(keys, fmt.Sprintf("%sevents_late_20260501_%02d0000_%09d.parquet", phantomSidecarPrefix, h, h+1))
	}
	return keys
}

// writeRealLate writes one late file with `rows` rows (event day 2026-04-01).
func writeRealLate(t *testing.T, tmp, name string, rows int) string {
	t.Helper()
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	writeLateParquet(t, db, lateDir, name, fmt.Sprintf(
		`SELECT TIMESTAMP '2026-04-01 12:00:00' + INTERVAL (r) MINUTE AS time, 'h'::VARCHAR AS host, r::BIGINT AS value FROM range(%d) t(r)`, rows))
	return filepath.Join(lateDir, name)
}

func newPhantomBucketReorg(t *testing.T, b *phantomListing, tmp string) *Reorganizer {
	t.Helper()
	scratch := filepath.Join(tmp, "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	return newReorg(b, scratch, zerolog.New(io.Discard))
}

func TestReorg_PhantomOnlyBucketsDoNotStarveTheDrain(t *testing.T) {
	for _, conc := range []int{1, 3} {
		t.Run(fmt.Sprintf("max_concurrent_%d", conc), func(t *testing.T) {
			b, tmp := newPhantomListing(t)
			phantoms := phantomOnlyBuckets(5)
			b.addPhantoms(phantoms...)
			real := writeRealLate(t, tmp, "events_late_20260520_030000_000000001.parquet", 3)
			r := newPhantomBucketReorg(t, b, tmp)
			r.MaxBucketsPerRun = 4 // the production default
			r.MaxConcurrent = conc

			if err := r.Run(context.Background()); err != nil {
				t.Fatalf("run: %v", err)
			}
			if fileExists(t, real) {
				t.Fatal("STALL: 5 phantom-only buckets held every max_buckets_per_run slot; the real late file was not drained")
			}
			if total, _ := outputRowsByDay(t, filepath.Join(tmp, "posthog", "events")); total != 3 {
				t.Fatalf("drained rows = %d, want 3", total)
			}
			if keys, _ := r.ManifestManager.List(context.Background()); len(keys) != 0 {
				t.Errorf("leftover manifests %v", keys)
			}
		})
	}
}

// Buckets that cannot be drained for a structural reason (the `time` column is
// not a TIMESTAMP, logged and skipped every cycle) are also no-ops and must
// not hold the newer buckets back either.
func TestReorg_SkippedNonTimestampBucketsDoNotStarveTheDrain(t *testing.T) {
	b, tmp := newPhantomListing(t)
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	var skipped []string
	for h := 0; h < 4; h++ {
		name := fmt.Sprintf("events_late_20260501_%02d0000_%09d.parquet", h, h+1)
		writeLateParquet(t, db, lateDir, name, `SELECT '2026-04-01 12:00:00'::VARCHAR AS time, 'h'::VARCHAR AS host, 1::BIGINT AS value`)
		skipped = append(skipped, filepath.Join(lateDir, name))
	}
	db.Close()
	real := writeRealLate(t, tmp, "events_late_20260520_030000_000000009.parquet", 2)
	r := newPhantomBucketReorg(t, b, tmp)
	r.MaxBucketsPerRun = 4

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if fileExists(t, real) {
		t.Fatal("STALL: 4 skipped (non-TIMESTAMP) buckets held every max_buckets_per_run slot")
	}
	if total, _ := outputRowsByDay(t, filepath.Join(tmp, "posthog", "events")); total != 2 {
		t.Fatalf("drained rows = %d, want 2", total)
	}
	for _, p := range skipped {
		if !fileExists(t, p) {
			t.Errorf("a skipped bucket's source was deleted: %s", p)
		}
	}
}

// Guard: a bucket that FAILS still uses its max_buckets_per_run slot, so a
// cycle during an S3 outage stays bounded instead of walking every bucket.
func TestReorg_FailingBucketsStillUseTheirSlot(t *testing.T) {
	b, tmp := newPhantomListing(t)
	var reals []string
	for h := 0; h < 6; h++ {
		reals = append(reals, writeRealLate(t, tmp, fmt.Sprintf("events_late_20260520_%02d0000_%09d.parquet", h, h+1), 1))
	}
	b.hookBackend.set(func(h *hookBackend) {
		h.readToHook = func(path string) error {
			b.mu.Lock()
			b.gets[path]++
			b.mu.Unlock()
			if strings.HasPrefix(path, phantomSidecarPrefix) {
				return transientReadErrors()["503_with_404_in_request_id"]
			}
			return nil
		}
	})
	r := newPhantomBucketReorg(t, b, tmp)
	r.MaxBucketsPerRun = 2
	_ = r.Run(context.Background())

	b.mu.Lock()
	attempted := 0
	for k := range b.gets {
		if strings.HasPrefix(k, phantomSidecarPrefix) {
			attempted++
		}
	}
	b.mu.Unlock()
	if attempted != 2 {
		t.Errorf("a failing cycle attempted %d buckets, want max_buckets_per_run=2", attempted)
	}
	for _, p := range reals {
		if !fileExists(t, p) {
			t.Errorf("a failed bucket's source was deleted: %s", p)
		}
	}
}

// Every cycle now walks every phantom-only bucket, so without memory a listed
// phantom would cost a GET and a HEAD every 15 minutes, forever, on the Ceph
// that is already struggling. A key proven gone is skipped without any request
// until PhantomRecheckInterval has passed, then verified again.
func TestReorg_ProvenPhantomIsNotRefetchedUntilRecheck(t *testing.T) {
	b, tmp := newPhantomListing(t)
	phantoms := phantomOnlyBuckets(2)
	b.addPhantoms(phantoms...)
	r := newPhantomBucketReorg(t, b, tmp)
	ctx := context.Background()

	before := reorgPhantomCount()
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run 1: %v", err)
	}
	if gets, heads := b.phantomGetsHeads(phantoms); gets != 2 || heads != 2 {
		t.Fatalf("run 1: GETs=%d HEADs=%d on phantoms, want 2 and 2", gets, heads)
	}
	if got := reorgPhantomCount() - before; got != 2 {
		t.Fatalf("run 1: phantom counter +%d, want +2", got)
	}

	real := writeRealLate(t, tmp, "events_late_20260520_030000_000000001.parquet", 2)
	before = reorgPhantomCount()
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if gets, heads := b.phantomGetsHeads(phantoms); gets != 0 || heads != 0 {
		t.Fatalf("run 2 (inside the recheck interval): GETs=%d HEADs=%d on proven phantoms, want 0", gets, heads)
	}
	if got := reorgPhantomCount() - before; got != 0 {
		t.Errorf("run 2: phantom counter +%d, want +0 (not re-verified)", got)
	}
	if fileExists(t, real) {
		t.Fatal("run 2: the real file must still drain")
	}

	r.PhantomRecheckInterval = time.Nanosecond
	before = reorgPhantomCount()
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run 3: %v", err)
	}
	if gets, heads := b.phantomGetsHeads(phantoms); gets != 2 || heads != 2 {
		t.Fatalf("run 3 (recheck due): GETs=%d HEADs=%d, want 2 and 2", gets, heads)
	}
	if got := reorgPhantomCount() - before; got != 2 {
		t.Errorf("run 3: phantom counter +%d, want +2", got)
	}
}

// Remembering a phantom only delays a key, it never loses one: a key that
// turns out to hold an object (re-uploaded, or a GET/HEAD that both lied) is
// drained with all its rows at the first recheck.
func TestReorg_RememberedPhantomThatBecomesRealDrainsAtRecheck(t *testing.T) {
	b, tmp := newPhantomListing(t)
	name := "events_late_20260520_030000_000000007.parquet"
	key := phantomSidecarPrefix + name
	b.addPhantoms(key)
	r := newPhantomBucketReorg(t, b, tmp)
	ctx := context.Background()
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run 1: %v", err)
	}

	// The object now exists at the key.
	b.mu.Lock()
	delete(b.phantoms, key)
	b.mu.Unlock()
	real := writeRealLate(t, tmp, name, 4)

	if err := r.Run(ctx); err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if !fileExists(t, real) {
		t.Fatal("run 2 is inside the recheck interval; the remembered key is expected to wait")
	}
	r.PhantomRecheckInterval = time.Nanosecond
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run 3: %v", err)
	}
	if fileExists(t, real) {
		t.Fatal("the key must drain once its phantom memory expires")
	}
	if total, _ := outputRowsByDay(t, filepath.Join(tmp, "posthog", "events")); total != 4 {
		t.Fatalf("drained rows = %d, want 4", total)
	}
}
