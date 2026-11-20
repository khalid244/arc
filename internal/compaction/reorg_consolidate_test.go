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
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
)

// Consolidated drain (reorg.consolidate_outputs): several closed hour-buckets
// of one sidecar are drained as ONE committed unit, so each target day gets
// one _reorg_ file per group instead of one per (bucket, day).

const consolidateDays = 4 // event days every late file spreads its rows over

// writeConsolidateBuckets writes filesPerBucket late files into each of the
// given ingest hours (2026-05-20 HH:00, long closed). Every file holds one
// midday row on each of consolidateDays event days (2026-04-01..), with a
// value unique across the whole fixture, so rows never collide. Returns the
// sidecar dir and the source keys grouped by hour.
func writeConsolidateBuckets(t *testing.T, tmp string, hours []int, filesPerBucket int) (string, map[int][]string) {
	t.Helper()
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	if err := os.MkdirAll(lateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	keys := map[int][]string{}
	for _, h := range hours {
		for f := 0; f < filesPerBucket; f++ {
			name := fmt.Sprintf("events_late_20260520_%02d0000_%d.parquet", h, f)
			writeLateParquet(t, db, lateDir, name, fmt.Sprintf(`
				SELECT TIMESTAMP '2026-04-01 12:00:00' + (d * INTERVAL 1 DAY) AS time,
				       'h%d'::VARCHAR AS host,
				       (%d * 1000 + %d * 10 + d)::BIGINT AS value
				FROM range(0, %d) t(d)`, h, h, f, consolidateDays))
			keys[h] = append(keys[h], "posthog/events_late/"+name)
		}
	}
	return lateDir, keys
}

// outputFilesByDay maps "Y/M/D" to the reorg output file names in that day.
func outputFilesByDay(t *testing.T, eventsDir string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	_ = filepath.Walk(eventsDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(p) != ".parquet" {
			return nil
		}
		rel, _ := filepath.Rel(eventsDir, p)
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 4 {
			t.Errorf("output %s is not at Y/M/D/file", rel)
			return nil
		}
		day := strings.Join(parts[:3], "/")
		out[day] = append(out[day], parts[3])
		return nil
	})
	return out
}

func newConsolidatingReorg(b storage.Backend, tmp string) *Reorganizer {
	scratch := filepath.Join(tmp, "scratch")
	_ = os.MkdirAll(scratch, 0o755)
	r := newReorg(b, scratch, zerolog.New(io.Discard))
	r.MaxBucketsPerRun = 0
	r.ConsolidateOutputs = true
	return r
}

// assertLosslessNoDup checks every source row is in the events tree exactly once.
func assertLosslessNoDup(t *testing.T, eventsDir string, wantRows int64) {
	t.Helper()
	total, perDay := outputRowsByDay(t, eventsDir)
	if total != wantRows {
		t.Errorf("rows in events tree = %d, want %d (fewer = loss, more = duplicate)", total, wantRows)
	}
	if d := distinctRowsTree(t, eventsDir); d != wantRows {
		t.Errorf("distinct rows = %d, want %d", d, wantRows)
	}
	if len(perDay) != consolidateDays {
		t.Errorf("event days = %d, want %d", len(perDay), consolidateDays)
	}
}

func TestReorg_Consolidate_OneOutputPerDayAcrossBuckets(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	backend, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	lateDir, _ := writeConsolidateBuckets(t, tmp, []int{3, 4, 5}, 2)

	r := newConsolidatingReorg(backend, tmp)
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if n := countParquetFlat(t, lateDir); n != 0 {
		t.Errorf("%d sources left in the sidecar, want 0", n)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	assertLosslessNoDup(t, eventsDir, 3*2*consolidateDays)
	jobs := map[string]bool{}
	for day, files := range outputFilesByDay(t, eventsDir) {
		if len(files) != 1 {
			t.Errorf("day %s has %d outputs %v, want 1 for the whole drain", day, len(files), files)
		}
		for _, f := range files {
			if !isReorgFile(f) {
				t.Errorf("output %s lacks the %s marker compaction keys on", f, reorgFileMarker)
			}
			jobs[f[:strings.LastIndexByte(f, '_')]] = true
		}
	}
	if len(jobs) != 1 {
		t.Errorf("outputs come from %d jobs %v, want 1", len(jobs), jobs)
	}
	if keys, _ := r.ManifestManager.List(ctx); len(keys) != 0 {
		t.Errorf("manifests left: %v", keys)
	}
}

// Default (consolidate_outputs=false) keeps one output per (bucket, day).
func TestReorg_Consolidate_DefaultOffKeepsOneOutputPerBucketPerDay(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	backend, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	writeConsolidateBuckets(t, tmp, []int{3, 4, 5}, 2)

	r := newReorg(backend, filepath.Join(tmp, "scratch"), zerolog.New(io.Discard))
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)
	r.MaxBucketsPerRun = 0
	if r.ConsolidateOutputs {
		t.Fatal("ConsolidateOutputs must default to false")
	}
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	assertLosslessNoDup(t, eventsDir, 3*2*consolidateDays)
	for day, files := range outputFilesByDay(t, eventsDir) {
		if len(files) != 3 {
			t.Errorf("day %s has %d outputs, want 3 (one per bucket)", day, len(files))
		}
	}
}

// A consolidated group is one manifest: a crash after some of its outputs
// were uploaded leaves a pending manifest owning every source of every bucket
// in the group, and recovery rolls the whole group back before the sources
// are drained again. The failure is past the manifest write, so the buckets
// are NOT retried in the same cycle (their outputs may be live); they keep
// their solo mark while the manifest holds them and drain one at a time once
// recovery releases them. End state: lossless, zero duplicates.
func TestReorg_Consolidate_CrashAfterPartialUpload_RollsBackWholeGroup(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	local, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	fb := &faultBackend{LocalBackend: local, failOnWriteN: 2}
	lateDir, keys := writeConsolidateBuckets(t, tmp, []int{3, 4, 5}, 2)

	r := newConsolidatingReorg(fb, tmp)
	_ = r.Run(ctx)

	if n := countParquetFlat(t, lateDir); n != 6 {
		t.Fatalf("DATA SAFETY: %d sources left after a failed upload, want all 6", n)
	}
	if fb.writes.Load() < 1 {
		t.Fatalf("fixture: no output was uploaded before the fault; the partial-upload state is not exercised")
	}
	mm := r.ManifestManager
	mkeys, _ := mm.List(ctx)
	if len(mkeys) != 1 {
		t.Fatalf("want 1 pending manifest for the group, got %v", mkeys)
	}
	man, err := mm.Read(ctx, mkeys[0])
	if err != nil {
		t.Fatal(err)
	}
	if man.Status != ReorgStatusPending {
		t.Errorf("manifest status %q, want pending", man.Status)
	}
	var want []string
	for _, h := range []int{3, 4, 5} {
		want = append(want, keys[h]...)
	}
	got := append([]string(nil), man.SourceFiles...)
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("manifest sources = %v, want every source of the group %v", got, want)
	}
	if len(man.PlannedOutputs) != consolidateDays {
		t.Errorf("planned outputs = %d, want %d (one per day)", len(man.PlannedOutputs), consolidateDays)
	}
	if !man.BucketHour.Equal(time.Date(2026, 5, 20, 3, 0, 0, 0, time.UTC)) {
		t.Errorf("BucketHour = %v, want the oldest bucket of the group", man.BucketHour)
	}

	// A cycle while the manifest is too young to recover: its sources are
	// filtered out of the drain, and the buckets must keep their solo mark
	// even though the drain runs (on a new bucket 06) without them.
	fb.failOnWriteN = 0
	writeConsolidateBuckets(t, tmp, []int{6}, 1)
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countParquetFlat(t, lateDir); n != 6 {
		t.Fatalf("sources = %d while the manifest holds them (and 06 drained), want 6", n)
	}
	for _, h := range []int{3, 4, 5} {
		if !r.isSolo("posthog", "events_late", time.Date(2026, 5, 20, h, 0, 0, 0, time.UTC)) {
			t.Errorf("bucket %02d lost its solo mark while a manifest held it", h)
		}
	}

	// Age the manifest past every recovery gate (orphan age, undated rollback).
	man.UpdatedAt = man.UpdatedAt.Add(-24 * time.Hour)
	man.CreatedAt = man.CreatedAt.Add(-24 * time.Hour)
	if err := writeRawManifest(ctx, fb, mkeys[0], man); err != nil {
		t.Fatal(err)
	}
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run (recovery): %v", err)
	}

	for _, po := range man.PlannedOutputs {
		if ok, _ := local.Exists(ctx, po.Path); ok {
			t.Errorf("output %s of the rolled-back group is still live", po.Path)
		}
	}
	if n := countParquetFlat(t, lateDir); n != 0 {
		t.Errorf("%d sources left after recovery + re-drain, want 0", n)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	assertLosslessNoDup(t, eventsDir, (3*2+1)*consolidateDays)
	for day, files := range outputFilesByDay(t, eventsDir) {
		if len(files) != 4 {
			t.Errorf("day %s has %d outputs after recovery, want 4 (06, then the released buckets one at a time)", day, len(files))
		}
	}
	if left, _ := mm.List(ctx); len(left) != 0 {
		t.Errorf("manifests left: %v", left)
	}
}

// A crash between MarkUploaded and the source delete: the sources stay held
// back by the manifest (no re-drain next to the live outputs), and recovery
// finishes the delete without writing anything twice.
func TestReorg_Consolidate_CrashAfterMarkUploaded_RecoveryFinishesDelete(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	local, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	fb := &faultBackend{LocalBackend: local}
	fb.failDeleteBatch.Store(true)
	lateDir, _ := writeConsolidateBuckets(t, tmp, []int{3, 4, 5}, 2)
	eventsDir := filepath.Join(tmp, "posthog", "events")

	r := newConsolidatingReorg(fb, tmp)
	_ = r.Run(ctx)
	mm := r.ManifestManager
	mkeys, _ := mm.List(ctx)
	if len(mkeys) != 1 {
		t.Fatalf("want 1 uploaded manifest, got %v", mkeys)
	}
	man, _ := mm.Read(ctx, mkeys[0])
	if man.Status != ReorgStatusUploaded {
		t.Fatalf("manifest status %q, want uploaded", man.Status)
	}
	fb.failDeleteBatch.Store(false)

	// The manifest is too young for recovery; the source filter must keep the
	// group's sources out of this drain.
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if n := countParquetFlat(t, lateDir); n != 6 {
		t.Fatalf("sources = %d, want 6 still held by the manifest", n)
	}
	assertLosslessNoDup(t, eventsDir, 3*2*consolidateDays)

	man.UpdatedAt = man.UpdatedAt.Add(-time.Hour)
	if err := writeRawManifest(ctx, fb, mkeys[0], man); err != nil {
		t.Fatal(err)
	}
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if n := countParquetFlat(t, lateDir); n != 0 {
		t.Errorf("sources = %d after recovery, want 0", n)
	}
	assertLosslessNoDup(t, eventsDir, 3*2*consolidateDays)
	for day, files := range outputFilesByDay(t, eventsDir) {
		if len(files) != 1 {
			t.Errorf("day %s has %d outputs, want 1", day, len(files))
		}
	}
	if left, _ := mm.List(ctx); len(left) != 0 {
		t.Errorf("manifests left: %v", left)
	}
}

// groupRecorder records the source set of every committed unit: the drain
// deletes each committed chunk's (or group's) sources in one DeleteBatch.
type groupRecorder struct {
	mu     sync.Mutex
	groups [][]string
}

func (g *groupRecorder) hook(paths []string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(paths) > 0 && strings.Contains(paths[0], "_late/") {
		g.groups = append(g.groups, append([]string(nil), paths...))
	}
	return nil
}

func sourceFileSize(t *testing.T, tmp, key string) int64 {
	t.Helper()
	st, err := os.Stat(filepath.Join(tmp, filepath.FromSlash(key)))
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func TestReorg_Consolidate_GroupBounds(t *testing.T) {
	hours := []int{3, 4, 5, 6}
	cases := []struct {
		name string
		set  func(r *Reorganizer, bucketBytes int64)
		// wantGroups lists the bucket hours of each committed unit, in order.
		wantGroups [][]int
	}{
		{"no caps: one group", func(r *Reorganizer, _ int64) {}, [][]int{{3, 4, 5, 6}}},
		{"max buckets 3", func(r *Reorganizer, _ int64) { r.ConsolidateMaxBuckets = 3 }, [][]int{{3, 4, 5}, {6}}},
		{"max files 4 (max_files_per_batch)", func(r *Reorganizer, _ int64) { r.MaxFilesPerBatch = 4 }, [][]int{{3, 4}, {5, 6}}},
		{"max bytes fits 2 buckets", func(r *Reorganizer, b int64) { r.ConsolidateMaxSourceBytes = 2*b + b/2 }, [][]int{{3, 4}, {5, 6}}},
		{"max bytes below one bucket: each bucket alone", func(r *Reorganizer, b int64) { r.ConsolidateMaxSourceBytes = b / 2 }, [][]int{{3}, {4}, {5}, {6}}},
		{"max buckets per run 2 caps the group", func(r *Reorganizer, _ int64) { r.MaxBucketsPerRun = 2 }, [][]int{{3, 4}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TZ", "UTC")
			ctx := context.Background()
			tmp := t.TempDir()
			local, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
			rec := &groupRecorder{}
			hb := &hookBackend{LocalBackend: local, deleteBHook: rec.hook}
			_, keys := writeConsolidateBuckets(t, tmp, hours, 2)
			var bucketBytes int64 // the largest bucket's source bytes
			sizes := map[string]int64{}
			for _, ks := range keys {
				var b int64
				for _, k := range ks {
					sizes[k] = sourceFileSize(t, tmp, k)
					b += sizes[k]
				}
				if b > bucketBytes {
					bucketBytes = b
				}
			}

			r := newConsolidatingReorg(hb, tmp)
			tc.set(r, bucketBytes)
			if err := r.Run(ctx); err != nil {
				t.Fatalf("Run: %v", err)
			}

			var got [][]int
			for _, g := range rec.groups {
				hs := map[int]bool{}
				var bytes int64
				for _, k := range g {
					ft, ok := parseFilenameTime(k)
					if !ok {
						t.Fatalf("unparseable source %s", k)
					}
					hs[ft.Hour()] = true
					bytes += sizes[k]
				}
				var hl []int
				for h := range hs {
					hl = append(hl, h)
				}
				sort.Ints(hl)
				got = append(got, hl)
				if r.MaxFilesPerBatch > 0 && len(g) > r.MaxFilesPerBatch {
					t.Errorf("group %v has %d files > max_files_per_batch %d", hl, len(g), r.MaxFilesPerBatch)
				}
				if r.ConsolidateMaxSourceBytes > 0 && len(hl) > 1 && bytes > r.ConsolidateMaxSourceBytes {
					t.Errorf("multi-bucket group %v holds %d source bytes > cap %d", hl, bytes, r.ConsolidateMaxSourceBytes)
				}
				if r.ConsolidateMaxBuckets > 0 && len(hl) > r.ConsolidateMaxBuckets {
					t.Errorf("group %v exceeds consolidate_max_buckets %d", hl, r.ConsolidateMaxBuckets)
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.wantGroups) {
				t.Errorf("committed groups = %v, want %v", got, tc.wantGroups)
			}
			drained := 0
			for _, g := range tc.wantGroups {
				drained += len(g)
			}
			assertLosslessNoDup(t, filepath.Join(tmp, "posthog", "events"), int64(drained*2*consolidateDays))
			for day, files := range outputFilesByDay(t, filepath.Join(tmp, "posthog", "events")) {
				if len(files) != len(tc.wantGroups) {
					t.Errorf("day %s has %d outputs, want %d (one per group)", day, len(files), len(tc.wantGroups))
				}
			}
		})
	}
}

// MaxBucketsPerRun keeps its meaning: the next run drains the next oldest
// buckets, again as one group.
func TestReorg_Consolidate_MaxBucketsPerRunAcrossRuns(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	backend, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	lateDir, _ := writeConsolidateBuckets(t, tmp, []int{3, 4, 5, 6}, 2)
	r := newConsolidatingReorg(backend, tmp)
	r.MaxBucketsPerRun = 2

	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countParquetFlat(t, lateDir); n != 4 {
		t.Fatalf("after run 1: %d sources left, want 4 (buckets 05 and 06)", n)
	}
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countParquetFlat(t, lateDir); n != 0 {
		t.Fatalf("after run 2: %d sources left, want 0", n)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	assertLosslessNoDup(t, eventsDir, 4*2*consolidateDays)
	for day, files := range outputFilesByDay(t, eventsDir) {
		if len(files) != 2 {
			t.Errorf("day %s has %d outputs, want 2 (one per run)", day, len(files))
		}
	}
}

// A group that fails before its manifest is written (here: a corrupt source
// in bucket 04 fails the COPY) wrote nothing, so its buckets are drained one
// at a time in the SAME cycle: only the bad bucket waits, as with
// consolidation off. The bad bucket stays solo, and its mark is dropped once
// it has drained.
func TestReorg_Consolidate_FailedGroupFallsBackToPerBucket(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	backend, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	lateDir, keys := writeConsolidateBuckets(t, tmp, []int{3, 4, 5}, 2)
	bad := "posthog/events_late/events_late_20260520_040000_9.parquet"
	if err := os.WriteFile(filepath.Join(tmp, filepath.FromSlash(bad)), []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := newConsolidatingReorg(backend, tmp)
	hour := func(h int) time.Time { return time.Date(2026, 5, 20, h, 0, 0, 0, time.UTC) }
	eventsDir := filepath.Join(tmp, "posthog", "events")

	for run := 1; run <= 2; run++ {
		if err := r.Run(ctx); err != nil {
			t.Fatal(err)
		}
		for _, h := range []int{3, 5} {
			for _, k := range keys[h] {
				if ok, _ := backend.Exists(ctx, k); ok {
					t.Errorf("run %d: %s of healthy bucket %02d not drained", run, k, h)
				}
			}
		}
		if n := countParquetFlat(t, lateDir); n != 3 {
			t.Errorf("run %d: %d sources left, want 3 (bucket 04 only)", run, n)
		}
		total, _ := outputRowsByDay(t, eventsDir)
		if total != 2*2*consolidateDays {
			t.Errorf("run %d: rows = %d, want %d", run, total, 2*2*consolidateDays)
		}
		if left, _ := r.ManifestManager.List(ctx); len(left) != 0 {
			t.Fatalf("run %d: a pre-manifest failure left manifests: %v", run, left)
		}
		if !r.isSolo("posthog", "events_late", hour(4)) {
			t.Errorf("run %d: the failing bucket 04 is not marked solo", run)
		}
	}

	// Fix the bucket: it drains alone; the next listing forgets its mark.
	if err := os.Remove(filepath.Join(tmp, filepath.FromSlash(bad))); err != nil {
		t.Fatal(err)
	}
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countParquetFlat(t, lateDir); n != 0 {
		t.Fatalf("%d sources left after the fix, want 0", n)
	}
	writeConsolidateBuckets(t, tmp, []int{6}, 1) // keeps the sidecar listed
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if r.isSolo("posthog", "events_late", hour(4)) {
		t.Error("the solo mark of drained bucket 04 was never forgotten")
	}
	assertLosslessNoDup(t, eventsDir, (3*2+1)*consolidateDays)
}

// A solo bucket never joins a group: the buckets on either side of it are
// drained without it, and a mark for a bucket that is not listed is dropped.
func TestReorg_Consolidate_SoloBucketDrainsAlone(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	local, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	rec := &groupRecorder{}
	hb := &hookBackend{LocalBackend: local, deleteBHook: rec.hook}
	writeConsolidateBuckets(t, tmp, []int{3, 4, 5, 6}, 1)
	r := newConsolidatingReorg(hb, tmp)
	hour := func(h int) time.Time { return time.Date(2026, 5, 20, h, 0, 0, 0, time.UTC) }
	r.markSolo("posthog", "events_late", []time.Time{hour(4), hour(9)})

	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	var got [][]int
	for _, g := range rec.groups {
		var hs []int
		seen := map[int]bool{}
		for _, k := range g {
			ft, _ := parseFilenameTime(k)
			if !seen[ft.Hour()] {
				seen[ft.Hour()] = true
				hs = append(hs, ft.Hour())
			}
		}
		sort.Ints(hs)
		got = append(got, hs)
	}
	if want := [][]int{{3}, {4}, {5, 6}}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("committed groups = %v, want %v", got, want)
	}
	if r.isSolo("posthog", "events_late", hour(9)) {
		t.Error("solo mark of unlisted bucket 09 was not dropped")
	}
	assertLosslessNoDup(t, filepath.Join(tmp, "posthog", "events"), 4*consolidateDays)
}

// A cancelled cycle (cycle_timeout, SIGTERM) says nothing about the buckets:
// they are not marked solo, and the next cycle drains them as one group.
func TestReorg_Consolidate_CancelDoesNotMarkSolo(t *testing.T) {
	t.Setenv("TZ", "UTC")
	tmp := t.TempDir()
	local, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	lateDir, _ := writeConsolidateBuckets(t, tmp, []int{3, 4, 5}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	hb := &hookBackend{LocalBackend: local}
	hb.readToHook = func(string) error {
		cancel()
		return context.Canceled
	}
	r := newConsolidatingReorg(hb, tmp)
	pausesBefore := metrics.Get().Snapshot()["reorg_grouping_paused_total"].(int64)

	_ = r.Run(ctx)
	if n := countParquetFlat(t, lateDir); n != 6 {
		t.Fatalf("sources = %d after a cancelled cycle, want 6", n)
	}
	if _, paused := r.groupingPausedUntil("posthog", "events_late"); paused {
		t.Error("a cancelled cycle paused grouping")
	}
	if got := metrics.Get().Snapshot()["reorg_grouping_paused_total"].(int64) - pausesBefore; got != 0 {
		t.Errorf("a cancelled cycle counted %d grouping pauses", got)
	}
	for _, h := range []int{3, 4, 5} {
		if r.isSolo("posthog", "events_late", time.Date(2026, 5, 20, h, 0, 0, 0, time.UTC)) {
			t.Errorf("bucket %02d marked solo by a cancelled cycle", h)
		}
	}

	hb.set(func(b *hookBackend) { b.readToHook = nil })
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	assertLosslessNoDup(t, eventsDir, 3*2*consolidateDays)
	for day, files := range outputFilesByDay(t, eventsDir) {
		if len(files) != 1 {
			t.Errorf("day %s has %d outputs, want 1 (regrouped after the cancel)", day, len(files))
		}
	}
}

// noListerBackend hides the optional storage interfaces (ObjectLister,
// DirectoryLister) of the backend it wraps.
type noListerBackend struct{ storage.Backend }

// Without source sizes a byte cap cannot be honoured, so the drain falls back
// to one bucket per unit instead of guessing; with no byte cap set, sizes are
// not needed and the buckets are still combined.
func TestReorg_Consolidate_UnknownSizesWithByteCapDrainsPerBucket(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxBytes   int64
		wantPerDay int
	}{
		{"byte cap set", 1 << 30, 3},
		{"no byte cap", 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TZ", "UTC")
			ctx := context.Background()
			tmp := t.TempDir()
			local, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
			writeConsolidateBuckets(t, tmp, []int{3, 4, 5}, 2)
			r := newConsolidatingReorg(noListerBackend{local}, tmp)
			r.ConsolidateMaxSourceBytes = tc.maxBytes
			if err := r.Run(ctx); err != nil {
				t.Fatal(err)
			}
			eventsDir := filepath.Join(tmp, "posthog", "events")
			assertLosslessNoDup(t, eventsDir, 3*2*consolidateDays)
			for day, files := range outputFilesByDay(t, eventsDir) {
				if len(files) != tc.wantPerDay {
					t.Errorf("day %s has %d outputs, want %d", day, len(files), tc.wantPerDay)
				}
			}
		})
	}
}

// processChunk sniffs the `time` type of a chunk's FIRST file only, and a
// group's files span several hours. A group led by a non-TIMESTAMP bucket is
// skipped as a whole (no error, nothing written); a group with one later is
// rejected by the COPY before its manifest. Either way the buckets are then
// drained one at a time in the same cycle, each sniffed on its own as with
// consolidation off, so the healthy ones drain and only the bad one stays.
func TestReorg_Consolidate_NonTimestampBucketFallsBackToPerBucket(t *testing.T) {
	for _, tc := range []struct {
		name    string
		badHour int
	}{
		{"first bucket (group skipped)", 2},
		{"later bucket (COPY fails)", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TZ", "UTC")
			ctx := context.Background()
			tmp := t.TempDir()
			backend, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
			lateDir, _ := writeConsolidateBuckets(t, tmp, []int{3, 4}, 2)
			db, _ := sql.Open("duckdb", "")
			writeLateParquet(t, db, lateDir, fmt.Sprintf("events_late_20260520_%02d0000_0.parquet", tc.badHour),
				`SELECT '2026-04-01 12:00:00'::VARCHAR AS time, 'x'::VARCHAR AS host, 1::BIGINT AS value`)
			db.Close()
			r := newConsolidatingReorg(backend, tmp)

			if err := r.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if n := countParquetFlat(t, lateDir); n != 1 {
				t.Errorf("%d sources left after one cycle, want 1 (the non-TIMESTAMP bucket only)", n)
			}
			total, _ := outputRowsByDay(t, filepath.Join(tmp, "posthog", "events"))
			if total != 2*2*consolidateDays {
				t.Errorf("rows = %d, want %d", total, 2*2*consolidateDays)
			}
		})
	}
}

// manifestWriteFault fails the Nth Write under _arc/reorg/ (1-indexed): the
// 1st is a group's manifest write, the 2nd its MarkUploaded rewrite.
type manifestWriteFault struct {
	*storage.LocalBackend
	failN int32
	n     atomic.Int32
}

func (b *manifestWriteFault) Write(ctx context.Context, path string, data []byte) error {
	if strings.HasPrefix(path, ReorgManifestBasePath+"/") {
		if n := b.n.Add(1); n == b.failN {
			return fmt.Errorf("injected manifest write failure #%d (%s)", n, path)
		}
	}
	return b.LocalBackend.Write(ctx, path, data)
}

// A group that fails at or after its manifest write may have outputs live
// and its sources held by a manifest, so its buckets must not be re-drained
// in the same cycle (that would write their rows a second time). They are
// marked solo and drain one at a time once recovery has resolved the group.
func TestReorg_Consolidate_CommitPhaseFailureIsNotRetriedInCycle(t *testing.T) {
	for _, tc := range []struct {
		name          string
		failN         int32
		wantRows      int64 // rows live after the failed cycle
		wantManifests int
	}{
		{"manifest write", 1, 0, 0},
		{"MarkUploaded", 2, 3 * 2 * consolidateDays, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TZ", "UTC")
			ctx := context.Background()
			tmp := t.TempDir()
			local, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
			fb := &manifestWriteFault{LocalBackend: local, failN: tc.failN}
			lateDir, _ := writeConsolidateBuckets(t, tmp, []int{3, 4, 5}, 2)
			eventsDir := filepath.Join(tmp, "posthog", "events")
			r := newConsolidatingReorg(fb, tmp)

			_ = r.Run(ctx)
			if n := countParquetFlat(t, lateDir); n != 6 {
				t.Fatalf("sources = %d after the failed cycle, want all 6 (re-drained in the same cycle?)", n)
			}
			total, _ := outputRowsByDay(t, eventsDir)
			if total != tc.wantRows {
				t.Errorf("live rows = %d, want %d (more = the group's sources were drained again)", total, tc.wantRows)
			}
			mkeys, _ := r.ManifestManager.List(ctx)
			if len(mkeys) != tc.wantManifests {
				t.Fatalf("manifests = %v, want %d", mkeys, tc.wantManifests)
			}
			for _, h := range []int{3, 4, 5} {
				if !r.isSolo("posthog", "events_late", time.Date(2026, 5, 20, h, 0, 0, 0, time.UTC)) {
					t.Errorf("bucket %02d not marked solo", h)
				}
			}

			// Age a pending manifest so recovery resolves it, then drain.
			for _, k := range mkeys {
				man, err := r.ManifestManager.Read(ctx, k)
				if err != nil {
					t.Fatal(err)
				}
				man.UpdatedAt = man.UpdatedAt.Add(-24 * time.Hour)
				if err := writeRawManifest(ctx, local, k, man); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if n := countParquetFlat(t, lateDir); n != 0 {
				t.Errorf("sources = %d after recovery, want 0", n)
			}
			assertLosslessNoDup(t, eventsDir, 3*2*consolidateDays)
			if left, _ := r.ManifestManager.List(ctx); len(left) != 0 {
				t.Errorf("manifests left: %v", left)
			}
		})
	}
}

// sizeDropBackend hides the given keys from ListObjects (the size listing)
// while List still shows them, as a key-format drift between the two would.
type sizeDropBackend struct {
	*hookBackend
	drop string
}

func (b *sizeDropBackend) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objs, err := b.hookBackend.ListObjects(ctx, prefix)
	out := objs[:0]
	for _, o := range objs {
		if !strings.Contains(o.Path, b.drop) {
			out = append(out, o)
		}
	}
	return out, err
}

// With a byte cap, a bucket whose size the listing does not report is
// drained alone rather than counted as 0 bytes; the others still group.
func TestReorg_Consolidate_UnknownSizeBucketDrainsAlone(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	local, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	rec := &groupRecorder{}
	b := &sizeDropBackend{hookBackend: &hookBackend{LocalBackend: local, deleteBHook: rec.hook}, drop: "_20260520_040000_"}
	writeConsolidateBuckets(t, tmp, []int{3, 4, 5, 6}, 1)
	r := newConsolidatingReorg(b, tmp)
	r.ConsolidateMaxSourceBytes = 1 << 30

	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := groupHours(t, rec), "[[3] [4] [5 6]]"; got != want {
		t.Errorf("committed groups = %s, want %s", got, want)
	}
	assertLosslessNoDup(t, filepath.Join(tmp, "posthog", "events"), 4*consolidateDays)
}

func groupHours(t *testing.T, rec *groupRecorder) string {
	t.Helper()
	var got [][]int
	for _, g := range rec.groups {
		seen := map[int]bool{}
		var hs []int
		for _, k := range g {
			ft, ok := parseFilenameTime(k)
			if !ok {
				t.Fatalf("unparseable %s", k)
			}
			if !seen[ft.Hour()] {
				seen[ft.Hour()] = true
				hs = append(hs, ft.Hour())
			}
		}
		sort.Ints(hs)
		got = append(got, hs)
	}
	return fmt.Sprint(got)
}

// A failure of the group itself (here: a schema conflict between buckets that
// each drain fine alone) is not repeated: after the same-cycle per-bucket
// fallback the sidecar drains one bucket at a time, for the rest of this cycle
// and until the backoff expires, then groups again. The pause is counted.
func TestReorg_Consolidate_GroupFailurePausesGrouping(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	local, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	rec := &groupRecorder{}
	hb := &hookBackend{LocalBackend: local, deleteBHook: rec.hook}
	lateDir, _ := writeConsolidateBuckets(t, tmp, []int{4, 5, 6}, 1)
	db, _ := sql.Open("duckdb", "")
	// Column `extra` is a STRUCT in bucket 03 and an INT in bucket 04:
	// union_by_name cannot combine them, so the group fails before its
	// manifest, while each bucket drains fine alone.
	writeLateParquet(t, db, lateDir, "events_late_20260520_030000_0.parquet",
		`SELECT TIMESTAMP '2026-04-01 12:00:00' AS time, 'a'::VARCHAR AS host, 1::BIGINT AS value, {'k': 1} AS extra`)
	writeLateParquet(t, db, lateDir, "events_late_20260520_040000_9.parquet",
		`SELECT TIMESTAMP '2026-04-01 12:00:00' AS time, 'b'::VARCHAR AS host, 2::BIGINT AS value, 7::INT AS extra`)
	db.Close()
	r := newConsolidatingReorg(hb, tmp)
	r.ConsolidateMaxBuckets = 2 // groups [03 04] then [05 06]
	paused := func() int64 { return metrics.Get().Snapshot()["reorg_grouping_paused_total"].(int64) }

	before := paused()
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countParquetFlat(t, lateDir); n != 0 {
		t.Fatalf("after the group failed and fell back: %d sources left, want 0", n)
	}
	// [03 04] fails and falls back; the pause then also keeps 05 and 06
	// apart within the same cycle.
	if got, want := groupHours(t, rec), "[[3] [4] [5] [6]]"; got != want {
		t.Fatalf("run 1 groups = %s, want %s", got, want)
	}
	if got := paused() - before; got != 1 {
		t.Errorf("grouping pauses counted = %d, want 1", got)
	}

	// New buckets while the pause holds: drained one at a time.
	rec.groups = nil
	writeConsolidateBuckets(t, tmp, []int{7, 8}, 1)
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := groupHours(t, rec), "[[7] [8]]"; got != want {
		t.Errorf("run 2 groups = %s, want %s (grouping paused)", got, want)
	}

	// Pause expired: grouping resumes.
	r.soloMu.Lock()
	for k := range r.groupingPaused {
		r.groupingPaused[k] = time.Now().Add(-time.Second)
	}
	r.soloMu.Unlock()
	rec.groups = nil
	writeConsolidateBuckets(t, tmp, []int{9, 10}, 1)
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := groupHours(t, rec), "[[9 10]]"; got != want {
		t.Errorf("run 3 groups = %s, want %s (pause expired)", got, want)
	}
	total, _ := outputRowsByDay(t, filepath.Join(tmp, "posthog", "events"))
	if want := int64(2 + 3*consolidateDays + 4*consolidateDays); total != want {
		t.Errorf("rows = %d, want %d", total, want)
	}
	if got := paused() - before; got != 1 {
		t.Errorf("grouping pauses counted = %d after three runs, want 1", got)
	}
}

// The per-bucket fallback after a group of phantom keys does not GET those
// keys a second time in the same cycle.
func TestReorg_Consolidate_FallbackSkipsProvenPhantoms(t *testing.T) {
	p, tmp := newPhantomListing(t)
	phantoms := phantomOnlyBuckets(3)
	p.addPhantoms(phantoms...)
	r := newPhantomBucketReorg(t, p, tmp)
	r.MaxBucketsPerRun = 0
	r.ConsolidateOutputs = true

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gets, heads := p.phantomGetsHeads(phantoms); gets != 3 || heads != 3 {
		t.Errorf("phantom GETs/HEADs = %d/%d in one cycle, want 3/3 (one each)", gets, heads)
	}
}

// A cycle cancelled during the per-bucket fallback stops there: the buckets
// after it are not attempted (or counted failed) with a dead context.
func TestReorg_Consolidate_CancelStopsFallback(t *testing.T) {
	t.Setenv("TZ", "UTC")
	tmp := t.TempDir()
	local, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	_, keys := writeConsolidateBuckets(t, tmp, []int{3, 4, 5}, 1)
	bad := "posthog/events_late/events_late_20260520_050000_9.parquet"
	if err := os.WriteFile(filepath.Join(tmp, filepath.FromSlash(bad)), []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var reads atomic.Int32
	hb := &hookBackend{LocalBackend: local}
	hb.readToHook = func(path string) error {
		// 1st read of bucket 03's key: the group; 2nd: its fallback.
		if path == keys[3][0] && reads.Add(1) == 2 {
			cancel()
			return context.Canceled
		}
		return nil
	}
	r := newConsolidatingReorg(hb, tmp)
	failed := func() int64 { return metrics.Get().Snapshot()["reorg_buckets_failed"].(int64) }

	before := failed()
	_ = r.Run(ctx)
	if got := failed() - before; got != 1 {
		t.Errorf("buckets counted failed = %d, want 1 (only 03; the fallback must stop at the cancel)", got)
	}
}

func TestPlanGroup_Bounds(t *testing.T) {
	hours := []time.Time{
		time.Date(2026, 5, 20, 3, 0, 0, 0, time.UTC),
		time.Date(2026, 5, 20, 4, 0, 0, 0, time.UTC),
		time.Date(2026, 5, 20, 5, 0, 0, 0, time.UTC),
	}
	buckets := map[time.Time][]string{hours[0]: {"k0"}, hours[1]: {"k1"}, hours[2]: {"k2"}}
	allSizes := map[string]int64{"k0": 10, "k1": 10, "k2": 10}
	without := func(k string) map[string]int64 {
		m := map[string]int64{}
		for kk, v := range allSizes {
			if kk != k {
				m[kk] = v
			}
		}
		return m
	}
	none := func(time.Time) bool { return false }
	soloAt := func(i int) func(time.Time) bool { return func(h time.Time) bool { return h.Equal(hours[i]) } }
	for _, tc := range []struct {
		name       string
		sizes      map[string]int64
		maxBuckets int
		maxFiles   int
		maxBytes   int64
		solo       func(time.Time) bool
		want       int
	}{
		{"no caps", allSizes, 0, 100, 0, none, 3},
		{"bytes exactly two buckets", allSizes, 0, 100, 20, none, 2},
		{"bytes one short of two", allSizes, 0, 100, 19, none, 1},
		{"files exactly two", allSizes, 0, 2, 0, none, 2},
		{"buckets cap", allSizes, 2, 100, 0, none, 2},
		{"unknown size joins nothing", without("k1"), 0, 100, 100, none, 1},
		{"unknown first bucket alone", without("k0"), 0, 100, 100, none, 1},
		{"no sizes listed", nil, 0, 100, 100, none, 1},
		{"unknown size ignored without byte cap", without("k1"), 0, 100, 0, none, 3},
		{"solo first", allSizes, 0, 100, 0, soloAt(0), 1},
		{"solo second stops the group", allSizes, 0, 100, 0, soloAt(1), 1},
		{"solo third", allSizes, 0, 100, 0, soloAt(2), 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := planGroup(hours, buckets, tc.sizes, tc.maxBuckets, tc.maxFiles, tc.maxBytes, tc.solo); got != tc.want {
				t.Errorf("planGroup = %d, want %d", got, tc.want)
			}
		})
	}
}

// Job-ID bases never repeat, however fast chunks start: output paths embed
// the jobID, and a repeat overwrites another chunk's day files (data loss).
func TestReorg_NextJobBaseIsUnique(t *testing.T) {
	r := &Reorganizer{}
	seen := make(map[int64]bool, 10000)
	prev := int64(0)
	for i := 0; i < 10000; i++ {
		b := r.nextJobBase()
		if seen[b] || b <= prev {
			t.Fatalf("job base %d repeated or not increasing after %d (call %d)", b, prev, i)
		}
		seen[b] = true
		prev = b
	}
}

// Concurrent callers (max_concurrent > 1 buckets) never share a job base, and
// each caller sees its own bases strictly increasing.
func TestReorg_NextJobBaseIsUniqueConcurrently(t *testing.T) {
	r := &Reorganizer{}
	const goroutines, perG = 16, 2000
	results := make([][]int64, goroutines)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			out := make([]int64, perG)
			for i := range out {
				out[i] = r.nextJobBase()
			}
			results[g] = out
		}(g)
	}
	wg.Wait()
	seen := make(map[int64]int, goroutines*perG)
	for g, out := range results {
		for i, b := range out {
			if i > 0 && b <= out[i-1] {
				t.Fatalf("goroutine %d: base %d not above its previous %d", g, b, out[i-1])
			}
			if other, dup := seen[b]; dup {
				t.Fatalf("base %d handed to goroutines %d and %d", b, other, g)
			}
			seen[b] = g
		}
	}
}

// Every output key uploaded in one run is distinct, across the chunks of a
// bucket, solo buckets and consolidated groups that all write the same event
// days: a repeated key would replace another chunk's rows.
func TestReorg_OutputKeysDistinctAcrossChunksInOneRun(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	local, _ := storage.NewLocalBackend(tmp, zerolog.New(io.Discard))
	var mu sync.Mutex
	var uploads []string
	hb := &hookBackend{LocalBackend: local}
	hb.writeRHook = func(_ context.Context, path string) error {
		mu.Lock()
		defer mu.Unlock()
		uploads = append(uploads, path)
		return nil
	}
	// Bucket 03 has 3 files and is solo: processBucket, chunks of 2 and 1 files.
	// 04 and 05 (1 file each) form one consolidated group of 2 files.
	writeConsolidateBuckets(t, tmp, []int{3}, 3)
	writeConsolidateBuckets(t, tmp, []int{4, 5}, 1)
	r := newConsolidatingReorg(hb, tmp)
	r.MaxFilesPerBatch = 2
	r.markSolo("posthog", "events_late", []time.Time{time.Date(2026, 5, 20, 3, 0, 0, 0, time.UTC)})

	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	const units = 2 + 1 // 03 in chunks of 2+1 files, then the [04 05] group
	if len(uploads) != units*consolidateDays {
		t.Fatalf("uploads = %d, want %d (%d units x %d days)", len(uploads), units*consolidateDays, units, consolidateDays)
	}
	seen := map[string]bool{}
	for _, k := range uploads {
		if seen[k] {
			t.Errorf("output key %s uploaded twice in one run", k)
		}
		seen[k] = true
	}
	assertLosslessNoDup(t, filepath.Join(tmp, "posthog", "events"), 5*consolidateDays)
}
