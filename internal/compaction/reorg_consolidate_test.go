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

	_ = r.Run(ctx)
	if n := countParquetFlat(t, lateDir); n != 6 {
		t.Fatalf("sources = %d after a cancelled cycle, want 6", n)
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
