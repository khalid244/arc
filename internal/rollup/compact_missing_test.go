package rollup

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// fakeStorage is a minimal in-memory Storage for compaction-filter unit tests.
// existing[key]=true means StatFile reports a positive size; absent keys report
// -1 (gone). statErr forces a transient (non-absent) error for a key.
type fakeStorage struct {
	existing map[string]bool
	statErr  map[string]error
	objs     map[string][]byte
	deleted  [][]string
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{existing: map[string]bool{}, statErr: map[string]error{}, objs: map[string][]byte{}}
}

func (f *fakeStorage) Read(_ context.Context, p string) ([]byte, error) {
	if b, ok := f.objs[p]; ok {
		return b, nil
	}
	return nil, errors.New("not found")
}
func (f *fakeStorage) Write(_ context.Context, p string, d []byte) error { f.objs[p] = d; return nil }
func (f *fakeStorage) DeleteBatch(_ context.Context, keys []string) error {
	f.deleted = append(f.deleted, keys)
	return nil
}
func (f *fakeStorage) StatFile(_ context.Context, p string) (int64, error) {
	if e, ok := f.statErr[p]; ok {
		return 0, e
	}
	if f.existing[p] {
		return 100, nil
	}
	return -1, nil
}
func (f *fakeStorage) ListDirectories(_ context.Context, _ string) ([]string, error) {
	return nil, nil // not exercised by the compaction-missing-files tests
}

var _ Storage = (*fakeStorage)(nil)

func testManager(stg Storage) *Manager {
	return &Manager{stg: stg, log: zerolog.New(io.Discard), cfg: Config{}.withDefaults(),
		s3:        S3Params{Bucket: "arc-test"},
		manifests: map[string]*Manifest{}, profiles: map[string]TableProfile{},
		dimRichBailed: map[string]bool{}, workload: NewWorkload()}
}

func uri(date string) string {
	return "s3://arc-test/_arc/rollup/default/events/by_region/" + date + ".parquet"
}

// TestPartitionExistingDailies splits present vs missing, and keeps a file whose
// stat errored transiently (never drops a good file on a flaky check).
func TestPartitionExistingDailies(t *testing.T) {
	fs := newFakeStorage()
	keep1 := "_arc/rollup/default/events/by_region/2026-05-01.parquet"
	keep2 := "_arc/rollup/default/events/by_region/2026-05-02.parquet"
	flaky := "_arc/rollup/default/events/by_region/2026-05-04.parquet"
	fs.existing[keep1] = true
	fs.existing[keep2] = true
	// 2026-05-03 is absent (StatFile -> -1); not registered anywhere.
	fs.statErr[flaky] = errors.New("connection reset") // transient

	m := testManager(fs)
	dailies := []DayEntry{
		{Date: "2026-05-01", URI: uri("2026-05-01")},
		{Date: "2026-05-02", URI: uri("2026-05-02")},
		{Date: "2026-05-03", URI: uri("2026-05-03")},
		{Date: "2026-05-04", URI: uri("2026-05-04")},
	}
	present, missing := m.partitionExistingDailies(context.Background(), dailies)
	if len(missing) != 1 || missing[0].Date != "2026-05-03" {
		t.Fatalf("missing = %v, want only 2026-05-03", datesOf(missing))
	}
	// keep1, keep2 exist; flaky (transient error) is kept too.
	if len(present) != 3 {
		t.Fatalf("present = %v, want 3 (incl. transient-error file kept)", datesOf(present))
	}
	wantPresent := map[string]bool{"2026-05-01": true, "2026-05-02": true, "2026-05-04": true}
	for _, d := range present {
		if !wantPresent[d.Date] {
			t.Errorf("unexpected present date %s", d.Date)
		}
	}
}

// TestPurgeMissingDailies removes stale daily entries from the manifest and
// republishes, without writing any cube file.
func TestPurgeMissingDailies(t *testing.T) {
	fs := newFakeStorage()
	m := testManager(fs)
	man := &Manifest{Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Days: []DayEntry{
			{Date: "2026-05-01", URI: uri("2026-05-01")},
			{Date: "2026-05-03", URI: uri("2026-05-03")},
		}}
	spec := man.Spec()
	missing := []DayEntry{{Date: "2026-05-03", URI: uri("2026-05-03")}}
	if err := m.purgeMissingDailies(context.Background(), spec, man, missing, nil, "2026-05"); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if len(man.Days) != 1 || man.Days[0].Date != "2026-05-01" {
		t.Fatalf("after purge Days=%v, want only 2026-05-01", datesOf(man.Days))
	}
	// A manifest object was written (the purge persisted).
	if len(fs.objs) == 0 {
		t.Error("expected manifest write on purge")
	}
}

// TestPurgeWritesEmptyMarker proves that when compaction rebuilds a missing daily
// and finds source genuinely empty, the purge records the day as a known-built,
// zero-row covered marker — so the read-path interior-gap net treats it as a
// legitimately-empty day (served from cube), NOT a purged hole (fall to source).
func TestPurgeWritesEmptyMarker(t *testing.T) {
	fs := newFakeStorage()
	m := testManager(fs)
	man := &Manifest{Source: "default.events", Grain: "hour", Dims: []string{"region"},
		Days: []DayEntry{
			dailyEntry2("2026-05-01"),
			{Date: "2026-05-03", URI: uri("2026-05-03")}, // missing file, source empty
		}}
	spec := man.Spec()
	missing := []DayEntry{{Date: "2026-05-03", URI: uri("2026-05-03")}}
	empty := []string{"2026-05-03"}
	if err := m.purgeMissingDailies(context.Background(), spec, man, missing, empty, "2026-05"); err != nil {
		t.Fatalf("purge: %v", err)
	}
	// The stale daily pointer is gone; a zero-row empty marker now records 2026-05-03
	// as known-built so coveredDays() includes it.
	if !man.coveredDays()["2026-05-03"] {
		t.Fatalf("2026-05-03 not marked covered after empty-marker purge; Days=%v", datesOf(man.Days))
	}
	// And the marker must not pollute file selection (it has no URI / bucket span).
	if got := man.DaysInRange("2026-05-03 00:00:00+00", "2026-05-04 00:00:00+00"); len(got) != 0 {
		t.Fatalf("empty marker leaked into DaysInRange: %v", uris(got))
	}
	// A query spanning the empty day must NOT report a gap (it's known-built).
	if man.HasInteriorGap("2026-05-01 00:00:00+00", "2026-05-04 00:00:00+00") {
		t.Fatal("known-empty day wrongly flagged as an interior gap")
	}
}

func dailyEntry2(date string) DayEntry {
	d, _ := parseTS(date)
	return DayEntry{Date: date, URI: uri(date),
		BucketLo: date + " 00:00:00+00", BucketHi: fmtTS(d.Add(24 * time.Hour)), Rows: 144}
}

// TestBuildThreadsDefault locks the OOM-guard default: build connections must
// bound DuckDB threads so a CPU-throttled container does not spawn host-core
// threads and OOM the sketch-heavy compaction COPY.
func TestBuildThreadsDefault(t *testing.T) {
	if got := (Config{}).withDefaults().BuildThreads; got != 4 {
		t.Fatalf("default BuildThreads = %d, want 4", got)
	}
	// An explicit positive override is preserved.
	if got := (Config{BuildThreads: 2}).withDefaults().BuildThreads; got != 2 {
		t.Fatalf("override BuildThreads = %d, want 2", got)
	}
	// A NEGATIVE value is the reachable "DuckDB host-core default" opt-out: it must
	// survive withDefaults (NOT be rewritten to 4) so configureBuildConn's `threads>0`
	// guard skips the SET and DuckDB picks its own thread count. This is the honesty
	// fix: previously 0 was documented as "host cores" but withDefaults forced it to 4,
	// making host-core threading unreachable.
	if got := (Config{BuildThreads: -1}).withDefaults().BuildThreads; got >= 0 {
		t.Fatalf("negative BuildThreads = %d, want it preserved <0 (DuckDB default)", got)
	}
}

func datesOf(ds []DayEntry) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.Date
	}
	return out
}
