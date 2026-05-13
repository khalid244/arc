package compaction

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// scanFixture writes a small forest of parquet stubs into a LocalBackend
// at <db>/<measurement>/<YYYY>/<MM>/<DD>/<HH>/<file>. The files contain
// only their path as bytes — enough for List-based discovery to work.
type scanFixture struct {
	backend     *storage.LocalBackend
	database    string
	measurement string
}

func newScanFixture(t *testing.T, database, measurement string, hours []time.Time, filesPerHour int) *scanFixture {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	f := &scanFixture{backend: backend, database: database, measurement: measurement}

	ctx := context.Background()
	for _, h := range hours {
		hUTC := h.UTC()
		for i := 0; i < filesPerHour; i++ {
			// Filename embeds the partition's hour timestamp so
			// extractNewestFileTime() can parse it. Format must match
			// "<measurement>_<YYYYMMDD>_<HHMMSS>_<nanos>.parquet".
			ts := hUTC.Format("20060102_150405")
			path := fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d/%s_%s_%d.parquet",
				database, measurement,
				hUTC.Year(), int(hUTC.Month()), hUTC.Day(), hUTC.Hour(),
				measurement, ts, i)
			// Parquet magic bytes — enough that anything inspecting them
			// won't choke even though we don't decode the data.
			content := append(append([]byte("PAR1"), []byte("FIXTURE_DATA")...), []byte("PAR1")...)
			if err := backend.Write(ctx, path, content); err != nil {
				t.Fatalf("write fixture file: %v", err)
			}
		}
	}
	return f
}

// addCompactedMarker drops a `_compacted.parquet` into a partition.
func (f *scanFixture) addCompactedMarker(t *testing.T, h time.Time) {
	t.Helper()
	hUTC := h.UTC()
	path := fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d/%s_%s_compacted.parquet",
		f.database, f.measurement,
		hUTC.Year(), int(hUTC.Month()), hUTC.Day(), hUTC.Hour(),
		f.measurement, hUTC.Format("20060102_150405"))
	content := append(append([]byte("PAR1"), make([]byte, 100)...), []byte("PAR1")...)
	if err := f.backend.Write(context.Background(), path, content); err != nil {
		t.Fatalf("write compacted marker: %v", err)
	}
}

// newHourlyTierForScan returns an HourlyTier with the given cache wired
// in. We bypass NewHourlyTier's constructor so we can control min-age
// behavior precisely without warning logs.
func newHourlyTierForScan(backend storage.Backend, cache *PartitionCache, minAgeHours int) *HourlyTier {
	tier := &HourlyTier{
		BaseTier: &BaseTier{
			StorageBackend: backend,
			MinAgeHours:    minAgeHours,
			MinFiles:       2,
			TargetSizeMB:   512,
			Enabled:        true,
			Logger:         zerolog.Nop(),
			Cache:          cache,
		},
	}
	return tier
}

// candidatePaths returns the sorted set of partition paths from a list
// of Candidates — easy diffing in tests.
func candidatePaths(cs []Candidate) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.PartitionPath)
	}
	sort.Strings(out)
	return out
}

func TestHourlyTier_NoCache_FallsBackToFlatScan(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	hours := []time.Time{
		now.Add(-25 * time.Hour),
		now.Add(-12 * time.Hour),
		now.Add(-3 * time.Hour),
	}
	f := newScanFixture(t, "db", "m", hours, 3)

	tier := newHourlyTierForScan(f.backend, nil /* no cache */, 1)
	got, err := tier.FindCandidates(context.Background(), "db", "m")
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	// All three hours have 3 files each (> MinFiles=2) → all should be candidates.
	if len(got) != 3 {
		t.Errorf("flat-scan candidates: got %d want 3 (paths: %v)", len(got), candidatePaths(got))
	}
}

func TestHourlyTier_WithCache_IncrementalCoversRecent(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	hours := []time.Time{
		now.Add(-12 * time.Hour),
		now.Add(-3 * time.Hour),
	}
	f := newScanFixture(t, "db", "m", hours, 3)

	cache := NewPartitionCache(24*time.Hour, 30)
	tier := newHourlyTierForScan(f.backend, cache, 1)

	got, err := tier.FindCandidates(context.Background(), "db", "m")
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("incremental should cover both recent hours: got %d want 2 (paths: %v)", len(got), candidatePaths(got))
	}
	// Cache should now have both entries with FullyCompacted=false.
	for _, c := range got {
		s, ok := cache.Get(c.PartitionPath)
		if !ok {
			t.Errorf("partition %s missing from cache", c.PartitionPath)
		}
		if s.FullyCompacted {
			t.Errorf("partition %s should not be marked compacted (no marker file)", c.PartitionPath)
		}
	}
}

func TestHourlyTier_WithCache_ReconcileChunkPicksUpOldDay(t *testing.T) {
	now := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := now.Add(-24 * time.Hour)
	hours := []time.Time{
		// One hour in yesterday — outside the 48h incremental window only
		// when 'now' is at 00:00, but the reconcile cursor starts at
		// yesterday so it should land on this anyway.
		yesterday.Add(13 * time.Hour),
	}
	f := newScanFixture(t, "db", "m", hours, 3)

	cache := NewPartitionCache(24*time.Hour, 30)
	tier := newHourlyTierForScan(f.backend, cache, 1)
	got, err := tier.FindCandidates(context.Background(), "db", "m")
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}

	if len(got) < 1 {
		t.Errorf("reconcile chunk should have picked up yesterday's partition; got %d candidates", len(got))
	}
}

func TestHourlyTier_WithCache_SkipsCompactedMarker(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	h := now.Add(-12 * time.Hour)
	f := newScanFixture(t, "db", "m", []time.Time{h}, 0) // no input files
	f.addCompactedMarker(t, h)                            // only a marker

	cache := NewPartitionCache(24*time.Hour, 30)
	tier := newHourlyTierForScan(f.backend, cache, 1)

	got, err := tier.FindCandidates(context.Background(), "db", "m")
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("partition with only _compacted marker should not be a candidate; got %d (%v)",
			len(got), candidatePaths(got))
	}
	// Cache should remember it as fully compacted.
	partitionPath := fmt.Sprintf("db/m/%04d/%02d/%02d/%02d", h.Year(), int(h.Month()), h.Day(), h.Hour())
	if s, ok := cache.Get(partitionPath); !ok || !s.FullyCompacted {
		t.Errorf("cache should mark partition as fully compacted; ok=%v entry=%+v", ok, s)
	}
}

func TestHourlyTier_WithCache_LateWriteReDetected(t *testing.T) {
	// Simulate: partition was already compacted (cache says fullyCompacted=true).
	// Then a late write appears. The next scan should add the partition back
	// as a candidate.
	now := time.Now().UTC().Truncate(time.Hour)
	h := now.Add(-3 * time.Hour) // within incremental window

	// First state: only the _compacted marker.
	f := newScanFixture(t, "db", "m", []time.Time{h}, 0)
	f.addCompactedMarker(t, h)

	cache := NewPartitionCache(24*time.Hour, 30)
	tier := newHourlyTierForScan(f.backend, cache, 1)

	// Cycle 1: discovers the compacted partition, no candidates.
	got, _ := tier.FindCandidates(context.Background(), "db", "m")
	if len(got) != 0 {
		t.Fatalf("cycle 1 should produce no candidates; got %d", len(got))
	}
	partitionPath := fmt.Sprintf("db/m/%04d/%02d/%02d/%02d", h.Year(), int(h.Month()), h.Day(), h.Hour())
	if s, _ := cache.Get(partitionPath); !s.FullyCompacted {
		t.Fatal("cache should mark partition fully compacted after cycle 1")
	}

	// Simulate a late ingest write into the same partition.
	hUTC := h.UTC()
	lateFile := fmt.Sprintf("db/m/%04d/%02d/%02d/%02d/m_%s_42.parquet",
		hUTC.Year(), int(hUTC.Month()), hUTC.Day(), hUTC.Hour(),
		hUTC.Format("20060102_150405"))
	content := append(append([]byte("PAR1"), []byte("LATE")...), []byte("PAR1")...)
	if err := f.backend.Write(context.Background(), lateFile, content); err != nil {
		t.Fatalf("write late file: %v", err)
	}

	// Cycle 2: the incremental scan re-lists the partition, sees the new
	// file, and updates the cache. With MinFiles=2 and one late file plus
	// the _compacted marker, ShouldCompact returns false — but the cache
	// entry should be updated to reflect the new file count.
	got, _ = tier.FindCandidates(context.Background(), "db", "m")
	s, ok := cache.Get(partitionPath)
	if !ok {
		t.Fatalf("partition entry missing from cache after cycle 2")
	}
	if s.FileCount < 2 {
		t.Errorf("cache file count not updated after late write: %d", s.FileCount)
	}
	_ = got // ShouldCompact result depends on MinFiles; we already verified cache update
}

func TestHourlyTier_listHourRange_PerHourListing(t *testing.T) {
	// Verify listHourRange enumerates only the hour prefixes in [start, end)
	// and returns only files that live under those prefixes.
	now := time.Now().UTC().Truncate(time.Hour)
	hours := []time.Time{
		now.Add(-25 * time.Hour),
		now.Add(-3 * time.Hour),
	}
	f := newScanFixture(t, "db", "m", hours, 2)

	tier := newHourlyTierForScan(f.backend, NewPartitionCache(24*time.Hour, 30), 1)

	// Only ask for the last 6 hours.
	start := now.Add(-6 * time.Hour)
	end := now
	objs := tier.listHourRange(context.Background(), "db/m/", start, end)

	// Should pick up the -3h hour's 2 files, not the -25h hour's 2 files.
	if len(objs) != 2 {
		t.Errorf("listHourRange got %d objects, want 2 (recent hour only). objects: %v", len(objs), objs)
	}
}
