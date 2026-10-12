package compaction

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// HourlyTier implements hourly compaction (Tier 1)
// Compacts small files within hourly partitions
type HourlyTier struct {
	*BaseTier
}

// HourlyTierConfig holds configuration for hourly compaction tier
type HourlyTierConfig struct {
	StorageBackend storage.Backend
	MinAgeHours    int   // Don't compact partitions younger than this (default: 1)
	MinFiles       int   // Only compact partitions with at least this many files (default: 10)
	MaxOutputBytes int64 // Per-batch input-size budget; 0 = count-only batching
	Enabled        bool  // Enable hourly compaction (default: true)
	Logger         zerolog.Logger
}

// NewHourlyTier creates a new hourly compaction tier
func NewHourlyTier(cfg *HourlyTierConfig) *HourlyTier {
	// MinAgeHours must be >= 1 to prevent race conditions with active ingestion.
	// With 0, compaction can download and delete files while late buffer flushes
	// are still writing to the same partition, causing data loss.
	overrodeMinAge := cfg.MinAgeHours < 1
	if overrodeMinAge {
		cfg.MinAgeHours = 1
	}
	if cfg.MinFiles == 0 {
		// 10 files: minimum input-file threshold for an hourly compaction pass.
		// Ingest flushes when it reaches 50,000 rows or 5 seconds have elapsed,
		// whichever comes first. Actual files per hour depend on ingest rate and
		// schema changes.
		cfg.MinFiles = 10
	}
	tier := &HourlyTier{
		BaseTier: NewBaseTier(&BaseTierConfig{
			StorageBackend: cfg.StorageBackend,
			MinAgeHours:    cfg.MinAgeHours,
			MinFiles:       cfg.MinFiles,
			MaxOutputBytes: cfg.MaxOutputBytes,
			Enabled:        cfg.Enabled,
			Logger:         cfg.Logger.With().Str("tier", "hourly").Logger(),
		}),
	}

	if overrodeMinAge {
		tier.Logger.Warn().
			Int("enforced_value", cfg.MinAgeHours).
			Msg("hourly_min_age_hours was < 1; overriding to 1 to prevent race conditions with active ingestion")
	}

	tier.Logger.Info().
		Int("min_age_hours", cfg.MinAgeHours).
		Int("min_files", cfg.MinFiles).
		Bool("enabled", cfg.Enabled).
		Msg("Hourly compaction tier initialized")

	return tier
}

// GetTierName returns the tier name
func (t *HourlyTier) GetTierName() string {
	return "hourly"
}

// GetPartitionLevel returns the partition level
func (t *HourlyTier) GetMinFiles() int {
	return t.MinFiles
}

func (t *HourlyTier) GetPartitionLevel() string {
	return "hour"
}

// FindCandidates finds hourly partitions that are candidates for compaction.
//
// Discovery goes through listHourPartitions: with the Manager's partition
// cache wired it lists only the recent hours plus one reconcile chunk per
// cycle, never the whole measurement prefix (a flat List of 100k+ keys every
// cycle is the Hetzner bottleneck the cache exists to remove). Upstream's
// listing-reuse path (#316) lists the full prefix once per measurement and is
// deliberately NOT used for discovery here; FindCandidatesFromListing remains
// for callers that already hold a listing.
func (t *HourlyTier) FindCandidates(ctx context.Context, database, measurement string) ([]Candidate, error) {
	if !t.Enabled {
		return nil, nil
	}

	cutoffTime := time.Now().UTC().Add(-time.Duration(t.MinAgeHours) * time.Hour)

	t.Logger.Debug().
		Str("database", database).
		Str("measurement", measurement).
		Time("cutoff", cutoffTime).
		Msg("Scanning for hourly compaction candidates")

	// List all hour partitions
	partitions, err := t.listHourPartitions(ctx, database, measurement, cutoffTime)
	if err != nil {
		return nil, err
	}
	return t.selectCandidates(database, measurement, partitions), nil
}

// FindCandidatesFromListing implements Tier: FindCandidates over a listing
// the caller already holds. It does no I/O and does not touch the partition
// cache.
func (t *HourlyTier) FindCandidatesFromListing(database, measurement string, objects []string) []Candidate {
	if !t.Enabled {
		return nil
	}
	cutoffTime := time.Now().UTC().Add(-time.Duration(t.MinAgeHours) * time.Hour)
	return t.selectCandidates(database, measurement, t.groupHourPartitions(database, measurement, objects, cutoffTime))
}

// selectCandidates keeps the partitions ShouldCompact accepts.
func (t *HourlyTier) selectCandidates(database, measurement string, partitions []Candidate) []Candidate {
	var candidates []Candidate
	for _, partition := range partitions {
		if t.ShouldCompact(partition.Files, partition.PartitionTime) {
			partition.Tier = t.GetTierName()
			partition.FileCount = len(partition.Files)
			candidates = append(candidates, partition)

			t.Logger.Info().
				Str("database", database).
				Str("partition", partition.PartitionPath).
				Int("file_count", len(partition.Files)).
				Msg("Found hourly compaction candidate")
		}
	}

	// Silence is success: only log at INFO when we actually found work.
	// Otherwise this fires for every measurement on every cycle (~50
	// lines per tick) and drowns out the lines operators care about.
	scanLog := t.Logger.Debug()
	if len(candidates) > 0 {
		scanLog = t.Logger.Info()
	}
	scanLog.
		Str("database", database).
		Str("measurement", measurement).
		Int("candidates", len(candidates)).
		Msg("Hourly compaction candidate scan complete")

	return candidates
}

// ShouldCompact determines if an hourly partition should be compacted
func (t *HourlyTier) ShouldCompact(files []string, partitionTime time.Time) bool {
	return t.ShouldCompactByFileSuffix(
		files,
		"_compacted.parquet",
		func(f string) bool {
			// All non-compacted files are valid input for hourly compaction
			return !strings.Contains(storage.StripRewriteSuffix(f), "_compacted.parquet")
		},
	)
}

// IsCompactedFile checks if a file is a compacted hourly file
func (t *HourlyTier) IsCompactedFile(filename string) bool {
	return strings.HasSuffix(storage.StripRewriteSuffix(filename), "_compacted.parquet")
}

// GetStats returns tier statistics
func (t *HourlyTier) GetStats() map[string]interface{} {
	return t.GetBaseStats(t.GetTierName())
}

// incrementalLookback bounds how far back the per-cycle "recent hours"
// scan looks. New ingest writes always land within this window; older
// partitions are reconciled by the rolling cursor instead.
const incrementalLookback = 48 * time.Hour

// listHourPartitions discovers hour partitions eligible for compaction
// using two complementary scans:
//
//  1. Incremental — list the recent N hours where new ingest writes land
//     so the cache stays fresh for partitions that may still be growing.
//
//  2. Reconcile chunk — list one day's worth of hour prefixes at the
//     measurement's rolling cursor position, updating cache entries for
//     anything that drifted. The cursor walks backward through history
//     one chunk per cycle, eventually covering every partition.
//
// Both scans are bounded — no flat List of the entire measurement prefix
// is ever issued, so per-cycle S3 traffic stays cheap regardless of how
// many years of data live under the table.
func (t *HourlyTier) listHourPartitions(ctx context.Context, database, measurement string, cutoffTime time.Time) ([]Candidate, error) {
	if t.Cache == nil {
		// No cache wired (tests bypass the Manager). Fall back to the
		// historical flat-scan behavior so tests stay deterministic.
		return t.listHourPartitionsFlat(ctx, database, measurement, cutoffTime)
	}

	prefix := database + "/" + measurement + "/"
	measurementKey := database + "/" + measurement

	allObjects := make([]string, 0, 2048)

	// --- (1) Incremental scan of the last incrementalLookback ---
	scanFrom := time.Now().UTC().Add(-incrementalLookback).Truncate(time.Hour)
	scanUntil := cutoffTime
	if scanFrom.After(scanUntil) {
		scanFrom = scanUntil
	}
	allObjects = append(allObjects, t.listHourRange(ctx, prefix, scanFrom, scanUntil)...)
	allObjects = append(allObjects, t.listDayLevelRange(ctx, prefix, scanFrom, scanUntil)...)

	// --- (2) Reconcile chunk at the rolling cursor ---
	rStart, rEnd := t.Cache.NextReconcileChunk(measurementKey)
	// Avoid double-listing if reconcile chunk overlaps the incremental window.
	if rEnd.After(scanFrom) {
		rEnd = scanFrom
	}
	if rStart.Before(rEnd) {
		allObjects = append(allObjects, t.listHourRange(ctx, prefix, rStart, rEnd)...)
		allObjects = append(allObjects, t.listDayLevelRange(ctx, prefix, rStart, rEnd)...)
	}

	// Demoted from INFO -- this fires per-measurement per-cycle and is
	// noise unless we're chasing a "why aren't we finding candidates"
	// question. Re-enable by setting log.level=debug.
	t.Logger.Debug().
		Str("database", database).
		Str("measurement", measurement).
		Int("object_count", len(allObjects)).
		Time("incremental_from", scanFrom).
		Time("reconcile_start", rStart).
		Time("reconcile_end", rEnd).
		Msg("Compaction discovery scan (incremental + reconcile)")

	partitions := groupFilesByHourPartition(allObjects, database, measurement, cutoffTime)

	// Update cache with everything we observed.
	for path, p := range partitions {
		t.Cache.Set(path, partitionState{
			FullyCompacted: !t.ShouldCompact(p.Files, p.PartitionTime),
			NewestFileTime: extractNewestFileTime(p.Files),
			FileCount:      len(p.Files),
		})
	}

	// Evict cache entries for hour partitions we expected to see in the
	// scanned ranges but didn't (files removed from storage, manual S3
	// edits, etc.). Without this the cache holds stale FullyCompacted=true
	// entries indefinitely after external deletes.
	t.pruneMissingFromCache(database, measurement, scanFrom, scanUntil, partitions)
	if rStart.Before(rEnd) {
		t.pruneMissingFromCache(database, measurement, rStart, rEnd, partitions)
	}

	return filterEligibleCandidates(partitions, cutoffTime, t.Logger), nil
}

// pruneMissingFromCache walks every hour boundary in [start, end) and
// evicts the cache entry if no observed partition matches that path.
// The caller has already issued List() for those prefixes via the scan,
// so an absent partition means it truly no longer exists in storage.
func (t *HourlyTier) pruneMissingFromCache(database, measurement string, start, end time.Time, observed map[string]*Candidate) {
	for h := start.Truncate(time.Hour); h.Before(end); h = h.Add(time.Hour) {
		hUTC := h.UTC()
		path := fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d",
			database, measurement,
			hUTC.Year(), int(hUTC.Month()), hUTC.Day(), hUTC.Hour())
		if _, ok := observed[path]; !ok {
			t.Cache.Invalidate(path)
		}
	}
}

// listHourRange enumerates all parquet files under <prefix>/YYYY/MM/DD/HH/
// for every hour in [start, end). Each per-hour List returns a small
// number of keys; running ~50-200 of these is dramatically cheaper than
// a single flat List of 100k keys.
func (t *HourlyTier) listHourRange(ctx context.Context, prefix string, start, end time.Time) []string {
	out := make([]string, 0, 512)
	for h := start.Truncate(time.Hour); h.Before(end); h = h.Add(time.Hour) {
		hourPrefix := fmt.Sprintf("%s%04d/%02d/%02d/%02d/",
			prefix, h.Year(), int(h.Month()), h.Day(), h.Hour())
		objs, err := t.StorageBackend.List(ctx, hourPrefix)
		if err != nil {
			continue
		}
		out = append(out, objs...)
	}
	return out
}

// listDayLevelRange picks up daily-compacted files written by DailyTier
// (<db>/<m>/YYYY/MM/DD/*_daily.parquet without an hour subdir). Without
// this, an hour-by-hour walk would skip them and the hourly tier would
// re-compact those days unnecessarily.
func (t *HourlyTier) listDayLevelRange(ctx context.Context, prefix string, start, end time.Time) []string {
	out := make([]string, 0)
	for d := start.Truncate(24 * time.Hour); d.Before(end); d = d.Add(24 * time.Hour) {
		dayPrefix := fmt.Sprintf("%s%04d/%02d/%02d/",
			prefix, d.Year(), int(d.Month()), d.Day())
		objs, err := t.StorageBackend.List(ctx, dayPrefix)
		if err != nil {
			continue
		}
		for _, o := range objs {
			// Only keep day-level files (6 path parts: db/m/yyyy/mm/dd/file).
			parts := strings.Split(o, "/")
			if len(parts) == 6 {
				out = append(out, o)
			}
		}
	}
	return out
}

// listHourPartitionsFlat is the legacy cold-path retained for tests
// that construct a tier without a Manager (and therefore without a
// PartitionCache). Production always uses the cached path above.
func (t *HourlyTier) listHourPartitionsFlat(ctx context.Context, database, measurement string, cutoffTime time.Time) ([]Candidate, error) {
	prefix := database + "/" + measurement + "/"
	objects, err := t.StorageBackend.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	t.Logger.Debug().
		Str("database", database).
		Str("measurement", measurement).
		Int("object_count", len(objects)).
		Msg("Flat scan (no cache)")

	partitions := groupFilesByHourPartition(objects, database, measurement, cutoffTime)
	return filterEligibleCandidates(partitions, cutoffTime, t.Logger), nil
}

// groupHourPartitions groups a measurement's objects into hour partitions
// old enough to compact.
func (t *HourlyTier) groupHourPartitions(database, measurement string, objects []string, cutoffTime time.Time) []Candidate {
	partitions := groupFilesByHourPartition(objects, database, measurement, cutoffTime)
	return filterEligibleCandidates(partitions, cutoffTime, t.Logger)
}

// groupFilesByHourPartition parses storage paths and groups them by hour
// partition. Used by both the flat and incremental scan paths.
func groupFilesByHourPartition(objects []string, database, measurement string, cutoffTime time.Time) map[string]*Candidate {
	partitions := make(map[string]*Candidate)
	prefix := measurementPrefix(database, measurement)

	for _, obj := range objects {
		// Parse the partition components RELATIVE to the database/measurement
		// prefix rather than at fixed indices: a spoke-namespace
		// pseudo-database ("rocket-01/telemetry") carries a slash, so
		// parts[0] would be the spoke segment and the year would land at
		// parts[3] (#619). The List prefix already scopes objects to this
		// database/measurement, so trimming the prefix is both simpler and
		// correct for plain and pseudo databases alike.
		rel, ok := strings.CutPrefix(obj, prefix)
		if !ok {
			continue
		}
		parts := strings.Split(rel, "/")
		if len(parts) < 5 {
			// year/month/day/hour/file.parquet
			continue
		}

		year, month, day, hour := parts[0], parts[1], parts[2], parts[3]

		// Validate hour is a valid hour (00-23)
		hourInt, err := strconv.Atoi(hour)
		if err != nil || hourInt < 0 || hourInt > 23 {
			continue // Not a valid hour, skip
		}

		// Parse partition time with proper error handling
		yearInt, err := strconv.Atoi(year)
		if err != nil {
			continue // Invalid year, skip
		}
		monthInt, err := strconv.Atoi(month)
		if err != nil || monthInt < 1 || monthInt > 12 {
			continue // Invalid month, skip
		}
		dayInt, err := strconv.Atoi(day)
		if err != nil || dayInt < 1 || dayInt > 31 {
			continue // Invalid day, skip
		}

		partitionTime := time.Date(yearInt, time.Month(monthInt), dayInt, hourInt, 0, 0, 0, time.UTC)

		// Check if partition is old enough
		if partitionTime.After(cutoffTime) {
			continue
		}

		partitionPath := filepath.Join(database, measurement, year, month, day, hour)

		if _, exists := partitions[partitionPath]; !exists {
			partitions[partitionPath] = &Candidate{
				Database:      database,
				Measurement:   measurement,
				PartitionPath: partitionPath,
				PartitionTime: partitionTime,
				Files:         []string{},
			}
		}
		partitions[partitionPath].Files = append(partitions[partitionPath].Files, obj)
	}

	return partitions
}

// filterEligibleCandidates excludes partitions whose newest file is still
// fresh enough that compacting could race with active ingest writes.
func filterEligibleCandidates(partitions map[string]*Candidate, cutoffTime time.Time, logger zerolog.Logger) []Candidate {
	result := make([]Candidate, 0, len(partitions))
	for _, p := range partitions {
		newestFileTime := extractNewestFileTime(p.Files)
		if !newestFileTime.IsZero() && newestFileTime.After(cutoffTime) {
			logger.Debug().
				Str("partition", p.PartitionPath).
				Time("newest_file", newestFileTime).
				Time("cutoff", cutoffTime).
				Msg("Skipping partition: has recent files")
			continue
		}
		result = append(result, *p)
	}
	return result
}
