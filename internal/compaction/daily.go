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

// dailyIncrementalLookback bounds the per-cycle "recent days" scan for
// the daily tier. 14 days gives a wide cushion past MinAgeHours=24 so
// any drift from late writes gets caught quickly; older partitions are
// reconciled by the rolling cursor.
// dailyIncrementalLookback widened 14 → 90 days after the 2026-05-20
// investigation showed older partitions accumulated new uncompacted
// files via late-arrival reorg but were never reached by the rolling
// cursor (in-memory, resets on pod restart). 90 days matches
// reconcile_window_days so every cycle covers the full backfill window.
// S3 LIST cost: ~90 prefixes × N (db, ms) per cycle, negligible.
const dailyIncrementalLookback = 90 * 24 * time.Hour

// DailyTier implements daily compaction (Tier 2)
// Compacts hourly-compacted files into daily files
type DailyTier struct {
	*BaseTier
	SkipFileAgeCheckDays int // Skip file creation time check for partitions older than this
}

// DailyTierConfig holds configuration for daily compaction tier
type DailyTierConfig struct {
	StorageBackend       storage.Backend
	MinAgeHours          int   // Don't compact days younger than this (default: 24)
	MinFiles             int   // Only compact days with at least this many files (default: 12)
	SkipFileAgeCheckDays int   // Skip file creation time check for partitions older than this (default: 7)
	MaxOutputBytes       int64 // Per-batch input-size budget; 0 = count-only batching
	Enabled              bool  // Enable daily compaction (default: true)
	Logger               zerolog.Logger
}

// NewDailyTier creates a new daily compaction tier
func NewDailyTier(cfg *DailyTierConfig) *DailyTier {
	// Set defaults
	if cfg.MinAgeHours == 0 {
		cfg.MinAgeHours = 24 // Full day must pass
	}
	if cfg.MinFiles == 0 {
		// 12 files: hourly compaction produces ~1 file/hour, so 12 ≈ half a day.
		// Ensures enough data volume to justify the cost of daily re-compaction.
		cfg.MinFiles = 12
	}
	if cfg.SkipFileAgeCheckDays <= 0 {
		// 7 days: for partitions older than this, skip the file creation time check
		// that normally prevents compacting in-progress data. After a week, any
		// backfill is assumed complete.
		cfg.SkipFileAgeCheckDays = 7
	}
	tier := &DailyTier{
		BaseTier: NewBaseTier(&BaseTierConfig{
			StorageBackend: cfg.StorageBackend,
			MinAgeHours:    cfg.MinAgeHours,
			MinFiles:       cfg.MinFiles,
			MaxOutputBytes: cfg.MaxOutputBytes,
			Enabled:        cfg.Enabled,
			Logger:         cfg.Logger.With().Str("tier", "daily").Logger(),
		}),
		SkipFileAgeCheckDays: cfg.SkipFileAgeCheckDays,
	}

	tier.Logger.Info().
		Int("min_age_hours", cfg.MinAgeHours).
		Int("min_files", cfg.MinFiles).
		Int("skip_file_age_check_days", cfg.SkipFileAgeCheckDays).
		Bool("enabled", cfg.Enabled).
		Msg("Daily compaction tier initialized")

	return tier
}

// GetTierName returns the tier name
func (t *DailyTier) GetTierName() string {
	return "daily"
}

// GetPartitionLevel returns the partition level
func (t *DailyTier) GetMinFiles() int {
	return t.MinFiles
}

func (t *DailyTier) GetPartitionLevel() string {
	return "day"
}

// FindCandidates finds daily partitions that are candidates for compaction.
//
// Discovery goes through listDayPartitions (incremental window plus one
// reconcile chunk when the Manager's partition cache is wired), never a flat
// List of the whole measurement prefix — see HourlyTier.FindCandidates.
func (t *DailyTier) FindCandidates(ctx context.Context, database, measurement string) ([]Candidate, error) {
	if !t.Enabled {
		return nil, nil
	}

	cutoffTime := time.Now().UTC().Add(-time.Duration(t.MinAgeHours) * time.Hour)

	t.Logger.Debug().
		Str("database", database).
		Str("measurement", measurement).
		Time("cutoff", cutoffTime).
		Msg("Scanning for daily compaction candidates")

	// List all day partitions
	partitions, err := t.listDayPartitions(ctx, database, measurement, cutoffTime)
	if err != nil {
		return nil, err
	}
	return t.selectCandidates(database, measurement, partitions), nil
}

// FindCandidatesFromListing implements Tier: FindCandidates over a listing
// the caller already holds. It does no I/O and does not touch the partition
// cache.
func (t *DailyTier) FindCandidatesFromListing(database, measurement string, objects []string) []Candidate {
	if !t.Enabled {
		return nil
	}
	cutoffTime := time.Now().UTC().Add(-time.Duration(t.MinAgeHours) * time.Hour)
	return t.selectCandidates(database, measurement, t.groupDayPartitions(database, measurement, objects, cutoffTime))
}

// selectCandidates keeps the partitions ShouldCompact accepts.
func (t *DailyTier) selectCandidates(database, measurement string, partitions []Candidate) []Candidate {
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
				Msg("Found daily compaction candidate")
		}
	}

	// Silence is success: only log at INFO when we actually found work.
	scanLog := t.Logger.Debug()
	if len(candidates) > 0 {
		scanLog = t.Logger.Info()
	}
	scanLog.
		Str("database", database).
		Str("measurement", measurement).
		Int("candidates", len(candidates)).
		Msg("Daily compaction candidate scan complete")

	return candidates
}

// ShouldCompact determines if a day partition should be compacted
// Daily tier compacts hourly files (7 path parts) into daily files (6 path parts)
func (t *DailyTier) ShouldCompact(files []string, partitionTime time.Time) bool {
	return t.ShouldCompactByFileSuffix(
		files,
		"_daily.parquet",
		func(f string) bool {
			// Hour-level files (database/measurement/year/month/day/hour/file.parquet)
			// are valid input for daily compaction.
			if isHourLevelFile(f) {
				return true
			}
			// Day-level reorg output (database/measurement/year/month/day/file.parquet):
			// the reorganizer drains late events straight to the day partition. Treat
			// it as foldable input so Case 4 (reorg-aware) re-merges it — and any
			// crash-induced duplicate of it — into the sealed _daily file instead of
			// leaving it to double-count.
			return isReorgFile(f) && isDayLevelFile(f)
		},
	)
}

// IsCompactedFile checks if a file is a compacted daily file
func (t *DailyTier) IsCompactedFile(filename string) bool {
	return strings.HasSuffix(storage.StripRewriteSuffix(filename), "_daily.parquet")
}

// GetStats returns tier statistics
func (t *DailyTier) GetStats() map[string]interface{} {
	return t.GetBaseStats(t.GetTierName())
}

// listDayPartitions discovers day partitions eligible for compaction
// using the same incremental + rolling-cursor pattern as the hourly tier.
// Recent days are always re-listed (catches new daily-compacted files
// landing from the hourly tier); older days are reconciled chunk-by-
// chunk as the rolling cursor walks backward through history.
func (t *DailyTier) listDayPartitions(ctx context.Context, database, measurement string, cutoffTime time.Time) ([]Candidate, error) {
	if t.Cache == nil {
		return t.listDayPartitionsFlat(ctx, database, measurement, cutoffTime)
	}

	prefix := database + "/" + measurement + "/"
	measurementKey := database + "/" + measurement

	allObjects := make([]string, 0, 1024)

	// --- (1) Incremental scan of recent days ---
	scanFrom := time.Now().UTC().Add(-dailyIncrementalLookback).Truncate(24 * time.Hour)
	scanUntil := cutoffTime
	if scanFrom.After(scanUntil) {
		scanFrom = scanUntil
	}
	allObjects = append(allObjects, t.listDayRange(ctx, prefix, scanFrom, scanUntil)...)

	// --- (2) Reconcile chunk at the rolling cursor ---
	rStart, rEnd := t.Cache.NextReconcileChunk(measurementKey + ":daily")
	if rEnd.After(scanFrom) {
		rEnd = scanFrom
	}
	if rStart.Before(rEnd) {
		allObjects = append(allObjects, t.listDayRange(ctx, prefix, rStart, rEnd)...)
	}

	// Demoted from INFO -- per-measurement per-cycle noise.
	t.Logger.Debug().
		Str("database", database).
		Str("measurement", measurement).
		Int("object_count", len(allObjects)).
		Time("incremental_from", scanFrom).
		Time("reconcile_start", rStart).
		Time("reconcile_end", rEnd).
		Msg("Daily compaction discovery scan")

	partitions := groupFilesByDayPartition(allObjects, database, measurement, cutoffTime)
	for path, p := range partitions {
		t.Cache.Set(path, partitionState{
			FullyCompacted: !t.ShouldCompact(p.Files, p.PartitionTime),
			NewestFileTime: extractNewestFileTime(p.Files),
			FileCount:      len(p.Files),
		})
	}
	// Evict stale cache entries for day partitions we expected to see
	// but didn't (parallel to the hourly tier's pruneMissingFromCache).
	t.pruneMissingFromCache(database, measurement, scanFrom, scanUntil, partitions)
	if rStart.Before(rEnd) {
		t.pruneMissingFromCache(database, measurement, rStart, rEnd, partitions)
	}
	return t.filterDayCandidates(partitions, cutoffTime), nil
}

// pruneMissingFromCache evicts day-partition cache entries within
// [start, end) that the latest scan did not observe.
func (t *DailyTier) pruneMissingFromCache(database, measurement string, start, end time.Time, observed map[string]*Candidate) {
	for d := start.Truncate(24 * time.Hour); d.Before(end); d = d.Add(24 * time.Hour) {
		dUTC := d.UTC()
		path := fmt.Sprintf("%s/%s/%04d/%02d/%02d",
			database, measurement,
			dUTC.Year(), int(dUTC.Month()), dUTC.Day())
		if _, ok := observed[path]; !ok {
			t.Cache.Invalidate(path)
		}
	}
}

// listDayRange enumerates objects under <prefix>YYYY/MM/DD/ for every
// day in [start, end). Each per-day List returns a small number of keys
// (typically <500 after daily compaction); cheap to repeat.
func (t *DailyTier) listDayRange(ctx context.Context, prefix string, start, end time.Time) []string {
	out := make([]string, 0, 512)
	for d := start.Truncate(24 * time.Hour); d.Before(end); d = d.Add(24 * time.Hour) {
		dayPrefix := fmt.Sprintf("%s%04d/%02d/%02d/",
			prefix, d.Year(), int(d.Month()), d.Day())
		objs, err := t.StorageBackend.List(ctx, dayPrefix)
		if err != nil {
			continue
		}
		out = append(out, objs...)
	}
	return out
}

// listDayPartitionsFlat is the legacy cold-path used only when the
// daily tier is constructed without a PartitionCache (i.e. in tests).
func (t *DailyTier) listDayPartitionsFlat(ctx context.Context, database, measurement string, cutoffTime time.Time) ([]Candidate, error) {
	prefix := database + "/" + measurement + "/"
	objects, err := t.StorageBackend.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	partitions := groupFilesByDayPartition(objects, database, measurement, cutoffTime)
	return t.filterDayCandidates(partitions, cutoffTime), nil
}

// filterDayCandidates excludes day partitions whose newest file is too
// fresh to safely compact (unless they're older than SkipFileAgeCheckDays).
func (t *DailyTier) filterDayCandidates(partitions map[string]*Candidate, cutoffTime time.Time) []Candidate {
	result := make([]Candidate, 0, len(partitions))
	skipAgeThreshold := time.Duration(t.SkipFileAgeCheckDays*24) * time.Hour
	for _, p := range partitions {
		partitionAge := time.Since(p.PartitionTime)
		if partitionAge <= skipAgeThreshold {
			newestFileTime := extractNewestFileTime(p.Files)
			if !newestFileTime.IsZero() && newestFileTime.After(cutoffTime) {
				t.Logger.Debug().
					Str("partition", p.PartitionPath).
					Time("newest_file", newestFileTime).
					Time("cutoff", cutoffTime).
					Msg("Skipping partition: has recent files")
				continue
			}
		}
		result = append(result, *p)
	}
	return result
}

// groupDayPartitions groups a measurement's objects into day partitions old
// enough to compact.
func (t *DailyTier) groupDayPartitions(database, measurement string, objects []string, cutoffTime time.Time) []Candidate {
	partitions := groupFilesByDayPartition(objects, database, measurement, cutoffTime)
	return t.filterDayCandidates(partitions, cutoffTime)
}

// groupFilesByDayPartition parses storage paths and groups them by day.
// Shared between the flat and incremental scan paths.
func groupFilesByDayPartition(objects []string, database, measurement string, cutoffTime time.Time) map[string]*Candidate {
	partitions := make(map[string]*Candidate)
	prefix := measurementPrefix(database, measurement)

	for _, obj := range objects {
		// Parse partition components RELATIVE to the database/measurement
		// prefix, not at fixed indices: a spoke-namespace pseudo-database
		// ("rocket-01/telemetry") carries a slash, which would shift every
		// fixed offset by one (#619). The List prefix already scopes the
		// objects.
		rel, ok := strings.CutPrefix(obj, prefix)
		if !ok {
			continue
		}
		parts := strings.Split(rel, "/")
		if len(parts) < 4 {
			// year/month/day/[hour/]file.parquet
			continue
		}

		year, month, day := parts[0], parts[1], parts[2]

		// Parse partition time (day level)
		yearInt, err := strconv.Atoi(year)
		if err != nil {
			continue
		}
		monthInt, err := strconv.Atoi(month)
		if err != nil {
			continue
		}
		dayInt, err := strconv.Atoi(day)
		if err != nil {
			continue
		}

		partitionTime := time.Date(yearInt, time.Month(monthInt), dayInt, 0, 0, 0, 0, time.UTC)

		// Check if partition is old enough
		if partitionTime.After(cutoffTime) {
			continue
		}

		// Build partition path (day level, includes database)
		partitionPath := filepath.Join(database, measurement, year, month, day)

		// Add to partition map
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

// extractNewestFileTime extracts the newest file creation time from a list of file paths.
// Supports two formats:
// - Hourly files: {measurement}_{YYYYMMDD_HHMMSS}_{nanos}.parquet
// - Daily files: {measurement}_{YYYYMMDD}_daily.parquet
// Returns zero time if no valid timestamps found.
//
// Unparseable filenames are SKIPPED — they don't bump the result toward
// "now". Treating any unparseable filename as freshly-written caused
// permanent starvation: a single misnamed file on a partition would defer
// daily compaction every cycle forever. The race that "treat as fresh"
// tried to dodge — an active writer producing a filename this parser
// doesn't recognise — is a code-coordination concern (writer / parser
// must agree on naming) rather than something the daily tier should
// silently work around.
func extractNewestFileTime(files []string) time.Time {
	var newest time.Time

	for _, file := range files {
		// Extract filename from path
		parts := strings.Split(file, "/")
		filename := parts[len(parts)-1]

		// Remove .parquet extension
		filename = storage.StripRewriteSuffix(filename)
		filename = strings.TrimSuffix(filename, ".parquet")

		// Check if it's a tier-compacted file: measurement_YYYYMMDD_HHMMSS_{nanos}_{daily|compacted}
		// Strip the tier suffix, then handle like a raw file
		tierSuffix := ""
		if strings.HasSuffix(filename, "_daily") {
			tierSuffix = "_daily"
		} else if strings.HasSuffix(filename, "_compacted") {
			tierSuffix = "_compacted"
		}

		if tierSuffix != "" {
			filename = strings.TrimSuffix(filename, tierSuffix)
			fileParts := strings.Split(filename, "_")
			if len(fileParts) < 3 {
				continue
			}
			// Try parsing last two parts as YYYYMMDD_HHMMSS (old format without nanos)
			dateTimePart := fileParts[len(fileParts)-2] + "_" + fileParts[len(fileParts)-1]
			fileTime, err := time.Parse("20060102_150405", dateTimePart)
			if err != nil && len(fileParts) >= 4 {
				// New format with nanos: ..._YYYYMMDD_HHMMSS_nanos — skip nanos, take 3rd and 2nd from end
				dateTimePart = fileParts[len(fileParts)-3] + "_" + fileParts[len(fileParts)-2]
				fileTime, err = time.Parse("20060102_150405", dateTimePart)
			}
			if err != nil {
				continue
			}
			if fileTime.After(newest) {
				newest = fileTime
			}
			continue
		}

		// Handle raw hourly file: measurement_YYYYMMDD_HHMMSS_nanos
		fileParts := strings.Split(filename, "_")
		if len(fileParts) < 3 {
			continue
		}

		// Get timestamp parts (second and third from end)
		// Format: ..._YYYYMMDD_HHMMSS_nanos
		dateTimePart := fileParts[len(fileParts)-3] + "_" + fileParts[len(fileParts)-2]

		// Parse timestamp: YYYYMMDD_HHMMSS
		fileTime, err := time.Parse("20060102_150405", dateTimePart)
		if err != nil {
			continue
		}

		// Keep track of newest
		if fileTime.After(newest) {
			newest = fileTime
		}
	}

	return newest
}

// isHourLevelFile reports whether a storage path names an HOUR-level file —
// the valid input for daily compaction — by the shape of its TAIL:
// …/year/month/day/hour/file.parquet. Absolute segment counts cannot work
// here since #619: a spoke-namespace pseudo-database adds a path level, so
// hour-level spoke files have 8 parts where plain ones have 7. The tail is
// unambiguous instead — a DAY-level file has the measurement name where the
// year would be, and measurements are never four-digit-numeric-with-numeric
// children all the way down.
func isHourLevelFile(f string) bool {
	parts := strings.Split(f, "/")
	if len(parts) < 5 {
		return false
	}
	hour, day, month, year := parts[len(parts)-2], parts[len(parts)-3], parts[len(parts)-4], parts[len(parts)-5]
	if n, err := strconv.Atoi(hour); err != nil || n < 0 || n > 23 {
		return false
	}
	if n, err := strconv.Atoi(day); err != nil || n < 1 || n > 31 {
		return false
	}
	if n, err := strconv.Atoi(month); err != nil || n < 1 || n > 12 {
		return false
	}
	if n, err := strconv.Atoi(year); err != nil || n < 1970 {
		return false
	}
	return true
}

// isDayLevelFile reports whether a storage path names a DAY-level file
// (…/year/month/day/file.parquet), by the shape of its tail like
// isHourLevelFile. An hour-level path never matches: its month segment would
// have to parse as a year.
func isDayLevelFile(f string) bool {
	parts := strings.Split(f, "/")
	if len(parts) < 4 {
		return false
	}
	day, month, year := parts[len(parts)-2], parts[len(parts)-3], parts[len(parts)-4]
	if n, err := strconv.Atoi(day); err != nil || n < 1 || n > 31 {
		return false
	}
	if n, err := strconv.Atoi(month); err != nil || n < 1 || n > 12 {
		return false
	}
	if n, err := strconv.Atoi(year); err != nil || n < 1970 {
		return false
	}
	return true
}
