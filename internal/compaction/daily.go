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
const dailyIncrementalLookback = 14 * 24 * time.Hour

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
	TargetSizeMB         int   // Target size for compacted files (default: 2048)
	MaxOutputBytes       int64 // 0 = single-file output; >0 = multi-file output via FILE_SIZE_BYTES
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
	if cfg.TargetSizeMB == 0 {
		// 2048 MB (2 GB): daily files should be large for efficient cold-tier reads.
		// Larger than hourly (512 MB) because daily data is read less frequently.
		cfg.TargetSizeMB = 2048
	}

	tier := &DailyTier{
		BaseTier: NewBaseTier(&BaseTierConfig{
			StorageBackend: cfg.StorageBackend,
			MinAgeHours:    cfg.MinAgeHours,
			MinFiles:       cfg.MinFiles,
			TargetSizeMB:   cfg.TargetSizeMB,
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
		Int("target_size_mb", cfg.TargetSizeMB).
		Bool("enabled", cfg.Enabled).
		Msg("Daily compaction tier initialized")

	return tier
}

// GetTierName returns the tier name
func (t *DailyTier) GetTierName() string {
	return "daily"
}

// GetPartitionLevel returns the partition level
func (t *DailyTier) GetPartitionLevel() string {
	return "day"
}

// FindCandidates finds daily partitions that are candidates for compaction
func (t *DailyTier) FindCandidates(ctx context.Context, database, measurement string) ([]Candidate, error) {
	if !t.Enabled {
		return nil, nil
	}

	var candidates []Candidate
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

	return candidates, nil
}

// ShouldCompact determines if a day partition should be compacted
// Daily tier compacts hourly files (7 path parts) into daily files (6 path parts)
func (t *DailyTier) ShouldCompact(files []string, partitionTime time.Time) bool {
	return t.ShouldCompactByFileSuffix(
		files,
		"_daily.parquet",
		func(f string) bool {
			// Hourly files have 7 path parts: database/measurement/year/month/day/hour/file.parquet
			// These are valid input for daily compaction
			parts := strings.Split(f, "/")
			return len(parts) == 7
		},
	)
}

// IsCompactedFile checks if a file is a compacted daily file
func (t *DailyTier) IsCompactedFile(filename string) bool {
	return strings.HasSuffix(filename, "_daily.parquet")
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

// groupFilesByDayPartition parses storage paths and groups them by day.
// Shared between the flat and incremental scan paths.
func groupFilesByDayPartition(objects []string, database, measurement string, cutoffTime time.Time) map[string]*Candidate {
	partitions := make(map[string]*Candidate)

	for _, obj := range objects {
		// Parse path: database/measurement/year/month/day/[hour/]file.parquet
		parts := strings.Split(obj, "/")
		if len(parts) < 6 {
			continue
		}

		db, meas, year, month, day := parts[0], parts[1], parts[2], parts[3], parts[4]

		// Validate database and measurement
		if db != database || meas != measurement {
			continue
		}

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
