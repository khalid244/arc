package compaction

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// DefaultMaxFilesPerBatch is the fallback batch cap when none is supplied
// via configuration. DuckDB can segfault/abort when read_parquet() spans
// too many files, so the manager splits large partitions into batches of
// at most this many files before scheduling each as its own subprocess.
// The right value depends on per-file size and the per-subprocess
// memory_limit; tune via compaction.max_files_per_batch in TOML.
const DefaultMaxFilesPerBatch = 2000

// Candidate represents a partition candidate for compaction
type Candidate struct {
	Database      string
	Measurement   string
	PartitionPath string
	Files         []string
	FileSizes     []int64 // Parallel to Files. Optional; nil means sizes unknown.
	FileCount     int
	Tier          string
	PartitionTime time.Time
	BatchNumber   int // Batch number when candidate is split (0 = not batched or first batch)
	TotalBatches  int // Total number of batches for this partition (0 = not batched)
}

// SplitCandidateByBudget splits a candidate into sub-batches honoring BOTH
// a per-batch file-count cap and a per-batch input-size budget (sum of
// FileSizes ≤ maxInputBytes). Either cap can be 0 to disable.
//
// Files whose individual size > maxInputBytes are SKIPPED (defense-in-depth
// against oversized inputs that would breach the budget alone; the suffix-
// based candidate filter should already exclude already-compacted files,
// this is the second line of defense).
//
// When Candidate.FileSizes is nil, falls back to count-only splitting
// (delegates to SplitCandidateIntoBatches).
func SplitCandidateByBudget(c Candidate, maxFilesPerBatch int, maxInputBytes int64) []Candidate {
	if len(c.Files) == 0 {
		return nil
	}
	// No size info → count-only.
	if c.FileSizes == nil {
		return SplitCandidateIntoBatches(c, maxFilesPerBatch)
	}
	// Budget disabled → count-only.
	if maxInputBytes <= 0 {
		return SplitCandidateIntoBatches(c, maxFilesPerBatch)
	}
	if len(c.FileSizes) != len(c.Files) {
		// Mismatched sizes: fall back to count-only rather than risk wrong packing.
		return SplitCandidateIntoBatches(c, maxFilesPerBatch)
	}

	type batchAcc struct {
		files []string
		sizes []int64
		sum   int64
	}
	var batches []batchAcc
	cur := batchAcc{}
	flush := func() {
		if len(cur.files) > 0 {
			batches = append(batches, cur)
			cur = batchAcc{}
		}
	}
	for i, f := range c.Files {
		sz := c.FileSizes[i]
		// Skip oversize files (alone they'd exceed the budget).
		if sz > maxInputBytes {
			continue
		}
		// Adding would breach the byte budget: flush first.
		if cur.sum+sz > maxInputBytes {
			flush()
		}
		// Adding would breach the file-count cap (if set): flush first.
		if maxFilesPerBatch > 0 && len(cur.files) >= maxFilesPerBatch {
			flush()
		}
		cur.files = append(cur.files, f)
		cur.sizes = append(cur.sizes, sz)
		cur.sum += sz
	}
	flush()

	out := make([]Candidate, 0, len(batches))
	for i, b := range batches {
		out = append(out, Candidate{
			Database:      c.Database,
			Measurement:   c.Measurement,
			PartitionPath: c.PartitionPath,
			Files:         b.files,
			FileSizes:     b.sizes,
			FileCount:     len(b.files),
			Tier:          c.Tier,
			PartitionTime: c.PartitionTime,
			BatchNumber:   i + 1,
			TotalBatches:  len(batches),
		})
	}
	return out
}

// SplitCandidateIntoBatches splits a candidate with many files into multiple candidates,
// each with at most maxFilesPerBatch files. This prevents DuckDB segfaults when processing
// thousands of files in a single read_parquet() call. A non-positive maxFilesPerBatch
// falls back to DefaultMaxFilesPerBatch.
func SplitCandidateIntoBatches(c Candidate, maxFilesPerBatch int) []Candidate {
	if maxFilesPerBatch <= 0 {
		maxFilesPerBatch = DefaultMaxFilesPerBatch
	}
	if len(c.Files) <= maxFilesPerBatch {
		return []Candidate{c}
	}

	numBatches := (len(c.Files) + maxFilesPerBatch - 1) / maxFilesPerBatch

	batches := make([]Candidate, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		start := i * maxFilesPerBatch
		end := start + maxFilesPerBatch
		if end > len(c.Files) {
			end = len(c.Files)
		}

		batch := Candidate{
			Database:      c.Database,
			Measurement:   c.Measurement,
			PartitionPath: c.PartitionPath,
			Files:         c.Files[start:end],
			FileCount:     end - start,
			Tier:          c.Tier,
			PartitionTime: c.PartitionTime,
			BatchNumber:   i + 1,
			TotalBatches:  numBatches,
		}
		batches = append(batches, batch)
	}

	return batches
}

// Tier defines the interface for compaction tiers (hourly, daily, weekly, monthly)
type Tier interface {
	// GetTierName returns the human-readable tier name (e.g., 'daily', 'weekly', 'monthly')
	GetTierName() string

	// GetPartitionLevel returns the partition level for this tier (e.g., 'day', 'week', 'month')
	GetPartitionLevel() string

	// FindCandidates finds partitions that are candidates for compaction at this tier level
	FindCandidates(ctx context.Context, database, measurement string) ([]Candidate, error)

	// ShouldCompact determines if a partition should be compacted based on tier-specific criteria
	ShouldCompact(files []string, partitionTime time.Time) bool

	// IsCompactedFile checks if a file is already a compacted file from this tier
	IsCompactedFile(filename string) bool

	// IsEnabled returns whether this tier is enabled
	IsEnabled() bool

	// GetStats returns tier statistics
	GetStats() map[string]interface{}

	// GetMaxOutputBytes returns the per-batch input-size budget for this tier.
	// 0 means "no budget" — the Manager will fall back to count-based splitting.
	GetMaxOutputBytes() int64
}

// BaseTier provides common functionality for all compaction tiers
type BaseTier struct {
	StorageBackend storage.Backend
	MinAgeHours    int
	MinFiles       int
	TargetSizeMB   int
	MaxOutputBytes int64
	Enabled        bool

	// Cache is set by Manager after construction; tiers consult it during
	// candidate discovery to skip listing already-compacted partitions.
	// nil when the tier runs outside a Manager (e.g. in tests).
	Cache *PartitionCache

	// Metrics
	TotalCompactions    int
	TotalFilesCompacted int
	TotalBytesSaved     int64

	Logger zerolog.Logger
	mu     sync.Mutex
}

// SetPartitionCache injects the Manager's shared partition cache.
func (t *BaseTier) SetPartitionCache(c *PartitionCache) {
	t.Cache = c
}

// BaseTierConfig holds configuration for creating a base tier
type BaseTierConfig struct {
	StorageBackend storage.Backend
	MinAgeHours    int
	MinFiles       int
	TargetSizeMB   int
	MaxOutputBytes int64
	Enabled        bool
	Logger         zerolog.Logger
}

// NewBaseTier creates a new base tier with common functionality
func NewBaseTier(cfg *BaseTierConfig) *BaseTier {
	return &BaseTier{
		StorageBackend: cfg.StorageBackend,
		MinAgeHours:    cfg.MinAgeHours,
		MinFiles:       cfg.MinFiles,
		TargetSizeMB:   cfg.TargetSizeMB,
		MaxOutputBytes: cfg.MaxOutputBytes,
		Enabled:        cfg.Enabled,
		Logger:         cfg.Logger,
	}
}

// IsEnabled returns whether this tier is enabled
func (t *BaseTier) IsEnabled() bool {
	return t.Enabled
}

// GetMaxOutputBytes returns the per-batch input-size budget for this tier.
func (t *BaseTier) GetMaxOutputBytes() int64 {
	return t.MaxOutputBytes
}

// GetBaseStats returns base statistics for a tier
func (t *BaseTier) GetBaseStats(tierName string) map[string]interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	return map[string]interface{}{
		"tier":                  tierName,
		"enabled":               t.Enabled,
		"min_age_hours":         t.MinAgeHours,
		"min_files":             t.MinFiles,
		"target_size_mb":        t.TargetSizeMB,
		"total_compactions":     t.TotalCompactions,
		"total_files_compacted": t.TotalFilesCompacted,
		"total_bytes_saved":     t.TotalBytesSaved,
		"total_bytes_saved_mb":  float64(t.TotalBytesSaved) / 1024 / 1024,
	}
}

// reorgFileMarker is the filename infix the reorganizer stamps into every
// output it writes (see enumerateOutputs in reorg.go). Tier compaction keys
// on it so a late-arrival reorg file gets folded into an already-sealed
// partition promptly — single source of truth shared with the writer so the
// two can't drift.
const reorgFileMarker = "_reorg_"

// isReorgFile reports whether a storage key is a reorganizer output, matching
// on the basename so a database/measurement path segment that happens to
// contain the marker can't false-positive.
func isReorgFile(key string) bool {
	base := key
	if i := strings.LastIndexByte(key, '/'); i >= 0 {
		base = key[i+1:]
	}
	return strings.Contains(base, reorgFileMarker)
}

// ShouldCompactByFileSuffix determines if compaction is needed based on file classification.
// This is a shared helper that implements the common compaction decision logic:
//   - compactedSuffix: suffix for files already compacted at this tier (e.g., "_compacted.parquet")
//   - isUncompactedInput: function to determine if a file is valid uncompacted input for this tier
//
// Returns true if:
//   - A late-arrival reorg file sits alongside an already-compacted file
//     (folds reorg output — and any crash-induced duplicate of it —
//     IMMEDIATELY, regardless of MinFiles; see Case 4)
//   - No compacted files exist AND enough uncompacted input files are present
//   - Compacted files exist AND enough new uncompacted input files have accumulated
//   - 2+ already-tier-compacted files exist with no new uncompacted input
//     (heals partitions left fragmented by past adaptive-split partial-success
//     bugs; without this, partitions with N _daily.parquet and 0 source files
//     stayed broken forever because nothing triggered re-merge).
func (t *BaseTier) ShouldCompactByFileSuffix(
	files []string,
	compactedSuffix string,
	isUncompactedInput func(string) bool,
) bool {
	var compactedFiles, uncompactedFiles, reorgFiles []string
	for _, f := range files {
		if len(f) >= len(compactedSuffix) && f[len(f)-len(compactedSuffix):] == compactedSuffix {
			compactedFiles = append(compactedFiles, f)
		} else if isUncompactedInput(f) {
			uncompactedFiles = append(uncompactedFiles, f)
			if isReorgFile(f) {
				reorgFiles = append(reorgFiles, f)
			}
		}
	}

	// Case 4 (reorg-aware): a late-arrival reorg file landed in a partition
	// that already holds a sealed (tier-compacted) file. The normal MinFiles
	// gate would leave a small reorg set — including a crash-induced DUPLICATE
	// pair — unfolded until unrelated files happen to accumulate, double-
	// counting queries the whole time. Folding the reorg file into the sealed
	// output now (re-merge + dedup) closes that window. Self-terminating: the
	// compaction output carries no _reorg_ marker, so this can't re-fire on its
	// own result. Checked BEFORE the MinFiles early-return below by design.
	if len(reorgFiles) > 0 && len(compactedFiles) > 0 {
		t.Logger.Debug().
			Int("reorg_files", len(reorgFiles)).
			Int("compacted", len(compactedFiles)).
			Msg("Reorg files on a sealed partition — folding regardless of MinFiles")
		return true
	}

	if len(files) < t.MinFiles {
		return false
	}

	// Case 1: No compacted files yet, and enough uncompacted files
	if len(compactedFiles) == 0 && len(uncompactedFiles) >= t.MinFiles {
		t.Logger.Debug().
			Int("uncompacted_count", len(uncompactedFiles)).
			Msg("First time compaction needed")
		return true
	}

	// Case 2: Has compacted files, but many new uncompacted files accumulated
	if len(compactedFiles) > 0 && len(uncompactedFiles) >= t.MinFiles {
		t.Logger.Debug().
			Int("compacted", len(compactedFiles)).
			Int("uncompacted", len(uncompactedFiles)).
			Msg("Re-compaction needed")
		return true
	}

	// Case 3: Partition is fragmented — 2+ already-tier-compacted outputs
	// with no new input. Re-merge them to converge on one file per partition.
	if len(compactedFiles) >= 2 && len(uncompactedFiles) == 0 {
		t.Logger.Debug().
			Int("compacted", len(compactedFiles)).
			Msg("Re-merging fragmented tier outputs")
		return true
	}

	return false
}
