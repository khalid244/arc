package compaction

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Batch-size bounds for a single compaction job.
//
// DuckDB can segfault/abort when a single read_parquet() call spans too many
// files, so compaction splits large partitions into batches. Each batch becomes
// an independent job with its own output file, upload, and manifest entry.
//
// Output size scales with INPUT file size, which in turn tracks the ingest
// buffer settings — this is a file-count bound, not a byte bound, so the size
// of a compacted file is not directly controlled here. Deployments on
// constrained or intermittent links (edge/field) may lower
// compaction.max_files_per_batch to get smaller, independently-transferable
// outputs, at the cost of more compaction jobs per partition.
const (
	// DefaultMaxFilesPerBatch is the default and the fallback used when the
	// configured value is out of range.
	DefaultMaxFilesPerBatch = 30

	// MinFilesPerBatch is the smallest usable batch size. It matches the
	// minBatchSize floor in compactFilesAdaptively: a batch below this size is
	// rejected there outright, so allowing it here would fail every batch of
	// every partition rather than merely producing small outputs.
	MinFilesPerBatch = 2

	// MaxAllowedFilesPerBatch caps the configured value. The whole point of
	// batching is to keep read_parquet() below DuckDB's crash threshold, and
	// the adaptive retry in compactFilesAdaptively only halves four times —
	// not enough to rescue an arbitrarily large value.
	MaxAllowedFilesPerBatch = 500
)

// clampFilesPerBatch coerces a configured batch size into the supported range.
// Out-of-range low values (including 0 and the fatal 1) fall back to the
// default; excessive values are capped. Returns the effective size and whether
// the input was adjusted, so callers can log the correction once at startup
// rather than on every partition.
func clampFilesPerBatch(n int) (int, bool) {
	switch {
	case n < MinFilesPerBatch:
		return DefaultMaxFilesPerBatch, true
	case n > MaxAllowedFilesPerBatch:
		return MaxAllowedFilesPerBatch, true
	default:
		return n, false
	}
}

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
	// SyncExempt marks a candidate produced by the hub's spoke-namespace
	// expander (#619): its files are RECEIVED data this node holds as a hub,
	// which the edge-sync delivery gate must not defer (they are never owed
	// upstream). False for every candidate a tier discovers itself.
	SyncExempt bool

	// BatchNumber and TotalBatches are 1-based and always set by
	// SplitCandidateIntoBatches, including for a partition that fits in a
	// single batch (1 of 1). Job and output-file identifiers incorporate
	// BatchNumber to keep sibling batches of one partition distinct, so a zero
	// value here would collide with batch 1. A Candidate constructed directly
	// (not via SplitCandidateIntoBatches) leaves these at 0.
	BatchNumber  int // 1-based batch index within the partition
	TotalBatches int // Total number of batches for this partition
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
			// SyncExempt must ride along, or batch 2+ of a spoke-namespace
			// partition would hit the delivery gate the whole candidate was
			// exempted from (#619).
			SyncExempt:   c.SyncExempt,
			BatchNumber:  i + 1,
			TotalBatches: len(batches),
		})
	}
	return out
}

// SplitCandidateIntoBatches splits a candidate with many files into multiple candidates,
// each with at most maxFilesPerBatch files. This prevents DuckDB segfaults when processing
// thousands of files in a single read_parquet() call.
//
// maxFilesPerBatch is clamped to [MinFilesPerBatch, MaxAllowedFilesPerBatch];
// out-of-range values fall back to DefaultMaxFilesPerBatch (see
// clampFilesPerBatch). Clamping happens here as well as at startup so a caller
// that bypasses the configured manager value cannot produce a zero divisor or a
// batch size that compactFilesAdaptively would reject outright.
//
// Every returned batch owns an independent Files backing array, including the
// single-batch case. Callers may mutate a batch's Files without affecting the
// input candidate or any sibling batch.
func SplitCandidateIntoBatches(c Candidate, maxFilesPerBatch int) []Candidate {
	maxFilesPerBatch, _ = clampFilesPerBatch(maxFilesPerBatch)

	if len(c.Files) <= maxFilesPerBatch {
		single := c
		single.Files = append([]string(nil), c.Files...)
		// A partition that fits in one batch is still batch 1 of 1 — callers
		// derive job and output identifiers from these fields, so leaving them
		// zero here would make the single-batch case indistinguishable from an
		// unbatched candidate.
		single.BatchNumber = 1
		single.TotalBatches = 1
		return []Candidate{single}
	}

	// Calculate number of batches needed
	numBatches := (len(c.Files) + maxFilesPerBatch - 1) / maxFilesPerBatch

	// A trailing remainder smaller than MinFilesPerBatch cannot be compacted:
	// compactFilesAdaptively rejects any batch below that floor on its first
	// attempt, so the remainder would fail every cycle and its files would
	// never compact. Drop the last batch and let the loop's end-clamp fold
	// those files into the (now final) batch instead, which overshoots
	// maxFilesPerBatch by at most MinFilesPerBatch-1 files — far safer than
	// leaving a partition permanently un-compacted at the tail.
	if rem := len(c.Files) % maxFilesPerBatch; rem != 0 && rem < MinFilesPerBatch && numBatches > 1 {
		numBatches--
	}

	batches := make([]Candidate, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		start := i * maxFilesPerBatch
		end := start + maxFilesPerBatch
		// The final batch absorbs every remaining file, so it spans its nominal
		// maxFilesPerBatch plus any remainder left by the adjustment above. A
		// non-final batch always ends within bounds — numBatches is derived
		// from len(Files), so only the last iteration can reach past it — which
		// is why this clamp keys off the index rather than comparing to length.
		if i == numBatches-1 {
			end = len(c.Files)
		}

		filesCopy := make([]string, end-start)
		copy(filesCopy, c.Files[start:end])

		batch := Candidate{
			Database:      c.Database,
			Measurement:   c.Measurement,
			PartitionPath: c.PartitionPath,
			Files:         filesCopy,
			FileCount:     end - start,
			Tier:          c.Tier,
			PartitionTime: c.PartitionTime,
			// Selective constructor: SyncExempt must ride along, or batch
			// 2+ of a spoke-namespace partition would hit the delivery gate
			// the whole candidate was exempted from (#619).
			SyncExempt:   c.SyncExempt,
			BatchNumber:  i + 1,
			TotalBatches: numBatches,
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

	// FindCandidatesFromListing is FindCandidates over an already-fetched
	// listing of the database/measurement/ prefix (as returned by
	// storage.Backend.List for measurementPrefix). It does no I/O, so a
	// caller holding one listing can run several tiers on it instead of
	// listing the store once per tier (#316). Objects outside the prefix
	// are ignored.
	FindCandidatesFromListing(database, measurement string, objects []string) []Candidate

	// ShouldCompact determines if a partition should be compacted based on tier-specific criteria
	ShouldCompact(files []string, partitionTime time.Time) bool

	// IsCompactedFile checks if a file is already a compacted file from this tier
	IsCompactedFile(filename string) bool

	// IsEnabled returns whether this tier is enabled
	IsEnabled() bool

	// GetMinFiles returns the tier's minimum-file threshold. The sync
	// eligibility filter re-applies it after removing undelivered files
	// from a candidate — a single manager-wide value would be wrong
	// (hourly defaults to 10, daily to 12).
	GetMinFiles() int

	// GetStats returns tier statistics
	GetStats() map[string]interface{}
}

// maxOutputBudgeter is implemented by tiers that bound each batch's total
// input size (the fork's hourly/daily *_max_output_size_mb, via BaseTier). It
// is optional rather than part of Tier so a Tier implementation that predates
// the budget still satisfies the interface and simply gets count-only
// batching.
type maxOutputBudgeter interface {
	// GetMaxOutputBytes returns the per-batch input-size budget for this tier.
	// 0 means "no budget" — the Manager will fall back to count-based splitting.
	GetMaxOutputBytes() int64
}

// tierMaxOutputBytes is the tier's per-batch input-size budget, or 0 (no
// budget) for a tier that does not implement maxOutputBudgeter.
func tierMaxOutputBytes(tier Tier) int64 {
	if b, ok := tier.(maxOutputBudgeter); ok {
		return b.GetMaxOutputBytes()
	}
	return 0
}

// measurementPrefix is the storage prefix every object of one measurement
// lives under, and the prefix the tier scanners parse partitions relative to.
func measurementPrefix(database, measurement string) string {
	return database + "/" + measurement + "/"
}

// BaseTier provides common functionality for all compaction tiers
type BaseTier struct {
	StorageBackend storage.Backend
	MinAgeHours    int
	MinFiles       int
	MaxOutputBytes int64
	Enabled        bool

	// Cache is set by Manager after construction; tiers consult it during
	// candidate discovery to skip listing already-compacted partitions.
	// nil when the tier runs outside a Manager (e.g. in tests).
	Cache *PartitionCache

	// Metrics
	totalCompactions    int
	totalFilesCompacted int
	totalBytesSaved     int64

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
		"total_compactions":     t.totalCompactions,
		"total_files_compacted": t.totalFilesCompacted,
		"total_bytes_saved":     t.totalBytesSaved,
		"total_bytes_saved_mb":  float64(t.totalBytesSaved) / 1024 / 1024,
	}
}

// RecordCompaction updates the tier's compaction metrics in a thread-safe manner.
func (t *BaseTier) RecordCompaction(filesCompacted int, bytesSaved int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.totalCompactions++
	t.totalFilesCompacted += filesCompacted
	t.totalBytesSaved += bytesSaved
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
		logical := storage.StripRewriteSuffix(f)
		if len(logical) >= len(compactedSuffix) && logical[len(logical)-len(compactedSuffix):] == compactedSuffix {
			compactedFiles = append(compactedFiles, f)
		} else if isUncompactedInput(logical) {
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

// listObjects lists every object of one measurement — the input both
// FindCandidates and, via the manager, FindCandidatesFromListing work from.
func (b *BaseTier) listObjects(ctx context.Context, database, measurement string) ([]string, error) {
	objects, err := b.StorageBackend.List(ctx, measurementPrefix(database, measurement))
	if err != nil {
		return nil, err
	}
	b.Logger.Debug().
		Str("database", database).
		Str("measurement", measurement).
		Int("object_count", len(objects)).
		Msg("Listed storage objects")
	return objects, nil
}
