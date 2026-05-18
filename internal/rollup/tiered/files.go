package tiered

import (
	"context"
	"fmt"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// FileIndex is the source of truth for which parquet files exist for a given
// (tier, variant). Implementations derive this from S3 LIST + ParseVariantPath
// rather than from a stored manifest.
type FileIndex interface {
	// FilesForTierVariant returns all file paths for (tier, variant).
	FilesForTierVariant(ctx context.Context, tier, variant string) ([]string, error)

	// FilesForTierVariantWindow returns paths overlapping [lo, hi).
	// An entry overlaps when bucketLo < hi AND bucketHi > lo.
	FilesForTierVariantWindow(ctx context.Context, tier, variant string, lo, hi time.Time) ([]string, error)

	// EarliestBucketLo returns the earliest bucketLo across all files for
	// (tier, variant), or zero+false when none.
	EarliestBucketLo(ctx context.Context, tier, variant string) (time.Time, bool, error)

	// Watermark returns max(bucketHi) across all files for (tier, variant),
	// or zero+false when none.
	Watermark(ctx context.Context, tier, variant string) (time.Time, bool, error)
}

// S3FileIndex implements FileIndex by listing the storage backend.
type S3FileIndex struct {
	Backend storage.Backend
	Table   string // "db.table"
}

func (idx *S3FileIndex) FilesForTierVariant(ctx context.Context, tier, variant string) ([]string, error) {
	prefix := fmt.Sprintf("_arc/rollup/%s/%s/", tablePath(idx.Table), tier)
	keys, err := idx.Backend.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, k := range keys {
		_, t, v, _, _, ok := ParseVariantPath(k)
		if ok && t == tier && v == variant {
			out = append(out, k)
		}
	}
	return out, nil
}

func (idx *S3FileIndex) FilesForTierVariantWindow(ctx context.Context, tier, variant string, lo, hi time.Time) ([]string, error) {
	all, err := idx.FilesForTierVariant(ctx, tier, variant)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, k := range all {
		_, _, _, elo, ehi, ok := ParseVariantPath(k)
		if !ok {
			continue
		}
		if elo.Before(hi) && ehi.After(lo) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (idx *S3FileIndex) EarliestBucketLo(ctx context.Context, tier, variant string) (time.Time, bool, error) {
	all, err := idx.FilesForTierVariant(ctx, tier, variant)
	if err != nil {
		return time.Time{}, false, err
	}
	var out time.Time
	found := false
	for _, k := range all {
		_, _, _, lo, _, ok := ParseVariantPath(k)
		if !ok {
			continue
		}
		if !found || lo.Before(out) {
			out = lo
			found = true
		}
	}
	return out, found, nil
}

func (idx *S3FileIndex) Watermark(ctx context.Context, tier, variant string) (time.Time, bool, error) {
	all, err := idx.FilesForTierVariant(ctx, tier, variant)
	if err != nil {
		return time.Time{}, false, err
	}
	var out time.Time
	found := false
	for _, k := range all {
		_, _, _, _, hi, ok := ParseVariantPath(k)
		if !ok {
			continue
		}
		if !found || hi.After(out) {
			out = hi
			found = true
		}
	}
	return out, found, nil
}

// cachedFileIndex wraps a FileIndex and caches FilesForTierVariant results so
// that multiple plans processing the same (tier, variant, bucket) window issue
// only one S3 LIST per tier prefix instead of one per plan.
type cachedFileIndex struct {
	inner FileIndex
	cache map[string][]string // key: "tier/variant"
}

func (c *cachedFileIndex) FilesForTierVariant(ctx context.Context, tier, variant string) ([]string, error) {
	if c.cache == nil {
		c.cache = make(map[string][]string)
	}
	key := tier + "/" + variant
	if paths, ok := c.cache[key]; ok {
		return paths, nil
	}
	paths, err := c.inner.FilesForTierVariant(ctx, tier, variant)
	if err != nil {
		return nil, err
	}
	c.cache[key] = paths
	return paths, nil
}

func (c *cachedFileIndex) FilesForTierVariantWindow(ctx context.Context, tier, variant string, lo, hi time.Time) ([]string, error) {
	all, err := c.FilesForTierVariant(ctx, tier, variant)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, k := range all {
		_, _, _, elo, ehi, ok := ParseVariantPath(k)
		if !ok {
			continue
		}
		if elo.Before(hi) && ehi.After(lo) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (c *cachedFileIndex) EarliestBucketLo(ctx context.Context, tier, variant string) (time.Time, bool, error) {
	all, err := c.FilesForTierVariant(ctx, tier, variant)
	if err != nil {
		return time.Time{}, false, err
	}
	var out time.Time
	found := false
	for _, k := range all {
		_, _, _, lo, _, ok := ParseVariantPath(k)
		if !ok {
			continue
		}
		if !found || lo.Before(out) {
			out = lo
			found = true
		}
	}
	return out, found, nil
}

func (c *cachedFileIndex) Watermark(ctx context.Context, tier, variant string) (time.Time, bool, error) {
	all, err := c.FilesForTierVariant(ctx, tier, variant)
	if err != nil {
		return time.Time{}, false, err
	}
	var out time.Time
	found := false
	for _, k := range all {
		_, _, _, _, hi, ok := ParseVariantPath(k)
		if !ok {
			continue
		}
		if !found || hi.After(out) {
			out = hi
			found = true
		}
	}
	return out, found, nil
}

// MemoryFileIndex implements FileIndex over an in-memory slice of paths.
// Used in unit tests.
type MemoryFileIndex struct {
	Paths []string
}

func (idx *MemoryFileIndex) FilesForTierVariant(ctx context.Context, tier, variant string) ([]string, error) {
	var out []string
	for _, p := range idx.Paths {
		_, t, v, _, _, ok := ParseVariantPath(p)
		if ok && t == tier && v == variant {
			out = append(out, p)
		}
	}
	return out, nil
}

func (idx *MemoryFileIndex) FilesForTierVariantWindow(ctx context.Context, tier, variant string, lo, hi time.Time) ([]string, error) {
	all, err := idx.FilesForTierVariant(ctx, tier, variant)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, k := range all {
		_, _, _, elo, ehi, ok := ParseVariantPath(k)
		if !ok {
			continue
		}
		if elo.Before(hi) && ehi.After(lo) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (idx *MemoryFileIndex) EarliestBucketLo(ctx context.Context, tier, variant string) (time.Time, bool, error) {
	all, err := idx.FilesForTierVariant(ctx, tier, variant)
	if err != nil {
		return time.Time{}, false, err
	}
	var out time.Time
	found := false
	for _, k := range all {
		_, _, _, lo, _, ok := ParseVariantPath(k)
		if !ok {
			continue
		}
		if !found || lo.Before(out) {
			out = lo
			found = true
		}
	}
	return out, found, nil
}

func (idx *MemoryFileIndex) Watermark(ctx context.Context, tier, variant string) (time.Time, bool, error) {
	all, err := idx.FilesForTierVariant(ctx, tier, variant)
	if err != nil {
		return time.Time{}, false, err
	}
	var out time.Time
	found := false
	for _, k := range all {
		_, _, _, _, hi, ok := ParseVariantPath(k)
		if !ok {
			continue
		}
		if !found || hi.After(out) {
			out = hi
			found = true
		}
	}
	return out, found, nil
}
