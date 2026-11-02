package pruning

// legacyFilterExistingRemotePathsOpts is filterExistingRemotePathsOpts as it
// was at 294496c2 (the image the prod query pods run), copied verbatim apart
// from its name. It is the reference the new implementation is checked
// against (remote_listing_test.go): same paths, same order, same verified /
// listing-failed verdicts, same "answered from the cache" signal.

import (
	"context"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

func (p *PartitionPruner) legacyFilterExistingRemotePathsOpts(ctx context.Context, paths []string, basePath string, maxAge time.Duration) ([]string, bool, int) {
	verified := true
	hits := 0
	if p.storage == nil {
		// No storage backend configured - return all paths and let DuckDB handle errors
		p.logger.Debug().Msg("No storage backend configured, skipping remote path filtering")
		return paths, false, 0
	}

	lister, ok := p.storage.(storage.DirectoryLister)
	if !ok {
		// Storage backend doesn't support directory listing
		p.logger.Debug().Msg("Storage backend doesn't support directory listing, skipping filtering")
		return paths, false, 0
	}

	// Extract unique parent directories to check
	// Path format: s3://bucket/db/measurement/2025/12/17/16/*.parquet
	// We need to check if the hour directory exists
	dirsToCheck := make(map[string]struct{})
	pathToDir := make(map[string]string)

	for _, path := range paths {
		// Remove the glob suffix to get the directory
		dir := strings.TrimSuffix(path, "/*.parquet")
		dirsToCheck[dir] = struct{}{}
		pathToDir[path] = dir
	}

	// Group directories by parent to minimize ListDirectories calls
	// This reduces API calls from O(hours) to O(days) for time-range queries
	type dirInfo struct {
		parent string
		target string
	}
	dirDetails := make(map[string]dirInfo) // full dir -> {parent, target}
	uniqueParents := make(map[string]bool)

	for dir := range dirsToCheck {
		lastSlash := strings.LastIndex(dir, "/")
		if lastSlash == -1 {
			continue
		}
		parentDir := dir[:lastSlash+1] // Include trailing slash
		targetName := dir[lastSlash+1:]
		dirDetails[dir] = dirInfo{parent: parentDir, target: targetName}
		uniqueParents[parentDir] = true
	}

	// Cache of parent -> set of existing child directories
	parentChildren := make(map[string]map[string]bool)

	// Fetch child listings for each unique parent (one API call per parent)
	for parentDir := range uniqueParents {
		cacheKey := "remote:parent:" + parentDir

		if cached, ok := p.globCache.getFresh(cacheKey, maxAge); ok {
			hits++
			// Cache hit - rebuild set from cached slice
			childSet := make(map[string]bool, len(cached))
			for _, child := range cached {
				childSet[child] = true
			}
			parentChildren[parentDir] = childSet
			p.logger.Debug().Str("parent", parentDir).Int("children", len(childSet)).Msg("Using cached parent listing")
			continue
		}

		// Cache miss - call ListDirectories once for this parent
		storagePrefix, ok := p.extractStoragePrefix(parentDir, basePath)
		if !ok {
			// Listing with an unresolvable prefix would scan the wrong key
			// space (or, with "", the entire bucket) and report every child
			// absent. Assume all exist, the same fail-open the listing-error
			// branch below takes.
			parentChildren[parentDir] = nil
			verified = false
			continue
		}
		listCtx, cancel := context.WithTimeout(ctx, p.listingTimeout())
		subdirs, err := lister.ListDirectories(listCtx, storagePrefix)
		cancel()

		if err != nil {
			// The existence of this parent's partitions is now unknown, so no
			// path set built here can be trusted: a guessed set either drops
			// rows or makes DuckDB fail "No files found" on a phantom glob,
			// and both used to reach the client as a silent partial or empty
			// result (QR-6). Record it for the query (the API answers 503) and
			// stop listing: every further call would only add another timeout
			// to a query that cannot be answered. Callers treat (nil, false)
			// as "unknown" and fall back to the whole-table glob.
			p.logger.Debug().Err(err).Str("prefix", storagePrefix).Msg("Partition listing failed; existence unverified")
			markListingFailed(ctx, storagePrefix, err)
			return nil, false, hits
		}

		// Build set of existing child directories
		childSet := make(map[string]bool, len(subdirs))
		childNames := make([]string, 0, len(subdirs))
		for _, subdir := range subdirs {
			subdir = strings.TrimSuffix(subdir, "/")
			if idx := strings.LastIndex(subdir, "/"); idx != -1 {
				subdir = subdir[idx+1:]
			}
			childSet[subdir] = true
			childNames = append(childNames, subdir)
		}

		// Cache by parent directory
		p.globCache.set(cacheKey, childNames)
		parentChildren[parentDir] = childSet
		p.logger.Debug().Str("parent", parentDir).Int("children", len(childNames)).Msg("Cached parent listing")
	}

	// Check which directories exist
	existingDirs := make(map[string]bool)
	for dir, info := range dirDetails {
		childSet := parentChildren[info.parent]
		if childSet == nil {
			// Parent listing failed - assume directory exists
			existingDirs[dir] = true
			p.logger.Debug().Str("dir", dir).Msg("Remote directory exists (assumed)")
		} else if childSet[info.target] {
			existingDirs[dir] = true
			p.logger.Debug().Str("dir", dir).Msg("Remote directory exists")
		} else {
			p.logger.Debug().Str("dir", dir).Msg("Remote directory does not exist")
		}
	}

	// Filter paths to only include existing directories/files
	existingPaths := []string{}
	for _, path := range paths {
		dir := pathToDir[path]
		if !existingDirs[dir] {
			continue
		}

		// For day-level paths (5 segments: db/measurement/year/month/day),
		// verify .parquet files exist directly at that level (not in subdirs).
		// This prevents "No files found" errors when daily compaction hasn't run yet.
		prefix, ok := p.extractStoragePrefix(dir+"/", basePath)
		if !ok {
			// Cannot resolve: keep the path rather than judging it empty.
			existingPaths = append(existingPaths, path)
			verified = false
			continue
		}
		if parts := strings.Split(strings.Trim(prefix, "/"), "/"); len(parts) == 5 {
			// Check cache first for day-level file existence
			cacheKey := "remote:dayfiles:" + prefix
			var hasFiles bool
			if cached, ok := p.globCache.getFresh(cacheKey, maxAge); ok {
				hits++
				hasFiles = len(cached) > 0
				p.logger.Debug().Str("prefix", prefix).Bool("has_files", hasFiles).Msg("Using cached day-level file check")
			} else {
				// Cache miss - make API call
				listCtx, cancel := context.WithTimeout(ctx, p.listingTimeout())
				files, err := p.storage.List(listCtx, prefix)
				cancel()
				if err != nil {
					// This used to DROP the day silently: a daily-compacted
					// day vanished from the result. Unknown, as above.
					p.logger.Debug().Err(err).Str("prefix", prefix).Msg("Day-level partition listing failed; existence unverified")
					markListingFailed(ctx, prefix, err)
					return nil, false, hits
				}
				// Find direct parquet files (not in subdirs)
				var directFiles []string
				for _, f := range files {
					remaining := strings.TrimPrefix(f, prefix)
					if remaining != "" && !strings.Contains(remaining, "/") && strings.HasSuffix(remaining, ".parquet") {
						directFiles = append(directFiles, remaining)
					}
				}
				// Cache the result
				p.globCache.set(cacheKey, directFiles)
				hasFiles = len(directFiles) > 0
				p.logger.Debug().Str("prefix", prefix).Int("files", len(directFiles)).Msg("Cached day-level file check")
			}
			if !hasFiles {
				p.logger.Debug().Str("path", path).Msg("Day-level path has no direct parquet files, skipping")
				continue
			}
		}

		existingPaths = append(existingPaths, path)
	}

	p.logger.Info().
		Int("original_count", len(paths)).
		Int("existing_count", len(existingPaths)).
		Msg("Filtered existing remote paths")

	return existingPaths, verified, hits
}
