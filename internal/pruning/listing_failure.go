package pruning

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Listing failures (QR-6).
//
// Partition pruning decides which partition globs a query reads from storage
// listings. When a listing fails or times out (a Hetzner/Ceph slow period, an
// outage, a circuit breaker that is open), the pruner cannot tell an absent
// partition from one it failed to see. Reading a guessed path set is how a
// query used to return a silently partial result (a day dropped because its
// day-level List failed) or a silently EMPTY one (a phantom hour glob makes
// DuckDB raise "No files found", which the handlers answered as empty).
//
// So a failed listing is recorded on the query's context. The API turns it into
// a 503 ("retry") instead of running the query, and the pruner never caches a
// path set built on one. The collector is attached per query
// (WithListingFailure); a pruner call without one still fails safe (the
// whole-table glob, never a partial list).

// ListingFailure records, for one query, that a storage listing partition
// pruning relied on failed or timed out. Safe for concurrent use.
type ListingFailure struct {
	mu     sync.Mutex
	failed bool
	reason string
}

type listingFailureCtxKey struct{}

// WithListingFailure attaches a fresh listing-failure collector to ctx.
func WithListingFailure(ctx context.Context) (context.Context, *ListingFailure) {
	f := &ListingFailure{}
	return context.WithValue(ctx, listingFailureCtxKey{}, f), f
}

// Failed reports whether a listing failed, and the first failure's reason.
func (f *ListingFailure) Failed() (bool, string) {
	if f == nil {
		return false, ""
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failed, f.reason
}

// markListingFailed records a failed listing on ctx's collector, if any. The
// first reason is kept: it names the listing that broke the query.
func markListingFailed(ctx context.Context, prefix string, err error) {
	f, ok := ctx.Value(listingFailureCtxKey{}).(*ListingFailure)
	if !ok || f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.failed {
		f.failed = true
		f.reason = fmt.Sprintf("listing %q: %v", prefix, err)
	}
}

// defaultListTimeout bounds each storage listing partition pruning makes.
const defaultListTimeout = 5 * time.Second

// SetListingTimeout sets the per-listing timeout (<= 0 restores the default).
// Not safe to call concurrently with queries; set it before serving.
func (p *PartitionPruner) SetListingTimeout(d time.Duration) { p.listTimeout = d }

// listingTimeout is the per-listing timeout (defaultListTimeout unless a test
// shortened it).
func (p *PartitionPruner) listingTimeout() time.Duration {
	if p.listTimeout > 0 {
		return p.listTimeout
	}
	return defaultListTimeout
}

// verifiedEmptyGlob is the file pattern of the narrow path the pruner returns
// for a time range that every listing VERIFIED empty. No Arc file is ever
// named like it, so DuckDB answers it with a fast "No files found" after a
// one-prefix listing, and the name tells the query handlers that this
// "No files found" is a proven empty result, not a stale or unverified one.
const verifiedEmptyGlob = "_arc_verified_empty_*.parquet"

// verifiedEmptyPath turns a generated partition glob ({dir}/*.parquet) into
// the proven-empty path under the same directory.
func verifiedEmptyPath(partitionGlob string) string {
	return strings.TrimSuffix(partitionGlob, "*.parquet") + verifiedEmptyGlob
}

// IsVerifiedEmptyPath reports whether path is the proven-empty path the pruner
// returns for a time range every listing verified empty.
func IsVerifiedEmptyPath(path string) bool {
	return strings.HasSuffix(path, "/"+verifiedEmptyGlob)
}

// partitionPathPattern matches a read under a partition directory:
// .../YYYY/MM/DD/*.parquet (daily compacted), .../YYYY/MM/DD/HH/*.parquet, or
// an explicit file in either (file-level time pruning).
var partitionPathPattern = regexp.MustCompile(`/\d{4}/\d{2}/\d{2}(?:/\d{2})?/[^/]*\.parquet$`)

// IsPartitionPath reports whether path is a partition read the pruner emits
// after listing storage (and not a proven-empty path). A "No files found" on
// such a path means a listing said the partition existed and it does not (a
// stale cache after a compaction on another pod, or an unverifiable listing):
// answering it as an empty result would be silently wrong. A whole-table glob
// ({db}/{measurement}/**/*.parquet) matching nothing is a genuinely empty
// measurement and is not a partition path.
func IsPartitionPath(path string) bool {
	if IsVerifiedEmptyPath(path) {
		return false
	}
	return partitionPathPattern.MatchString(path)
}
