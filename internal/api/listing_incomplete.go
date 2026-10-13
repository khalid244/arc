package api

import (
	"context"
	"errors"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/pruning"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// Incomplete storage listings (QR-6).
//
// Partition pruning turns a query's time range into the partition globs that
// exist, from storage listings. Two things used to turn into silently wrong
// answers:
//
//   - a listing that failed or timed out (Hetzner slow periods): a day whose
//     day-level List failed was dropped from the read (partial result), and a
//     parent whose listing failed had every child assumed present, so DuckDB
//     hit "No files found" on a phantom glob;
//   - "No files found" itself, which every handler answered as an EMPTY
//     success. That is right for a measurement with no files at all, and for
//     a range every listing verified empty, but not for a partition a listing
//     said existed (a stale listing cache after a daily compaction on another
//     pod deleted its hour directories, or an unverifiable listing).
//
// Now a failed listing makes the transform fail with listingIncompleteError
// (so do unreadable compaction manifests, P3Q-1: without them the inputs of
// an uploaded compaction cannot be hidden and their rows count twice), and a
// "No files found" on a listed partition (isUnverifiedNoFiles) is
// retried once with fresh listings and, if it persists, refused. Both are
// answered 503 with listingIncompleteMessage, Retry-After and
// arc_query_listing_incomplete_total. A genuine empty is still an empty 200.

// listingIncompleteMessage is the client-facing error of a refused query.
const listingIncompleteMessage = "storage listing failed; result would be incomplete, retry"

// listingIncompleteRetryAfter is the Retry-After (seconds) sent with the 503.
const listingIncompleteRetryAfter = "5"

// listingIncompleteError is a query that cannot be answered completely
// because the storage listing behind its partition paths could not be
// verified.
type listingIncompleteError struct {
	detail string
}

func (e *listingIncompleteError) Error() string {
	if e.detail == "" {
		return listingIncompleteMessage
	}
	return listingIncompleteMessage + " (" + e.detail + ")"
}

// isListingIncomplete reports whether err is (or wraps) a listingIncompleteError.
func isListingIncomplete(err error) bool {
	var e *listingIncompleteError
	return errors.As(err, &e)
}

// listingFailureError returns the transform error for a listing failure
// recorded during a transform, or nil when every listing succeeded.
func listingFailureError(f *pruning.ListingFailure) error {
	if failed, reason := f.Failed(); failed {
		return &listingIncompleteError{detail: sqlutil.SanitizeErrText(reason)}
	}
	return nil
}

// transformErrorStatus is the HTTP status for a SQL transform error: 503 for
// an incomplete listing (transient, retry), 400 for everything else (a name or
// statement the server cannot turn into a read).
func transformErrorStatus(err error) int {
	if isListingIncomplete(err) {
		return fiber.StatusServiceUnavailable
	}
	return fiber.StatusBadRequest
}

// sourceQuery carries the request's own SQL through execution, so a read that
// hit "No files found" on a listed partition can be re-derived from SOURCE
// with fresh listings, and so the caller's own read_parquet paths are told
// apart from the ones the transform emitted.
type sourceQuery struct {
	reqSQL   string
	headerDB string
	// rollupServed: the executed SQL is a rollup cube read (its source
	// fallback is a single-relation read of the source table).
	rollupServed bool
	// noSource: rollup-only mode, which must never touch source.
	noSource bool
}

type sourceQueryCtxKey struct{}

func withSourceQuery(ctx context.Context, reqSQL, headerDB string, rollupServed, noSource bool) context.Context {
	return context.WithValue(ctx, sourceQueryCtxKey{}, &sourceQuery{reqSQL: reqSQL, headerDB: headerDB, rollupServed: rollupServed, noSource: noSource})
}

func sourceQueryFrom(ctx context.Context) *sourceQuery {
	sq, _ := ctx.Value(sourceQueryCtxKey{}).(*sourceQuery)
	return sq
}

// noFilesPattern returns the glob a DuckDB "No files found" error names.
func noFilesPattern(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	const marker = "No files found that match the pattern \""
	msg := err.Error()
	i := strings.Index(msg, marker)
	if i < 0 {
		return "", false
	}
	rest := msg[i+len(marker):]
	j := strings.Index(rest, "\"")
	if j < 0 {
		return "", false
	}
	return rest[:j], true
}

// isUnverifiedNoFiles reports whether err is a "No files found" on a partition
// path the transform read because a listing reported it (pruning.IsPartitionPath):
// answering it as an empty result would drop that partition's rows. A whole-table
// glob matching nothing (a genuinely empty measurement), the pruner's
// proven-empty path, and a path the caller wrote into its own SQL are not.
func (h *QueryHandler) isUnverifiedNoFiles(ctx context.Context, err error) bool {
	pattern, ok := noFilesPattern(err)
	if !ok || !pruning.IsPartitionPath(pattern) {
		return false
	}
	if sq := sourceQueryFrom(ctx); sq != nil && strings.Contains(sq.reqSQL, pattern) {
		return false
	}
	return true
}

// retryUnverifiedNoFiles re-runs a query whose read hit "No files found" on a
// listed partition once, from SOURCE with fresh listings: the usual cause is a
// listing cached on this pod while a compaction on another pod deleted the
// partition, and a fresh listing finds where its rows went. The listing caches
// are dropped either way, so the next query re-lists. Returns the error left:
// nil on success, a listingIncompleteError when the fresh listing fails, or
// the original error when no retry is possible (rollup-only, unknown SQL).
func (h *QueryHandler) retryUnverifiedNoFiles(c *fiber.Ctx, ctx context.Context, err error, run func(string) error, logPrefix string) error {
	if err == nil || ctx.Err() != nil || !h.isUnverifiedNoFiles(ctx, err) {
		return err
	}
	pattern, _ := noFilesPattern(err)
	h.InvalidateCaches()
	sq := sourceQueryFrom(ctx)
	if sq == nil || sq.noSource {
		return err
	}
	h.logger.Warn().Str("pattern", pattern).
		Msg(logPrefix + ": no files on a partition a listing reported (stale or unverified listing); retrying from source with fresh listings")
	fresh, _, terr := h.getTransformedSQL(ctx, sq.reqSQL, sq.headerDB)
	if terr != nil {
		return terr
	}
	c.Set("X-Arc-Listing-Retry", "fresh")
	err = run(fresh)
	return h.retryOnSchemaDrift(c, ctx, fresh, nil, err, run, logPrefix)
}

// refusesAsListingIncomplete reports whether a failed query must be answered
// 503 (see the file comment), dropping the listing caches for a "No files
// found" so the next attempt re-lists. noFiles is the caller's
// isNoFilesFoundError && !missingAnchor verdict for err.
func (h *QueryHandler) refusesAsListingIncomplete(ctx context.Context, err error, noFiles bool) bool {
	if isListingIncomplete(err) {
		return true
	}
	if noFiles && h.isUnverifiedNoFiles(ctx, err) {
		h.InvalidateCaches()
		return true
	}
	return false
}

// listingIncompleteDetail is the client-facing error for a refused query.
func listingIncompleteDetail(err error) string {
	if isListingIncomplete(err) {
		return err.Error()
	}
	if pattern, ok := noFilesPattern(err); ok {
		return (&listingIncompleteError{detail: "no files at listed partition " + pattern}).Error()
	}
	return listingIncompleteMessage
}

// noteListingIncomplete counts and logs one query refused as listing-incomplete,
// and sets Retry-After on the response.
func (h *QueryHandler) noteListingIncomplete(c *fiber.Ctx, err error, sql string) {
	metrics.Get().IncQueryListingIncomplete()
	c.Set("Retry-After", listingIncompleteRetryAfter)
	h.logger.Warn().Err(err).Str("sql", sqlutil.ForLog(sql)).
		Msg("Query refused (503): storage listing could not be verified; result would be incomplete")
}
