package api

import (
	"context"
	"regexp"
	"strings"

	"github.com/gofiber/fiber/v2"
)

// missingObjectRe matches an object URL named in a DuckDB/httpfs error.
var missingObjectRe = regexp.MustCompile(`(?:s3a?|gcs?|r2|az|azure|abfss|https?)://[^\s'"()]+?\.parquet`)

// sqlLiteralRe matches a single-quoted SQL string literal (no escaped quotes:
// storage paths never contain one).
var sqlLiteralRe = regexp.MustCompile(`'([^']*)'`)

// missingRawSourceObject returns the raw SOURCE parquet object a query's err
// reports missing (HTTP 404 / NoSuchKey), or "" when err is not that. A cube
// object (any path under _arc/) is never returned: a missing cube file is a
// stale manifest pointer, which only the source fallback recovers.
//
// The usual cause is compaction: it deletes its input files as soon as the
// compacted file lands, so a glob DuckDB expanded moments earlier (or a
// listing cache_httpfs still holds) can name a file that no longer exists. Re-expanding the glob finds the
// compacted file instead (same partition), so a retry of the same query is
// complete.
func missingRawSourceObject(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if !strings.Contains(s, "404") && !strings.Contains(s, "NoSuchKey") && !strings.Contains(s, "Not Found") {
		return ""
	}
	objs := missingObjectRe.FindAllString(s, -1)
	if len(objs) == 0 {
		return ""
	}
	for _, o := range objs {
		if strings.Contains(o, "/_arc/") {
			return ""
		}
	}
	return objs[0]
}

// tailGlobsCovering returns the glob literals in sql that can expand to obj,
// matched on the literal's directory prefix before its first wildcard. Paths
// are compared without scheme and host, because httpfs reports s3:// reads
// as http(s)://endpoint/bucket/... URLs (path style) or, by default, as
// http(s)://bucket.endpoint/... URLs (virtual-hosted).
func tailGlobsCovering(sql, obj string) []string {
	objKey := objectKey(obj)
	host := urlHost(obj)
	var out []string
	for _, m := range sqlLiteralRe.FindAllStringSubmatch(sql, -1) {
		lit := m[1]
		i := strings.IndexAny(lit, "*?[")
		if i < 0 {
			continue
		}
		prefix := objectKey(lit[:i])
		if prefix == "" {
			continue
		}
		if strings.HasPrefix(objKey, prefix) || virtualHostedUnder(host, objKey, prefix) {
			out = append(out, lit)
		}
	}
	return out
}

// virtualHostedUnder reports whether the key of a virtual-hosted URL on host
// lies under prefix ("bucket/dir/..."): the host starts with "bucket." and the
// key with the rest of prefix.
func virtualHostedUnder(host, key, prefix string) bool {
	bucket, rest, ok := strings.Cut(prefix, "/")
	return ok && host != "" && strings.HasPrefix(host, bucket+".") && strings.HasPrefix(key, rest)
}

// urlHost returns the host of an http(s) URL, or "".
func urlHost(p string) string {
	for _, scheme := range []string{"http://", "https://"} {
		if rest, ok := strings.CutPrefix(p, scheme); ok {
			host, _, _ := strings.Cut(rest, "/")
			return host
		}
	}
	return ""
}

// objectKey strips the scheme and, for http(s) URLs, the host, leaving
// "bucket/key" (or a local path).
func objectKey(p string) string {
	if i := strings.Index(p, "://"); i >= 0 {
		scheme := p[:i]
		p = p[i+3:]
		if scheme == "http" || scheme == "https" {
			if j := strings.IndexByte(p, '/'); j >= 0 {
				p = p[j+1:]
			} else {
				p = ""
			}
		}
	}
	return p
}

// retryMissingRawTailFile re-runs a query ONCE, unchanged, when it failed
// because a raw source file it read vanished (see missingRawSourceObject):
// the fresh tail of a rollup-served query, or any raw query (rollup nil: no
// cube serves it, X-Arc-No-Rollup, the router declined). Only that object's
// and its covering globs' cache_httpfs entries are dropped first, so DuckDB
// re-lists just those partitions; every other cache stays warm. Without the
// retry a raw query fails on every run until the cached listing expires
// (review 2026-11-10 Q3). Returns the retry's error, or err unchanged when
// the retry does not apply. A failed retry then takes the existing path
// (source fallback, or 503 when its listing fails), so a result is never
// missing rows.
func (h *QueryHandler) retryMissingRawTailFile(c *fiber.Ctx, ctx context.Context, convertedSQL string, rollup *rollupExec, err error, run func(string) error, logPrefix string) error {
	if (rollup != nil && rollup.rawTailRetried) || err == nil || ctx.Err() != nil {
		return err
	}
	obj := missingRawSourceObject(err)
	if obj == "" {
		return err
	}
	globs := tailGlobsCovering(convertedSQL, obj)
	if h.db != nil {
		h.db.ClearHTTPCacheForPaths(append([]string{obj}, globs...)...)
	}
	if rollup == nil {
		h.logger.Warn().Err(err).Str("object", obj).Strs("globs", globs).
			Msg(logPrefix + ": raw file vanished (compaction); retrying the same query once with that partition re-listed")
		c.Set("X-Arc-Retry", "raw-file-missing")
		return run(convertedSQL)
	}
	rollup.rawTailRetried = true
	h.logger.Warn().Err(err).Str("object", obj).Strs("globs", globs).
		Msg(logPrefix + ": rollup fresh-tail raw file vanished (compaction); retrying the same query once with that partition re-listed")
	c.Set("X-Arc-Rollup-Retry", "raw-file-missing")
	return run(convertedSQL)
}
