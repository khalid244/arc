package api

import (
	"context"
	"strings"
	"sync"
	"time"
)

// Glob prewarm (query.glob_prewarm_parallelism).
//
// A converted query names its partitions as glob literals, one per day or
// hour folder, and DuckDB expands them one LIST after another while it binds
// the query. With the data blocks cached that is nearly all of a query's
// time on a slow object store: measured 2026-11-16 on real downloads data
// behind a 300 ms LIST, a warm 30-day query took 7.4 s for 25 LISTs and
// 0.11 s once they were cached, a 60-day one 35 s against 0.25 s.
//
// prewarmGlobs lists the same globs ahead of the query, several at once,
// through DuckDB's glob() so the answers land in cache_httpfs's glob cache.
// The query then expands every glob from that cache. Nothing else changes:
// the SQL, the globs, the glob TTL (query.s3_glob_cache_ttl_seconds) and so
// the age of the listing a query may use are the same as without it, and so
// are the compaction input exclusion and the raw-file retry built on them.

// remoteGlobSchemes are the object-store URL schemes whose globs cost a LIST.
var remoteGlobSchemes = []string{"s3://", "azure://", "az://", "gs://", "gcs://", "r2://"}

// remoteGlobsIn returns the distinct remote glob literals of sql, in order.
func remoteGlobsIn(sql string) []string {
	var out []string
	seen := make(map[string]bool)
	for _, m := range sqlLiteralRe.FindAllStringSubmatch(sql, -1) {
		lit := m[1]
		if seen[lit] || !strings.ContainsAny(lit, "*?[") {
			continue
		}
		for _, scheme := range remoteGlobSchemes {
			if strings.HasPrefix(lit, scheme) {
				seen[lit] = true
				out = append(out, lit)
				break
			}
		}
	}
	return out
}

// SetGlobPrewarm sets how many partition globs one query lists at once ahead
// of itself (0 = off) and sharedLimit, how many LISTs all queries together
// may run beyond one each (0 = parallelism). A query always lists one
// glob at a time by itself, as it did before prewarm; the rest of its
// parallelism comes out of the shared limit, so queries that arrive together
// cannot multiply the burst on the object store or fill the DuckDB pool.
func (h *QueryHandler) SetGlobPrewarm(parallelism, sharedLimit int) {
	h.globPrewarmParallelism = max(parallelism, 0)
	h.globPrewarmShared = nil
	if h.globPrewarmParallelism == 0 {
		return
	}
	if sharedLimit <= 0 {
		sharedLimit = h.globPrewarmParallelism
	}
	h.globPrewarmShared = make(chan struct{}, sharedLimit)
}

// prewarmGlobs lists sql's remote globs through DuckDB, at most
// globPrewarmParallelism at a time. A single glob gains nothing and is left
// to the query. Errors are dropped: the query lists that glob itself and
// reports the failure the way it always did.
func (h *QueryHandler) prewarmGlobs(ctx context.Context, sql string) {
	if h.globPrewarmParallelism <= 0 || h.db == nil {
		return
	}
	globs := remoteGlobsIn(sql)
	if len(globs) < 2 {
		return
	}
	start := time.Now()
	list := func(g string) {
		var n int64
		_ = h.db.DB().QueryRowContext(ctx, "SELECT count(*) FROM glob('"+g+"')").Scan(&n)
	}
	work := make(chan string)
	handedOut := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for g := range work {
			list(g)
		}
	}()
	// The extra listers take a shared slot before a glob, never the other way
	// round, so a glob is never held waiting on other queries.
	for i := 1; i < min(h.globPrewarmParallelism, len(globs)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case h.globPrewarmShared <- struct{}{}:
				case <-handedOut:
					return
				case <-ctx.Done():
					return
				}
				g, ok := <-work
				if ok {
					list(g)
				}
				<-h.globPrewarmShared
				if !ok {
					return
				}
			}
		}()
	}
feed:
	for _, g := range globs {
		select {
		case work <- g:
		case <-ctx.Done():
			break feed
		}
	}
	close(handedOut)
	close(work)
	wg.Wait()
	h.logger.Debug().Int("globs", len(globs)).Int("parallelism", h.globPrewarmParallelism).
		Dur("elapsed", time.Since(start)).Msg("Listed the query's partition globs ahead")
}
