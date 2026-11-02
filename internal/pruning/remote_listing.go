package pruning

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Remote partition existence (S3/Azure).
//
// filterExistingRemotePathsOpts decides which generated partition globs
// exist. It used to make one delimiter listing (ListDirectories) per parent
// directory -- one per covered day for the hour paths and one per month for
// the day paths -- plus one recursive List per existing day for its
// day-level files, all in sequence: about two calls per day, so a 31-day
// range made ~63 sequential LISTs. Hetzner Object Storage answers a LIST in
// one to several seconds, so pruning alone cost tens of seconds and every
// extra call was one more chance to time out (a 503, QR-6).
//
// One recursive List of a day directory (db/meas/YYYY/MM/DD/) answers both
// questions for that day: a key under DD/HH/ shows that hour folder exists,
// and a key DD/x.parquet is a day-level (daily compacted) file. The old
// day-level List already WAS that recursive listing, so the per-day
// ListDirectories added nothing it did not already return.
//
// The listing unit is the day, or the whole month when the range needs at
// least monthListMinDays days of it or the per-day listings would not fit in
// one parallel round (planRemoteListings):
//
//   - never above the month: the common prefix of a range that crosses a
//     month or a year boundary is db/meas/YYYY/ or db/meas/, and a recursive
//     listing there walks every key of the year or of the table;
//   - a compacted day is ~20 keys (prod posthog/events: ~3 daily files and
//     ~18 reorg files), so a month is ~650 keys, one 1000-key page: a month
//     costs what one day costs;
//   - below monthListMinDays days, per-day listings all run in a single
//     parallel round anyway, and they do not read the rest of a month whose
//     current day may still hold uncompacted raw files.
//
// At most remoteListParallelism listings are in flight. One day is one LIST,
// a 7- or 31-day range one or two (a few more when only 1-3 days fall in the
// second month), 90 days three or four; all in one parallel round. The old
// code needed 3, 15, 63 and 183 sequential calls for 1, 7, 31 and 90 days.
//
// What is returned is unchanged: the same paths, in input order, with the
// same verified / listing-failed semantics. One edge differs, in the safe
// direction: List never returns a key the backend refuses (a directory
// marker "x/", a ".part" staging name), so an hour folder holding ONLY such a
// key is now absent; the delimiter listing reported it present and DuckDB
// then failed the query with "No files found" on its empty glob.

// remoteListParallelism bounds the listings one pruning pass has in flight.
// Hetzner gets slower above ~12 concurrent LIST streams, and several queries
// (dashboard panels) prune at once.
const remoteListParallelism = 4

// monthListMinDays is how many days of one month a range must need before
// the whole month is listed in one recursive List instead of day by day.
// Below it, the per-day listings fit in one parallel round.
const monthListMinDays = remoteListParallelism + 1

// remoteListingCachePrefix keys a directory's listing summary in globCache.
const remoteListingCachePrefix = "remote:tree:"

// directParquetEntry marks, in a cached summary, that the directory holds a
// .parquet file directly. It cannot clash with a child entry, which always
// ends in "/".
const directParquetEntry = "*.parquet"

// dirSummary is what a recursive listing says about one directory: the
// names of its child directories (as a delimiter listing would report them)
// and whether it holds .parquet files directly.
type dirSummary struct {
	children map[string]bool
	direct   bool
}

// add records one key, given relative to the directory.
func (d *dirSummary) add(rem string) {
	if i := strings.IndexByte(rem, '/'); i >= 0 {
		// The child a delimiter listing would report. S3 ListDirectories
		// drops empty and dot names; so does this.
		if child := rem[:i]; child != "" && child[0] != '.' {
			if d.children == nil {
				d.children = make(map[string]bool)
			}
			d.children[child] = true
		}
		return
	}
	if rem != "" && strings.HasSuffix(rem, ".parquet") {
		d.direct = true
	}
}

func (d *dirSummary) entries() []string {
	out := make([]string, 0, len(d.children)+1)
	if d.direct {
		out = append(out, directParquetEntry)
	}
	for c := range d.children {
		out = append(out, c+"/")
	}
	return out
}

func summaryFromEntries(entries []string) *dirSummary {
	d := &dirSummary{}
	for _, e := range entries {
		if e == directParquetEntry {
			d.direct = true
		} else if name, ok := strings.CutSuffix(e, "/"); ok {
			if d.children == nil {
				d.children = make(map[string]bool)
			}
			d.children[name] = true
		}
	}
	return d
}

// remotePathCheck is how one input path is judged.
type remotePathCheck struct {
	kind   remoteCheckKind
	anchor string // directory whose summary decides (relative, trailing "/")
	target string // last segment of the path's directory
}

type remoteCheckKind uint8

const (
	checkDrop     remoteCheckKind = iota // no parent directory: never exists
	checkKeep                            // unresolvable prefix: kept, unverified
	checkChild                           // target must be a child dir of anchor
	checkDayFiles                        // anchor (the day) must hold .parquet directly
)

// remoteListUnit is one listing and the directories it answers: a recursive
// List of a day or a month directory, or (delimited) a ListDirectories of any
// other parent, which GeneratePartitionPaths never produces; it keeps a
// recursive listing from ever running above a month.
type remoteListUnit struct {
	prefix    string
	anchors   []string
	month     bool
	delimited bool
}

func (p *PartitionPruner) filterExistingRemotePathsOpts(ctx context.Context, paths []string, basePath string, maxAge time.Duration) ([]string, bool, int) {
	if p.storage == nil {
		// No storage backend configured - return all paths and let DuckDB handle errors
		p.logger.Debug().Msg("No storage backend configured, skipping remote path filtering")
		return paths, false, 0
	}
	if _, ok := p.storage.(storage.DirectoryLister); !ok {
		// Storage backend doesn't support directory listing
		p.logger.Debug().Msg("Storage backend doesn't support directory listing, skipping filtering")
		return paths, false, 0
	}
	start := time.Now()
	verified := true
	hits := 0

	// Path format: s3://bucket/db/measurement/2025/12/17/16/*.parquet (hour)
	// or s3://bucket/db/measurement/2025/12/17/*.parquet (day). An hour path
	// exists when its folder is a child of the day; a day path when the day
	// holds .parquet files directly. Either way the day's summary decides.
	checks := make([]remotePathCheck, len(paths))
	summaries := make(map[string]*dirSummary)
	for i, path := range paths {
		dir := strings.TrimSuffix(path, "/*.parquet")
		lastSlash := strings.LastIndex(dir, "/")
		if lastSlash == -1 {
			checks[i].kind = checkDrop
			continue
		}
		parentRel, ok := p.extractStoragePrefix(dir[:lastSlash+1], basePath)
		if !ok {
			// Listing with an unresolvable prefix would scan the wrong key
			// space (or, with "", the entire bucket) and report every child
			// absent. Keep the path, unverified.
			checks[i].kind = checkKeep
			verified = false
			continue
		}
		target := dir[lastSlash+1:]
		dirRel := parentRel + target + "/"
		if isDayDir(dirRel) {
			// db/measurement/year/month/day: .parquet files must exist
			// directly at that level (not only in hour subdirs), or DuckDB
			// fails "No files found" before daily compaction has run. The
			// day being a child of its month follows from that.
			if target == "" || target[0] == '.' {
				checks[i].kind = checkDrop
				continue
			}
			checks[i] = remotePathCheck{kind: checkDayFiles, anchor: dirRel, target: target}
		} else {
			checks[i] = remotePathCheck{kind: checkChild, anchor: parentRel, target: target}
		}
		summaries[checks[i].anchor] = nil
	}

	var missing []string
	for anchor := range summaries {
		if cached, ok := p.globCache.getFresh(remoteListingCachePrefix+anchor, maxAge); ok {
			hits++
			summaries[anchor] = summaryFromEntries(cached)
			continue
		}
		missing = append(missing, anchor)
	}

	units := planRemoteListings(missing)
	if len(units) > 0 {
		if !p.runRemoteListings(ctx, units, summaries) {
			// The existence of some partitions is now unknown, so no path
			// set built here can be trusted: a guessed set either drops rows
			// or makes DuckDB fail "No files found" on a phantom glob, and
			// both used to reach the client as a silent partial or empty
			// result (QR-6). The failure is recorded on ctx (the API answers
			// 503). Callers treat (nil, false) as "unknown" and fall back to
			// the whole-table glob.
			return nil, false, hits
		}
	}

	existingPaths := []string{}
	for i, path := range paths {
		c := checks[i]
		switch c.kind {
		case checkDrop:
			continue
		case checkKeep:
			existingPaths = append(existingPaths, path)
			continue
		}
		s := summaries[c.anchor]
		if s == nil {
			continue
		}
		if (c.kind == checkDayFiles && s.direct) || (c.kind == checkChild && s.children[c.target]) {
			existingPaths = append(existingPaths, path)
		}
	}

	p.logger.Info().
		Int("original_count", len(paths)).
		Int("existing_count", len(existingPaths)).
		Int("list_calls", len(units)).
		Int("cache_hits", hits).
		Dur("elapsed", time.Since(start)).
		Msg("Filtered existing remote paths")

	return existingPaths, verified, hits
}

// isDayDir reports whether a relative directory prefix has the five
// segments of db/measurement/year/month/day (counted the way the day-level
// check always counted them).
func isDayDir(dir string) bool {
	return len(strings.Split(strings.Trim(dir, "/"), "/")) == 5
}

// parentDir returns the parent of a directory prefix
// ("db/m/2026/10/07/" -> "db/m/2026/10/").
func parentDir(dir string) string {
	trimmed := strings.TrimSuffix(dir, "/")
	return trimmed[:strings.LastIndex(trimmed, "/")+1]
}

// planRemoteListings groups the directories that need a listing into list
// units. A day directory is listed with its whole month when the range needs
// at least monthListMinDays days of that month; otherwise on its own, unless
// that leaves more than remoteListParallelism units (more than one parallel
// round), in which case the month with the most per-day units is listed whole
// instead, until the units fit in one round or no month has two days left.
// Any other directory (never produced by GeneratePartitionPaths) gets the
// delimiter listing the pruner always made for it.
func planRemoteListings(anchors []string) []remoteListUnit {
	var units []remoteListUnit
	byMonth := make(map[string][]string)
	for _, a := range anchors {
		if isDayDir(a) {
			m := parentDir(a)
			byMonth[m] = append(byMonth[m], a)
			continue
		}
		units = append(units, remoteListUnit{prefix: a, anchors: []string{a}, delimited: true})
	}
	months := make([]string, 0, len(byMonth))
	for m := range byMonth {
		months = append(months, m)
	}
	sort.Strings(months)
	whole := make(map[string]bool, len(months))
	count := len(units)
	for _, m := range months {
		if len(byMonth[m]) >= monthListMinDays {
			whole[m] = true
			count++
		} else {
			count += len(byMonth[m])
		}
	}
	for count > remoteListParallelism {
		best := ""
		for _, m := range months {
			if !whole[m] && len(byMonth[m]) > 1 && (best == "" || len(byMonth[m]) > len(byMonth[best])) {
				best = m
			}
		}
		if best == "" {
			break
		}
		whole[best] = true
		count -= len(byMonth[best]) - 1
	}
	for _, m := range months {
		days := byMonth[m]
		if whole[m] {
			sort.Strings(days)
			units = append(units, remoteListUnit{prefix: m, anchors: days, month: true})
			continue
		}
		for _, d := range days {
			units = append(units, remoteListUnit{prefix: d, anchors: []string{d}})
		}
	}
	sort.Slice(units, func(i, j int) bool { return units[i].prefix < units[j].prefix })
	return units
}

// runRemoteListings runs the units with at most remoteListParallelism in
// flight, each bounded by the listing timeout, fills summaries for every
// anchor and caches each unit's summaries as it completes. On the first
// failure it records the failure on ctx, cancels the rest and reports false.
func (p *PartitionPruner) runRemoteListings(ctx context.Context, units []remoteListUnit, summaries map[string]*dirSummary) bool {
	lister, _ := p.storage.(storage.DirectoryLister)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu     sync.Mutex
		failed bool
		wg     sync.WaitGroup
	)
	fail := func(prefix string, err error) {
		mu.Lock()
		defer mu.Unlock()
		if failed {
			return // the first failure names the listing that broke the query
		}
		failed = true
		p.logger.Debug().Err(err).Str("prefix", prefix).Msg("Partition listing failed; existence unverified")
		markListingFailed(ctx, prefix, err)
		cancel()
	}

	// A fixed pool fed in prefix order: listings start in a deterministic
	// order, and once one fails no further listing is sent.
	work := make(chan remoteListUnit)
	for i := 0; i < min(remoteListParallelism, len(units)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range work {
				if err := runCtx.Err(); err != nil {
					fail(u.prefix, err)
					continue
				}
				got, failedPrefix, err := p.listUnitOrDays(runCtx, lister, u)
				if err != nil {
					fail(failedPrefix, err)
					continue
				}
				mu.Lock()
				for anchor, s := range got {
					summaries[anchor] = s
				}
				mu.Unlock()
			}
		}()
	}
feed:
	for _, u := range units {
		select {
		case work <- u:
		case <-runCtx.Done():
			// An earlier unit failed (already recorded) or the query's own
			// context ended: then this listing never ran and its partitions
			// are unknown, not absent.
			fail(u.prefix, runCtx.Err())
			break feed
		}
	}
	close(work)
	wg.Wait()
	return !failed
}

// listUnitOrDays lists one unit. A whole-month listing that runs out of the
// listing timeout while the query is still alive (a month whose newest days
// are not compacted yet can hold thousands of keys) is retried day by day,
// each day with its own timeout, so a large month is slower but never fails
// where per-day listings would have succeeded. It returns the prefix that
// failed along with the error.
func (p *PartitionPruner) listUnitOrDays(ctx context.Context, lister storage.DirectoryLister, u remoteListUnit) (map[string]*dirSummary, string, error) {
	got, timedOut, err := p.listUnit(ctx, lister, u)
	if err == nil || !u.month || !timedOut {
		return got, u.prefix, err
	}
	p.logger.Warn().Str("prefix", u.prefix).Int("days", len(u.anchors)).Dur("timeout", p.listingTimeout()).
		Msg("Month partition listing timed out; listing its days one by one")
	all := make(map[string]*dirSummary, len(u.anchors))
	for _, d := range u.anchors {
		dayGot, _, err := p.listUnit(ctx, lister, remoteListUnit{prefix: d, anchors: []string{d}})
		if err != nil {
			return nil, d, err
		}
		for anchor, s := range dayGot {
			all[anchor] = s
		}
	}
	return all, u.prefix, nil
}

// listUnit runs one listing under its own listing timeout and caches the
// summary of every anchor it covers. timedOut is true only when the listing's
// own timeout expired while ctx was still alive.
func (p *PartitionPruner) listUnit(ctx context.Context, lister storage.DirectoryLister, u remoteListUnit) (map[string]*dirSummary, bool, error) {
	listCtx, listCancel := context.WithTimeout(ctx, p.listingTimeout())
	defer listCancel()
	var keys []string
	var err error
	if u.delimited {
		keys, err = lister.ListDirectories(listCtx, u.prefix)
	} else {
		keys, err = p.storage.List(listCtx, u.prefix)
	}
	if err != nil {
		timedOut := ctx.Err() == nil && listCtx.Err() == context.DeadlineExceeded
		return nil, timedOut, err
	}
	got := summarizeListing(u, keys)
	for anchor, s := range got {
		p.globCache.set(remoteListingCachePrefix+anchor, s.entries())
	}
	p.logger.Debug().Str("prefix", u.prefix).Int("keys", len(keys)).Int("dirs", len(got)).Msg("Listed partition prefix")
	return got, false, nil
}

// summarizeListing turns one unit's keys into a summary for every anchor the
// unit was listed for (empty when no key is under it). A month listing also
// sees days the range did not ask for; they are not cached, so the cache only
// ever holds what the per-day listings it replaces would have held.
func summarizeListing(u remoteListUnit, keys []string) map[string]*dirSummary {
	out := make(map[string]*dirSummary, len(u.anchors))
	for _, a := range u.anchors {
		out[a] = &dirSummary{}
	}
	if u.delimited {
		// Child directory names, normalized as the pruner always did.
		s := out[u.prefix]
		s.children = make(map[string]bool, len(keys))
		for _, d := range keys {
			d = strings.TrimSuffix(d, "/")
			if idx := strings.LastIndex(d, "/"); idx != -1 {
				d = d[idx+1:]
			}
			s.children[d] = true
		}
		return out
	}
	for _, k := range keys {
		rem, ok := strings.CutPrefix(k, u.prefix)
		if !ok {
			continue
		}
		if !u.month {
			out[u.prefix].add(rem)
			continue
		}
		i := strings.IndexByte(rem, '/')
		if i <= 0 {
			continue // a file directly in the month, or an empty day name
		}
		if s := out[u.prefix+rem[:i+1]]; s != nil {
			s.add(rem[i+1:])
		}
	}
	return out
}
