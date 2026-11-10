package pruning

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
//
// Three options (ListingOptions, config query.listing_*), all off by default,
// cut the listings a slow object store has to answer:
//
//   - Recent days. The newest days hold the raw tail: until compaction merges
//     them, each hour folder of today holds a thousand or more small files,
//     and a recursive listing of today, or of a month that contains it, pages
//     through all of them (11-10: 3,464 keys, 4 pages, 1.3 s healthy; on a
//     slow morning each page took 35-142 s and the listing timed out) when the
//     pruner only needs the hour folder names. A recent day is listed one
//     level deep (storage.LevelLister: one delimiter request returns its hour
//     folders and its day-level files), and a month listing of older days
//     stops before the first recent day (storage.BoundedLister). A backend
//     without those listings keeps the recursive listing for a recent day,
//     alone, and never lists a month holding recent days whole.
//   - Single-flight. Dashboard panels prune the same range at the same moment;
//     identical listings in flight at once share one request (listFlights).
//   - Stale reuse. A failed or timed-out listing of days older than the
//     protected newest days answers from their last good listing, if it is
//     recent enough, instead of refusing the query (staleSummaries).

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
// recursive listing from ever running above a month. A recent day is listed
// one level deep (level); a month listing that must not read its recent days
// stops at end.
type remoteListUnit struct {
	prefix    string
	anchors   []string
	month     bool
	delimited bool
	level     bool
	end       string
}

// flightKey names the listing a unit makes, for single-flight.
func (u remoteListUnit) flightKey() string {
	kind := "list"
	switch {
	case u.delimited:
		kind = "dirs"
	case u.level:
		kind = "level"
	}
	return kind + "\x00" + u.prefix + "\x00" + u.end
}

// ListingOptions tunes the remote listings (see the file comment). The zero
// value is the behaviour without them.
type ListingOptions struct {
	// RecentDays is how many of the newest days (UTC, counting today; later
	// days are recent too) are listed one level deep and kept out of month
	// listings. 0 is off.
	RecentDays int
	// SingleFlight shares one request among identical listings in flight.
	SingleFlight bool
	// StaleReuse is how old a day's last good listing may be to answer for a
	// failed one. 0 is off.
	StaleReuse time.Duration
}

// MaxListingStaleReuse caps ListingOptions.StaleReuse.
const MaxListingStaleReuse = 24 * time.Hour

// StaleReuseSeconds converts query.listing_stale_reuse_seconds to a Duration,
// capped at MaxListingStaleReuse before the multiplication so a huge value
// cannot overflow. Zero or negative is off.
func StaleReuseSeconds(seconds int) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(min(seconds, int(MaxListingStaleReuse/time.Second))) * time.Second
}

// SetListingOptions sets the listing options. Not safe to call concurrently
// with queries; set them before serving, like SetListingTimeout.
func (p *PartitionPruner) SetListingOptions(o ListingOptions) {
	p.recentDays = max(o.RecentDays, 0)
	p.singleFlight = o.SingleFlight
	p.staleReuse = min(max(o.StaleReuse, 0), MaxListingStaleReuse)
	p.globCache.mu.Lock()
	p.globCache.retain = p.staleReuse
	p.globCache.mu.Unlock()
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

	units := planRemoteListings(missing, p.listPlan())
	reused := 0
	if len(units) > 0 {
		// A caller asking for listings fresher than maxAge (the empty-range
		// proof) never gets a reused one.
		ok, n := p.runRemoteListings(ctx, units, summaries, maxAge <= 0)
		if !ok {
			// The existence of some partitions is now unknown, so no path
			// set built here can be trusted: a guessed set either drops rows
			// or makes DuckDB fail "No files found" on a phantom glob, and
			// both used to reach the client as a silent partial or empty
			// result (QR-6). The failure is recorded on ctx (the API answers
			// 503). Callers treat (nil, false) as "unknown" and fall back to
			// the whole-table glob.
			return nil, false, hits
		}
		if n > 0 {
			// Counted as cache hits, so an empty answer that leaned on them is
			// listed afresh before it is believed (proveEmpty,
			// ExistingPartitionPathsVerdict), and flagged so no cache keeps a
			// path set built on them.
			reused = n
			hits += n
			markStaleListing(ctx)
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
		Int("stale_reused", reused).
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
// delimiter listing the pruner always made for it. A recent day (plan) is a
// unit of its own, one level deep when the backend can; a month holding
// recent days is listed whole only by a listing that stops before them.
func planRemoteListings(anchors []string, plan listPlan) []remoteListUnit {
	var units []remoteListUnit
	byMonth := make(map[string][]string)
	for _, a := range anchors {
		if isDayDir(a) {
			if plan.recent(a) {
				units = append(units, remoteListUnit{prefix: a, anchors: []string{a}, level: plan.level})
				continue
			}
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
	canWhole := func(m string) bool { return plan.bounded || plan.monthEnd(m) == "" }
	whole := make(map[string]bool, len(months))
	count := len(units)
	for _, m := range months {
		if len(byMonth[m]) >= monthListMinDays && canWhole(m) {
			whole[m] = true
			count++
		} else {
			count += len(byMonth[m])
		}
	}
	for count > remoteListParallelism {
		best := ""
		for _, m := range months {
			if !whole[m] && canWhole(m) && len(byMonth[m]) > 1 && (best == "" || len(byMonth[m]) > len(byMonth[best])) {
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
			units = append(units, remoteListUnit{prefix: m, anchors: days, month: true, end: plan.monthEnd(m)})
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
// It also returns how many anchors were answered from a reused listing
// (allowStale, staleSummaries).
func (p *PartitionPruner) runRemoteListings(ctx context.Context, units []remoteListUnit, summaries map[string]*dirSummary, allowStale bool) (bool, int) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu     sync.Mutex
		failed bool
		reused int
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
				got, n, failedPrefix, err := p.listUnitOrDays(runCtx, u, allowStale)
				if err != nil {
					fail(failedPrefix, err)
					continue
				}
				mu.Lock()
				for anchor, s := range got {
					summaries[anchor] = s
				}
				reused += n
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
	return !failed, reused
}

// listUnitOrDays lists one unit. A whole-month listing that runs out of the
// listing timeout while the query is still alive (a month whose newest days
// are not compacted yet can hold thousands of keys) is retried day by day,
// each day with its own timeout, so a large month is slower but never fails
// where per-day listings would have succeeded. With allowStale, a failed
// listing is answered from the anchors' last good listings when they qualify
// (staleSummaries), and the day-by-day retry does not list a day that does.
// It returns the summaries, how many of them were reused, and the prefix that
// failed along with the error.
func (p *PartitionPruner) listUnitOrDays(ctx context.Context, u remoteListUnit, allowStale bool) (map[string]*dirSummary, int, string, error) {
	got, timedOut, err := p.listUnit(ctx, u)
	if err == nil {
		return got, 0, u.prefix, nil
	}
	if !u.month || !timedOut {
		if allowStale {
			if stale, oldest, ok := p.staleSummaries(ctx, u.anchors); ok {
				p.logStaleReuse(u.prefix, len(stale), oldest, err)
				return stale, len(stale), u.prefix, nil
			}
		}
		return nil, 0, u.prefix, err
	}
	p.logger.Warn().Str("prefix", u.prefix).Int("days", len(u.anchors)).Dur("timeout", p.listingTimeout()).
		Msg("Month partition listing timed out; listing its days one by one")
	all := make(map[string]*dirSummary, len(u.anchors))
	reused := 0
	var oldest time.Duration
	for _, d := range u.anchors {
		if allowStale {
			if stale, age, ok := p.staleSummaries(ctx, []string{d}); ok {
				all[d] = stale[d]
				reused++
				oldest = max(oldest, age)
				continue
			}
		}
		dayGot, _, err := p.listUnit(ctx, remoteListUnit{prefix: d, anchors: []string{d}})
		if err != nil {
			return nil, 0, d, err
		}
		for anchor, s := range dayGot {
			all[anchor] = s
		}
	}
	if reused > 0 {
		p.logStaleReuse(u.prefix, reused, oldest, err)
	}
	return all, reused, u.prefix, nil
}

// listUnit runs one listing under its own listing timeout and caches the
// summary of every anchor it covers. timedOut is true only when the listing's
// own timeout expired while ctx was still alive.
func (p *PartitionPruner) listUnit(ctx context.Context, u remoteListUnit) (map[string]*dirSummary, bool, error) {
	keys, timedOut, err := p.sharedListing(ctx, u)
	if err != nil {
		return nil, timedOut, err
	}
	got := summarizeListing(u, keys)
	for anchor, s := range got {
		p.globCache.set(remoteListingCachePrefix+anchor, s.entries())
	}
	p.logger.Debug().Str("prefix", u.prefix).Int("keys", len(keys)).Int("dirs", len(got)).Msg("Listed partition prefix")
	return got, false, nil
}

// sharedListing is timedListing, shared with the identical listings in flight
// when SingleFlight is on (listFlights). The shared request runs detached from
// ctx, so this caller going away (its query cancelled, or failed by another
// listing) neither cancels it nor fails the other callers waiting on it; it
// stays bounded by the listing timeout.
func (p *PartitionPruner) sharedListing(ctx context.Context, u remoteListUnit) ([]string, bool, error) {
	if !p.singleFlight {
		return p.timedListing(ctx, u)
	}
	key := u.flightKey()
	f, start := p.flights.join(key)
	if start {
		detached := context.WithoutCancel(ctx)
		go func() {
			f.keys, f.timedOut, f.err = p.timedListing(detached, u)
			p.flights.finish(key, f)
		}()
	}
	select {
	case <-f.done:
		return f.keys, f.timedOut && ctx.Err() == nil, f.err
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

// timedListing makes one unit's storage call under the listing timeout.
// timedOut is true only when that timeout expired while ctx was still alive.
func (p *PartitionPruner) timedListing(ctx context.Context, u remoteListUnit) ([]string, bool, error) {
	listCtx, listCancel := context.WithTimeout(ctx, p.listingTimeout())
	defer listCancel()
	keys, err := p.fetchUnit(listCtx, u)
	if err != nil {
		timedOut := ctx.Err() == nil && listCtx.Err() == context.DeadlineExceeded
		return nil, timedOut, err
	}
	return keys, false, nil
}

// fetchUnit makes one unit's storage call. A one-level listing's
// subdirectories come back as "<prefix><dir>/" keys, which summarize exactly
// like the keys of a recursive listing of the day. Only the planner chooses a
// one-level or bounded unit, and only when the backend has the listing
// (listPlan).
func (p *PartitionPruner) fetchUnit(ctx context.Context, u remoteListUnit) ([]string, error) {
	switch {
	case u.delimited:
		return p.storage.(storage.DirectoryLister).ListDirectories(ctx, u.prefix)
	case u.level:
		dirs, files, err := p.storage.(storage.LevelLister).ListLevel(ctx, u.prefix)
		if err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(files)+len(dirs))
		keys = append(keys, files...)
		for _, d := range dirs {
			keys = append(keys, u.prefix+d+"/")
		}
		return keys, nil
	case u.end != "":
		return p.storage.(storage.BoundedLister).ListBefore(ctx, u.prefix, u.end)
	default:
		return p.storage.List(ctx, u.prefix)
	}
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

// listPlan is what planRemoteListings needs besides the anchors.
type listPlan struct {
	recentFrom time.Time // days on or after it are recent; zero: none are
	level      bool      // recent days can be listed one level deep (storage.LevelLister)
	bounded    bool      // a month listing can stop at a day (storage.BoundedLister)
}

// newListPlan makes the plan for the newest recentDays days as of now.
func newListPlan(now time.Time, recentDays int, level, bounded bool) listPlan {
	if recentDays <= 0 {
		return listPlan{}
	}
	return listPlan{recentFrom: utcDay(now).AddDate(0, 0, 1-recentDays), level: level, bounded: bounded}
}

// listPlan is the plan for this pruner's options and backend. A capability
// counts only when both the backend and what it wraps have it: the resilient
// wrapper implements every listing and fails the ones its backend lacks.
func (p *PartitionPruner) listPlan() listPlan {
	if p.recentDays <= 0 {
		return listPlan{}
	}
	return newListPlan(p.nowFn(), p.recentDays, backendHas[storage.LevelLister](p.storage), backendHas[storage.BoundedLister](p.storage))
}

func backendHas[T any](b storage.Backend) bool {
	_, outer := any(b).(T)
	_, inner := any(storage.Unwrap(b)).(T)
	return outer && inner
}

// recent reports whether a day directory is one of the plan's recent days.
func (lp listPlan) recent(dir string) bool {
	if lp.recentFrom.IsZero() {
		return false
	}
	d, ok := anchorDay(dir)
	return ok && !d.Before(lp.recentFrom)
}

// monthEnd is where a recursive listing of month directory m stops: its first
// recent day, or "" when none of its days is recent. A month after the first
// recent day's has no older day to list.
func (lp listPlan) monthEnd(m string) string {
	if lp.recentFrom.IsZero() || !strings.HasSuffix(m, lp.recentFrom.Format("2006/01/")) {
		return ""
	}
	return m + lp.recentFrom.Format("02") + "/"
}

func utcDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// anchorDay parses the date of a day directory (db/meas/YYYY/MM/DD/).
func anchorDay(dir string) (time.Time, bool) {
	segs := strings.Split(strings.Trim(dir, "/"), "/")
	if len(segs) != 5 {
		return time.Time{}, false
	}
	t, err := time.Parse("2006/01/02", strings.Join(segs[2:], "/"))
	return t, err == nil
}

// listFlights de-duplicates identical listings in flight (SingleFlight). A
// flight's result, failure included, goes only to the callers already waiting
// on it: the flight is dropped when it ends, and only a successful listing
// reaches the cache (each caller stores its own anchors' summaries), so a
// failure is never remembered as an answer.
type listFlights struct {
	mu sync.Mutex
	m  map[string]*listFlight
}

type listFlight struct {
	done     chan struct{}
	keys     []string
	timedOut bool
	err      error
}

// join returns the flight in progress for key, or a new one the caller must
// start (start == true).
func (fs *listFlights) join(key string) (f *listFlight, start bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if f, ok := fs.m[key]; ok {
		return f, false
	}
	if fs.m == nil {
		fs.m = make(map[string]*listFlight)
	}
	f = &listFlight{done: make(chan struct{})}
	fs.m[key] = f
	return f, true
}

// finish ends a flight; the next caller starts a new one.
func (fs *listFlights) finish(key string, f *listFlight) {
	fs.mu.Lock()
	if fs.m[key] == f {
		delete(fs.m, key)
	}
	fs.mu.Unlock()
	close(f.done)
}

// forget makes every later caller start a new flight. Dropping the listing
// caches calls it: the "No files found" retry must list afresh, not join a
// listing that started before the files went away.
func (fs *listFlights) forget() {
	fs.mu.Lock()
	fs.m = nil
	fs.mu.Unlock()
}

// staleProtectedDays is how many of the newest days stale reuse never
// answers for, whatever RecentDays is: today changes by the minute, and
// yesterday gets its daily compaction during today's morning queries.
const staleProtectedDays = 2

// staleSummaries answers a failed listing of anchors from their last good
// listings, or reports false. Every anchor must be a day older than the
// protected newest days (max(RecentDays, staleProtectedDays), counting
// today), listed successfully at most StaleReuse ago (globCache keeps the
// entry that long past its TTL), and not waiting for its daily compaction: on
// a day with hour folders and no day-level file, the compaction can upload the
// day file and have queries hide the hour files it merged while the reused
// listing still says there is no day file, dropping the day's rows behind a
// 200. It also returns the age of the oldest listing used.
//
// A reused listing that names an hour folder compaction has removed since
// fails the read with "No files found"; that retry drops the listing caches,
// reused entries included, and lists again (the API's retryUnverifiedNoFiles).
func (p *PartitionPruner) staleSummaries(ctx context.Context, anchors []string) (map[string]*dirSummary, time.Duration, bool) {
	if p.staleReuse <= 0 || ctx.Err() != nil {
		return nil, 0, false
	}
	protectedFrom := utcDay(p.nowFn()).AddDate(0, 0, 1-max(p.recentDays, staleProtectedDays))
	out := make(map[string]*dirSummary, len(anchors))
	var oldest time.Duration
	for _, a := range anchors {
		d, ok := anchorDay(a)
		if !ok || !d.Before(protectedFrom) {
			return nil, 0, false
		}
		entries, age, ok := p.globCache.getStale(remoteListingCachePrefix+a, p.staleReuse)
		if !ok {
			return nil, 0, false
		}
		s := summaryFromEntries(entries)
		if len(s.children) > 0 && !s.direct {
			return nil, 0, false
		}
		out[a] = s
		oldest = max(oldest, age)
	}
	return out, oldest, true
}

func (p *PartitionPruner) logStaleReuse(prefix string, days int, oldest time.Duration, err error) {
	p.logger.Warn().Err(err).Str("prefix", prefix).Int("days", days).Dur("oldest_listing_age", oldest).
		Msg("Partition listing failed; reusing the last good listing of older days")
}

type staleListingKey struct{}

// withStaleListing attaches a flag that is set when the pruning pass under
// ctx answers from a reused listing, so the caller does not cache its result.
func withStaleListing(ctx context.Context) (context.Context, *atomic.Bool) {
	f := new(atomic.Bool)
	return context.WithValue(ctx, staleListingKey{}, f), f
}

// markStaleListing sets ctx's stale-listing flag, if any, and marks the
// result volatile so the SQL transform cache does not keep it either.
func markStaleListing(ctx context.Context) {
	if f, ok := ctx.Value(staleListingKey{}).(*atomic.Bool); ok {
		f.Store(true)
	}
	markVolatile(ctx)
}
