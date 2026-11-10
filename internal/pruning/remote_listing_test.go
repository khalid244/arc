package pruning

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// objStore is an in-memory object store with S3's listing semantics, so the
// legacy and the new remote existence filter can be compared on the same
// keys and their requests counted:
//   - List is recursive, costs one request per 1000 keys and, like
//     S3Backend.List, drops keys storage.ValidateKey refuses;
//   - ListDirectories uses the "/" delimiter: one entry per common prefix,
//     contents and common prefixes sharing the 1000-entry page, empty and
//     dot names dropped, like S3Backend.ListDirectories.
type objStore struct {
	mockS3Backend
	mu       sync.Mutex
	keys     []string // sorted
	prefixes []string // every listing, in arrival order
	latency  time.Duration
	failOn   func(prefix string) bool
	hang     bool
	hangOn   func(prefix string) bool
	gate     chan struct{} // when set, every listing waits for it to close
	levels   []string      // ListLevel prefixes, in arrival order
	ends     []string      // ListBefore "prefix|end", in arrival order

	listCalls   atomic.Int64
	dirCalls    atomic.Int64
	levelCalls  atomic.Int64
	requests    atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
}

func newObjStore(keys ...string) *objStore {
	s := &objStore{}
	s.add(keys...)
	return s
}

func (s *objStore) add(keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := make(map[string]bool, len(s.keys)+len(keys))
	for _, k := range s.keys {
		set[k] = true
	}
	for _, k := range keys {
		set[k] = true
	}
	s.keys = s.keys[:0]
	for k := range set {
		s.keys = append(s.keys, k)
	}
	sort.Strings(s.keys)
}

func (s *objStore) under(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for i := sort.SearchStrings(s.keys, prefix); i < len(s.keys) && strings.HasPrefix(s.keys[i], prefix); i++ {
		out = append(out, s.keys[i])
	}
	return out
}

func (s *objStore) clone() *objStore { return newObjStore(s.under("")...) }

func (s *objStore) calls() int64 { return s.listCalls.Load() + s.dirCalls.Load() }

func (s *objStore) listedPrefixes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.prefixes...)
}

func pagesFor(entries int) int64 {
	if entries == 0 {
		return 1
	}
	return int64((entries + 999) / 1000)
}

// serve accounts for one listing of pages requests, honouring latency,
// injected failures and the caller's context.
func (s *objStore) serve(ctx context.Context, prefix string, pages int64) error {
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for {
		m := s.maxInFlight.Load()
		if n <= m || s.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	s.mu.Lock()
	s.prefixes = append(s.prefixes, prefix)
	fail := s.failOn != nil && s.failOn(prefix)
	hang := s.hang || (s.hangOn != nil && s.hangOn(prefix))
	gate := s.gate
	s.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if hang {
		<-ctx.Done()
		return ctx.Err()
	}
	if fail {
		s.requests.Add(1)
		return fmt.Errorf("simulated ListObjectsV2 503 SlowDown on %q", prefix)
	}
	for i := int64(0); i < pages; i++ {
		s.requests.Add(1)
		if s.latency > 0 {
			select {
			case <-time.After(s.latency):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (s *objStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.listCalls.Add(1)
	raw := s.under(prefix)
	if err := s.serve(ctx, prefix, pagesFor(len(raw))); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(raw))
	for _, k := range raw {
		if storage.ValidateKey(k) == nil {
			out = append(out, k)
		}
	}
	return out, nil
}

func (s *objStore) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	s.dirCalls.Add(1)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	raw := s.under(prefix)
	seen := make(map[string]bool)
	var dirs []string
	entries := 0
	for _, k := range raw {
		rem := k[len(prefix):]
		i := strings.IndexByte(rem, '/')
		if i < 0 {
			entries++
			continue
		}
		cp := rem[:i]
		if seen[cp] {
			continue
		}
		seen[cp] = true
		entries++
		if cp != "" && !strings.HasPrefix(cp, ".") {
			dirs = append(dirs, cp)
		}
	}
	if err := s.serve(ctx, prefix, pagesFor(entries)); err != nil {
		return nil, err
	}
	return dirs, nil
}

func day(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }

// prodLayout mirrors prod posthog/events for the days in [from, to): 3 daily
// files and 18 small reorg files directly in each day, raw files in hour
// folders only for the newest three hours, and a sibling <meas>_late
// measurement (which no listing of <meas>/ may ever reach).
func prodLayout(db, meas string, from, to time.Time) []string {
	var keys []string
	for d := from; d.Before(to); d = d.AddDate(0, 0, 1) {
		dir := fmt.Sprintf("%s/%s/%s/", db, meas, d.Format("2006/01/02"))
		for i := 0; i < 3; i++ {
			keys = append(keys, fmt.Sprintf("%s%s_%d_daily.parquet", dir, meas, i))
		}
		for i := 0; i < 18; i++ {
			keys = append(keys, fmt.Sprintf("%s%s_reorg_%02d.parquet", dir, meas, i))
		}
		keys = append(keys, fmt.Sprintf("%s/%s_late/%s/03/late.parquet", db, meas, d.Format("2006/01/02")))
	}
	for h := 1; h <= 3; h++ {
		dir := fmt.Sprintf("%s/%s/%s/", db, meas, to.Add(-time.Duration(h)*time.Hour).Format("2006/01/02/15"))
		for i := 0; i < 5; i++ {
			keys = append(keys, fmt.Sprintf("%s%s_raw_%d.parquet", dir, meas, i))
		}
	}
	return keys
}

type filterRun struct {
	paths    []string
	verified bool
	hits     int
	failed   bool
	reason   string
	elapsed  time.Duration
}

func runFilter(ctx context.Context, p *PartitionPruner, legacy bool, paths []string, base string, maxAge time.Duration) filterRun {
	ctx, lf := WithListingFailure(ctx)
	start := time.Now()
	var r filterRun
	if legacy {
		r.paths, r.verified, r.hits = p.legacyFilterExistingRemotePathsOpts(ctx, paths, base, maxAge)
	} else {
		r.paths, r.verified, r.hits = p.filterExistingRemotePathsOpts(ctx, paths, base, maxAge)
	}
	r.elapsed = time.Since(start)
	r.failed, r.reason = lf.Failed()
	return r
}

func newRemotePruner(s *objStore) *PartitionPruner {
	p := NewPartitionPruner(zerolog.Nop())
	p.SetStorageBackend(s)
	return p
}

func genPaths(t *testing.T, base, db, meas string, from, to time.Time) []string {
	t.Helper()
	paths := NewPartitionPruner(zerolog.Nop()).GeneratePartitionPaths(context.Background(), base, db, meas, &TimeRange{Start: from, End: to})
	if len(paths) == 0 {
		t.Fatalf("no paths generated for [%s, %s)", from, to)
	}
	return paths
}

func sameResult(t *testing.T, label string, old, new filterRun, compareHits bool) {
	t.Helper()
	if !reflect.DeepEqual(old.paths, new.paths) || old.verified != new.verified || old.failed != new.failed {
		t.Fatalf("%s: results differ\nlegacy: verified=%v failed=%v %d paths %v\nnew:    verified=%v failed=%v %d paths %v",
			label, old.verified, old.failed, len(old.paths), old.paths, new.verified, new.failed, len(new.paths), new.paths)
	}
	if compareHits && (old.hits > 0) != (new.hits > 0) {
		t.Fatalf("%s: cache-hit signal differs: legacy hits=%d new hits=%d", label, old.hits, new.hits)
	}
}

// TestRemoteListing_CallCounts pins the request pattern on a prod-like layout:
// LIST calls (and S3 requests, i.e. 1000-key pages) for the old and the new
// filter, with identical results. A 2ms per-request latency makes the
// sequential-vs-parallel wall time visible in -v output.
func TestRemoteListing_CallCounts(t *testing.T) {
	const base = "s3://bucket"
	layout := prodLayout("posthog", "events", day(2026, 6, 1), day(2027, 1, 11))
	cases := []struct {
		name             string
		from, to         time.Time
		wantOld, wantNew int64
	}{
		{"1 day", day(2026, 10, 15), day(2026, 10, 16), 3, 1},
		{"7 days", day(2026, 10, 8), day(2026, 10, 15), 15, 1},
		{"31 days", day(2026, 10, 1), day(2026, 11, 1), 63, 1},
		{"31 days across a month boundary", day(2026, 10, 15), day(2026, 11, 15), 64, 2},
		{"31 days, 2 of them in the next month", day(2026, 10, 3), day(2026, 11, 3), 64, 3},
		{"7 days across a month boundary", day(2026, 10, 28), day(2026, 11, 4), 16, 4},
		{"7 days across a year boundary", day(2026, 12, 28), day(2027, 1, 4), 16, 4},
		{"31 days across a year boundary", day(2026, 12, 15), day(2027, 1, 15), 60, 2},
		{"90 days", day(2026, 8, 1), day(2026, 10, 30), 183, 3},
		{"6 hours", day(2026, 10, 15).Add(6 * time.Hour), day(2026, 10, 15).Add(12 * time.Hour), 3, 1},
	}
	t.Logf("%-38s %8s %8s %10s | %8s %8s %10s", "range", "old LIST", "old req", "old wall", "new LIST", "new req", "new wall")
	for _, tc := range cases {
		paths := genPaths(t, base, "posthog", "events", tc.from, tc.to)
		oldStore, newStore := newObjStore(layout...), newObjStore(layout...)
		oldStore.latency, newStore.latency = 2*time.Millisecond, 2*time.Millisecond
		old := runFilter(context.Background(), newRemotePruner(oldStore), true, paths, base, 0)
		got := runFilter(context.Background(), newRemotePruner(newStore), false, paths, base, 0)
		sameResult(t, tc.name, old, got, true)
		if !got.verified || len(got.paths) == 0 {
			t.Fatalf("%s: expected a verified, non-empty set: %+v", tc.name, got)
		}
		t.Logf("%-38s %8d %8d %10s | %8d %8d %10s", tc.name,
			oldStore.calls(), oldStore.requests.Load(), old.elapsed.Round(time.Millisecond),
			newStore.calls(), newStore.requests.Load(), got.elapsed.Round(time.Millisecond))
		if oldStore.calls() != tc.wantOld {
			t.Errorf("%s: legacy made %d LIST calls, want %d", tc.name, oldStore.calls(), tc.wantOld)
		}
		if newStore.calls() != tc.wantNew || newStore.dirCalls.Load() != 0 {
			t.Errorf("%s: new filter made %d LIST calls (%d delimited), want %d recursive", tc.name, newStore.calls(), newStore.dirCalls.Load(), tc.wantNew)
		}
		if newStore.maxInFlight.Load() > remoteListParallelism {
			t.Errorf("%s: %d listings in flight, cap is %d", tc.name, newStore.maxInFlight.Load(), remoteListParallelism)
		}
		for _, pfx := range newStore.listedPrefixes() {
			if n := len(strings.Split(strings.Trim(pfx, "/"), "/")); n != 4 && n != 5 {
				t.Errorf("%s: listed %q; recursive listings must stay at a month or a day", tc.name, pfx)
			}
		}
	}
}

// randomLayout builds a measurement with every shape a day can have: empty,
// day-level files only, hour folders only, both, non-parquet only, nested
// folders, dot folders, noise next to real files (".part" staging names and
// directory markers), whole months missing, stray files at month and year
// level, a sibling <meas>_late measurement, and dense days and months that
// span several 1000-key pages.
func randomLayout(r *rand.Rand, db, meas string, from time.Time, days int) []string {
	var keys []string
	root := db + "/" + meas + "/"
	skipMonth := map[string]bool{}
	denseMonth := map[string]bool{}
	for d := 0; d < days; d++ {
		t := from.AddDate(0, 0, d)
		month := t.Format("2006/01")
		if _, seen := skipMonth[month]; !seen {
			skipMonth[month] = r.Intn(10) == 0
			denseMonth[month] = r.Intn(12) == 0
			keys = append(keys, root+month+"/_month_manifest.json", root+t.Format("2006")+"/_year.json")
		}
		if skipMonth[month] {
			continue
		}
		dir := root + t.Format("2006/01/02") + "/"
		hourFiles := func(hours, files int) {
			for _, h := range r.Perm(24)[:hours] {
				hd := fmt.Sprintf("%s%02d/", dir, h)
				for i := 0; i < files; i++ {
					keys = append(keys, fmt.Sprintf("%s%s_%02d_%d.parquet", hd, meas, h, i))
				}
				if r.Intn(5) == 0 {
					keys = append(keys, hd, hd+"x.parquet.part") // noise beside a real file
				}
			}
		}
		dayFiles := func(n int) {
			for i := 0; i < n; i++ {
				keys = append(keys, fmt.Sprintf("%s%s_reorg_%02d.parquet", dir, meas, i))
			}
			if r.Intn(5) == 0 {
				keys = append(keys, dir, dir+"y.parquet.part")
			}
		}
		switch k := r.Intn(100); {
		case k < 15: // empty day
		case k < 40:
			dayFiles(1 + r.Intn(21))
		case k < 60:
			hourFiles(1+r.Intn(24), 1+r.Intn(3))
		case k < 80:
			dayFiles(1 + r.Intn(5))
			hourFiles(1+r.Intn(6), 1+r.Intn(3))
		case k < 85: // non-parquet only: hour exists, day-level files do not
			keys = append(keys, dir+"_manifest.json", fmt.Sprintf("%s%02d/x.json", dir, r.Intn(24)))
		case k < 90: // an hour folder holding only a nested folder
			keys = append(keys, fmt.Sprintf("%s%02d/sub/z.parquet", dir, r.Intn(24)))
		case k < 95: // dot folders are not partitions
			keys = append(keys, dir+".tmp/a.parquet", fmt.Sprintf("%s.%02d/b.parquet", dir, r.Intn(24)))
			dayFiles(r.Intn(2))
		default: // dense: one day spans two 1000-key pages
			h := fmt.Sprintf("%s%02d/", dir, r.Intn(24))
			for i := 0; i < 1001+r.Intn(300); i++ {
				keys = append(keys, fmt.Sprintf("%sraw_%04d.parquet", h, i))
			}
		}
		if denseMonth[month] {
			hourFiles(2, 25) // ~50 keys a day: a month spans 1-2 pages
		}
		if r.Intn(4) == 0 {
			keys = append(keys, fmt.Sprintf("%s/%s_late/%s/%02d/late.parquet", db, meas, t.Format("2006/01/02"), r.Intn(24)))
		}
	}
	return keys
}

// TestRemoteListing_EquivalentToLegacy runs the legacy and the new filter on
// many random layouts and ranges and requires identical answers: cold, then
// with the caches warmed by earlier ranges, then fully cached, and finally
// after the store changed, with every cached listing aged past the freshness
// window the empty-range proof uses.
func TestRemoteListing_EquivalentToLegacy(t *testing.T) {
	const db, meas = "db", "events"
	var oldCalls, newCalls, oldReq, newReq int64
	checked := 0
	for seed := int64(1); seed <= 60; seed++ {
		r := rand.New(rand.NewSource(seed))
		from := day(2026, time.Month(1+r.Intn(12)), 1+r.Intn(28))
		layoutDays := 30 + r.Intn(150)
		layout := randomLayout(r, db, meas, from, layoutDays)
		oldStore := newObjStore(layout...)
		newStore := oldStore.clone()
		pOld, pNew := newRemotePruner(oldStore), newRemotePruner(newStore)
		base := "s3://bucket"
		if seed%3 == 0 {
			// A root with a prefix: extractStoragePrefix strips it.
			base = "s3://bucket/tenant"
		}

		type query struct {
			label  string
			paths  []string
			extras bool
		}
		var queries []query
		for q := 0; q < 8; q++ {
			start := from.Add(time.Duration(r.Intn(layoutDays*24+48)-48)*time.Hour + time.Duration(r.Intn(60))*time.Minute)
			spans := []time.Duration{
				time.Duration(1+r.Intn(6)) * time.Hour,
				time.Duration(1+r.Intn(3)) * 24 * time.Hour,
				time.Duration(5+r.Intn(6)) * 24 * time.Hour,
				time.Duration(25+r.Intn(16)) * 24 * time.Hour,
				time.Duration(80+r.Intn(21)) * 24 * time.Hour,
			}
			span := spans[r.Intn(len(spans))]
			paths := genPaths(t, base, db, meas, start, start.Add(span))
			extras := r.Intn(5) == 0
			if extras {
				// Shapes the generator never makes still get the old answer:
				// a path outside the root (kept, unverified), an explicit
				// file, a duplicate, and a shuffled order.
				paths = append(paths,
					"s3://other-bucket/"+db+"/"+meas+"/2026/10/01/05/*.parquet",
					base+"/"+db+"/"+meas+"/"+start.Format("2006/01/02/15")+"/f.parquet",
					paths[r.Intn(len(paths))])
				r.Shuffle(len(paths), func(i, j int) { paths[i], paths[j] = paths[j], paths[i] })
			}
			queries = append(queries, query{fmt.Sprintf("seed %d query %d [%s +%s] base %s", seed, q, start.Format(time.RFC3339), span, base), paths, extras})
		}

		for i, q := range queries {
			old := runFilter(context.Background(), pOld, true, q.paths, base, 0)
			got := runFilter(context.Background(), pNew, false, q.paths, base, 0)
			// Hits are compared only where both caches hold the same
			// directories: the first query (both cold) and the reruns below.
			// Not with a duplicated day path: the legacy filter counted its
			// own just-written day-files entry as a hit for the second copy.
			sameResult(t, q.label+" (warming)", old, got, i == 0 && !q.extras)
			checked++
		}
		for _, q := range queries {
			old := runFilter(context.Background(), pOld, true, q.paths, base, 0)
			got := runFilter(context.Background(), pNew, false, q.paths, base, 0)
			sameResult(t, q.label+" (cached)", old, got, true)
			checked++
		}

		// New partitions land; both caches are older than the freshness
		// window, so a fresh pass must see them in both.
		var added []string
		for i := 0; i < 5; i++ {
			d := from.AddDate(0, 0, r.Intn(layoutDays))
			dir := db + "/" + meas + "/" + d.Format("2006/01/02") + "/"
			added = append(added, fmt.Sprintf("%s%02d/new.parquet", dir, r.Intn(24)), dir+"new_daily.parquet")
		}
		oldStore.add(added...)
		newStore.add(added...)
		for _, p := range []*PartitionPruner{pOld, pNew} {
			p.globCache.mu.Lock()
			for k, e := range p.globCache.entries {
				e.setAt = e.setAt.Add(-3 * time.Second)
				p.globCache.entries[k] = e
			}
			p.globCache.mu.Unlock()
		}
		for _, q := range queries {
			old := runFilter(context.Background(), pOld, true, q.paths, base, emptyProofFreshness)
			got := runFilter(context.Background(), pNew, false, q.paths, base, emptyProofFreshness)
			sameResult(t, q.label+" (fresh after change)", old, got, false)
			checked++
		}
		oldCalls += oldStore.calls()
		newCalls += newStore.calls()
		oldReq += oldStore.requests.Load()
		newReq += newStore.requests.Load()
	}
	t.Logf("%d comparisons on 60 layouts: legacy %d LIST calls (%d requests), new %d LIST calls (%d requests)",
		checked, oldCalls, oldReq, newCalls, newReq)
}

// Pagination: a day and a month each spanning more than one 1000-key page are
// read completely (an hour that only appears on the second page is found).
func TestRemoteListing_Pagination(t *testing.T) {
	const base = "s3://bucket"
	var keys []string
	for i := 0; i < 1500; i++ {
		keys = append(keys, fmt.Sprintf("db/m/2026/10/07/00/raw_%04d.parquet", i))
	}
	keys = append(keys, "db/m/2026/10/07/23/last.parquet", "db/m/2026/10/07/zz_daily.parquet")
	for d := 1; d <= 31; d++ {
		for i := 0; i < 40; i++ {
			keys = append(keys, fmt.Sprintf("db/m/2026/10/%02d/12/f_%02d.parquet", d, i))
		}
	}
	for _, tc := range []struct {
		name     string
		from, to time.Time
		wantReq  int64
	}{
		{"one dense day", day(2026, 10, 7), day(2026, 10, 8), 2},
		{"dense month", day(2026, 10, 1), day(2026, 11, 1), 3}, // 2742 keys
	} {
		paths := genPaths(t, base, "db", "m", tc.from, tc.to)
		oldStore := newObjStore(keys...)
		newStore := oldStore.clone()
		old := runFilter(context.Background(), newRemotePruner(oldStore), true, paths, base, 0)
		got := runFilter(context.Background(), newRemotePruner(newStore), false, paths, base, 0)
		sameResult(t, tc.name, old, got, true)
		if newStore.requests.Load() != tc.wantReq {
			t.Errorf("%s: %d requests, want %d pages", tc.name, newStore.requests.Load(), tc.wantReq)
		}
		joined := strings.Join(got.paths, " ")
		for _, want := range []string{"db/m/2026/10/07/23/*.parquet", "db/m/2026/10/07/*.parquet", "db/m/2026/10/07/00/*.parquet"} {
			if !strings.Contains(joined, want) {
				t.Errorf("%s: missing %s", tc.name, want)
			}
		}
	}
}

// A listing that fails or times out never yields a guessed set: the pass
// returns (nil, false), the failure is on the query's context (the API
// answers 503), OptimizeTablePath falls back to the whole-table glob without
// caching, and ExistingPartitionPaths reports unknown + failed, exactly as
// the legacy filter did.
func TestRemoteListing_ListingFailure(t *testing.T) {
	const base = "s3://bucket"
	const glob = base + "/posthog/events/**/*.parquet"
	layout := prodLayout("posthog", "events", day(2026, 9, 1), day(2026, 11, 3))
	cases := []struct {
		name     string
		from, to time.Time
		failOn   func(string) bool
	}{
		{"month listing fails", day(2026, 10, 1), day(2026, 11, 1), func(p string) bool { return p == "posthog/events/2026/10/" }},
		{"one day of three fails", day(2026, 10, 14), day(2026, 10, 17), func(p string) bool { return p == "posthog/events/2026/10/15/" }},
		{"every listing fails", day(2026, 9, 20), day(2026, 10, 20), func(string) bool { return true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			paths := genPaths(t, base, "posthog", "events", tc.from, tc.to)
			oldStore, newStore := newObjStore(layout...), newObjStore(layout...)
			oldStore.failOn, newStore.failOn = tc.failOn, tc.failOn
			old := runFilter(context.Background(), newRemotePruner(oldStore), true, paths, base, 0)
			pNew := newRemotePruner(newStore)
			got := runFilter(context.Background(), pNew, false, paths, base, 0)
			sameResult(t, tc.name, old, got, false)
			if got.paths != nil || got.verified || !got.failed || !strings.Contains(got.reason, "SlowDown") {
				t.Fatalf("want (nil, unverified, failed): %+v", got)
			}

			sql := fmt.Sprintf("SELECT count(*) FROM events WHERE time >= '%s' AND time < '%s'", tc.from.Format(time.RFC3339), tc.to.Format(time.RFC3339))
			ctx, lf := WithListingFailure(context.Background())
			ctx, vol := WithVolatileResult(ctx)
			res, optimized := newRemotePruner(newStore).OptimizeTablePath(ctx, glob, sql)
			if failed, _ := lf.Failed(); !failed || optimized || res != glob || !vol.Volatile {
				t.Fatalf("OptimizeTablePath: res=%v optimized=%v failed=%v volatile=%v", res, optimized, failed, vol.Volatile)
			}
			pp, ok, failed := newRemotePruner(newStore).ExistingPartitionPathsVerdict(glob, tc.from, tc.to)
			if pp != nil || ok || !failed {
				t.Fatalf("ExistingPartitionPathsVerdict = (%v, %v, %v), want (nil, false, true)", pp, ok, failed)
			}

			// Storage recovers: the next pass lists what failed and answers.
			newStore.failOn = nil
			oldStore.failOn = nil
			again := runFilter(context.Background(), pNew, false, paths, base, 0)
			ref := runFilter(context.Background(), newRemotePruner(oldStore), true, paths, base, 0)
			sameResult(t, tc.name+" (recovered)", ref, again, false)
			if !again.verified || again.failed || len(again.paths) == 0 {
				t.Fatalf("recovered pass: %+v", again)
			}
		})
	}

	t.Run("query context already cancelled", func(t *testing.T) {
		// Nothing was listed, so nothing may be reported absent.
		paths := genPaths(t, base, "posthog", "events", day(2026, 10, 1), day(2026, 10, 8))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		old := runFilter(ctx, newRemotePruner(newObjStore(layout...)), true, paths, base, 0)
		got := runFilter(ctx, newRemotePruner(newObjStore(layout...)), false, paths, base, 0)
		sameResult(t, "cancelled", old, got, false)
		if got.paths != nil || got.verified || !got.failed {
			t.Fatalf("a cancelled query must be unknown + failed, never verified-empty: %+v", got)
		}
	})

	t.Run("listing timeout", func(t *testing.T) {
		paths := genPaths(t, base, "posthog", "events", day(2026, 9, 1), day(2026, 11, 1))
		s := newObjStore(layout...)
		s.hang = true
		p := newRemotePruner(s)
		p.SetListingTimeout(100 * time.Millisecond)
		got := runFilter(context.Background(), p, false, paths, base, 0)
		if got.paths != nil || got.verified || !got.failed {
			t.Fatalf("a timed-out listing must be unknown + failed: %+v", got)
		}
		if got.elapsed > 2*time.Second {
			t.Fatalf("parallel listings must share the timeout, took %v", got.elapsed)
		}
	})

	t.Run("month timeout falls back to days", func(t *testing.T) {
		// Every month listing hangs past the timeout; day listings answer.
		// The result must match the legacy per-day code, verified.
		paths := genPaths(t, base, "posthog", "events", day(2026, 9, 20), day(2026, 11, 1))
		isMonth := func(prefix string) bool { return strings.Count(strings.Trim(prefix, "/"), "/") == 3 }
		s := newObjStore(layout...)
		s.hangOn = isMonth
		p := newRemotePruner(s)
		p.SetListingTimeout(100 * time.Millisecond)
		got := runFilter(context.Background(), p, false, paths, base, 0)
		old := runFilter(context.Background(), newRemotePruner(newObjStore(layout...)), true, paths, base, 0)
		sameResult(t, "month fallback", old, got, false)
		if got.failed || !got.verified {
			t.Fatalf("a timed-out month must fall back to day listings, not fail: %+v", got)
		}
		var months int
		for _, pre := range s.listedPrefixes() {
			if isMonth(pre) {
				months++
			}
		}
		if months == 0 {
			t.Fatal("expected a whole-month listing to be attempted first")
		}
	})

	t.Run("first failure stops the rest", func(t *testing.T) {
		// 13 month listings; the first fails at once, the others are slow.
		// Listings not yet started are never sent.
		paths := genPaths(t, base, "posthog", "events", day(2025, 12, 15), day(2026, 12, 15))
		s := newObjStore(layout...)
		s.latency = 200 * time.Millisecond
		s.failOn = func(p string) bool { return p == "posthog/events/2025/12/" }
		got := runFilter(context.Background(), newRemotePruner(s), false, paths, base, 0)
		if got.paths != nil || !got.failed {
			t.Fatalf("want failure: %+v", got)
		}
		if n := s.listCalls.Load(); n > remoteListParallelism {
			t.Fatalf("%d listings sent after the first failure; at most the %d in flight may run", n, remoteListParallelism)
		}
		if got.elapsed > time.Second {
			t.Fatalf("in-flight listings must be cancelled, took %v", got.elapsed)
		}
	})
}

// At most remoteListParallelism listings are in flight, and a range needing
// more units than that takes ceil(units/cap) rounds, not one per unit.
func TestRemoteListing_ParallelismBound(t *testing.T) {
	const base = "s3://bucket"
	layout := prodLayout("db", "m", day(2025, 1, 1), day(2026, 1, 1))
	paths := genPaths(t, base, "db", "m", day(2025, 1, 1), day(2026, 1, 1)) // 12 months
	s := newObjStore(layout...)
	s.latency = 30 * time.Millisecond
	got := runFilter(context.Background(), newRemotePruner(s), false, paths, base, 0)
	if !got.verified || len(got.paths) == 0 {
		t.Fatalf("%+v", got)
	}
	if s.listCalls.Load() != 12 || s.maxInFlight.Load() != remoteListParallelism {
		t.Fatalf("calls=%d max in flight=%d, want 12 and %d", s.listCalls.Load(), s.maxInFlight.Load(), remoteListParallelism)
	}
	if got.elapsed > 12*30*time.Millisecond {
		t.Fatalf("listings ran sequentially: %v", got.elapsed)
	}
}

func TestPlanRemoteListings(t *testing.T) {
	days := func(month string, from, to int) []string {
		var out []string
		for d := from; d <= to; d++ {
			out = append(out, fmt.Sprintf("%s%02d/", month, d))
		}
		return out
	}
	oct, nov := "db/m/2026/10/", "db/m/2026/11/"
	cat := func(parts ...[]string) []string {
		var out []string
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	cases := []struct {
		name    string
		anchors []string
		want    []string // unit prefixes
	}{
		{"one day", days(oct, 7, 7), days(oct, 7, 7)},
		{"four days: one round per day", days(oct, 1, 4), days(oct, 1, 4)},
		{"five days: the month", days(oct, 1, 5), []string{oct}},
		{"4 + 3 across months: the bigger month whole", cat(days(oct, 28, 31), days(nov, 1, 3)), cat([]string{oct}, days(nov, 1, 3))},
		{"29 + 2: month and two days", cat(days(oct, 3, 31), days(nov, 1, 2)), cat([]string{oct}, days(nov, 1, 2))},
		{"17 + 14: two months", cat(days(oct, 15, 31), days(nov, 1, 14)), []string{oct, nov}},
		{"non-day parent: delimiter listing", []string{"db/m/2026/10/07/05/"}, []string{"db/m/2026/10/07/05/"}},
	}
	for _, tc := range cases {
		units := planRemoteListings(tc.anchors, listPlan{})
		var got []string
		covered := 0
		for _, u := range units {
			got = append(got, u.prefix)
			covered += len(u.anchors)
			day, month := isDayDir(u.prefix), len(strings.Split(strings.Trim(u.prefix, "/"), "/")) == 4
			if u.delimited == (day || (u.month && month)) || u.month != (month && !u.delimited) {
				t.Errorf("%s: unit %+v has the wrong kind", tc.name, u)
			}
		}
		if !reflect.DeepEqual(got, tc.want) || covered != len(tc.anchors) {
			t.Errorf("%s: units %v (covering %d), want %v (covering %d)", tc.name, got, covered, tc.want, len(tc.anchors))
		}
	}
}

// The one place the answer differs from the legacy filter, in the safe
// direction: an hour folder whose only keys are ones the backend's List
// refuses (a directory marker, a ".part" staging name). The delimiter listing
// reported the folder, the pruner kept its glob, and DuckDB then failed the
// query with "No files found" on it (a 503 since QR-6). From List the folder
// holds nothing, so it is not a partition.
func TestRemoteListing_RefusedKeysOnlyHourIsAbsent(t *testing.T) {
	const base = "s3://bucket"
	keys := []string{
		"db/m/2026/10/07/05/a.parquet",
		"db/m/2026/10/07/06/",               // directory marker only
		"db/m/2026/10/07/07/b.parquet.part", // staging name only
		"db/m/2026/10/07/08/",               // marker beside a real file: present
		"db/m/2026/10/07/08/c.parquet",
	}
	paths := genPaths(t, base, "db", "m", day(2026, 10, 7).Add(5*time.Hour), day(2026, 10, 7).Add(9*time.Hour))
	old := runFilter(context.Background(), newRemotePruner(newObjStore(keys...)), true, paths, base, 0)
	got := runFilter(context.Background(), newRemotePruner(newObjStore(keys...)), false, paths, base, 0)
	sort.Strings(old.paths)
	sort.Strings(got.paths)
	wantOld := []string{base + "/db/m/2026/10/07/05/*.parquet", base + "/db/m/2026/10/07/06/*.parquet", base + "/db/m/2026/10/07/07/*.parquet", base + "/db/m/2026/10/07/08/*.parquet"}
	wantNew := []string{base + "/db/m/2026/10/07/05/*.parquet", base + "/db/m/2026/10/07/08/*.parquet"}
	if !reflect.DeepEqual(old.paths, wantOld) || !reflect.DeepEqual(got.paths, wantNew) {
		t.Fatalf("legacy %v (want %v)\nnew %v (want %v)", old.paths, wantOld, got.paths, wantNew)
	}
}

func TestSetCacheTTLs(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	if p.globCache.ttl != GlobCacheTTL || p.partitionCache.ttl != PartitionCacheTTL {
		t.Fatalf("defaults: %v / %v", p.globCache.ttl, p.partitionCache.ttl)
	}
	p.SetCacheTTLs(90*time.Second, 0)
	if p.globCache.ttl != 90*time.Second || p.partitionCache.ttl != PartitionCacheTTL {
		t.Fatalf("glob override: %v / %v", p.globCache.ttl, p.partitionCache.ttl)
	}
	p.SetCacheTTLs(-1, 5*time.Minute)
	if p.globCache.ttl != GlobCacheTTL || p.partitionCache.ttl != 5*time.Minute {
		t.Fatalf("partition override: %v / %v", p.globCache.ttl, p.partitionCache.ttl)
	}
	if got := p.GetGlobCacheStats()["ttl_seconds"]; got != GlobCacheTTL.Seconds() {
		t.Fatalf("stats ttl_seconds = %v", got)
	}
	// An entry set after the change carries the new TTL.
	p.SetCacheTTLs(20*time.Millisecond, 0)
	p.globCache.set("k", []string{"v"})
	time.Sleep(40 * time.Millisecond)
	if _, ok := p.globCache.get("k"); ok {
		t.Fatal("entry outlived the configured TTL")
	}
}
