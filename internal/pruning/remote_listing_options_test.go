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

// ListLevel is S3's delimiter listing returning both halves: common
// prefixes (filtered like ListDirectories) and the keys directly under the
// prefix (filtered like List), sharing the 1000-entry page.
func (s *objStore) ListLevel(ctx context.Context, prefix string) ([]string, []string, error) {
	s.levelCalls.Add(1)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	s.mu.Lock()
	s.levels = append(s.levels, prefix)
	s.mu.Unlock()
	seen := make(map[string]bool)
	var dirs, files []string
	entries := 0
	for _, k := range s.under(prefix) {
		rem := k[len(prefix):]
		i := strings.IndexByte(rem, '/')
		if i < 0 {
			entries++
			if storage.ValidateKey(k) == nil {
				files = append(files, k)
			}
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
		return nil, nil, err
	}
	return dirs, files, nil
}

// ListBefore is List stopping at end: S3 still returns whole pages, so the
// page holding the first key at or after end is fetched, and none after it.
func (s *objStore) ListBefore(ctx context.Context, prefix, end string) ([]string, error) {
	s.listCalls.Add(1)
	s.mu.Lock()
	s.ends = append(s.ends, prefix+"|"+end)
	s.mu.Unlock()
	raw := s.under(prefix)
	n := len(raw)
	if end != "" {
		n = sort.SearchStrings(raw, end)
	}
	pages := pagesFor(n)
	if n < len(raw) {
		pages = int64(n/1000) + 1
	}
	if err := s.serve(ctx, prefix, pages); err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for _, k := range raw[:n] {
		if storage.ValidateKey(k) == nil {
			out = append(out, k)
		}
	}
	return out, nil
}

func (s *objStore) levelPrefixes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.levels...)
}

func (s *objStore) boundedListings() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ends...)
}

// plainStore is a backend with List and ListDirectories only (no one-level or
// bounded listing).
type plainStore struct {
	storage.Backend
	storage.DirectoryLister
}

// rawTailLayout is prod posthog/events on 2026-11-10 at 14:00Z: compacted
// days up to 11-08, yesterday in 24 hour folders, and today's raw tail of 14
// hour folders holding 250 files each beside two reorg files.
func rawTailLayout(db, meas string) []string {
	keys := prodLayout(db, meas, day(2026, 10, 1), day(2026, 11, 9))
	for h := 0; h < 24; h++ {
		for i := 0; i < 4; i++ {
			keys = append(keys, fmt.Sprintf("%s/%s/2026/11/09/%02d/%s_%d.parquet", db, meas, h, meas, i))
		}
	}
	for h := 0; h < 14; h++ {
		for i := 0; i < 250; i++ {
			keys = append(keys, fmt.Sprintf("%s/%s/2026/11/10/%02d/%s_raw_%03d.parquet", db, meas, h, meas, i))
		}
	}
	return append(keys, db+"/"+meas+"/2026/11/10/"+meas+"_reorg_00.parquet", db+"/"+meas+"/2026/11/10/"+meas+"_reorg_01.parquet")
}

var rawTailNow = day(2026, 11, 10).Add(14*time.Hour + 5*time.Minute)

func newTailPruner(s storage.Backend, o ListingOptions) *PartitionPruner {
	p := NewPartitionPruner(zerolog.Nop())
	p.SetStorageBackend(s)
	p.nowFn = func() time.Time { return rawTailNow }
	p.SetListingOptions(o)
	return p
}

// ageGlobCache moves every cached listing d into the past.
func ageGlobCache(p *PartitionPruner, d time.Duration) {
	p.globCache.mu.Lock()
	defer p.globCache.mu.Unlock()
	for k, e := range p.globCache.entries {
		e.setAt = e.setAt.Add(-d)
		e.expiresAt = e.expiresAt.Add(-d)
		e.keepUntil = e.keepUntil.Add(-d)
		p.globCache.entries[k] = e
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPlanRemoteListings_RecentDays(t *testing.T) {
	days := func(month string, from, to int) []string {
		var out []string
		for d := from; d <= to; d++ {
			out = append(out, fmt.Sprintf("%s%02d/", month, d))
		}
		return out
	}
	oct, nov, dec := "db/m/2026/10/", "db/m/2026/11/", "db/m/2026/12/"
	cat := func(parts ...[]string) []string {
		var out []string
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	// want: one entry per unit, "prefix kind [end]" with kind month, day,
	// level or dirs.
	units := func(kind string, prefixes ...string) []string {
		out := make([]string, len(prefixes))
		for i, p := range prefixes {
			out[i] = p + " " + kind
		}
		return out
	}
	nov10 := day(2026, 11, 10).Add(14 * time.Hour)
	cases := []struct {
		name           string
		anchors        []string
		now            time.Time
		recent         int
		level, bounded bool
		want           []string
	}{
		{"7 days: month stops before the 2 recent days, which are listed one level",
			days(nov, 4, 10), nov10, 2, true, true,
			cat([]string{nov + " month " + nov + "09/"}, units("level", days(nov, 9, 10)...))},
		{"recent days off: the base plan", days(nov, 4, 10), nov10, 0, true, true, []string{nov + " month"}},
		{"no one-level listing: recent days listed recursively, alone",
			days(nov, 4, 10), nov10, 2, false, true,
			cat([]string{nov + " month " + nov + "09/"}, units("day", days(nov, 9, 10)...))},
		{"no bounded listing: a month holding recent days is never listed whole",
			days(nov, 4, 10), nov10, 2, true, false,
			cat(units("day", days(nov, 4, 8)...), units("level", days(nov, 9, 10)...))},
		{"31 days across months", cat(days(oct, 11, 31), days(nov, 1, 10)), nov10, 2, true, true,
			cat([]string{oct + " month", nov + " month " + nov + "09/"}, units("level", days(nov, 9, 10)...))},
		{"old days of the current month: the month still stops before its recent days",
			days(nov, 1, 5), nov10, 2, true, true, []string{nov + " month " + nov + "09/"}},
		{"an older month is unbounded", days(oct, 1, 7), nov10, 2, true, true, []string{oct + " month"}},
		{"recent days start on the last of the month", cat(days(nov, 24, 30), days(dec, 1, 1)), day(2026, 12, 1).Add(time.Hour), 2, true, true,
			cat([]string{nov + " month " + nov + "30/"}, units("level", nov+"30/", dec+"01/"))},
		{"one recent day", days(nov, 4, 10), nov10, 1, true, true,
			[]string{nov + " month " + nov + "10/", nov + "10/ level"}},
		{"future days are recent", days(nov, 10, 11), nov10, 2, true, true, units("level", days(nov, 10, 11)...)},
		{"few old days stay per-day", days(nov, 7, 10), nov10, 2, true, true,
			cat(units("day", days(nov, 7, 8)...), units("level", days(nov, 9, 10)...))},
		{"old days merged into the month to fit one round", days(nov, 6, 10), nov10, 2, true, true,
			cat([]string{nov + " month " + nov + "09/"}, units("level", days(nov, 9, 10)...))},
		{"non-day parent: delimiter listing", []string{"db/m/2026/11/10/05/"}, nov10, 2, true, true, []string{"db/m/2026/11/10/05/ dirs"}},
	}
	for _, tc := range cases {
		got := planRemoteListings(tc.anchors, newListPlan(tc.now, tc.recent, tc.level, tc.bounded))
		var desc []string
		covered := 0
		for _, u := range got {
			kind := "day"
			switch {
			case u.delimited:
				kind = "dirs"
			case u.level:
				kind = "level"
			case u.month:
				kind = "month"
			}
			d := u.prefix + " " + kind
			if u.end != "" {
				d += " " + u.end
			}
			desc = append(desc, d)
			covered += len(u.anchors)
			for _, a := range u.anchors {
				if u.end != "" && a >= u.end {
					t.Errorf("%s: anchor %s is not before the unit's end %s", tc.name, a, u.end)
				}
			}
		}
		if !reflect.DeepEqual(desc, tc.want) || covered != len(tc.anchors) {
			t.Errorf("%s:\n got  %v (covering %d)\n want %v (covering %d)", tc.name, desc, covered, tc.want, len(tc.anchors))
		}
	}
}

// The 7-day dashboard query of 11-10: the base plan lists November
// recursively, which pages through today's raw tail (4 requests). With the
// newest 2 days kept out, the month listing stops at 11-09 (1 request) and
// each recent day is one delimiter request. Same answer as the legacy filter.
func TestRemoteListing_RecentDaysUseDelimiterListing(t *testing.T) {
	const base = "s3://bucket"
	layout := rawTailLayout("posthog", "events")
	paths := genPaths(t, base, "posthog", "events", day(2026, 11, 4), day(2026, 11, 10).Add(15*time.Hour))
	ref := runFilter(context.Background(), newRemotePruner(newObjStore(layout...)), true, paths, base, 0)

	t.Run("default: unchanged", func(t *testing.T) {
		s := newObjStore(layout...)
		got := runFilter(context.Background(), newTailPruner(s, ListingOptions{}), false, paths, base, 0)
		sameResult(t, "default", ref, got, true)
		if s.listCalls.Load() != 1 || s.requests.Load() != 4 || s.levelCalls.Load() != 0 || len(s.boundedListings()) != 0 {
			t.Fatalf("calls=%d requests=%d level=%d bounded=%v, want the base plan: one unbounded month listing of 4 pages",
				s.listCalls.Load(), s.requests.Load(), s.levelCalls.Load(), s.boundedListings())
		}
	})

	t.Run("recent days on", func(t *testing.T) {
		s := newObjStore(layout...)
		p := newTailPruner(s, ListingOptions{RecentDays: 2})
		got := runFilter(context.Background(), p, false, paths, base, 0)
		sameResult(t, "recent", ref, got, true)
		wantLevels := []string{"posthog/events/2026/11/09/", "posthog/events/2026/11/10/"}
		levels := s.levelPrefixes()
		sort.Strings(levels)
		if !reflect.DeepEqual(levels, wantLevels) {
			t.Fatalf("one-level listings %v, want %v", levels, wantLevels)
		}
		if b := s.boundedListings(); !reflect.DeepEqual(b, []string{"posthog/events/2026/11/|posthog/events/2026/11/09/"}) {
			t.Fatalf("recursive listings %v, want November stopping at 11-09", b)
		}
		if s.listCalls.Load() != 1 || s.requests.Load() != 3 {
			t.Fatalf("calls=%d requests=%d, want 1 recursive call and 3 requests in all", s.listCalls.Load(), s.requests.Load())
		}
		// Every anchor is cached: the rerun lists nothing.
		again := runFilter(context.Background(), p, false, paths, base, 0)
		sameResult(t, "recent cached", ref, again, false)
		if s.listCalls.Load() != 1 || s.levelCalls.Load() != 2 || again.hits == 0 {
			t.Fatalf("rerun listed again: calls=%d level=%d hits=%d", s.listCalls.Load(), s.levelCalls.Load(), again.hits)
		}
	})

	t.Run("backend without one-level or bounded listing", func(t *testing.T) {
		s := newObjStore(layout...)
		got := runFilter(context.Background(), newTailPruner(plainStore{s, s}, ListingOptions{RecentDays: 2}), false, paths, base, 0)
		sameResult(t, "plain", ref, got, true)
		for _, pfx := range s.listedPrefixes() {
			if pfx == "posthog/events/2026/11/" {
				t.Fatalf("a month holding the recent days was listed whole: %v", s.listedPrefixes())
			}
		}
		if s.levelCalls.Load() != 0 || s.dirCalls.Load() != 0 || s.listCalls.Load() != 7 {
			t.Fatalf("calls=%d level=%d dirs=%d, want 7 per-day recursive listings", s.listCalls.Load(), s.levelCalls.Load(), s.dirCalls.Load())
		}
	})
}

// Random layouts and a random "now" inside them: with recent days on (and
// single-flight on every other seed) the answers stay the legacy filter's.
func TestRemoteListing_RecentDaysEquivalentToLegacy(t *testing.T) {
	const db, meas, base = "db", "events", "s3://bucket"
	var levels, bounded int
	for seed := int64(1); seed <= 40; seed++ {
		r := rand.New(rand.NewSource(seed))
		from := day(2026, time.Month(1+r.Intn(12)), 1+r.Intn(28))
		layoutDays := 30 + r.Intn(90)
		layout := randomLayout(r, db, meas, from, layoutDays)
		now := from.AddDate(0, 0, 3+r.Intn(layoutDays)).Add(time.Duration(r.Intn(24*60)) * time.Minute)
		oldStore := newObjStore(layout...)
		newStore := oldStore.clone()
		pOld := newRemotePruner(oldStore)
		pNew := newRemotePruner(newStore)
		pNew.nowFn = func() time.Time { return now }
		pNew.SetListingOptions(ListingOptions{RecentDays: 1 + r.Intn(3), SingleFlight: seed%2 == 0})
		var queries [][]string
		for q := 0; q < 6; q++ {
			end := now.Add(time.Duration(r.Intn(72)-24) * time.Hour)
			spans := []time.Duration{6 * time.Hour, 2 * 24 * time.Hour, 7 * 24 * time.Hour, 31 * 24 * time.Hour}
			queries = append(queries, genPaths(t, base, db, meas, end.Add(-spans[r.Intn(len(spans))]), end))
		}
		for pass := 0; pass < 2; pass++ {
			for i, paths := range queries {
				old := runFilter(context.Background(), pOld, true, paths, base, 0)
				got := runFilter(context.Background(), pNew, false, paths, base, 0)
				sameResult(t, fmt.Sprintf("seed %d now %s query %d pass %d", seed, now.Format(time.RFC3339), i, pass), old, got, pass == 0 && i == 0)
			}
		}
		levels += int(newStore.levelCalls.Load())
		for _, b := range newStore.boundedListings() {
			if !strings.HasSuffix(b, "|") {
				bounded++
			}
		}
	}
	if levels == 0 || bounded == 0 {
		t.Fatalf("the random ranges never exercised the new listings: %d one-level, %d bounded", levels, bounded)
	}
}

// Concurrent queries that need the same listing share one request.
func TestRemoteListing_SingleFlightShares(t *testing.T) {
	const base = "s3://bucket"
	layout := rawTailLayout("posthog", "events")
	paths := genPaths(t, base, "posthog", "events", day(2026, 11, 4), day(2026, 11, 10).Add(15*time.Hour))
	ref := runFilter(context.Background(), newRemotePruner(newObjStore(layout...)), true, paths, base, 0)
	const queries = 6
	const unitsPerQuery = 3 // November up to 11-09, and 11-09 and 11-10 one level each

	run := func(t *testing.T, singleFlight bool) (*objStore, []filterRun) {
		s := newObjStore(layout...)
		s.gate = make(chan struct{})
		p := newTailPruner(s, ListingOptions{RecentDays: 2, SingleFlight: singleFlight})
		results := make([]filterRun, queries)
		var wg sync.WaitGroup
		var done atomic.Int64
		for i := range results {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = runFilter(context.Background(), p, false, paths, base, 0)
				done.Add(1)
			}()
		}
		if singleFlight {
			waitFor(t, "the shared listings to start", func() bool { return s.inFlight.Load() == unitsPerQuery })
			time.Sleep(100 * time.Millisecond) // let the other queries join
		} else {
			waitFor(t, "every query's listings to start", func() bool { return s.inFlight.Load() == queries*unitsPerQuery })
		}
		if done.Load() != 0 {
			t.Fatal("a query finished while its listings were held")
		}
		close(s.gate)
		wg.Wait()
		for i, r := range results {
			sameResult(t, fmt.Sprintf("query %d", i), ref, r, false)
		}
		return s, results
	}

	t.Run("off: one request per query", func(t *testing.T) {
		s, _ := run(t, false)
		if n := s.calls() + s.levelCalls.Load(); n != queries*unitsPerQuery {
			t.Fatalf("%d listings, want %d", n, queries*unitsPerQuery)
		}
	})
	t.Run("on: one request in all", func(t *testing.T) {
		s, _ := run(t, true)
		if n := s.calls() + s.levelCalls.Load(); n != unitsPerQuery {
			t.Fatalf("%d listings, want %d", n, unitsPerQuery)
		}
		if s.maxInFlight.Load() != unitsPerQuery {
			t.Fatalf("%d listings in flight at once, want %d", s.maxInFlight.Load(), unitsPerQuery)
		}
	})
}

// The query that started a shared listing goes away; the listing goes on for
// the queries still waiting on it.
func TestRemoteListing_SingleFlightIsolatesCancellation(t *testing.T) {
	const base = "s3://bucket"
	layout := rawTailLayout("posthog", "events")
	paths := genPaths(t, base, "posthog", "events", day(2026, 11, 4), day(2026, 11, 10).Add(15*time.Hour))
	ref := runFilter(context.Background(), newRemotePruner(newObjStore(layout...)), true, paths, base, 0)

	s := newObjStore(layout...)
	s.gate = make(chan struct{})
	p := newTailPruner(s, ListingOptions{RecentDays: 2, SingleFlight: true})

	ctxA, cancelA := context.WithCancel(context.Background())
	resA := make(chan filterRun, 1)
	go func() { resA <- runFilter(ctxA, p, false, paths, base, 0) }()
	waitFor(t, "A's listings to start", func() bool { return s.inFlight.Load() == 3 })
	resB := make(chan filterRun, 1)
	go func() { resB <- runFilter(context.Background(), p, false, paths, base, 0) }()
	time.Sleep(100 * time.Millisecond)

	cancelA()
	a := <-resA
	if a.paths != nil || a.verified || !a.failed {
		t.Fatalf("the cancelled query must be unknown + failed: %+v", a)
	}
	time.Sleep(50 * time.Millisecond)
	if n := s.inFlight.Load(); n != 3 {
		t.Fatalf("cancelling A cancelled the shared listings: %d still in flight, want 3", n)
	}
	close(s.gate)
	b := <-resB
	sameResult(t, "B", ref, b, false)
	if !b.verified || b.failed {
		t.Fatalf("B must be answered by the listings A started: %+v", b)
	}
	if n := s.calls() + s.levelCalls.Load(); n != 3 {
		t.Fatalf("%d listings, want the 3 A started", n)
	}
}

// A shared listing that fails fails every query waiting on it, and is not
// remembered: the next query lists again.
func TestRemoteListing_SingleFlightFailureNotCached(t *testing.T) {
	const base = "s3://bucket"
	layout := rawTailLayout("posthog", "events")
	paths := genPaths(t, base, "posthog", "events", day(2026, 11, 4), day(2026, 11, 10).Add(15*time.Hour))
	const today = "posthog/events/2026/11/10/"

	s := newObjStore(layout...)
	s.gate = make(chan struct{})
	s.failOn = func(p string) bool { return p == today }
	p := newTailPruner(s, ListingOptions{RecentDays: 2, SingleFlight: true})
	res := make(chan filterRun, 2)
	for i := 0; i < 2; i++ {
		go func() { res <- runFilter(context.Background(), p, false, paths, base, 0) }()
	}
	waitFor(t, "the listings to start", func() bool { return s.inFlight.Load() >= 1 })
	time.Sleep(100 * time.Millisecond)
	close(s.gate)
	for i := 0; i < 2; i++ {
		if r := <-res; r.paths != nil || !r.failed || !strings.Contains(r.reason, "SlowDown") {
			t.Fatalf("query %d: want the shared failure: %+v", i, r)
		}
	}
	if s.levelCalls.Load() != 2 {
		t.Fatalf("level listings = %d, want 2 (11-09, and 11-10 once for both queries)", s.levelCalls.Load())
	}
	if _, ok := p.globCache.get(remoteListingCachePrefix + today); ok {
		t.Fatal("the failed listing left a cache entry")
	}

	s.mu.Lock()
	s.failOn = nil
	s.mu.Unlock()
	got := runFilter(context.Background(), p, false, paths, base, 0)
	if !got.verified || got.failed || len(got.paths) == 0 {
		t.Fatalf("recovered query: %+v", got)
	}
	if n := s.levelCalls.Load(); n < 3 {
		t.Fatalf("the failed listing was not retried: %d level listings", n)
	}
}

// After the listing caches are dropped (the "No files found" retry, a
// compaction), a query does not join a listing that started before.
func TestRemoteListing_SingleFlightInvalidate(t *testing.T) {
	const base = "s3://bucket"
	layout := rawTailLayout("posthog", "events")
	paths := genPaths(t, base, "posthog", "events", day(2026, 11, 4), day(2026, 11, 10).Add(15*time.Hour))
	s := newObjStore(layout...)
	s.gate = make(chan struct{})
	p := newTailPruner(s, ListingOptions{RecentDays: 2, SingleFlight: true})
	res := make(chan filterRun, 2)
	go func() { res <- runFilter(context.Background(), p, false, paths, base, 0) }()
	waitFor(t, "the first listings", func() bool { return s.inFlight.Load() == 3 })
	p.InvalidateGlobCache()
	go func() { res <- runFilter(context.Background(), p, false, paths, base, 0) }()
	waitFor(t, "fresh listings after the invalidation", func() bool { return s.inFlight.Load() == 6 })
	close(s.gate)
	for i := 0; i < 2; i++ {
		if r := <-res; !r.verified || r.failed {
			t.Fatalf("query %d: %+v", i, r)
		}
	}
}

// staleSetup lists the 7-day range once (filling the caches), ages every
// cached listing past the glob TTL, and returns the store for the caller to
// break.
func staleSetup(t *testing.T, layout []string, o ListingOptions) (*objStore, *PartitionPruner, []string, filterRun) {
	t.Helper()
	const base = "s3://bucket"
	paths := genPaths(t, base, "posthog", "events", day(2026, 11, 4), day(2026, 11, 10).Add(15*time.Hour))
	s := newObjStore(layout...)
	p := newTailPruner(s, o)
	first := runFilter(context.Background(), p, false, paths, base, 0)
	if !first.verified || first.failed || len(first.paths) == 0 {
		t.Fatalf("first pass: %+v", first)
	}
	ageGlobCache(p, 2*time.Minute)
	return s, p, paths, first
}

func TestRemoteListing_StaleReuse(t *testing.T) {
	const base = "s3://bucket"
	const glob = base + "/posthog/events/**/*.parquet"
	const month, today = "posthog/events/2026/11/", "posthog/events/2026/11/10/"
	layout := rawTailLayout("posthog", "events")
	on := ListingOptions{RecentDays: 2, StaleReuse: 10 * time.Minute}
	failMonth := func(p string) bool { return p == month }

	t.Run("old days reuse the last good listing", func(t *testing.T) {
		s, p, paths, first := staleSetup(t, layout, on)
		s.failOn = failMonth
		got := runFilter(context.Background(), p, false, paths, base, 0)
		if !reflect.DeepEqual(got.paths, first.paths) || !got.verified || got.failed {
			t.Fatalf("want the first answer, verified: %+v", got)
		}
		if got.hits < 5 {
			t.Fatalf("hits = %d, want the 5 reused days counted", got.hits)
		}
		if len(s.levelPrefixes()) != 4 {
			t.Fatalf("the recent days must be listed afresh: %v", s.levelPrefixes())
		}
	})

	t.Run("not cached downstream", func(t *testing.T) {
		s, p, _, _ := staleSetup(t, layout, on)
		s.failOn = failMonth
		sql := "SELECT count(*) FROM events WHERE time >= '2026-11-04T00:00:00Z' AND time < '2026-11-10T15:00:00Z'"
		ctx, lf := WithListingFailure(context.Background())
		ctx, vol := WithVolatileResult(ctx)
		res, optimized := p.OptimizeTablePath(ctx, glob, sql)
		if failed, _ := lf.Failed(); failed || !optimized || res == glob || !vol.Volatile {
			t.Fatalf("OptimizeTablePath: optimized=%v failed=%v volatile=%v", optimized, failed, vol.Volatile)
		}
		if _, _, ok := p.partitionCache.get(p.partitionCache.cacheKey(glob, sql)); ok {
			t.Fatal("a path set built on a stale listing was cached")
		}
		pp, ok, failed := p.ExistingPartitionPathsVerdict(glob, day(2026, 11, 4), day(2026, 11, 10).Add(15*time.Hour))
		if len(pp) == 0 || !ok || failed {
			t.Fatalf("ExistingPartitionPathsVerdict = (%d paths, %v, %v)", len(pp), ok, failed)
		}
		if _, _, cached := p.partitionCache.get(p.partitionCache.cacheKey(glob, fmt.Sprintf("exist:%d-%d", day(2026, 11, 4).UnixNano(), day(2026, 11, 10).Add(15*time.Hour).UnixNano()))); cached {
			t.Fatal("ExistingPartitionPathsVerdict cached a stale answer")
		}
	})

	refused := func(t *testing.T, label string, got filterRun) {
		t.Helper()
		if got.paths != nil || got.verified || !got.failed {
			t.Fatalf("%s: want the query refused (unknown + failed): %+v", label, got)
		}
	}

	t.Run("default: refused as before", func(t *testing.T) {
		s, p, paths, _ := staleSetup(t, layout, ListingOptions{RecentDays: 2})
		s.failOn = failMonth
		refused(t, "stale reuse off", runFilter(context.Background(), p, false, paths, base, 0))
	})

	t.Run("never for recent days", func(t *testing.T) {
		s, p, paths, _ := staleSetup(t, layout, on)
		s.failOn = func(p string) bool { return p == today }
		refused(t, "today", runFilter(context.Background(), p, false, paths, base, 0))
	})

	t.Run("never for yesterday, even with one recent day", func(t *testing.T) {
		// 11-09 compacted, so only its age keeps it from being reused.
		keys := append(append([]string(nil), layout...), "posthog/events/2026/11/09/events_daily.parquet")
		s, p, paths, _ := staleSetup(t, keys, ListingOptions{RecentDays: 1, StaleReuse: 10 * time.Minute})
		s.failOn = func(p string) bool { return p == month }
		refused(t, "month holding 11-09", runFilter(context.Background(), p, false, paths, base, 0))
	})

	t.Run("only within the age", func(t *testing.T) {
		s, p, paths, _ := staleSetup(t, layout, on)
		ageGlobCache(p, 9*time.Minute) // 11 minutes old
		s.failOn = failMonth
		refused(t, "too old", runFilter(context.Background(), p, false, paths, base, 0))
	})

	t.Run("never for a freshness proof", func(t *testing.T) {
		s, p, paths, _ := staleSetup(t, layout, on)
		s.failOn = failMonth
		refused(t, "maxAge", runFilter(context.Background(), p, false, paths, base, emptyProofFreshness))
	})

	t.Run("dropped with the listing caches", func(t *testing.T) {
		// The "No files found" retry (a stale listing named an hour folder a
		// compaction removed) drops the caches: it must re-list, not reuse.
		s, p, paths, _ := staleSetup(t, layout, on)
		p.InvalidateGlobCache()
		s.failOn = failMonth
		refused(t, "after invalidate", runFilter(context.Background(), p, false, paths, base, 0))
	})

	t.Run("not for a day waiting for daily compaction", func(t *testing.T) {
		// 11-06 holds hour folders and no day file: its compaction can upload
		// the day file and hide the hour files at any moment, and a reused
		// listing would not read the day file.
		var keys []string
		for _, k := range layout {
			if !strings.HasPrefix(k, "posthog/events/2026/11/06/") {
				keys = append(keys, k)
			}
		}
		keys = append(keys, "posthog/events/2026/11/06/07/a.parquet", "posthog/events/2026/11/06/08/b.parquet")
		s, p, paths, _ := staleSetup(t, keys, on)
		s.failOn = failMonth
		refused(t, "uncompacted day", runFilter(context.Background(), p, false, paths, base, 0))
	})

	t.Run("month timeout: reused days skip the per-day retry", func(t *testing.T) {
		var keys []string
		for _, k := range layout {
			if !strings.HasPrefix(k, "posthog/events/2026/11/06/") {
				keys = append(keys, k)
			}
		}
		keys = append(keys, "posthog/events/2026/11/06/07/a.parquet")
		s, p, paths, first := staleSetup(t, keys, on)
		p.SetListingTimeout(100 * time.Millisecond)
		s.hangOn = func(p string) bool { return p == month }
		before := len(s.listedPrefixes())
		got := runFilter(context.Background(), p, false, paths, base, 0)
		if !reflect.DeepEqual(got.paths, first.paths) || !got.verified || got.failed {
			t.Fatalf("want the first answer, verified: %+v", got)
		}
		var days []string
		for _, pfx := range s.listedPrefixes()[before:] {
			if isDayDir(pfx) {
				days = append(days, pfx)
			}
		}
		sort.Strings(days)
		want := []string{"posthog/events/2026/11/06/", "posthog/events/2026/11/09/", "posthog/events/2026/11/10/"}
		if !reflect.DeepEqual(days, want) {
			t.Fatalf("day listings after the month timed out: %v, want only the uncompacted day and the recent days %v", days, want)
		}
	})
}

// Stale reuse keeps a listing past its TTL for the reuse age; a normal read
// still treats it as expired, and the default keeps the old eviction.
func TestGlobCacheStaleRetention(t *testing.T) {
	c := newGlobCache(20 * time.Millisecond)
	c.retain = time.Minute
	c.set("k", []string{"v"})
	time.Sleep(40 * time.Millisecond)
	if _, ok := c.get("k"); ok {
		t.Fatal("expired entry served as fresh")
	}
	if n := c.cleanup(); n != 0 {
		t.Fatalf("cleanup removed %d retained entries", n)
	}
	if v, age, ok := c.getStale("k", time.Minute); !ok || !reflect.DeepEqual(v, []string{"v"}) || age < 40*time.Millisecond {
		t.Fatalf("getStale = %v %v %v", v, age, ok)
	}
	if _, _, ok := c.getStale("k", 10*time.Millisecond); ok {
		t.Fatal("getStale returned an entry older than maxAge")
	}

	d := newGlobCache(20 * time.Millisecond)
	d.set("k", []string{"v"})
	time.Sleep(40 * time.Millisecond)
	if _, _, ok := d.getStale("k", time.Minute); ok {
		t.Fatal("without retention an expired entry must be gone")
	}
	if n := d.cleanup(); n != 1 {
		t.Fatalf("cleanup removed %d, want 1", n)
	}
}

func TestSetListingOptions(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	if p.recentDays != 0 || p.singleFlight || p.staleReuse != 0 || p.globCache.retain != 0 {
		t.Fatalf("defaults: %d %v %v %v", p.recentDays, p.singleFlight, p.staleReuse, p.globCache.retain)
	}
	p.SetListingOptions(ListingOptions{RecentDays: 2, SingleFlight: true, StaleReuse: 30 * time.Minute})
	if p.recentDays != 2 || !p.singleFlight || p.staleReuse != 30*time.Minute || p.globCache.retain != 30*time.Minute {
		t.Fatalf("set: %d %v %v %v", p.recentDays, p.singleFlight, p.staleReuse, p.globCache.retain)
	}
	if got := StaleReuseSeconds(1 << 40); got != MaxListingStaleReuse {
		t.Fatalf("StaleReuseSeconds cap = %v", got)
	}
	if StaleReuseSeconds(0) != 0 || StaleReuseSeconds(-5) != 0 || StaleReuseSeconds(90) != 90*time.Second {
		t.Fatal("StaleReuseSeconds conversion")
	}
}
