package rollup

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRouter_PublishRace is the race-detector gate for the manifest-publication
// fix. It reproduces the production topology that raced before the fix:
//
//   - ONE build-owned *Manifest is mutated IN PLACE on a build goroutine exactly
//     as the Manager does — Upsert (append + sort.Slice), the `Days = kept`
//     filter (compaction/purge), and a re-publish through NewRouter — while
//   - many reader goroutines drive the lock-free read path against the PUBLISHED
//     router: DaysInRange / Coverage / HasInteriorGap / Route, the same calls a
//     live query makes.
//
// Before the fix, NewRouter stored the build-owned pointer directly, so the
// router served the very slice the build goroutine was append/sort/filter-ing —
// concurrent slice read + slice mutate, an unsynchronized Go data race that
// `go test -race` flags (torn reads, possible panic). After the fix, NewRouter
// deep-copies via clone(): the router serves only frozen snapshots, so the build
// mutating its own working copy can never touch what a reader iterates. This test
// MUST be clean under -race; that is the primary proof the publication is
// race-free without any lock on the per-query path.
func TestRouter_PublishRace(t *testing.T) {
	spec := CubeSpec{
		Source: "default.downloads", Grain: "hour", Dims: []string{"status"},
		Aggs: []Aggregate{
			{Kind: AggCount},
			{Kind: AggCondSum, Cond: `"status" = 200`, CondCols: []string{"status"}, ThenK: "1", ElseK: "0", FromCount: true},
		},
	}
	key := cubeKeyOf(spec)

	// The build's working manifest — the single mutable object the build owns and
	// mutates in place, exactly like cb.man / man in the Manager.
	build := &Manifest{
		CubeID: "downloads_status", Source: spec.Source, Grain: spec.Grain,
		Dims: spec.Dims, Aggs: spec.Aggs, SchemaHash: spec.SchemaHash(),
	}
	base := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	dayEntry := func(i int) DayEntry {
		d := base.AddDate(0, 0, i)
		lo := d.Format("2006-01-02 15:04:05+00")
		hi := d.AddDate(0, 0, 1).Format("2006-01-02 15:04:05+00")
		date := d.Format("2006-01-02")
		return DayEntry{
			Date: date, URI: "s3://b/" + date + ".parquet",
			SchemaHash: spec.SchemaHash(), BucketLo: lo, BucketHi: hi, Rows: int64(100 + i),
			Covers: []string{date}, // a nested slice the clone must also deep-copy
		}
	}
	// Seed several days so DaysInRange / Coverage / HasInteriorGap have real work.
	for i := 0; i < 8; i++ {
		build.Upsert(dayEntry(i))
	}

	noWM := func(string) string { return "" } // fully sealed: cube-only read path
	srcExpr := func(string) string { return "['s3://b/src/**/*.parquet']" }

	// published holds the current router, swapped under a mutex exactly like the
	// Manager's updateRouter -> rebuildRouterLocked. Readers take a snapshot of the
	// pointer under RLock (the only synchronization the read path needs); once they
	// hold the *Router they touch its frozen clones with no further lock.
	var mu sync.RWMutex
	published := NewRouter([]*Manifest{build}, "time", srcExpr, noWM)
	getRouter := func() *Router {
		mu.RLock()
		defer mu.RUnlock()
		return published
	}
	republish := func() {
		r := NewRouter([]*Manifest{build}, "time", srcExpr, noWM)
		mu.Lock()
		published = r
		mu.Unlock()
	}

	const (
		readers = 8
		dur     = 400 * time.Millisecond
	)
	var stop atomic.Bool
	var wg sync.WaitGroup

	// Build goroutine: mutate the build-owned manifest IN PLACE — the exact set of
	// mutations the Manager performs — and re-publish through NewRouter.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 8
		for !stop.Load() {
			// Upsert: append + sort.Slice on build.Days (manifest.go).
			build.Upsert(dayEntry(i % 40))
			i++
			// `Days = kept` filter: the compaction/purge in-place rewrite (manager.go).
			if len(build.Days) > 12 {
				kept := build.Days[:0]
				for j, d := range build.Days {
					if j%3 != 0 { // drop every third — mimics purge/compact dropping entries
						kept = append(kept, d)
					}
				}
				build.Days = kept
			}
			// Mutate a nested Covers slice in place (compactMonth append pattern).
			if len(build.Days) > 0 {
				build.Days[0].Covers = append(build.Days[0].Covers, fmt.Sprintf("x%d", i))
			}
			republish()
		}
	}()

	// Reader goroutines: the lock-free read path against the published snapshot.
	const lo = "2025-12-03 00:00:00+00"
	const hi = "2025-12-20 00:00:00+00"
	const querySQL = `SELECT status, COUNT(*) FROM downloads ` +
		`WHERE time >= TIMESTAMPTZ '2025-12-03' AND time < TIMESTAMPTZ '2025-12-20' GROUP BY status`
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				rt := getRouter()
				m := rt.Manifests[key]
				if m == nil {
					continue
				}
				_ = m.DaysInRange(lo, hi)
				_, _, _ = m.Coverage()
				_ = m.HasInteriorGap(lo, hi)
				_ = m.BuiltDays()
				_ = rt.Route(querySQL) // full parse + prune + emit
			}
		}()
	}

	time.Sleep(dur)
	stop.Store(true)
	wg.Wait()
}
