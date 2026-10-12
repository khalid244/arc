package rollup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Rebuild limits (rebuild_limit.go): a CHANGED day is rebuilt at most every
// limit-for-its-age; days that were never built, lost their cube file, or have no
// fingerprint never wait.

func TestParseRebuildLimits(t *testing.T) {
	got, err := parseRebuildLimits(" 30d:72h, 1d:4h ,7d:24h")
	if err != nil {
		t.Fatal(err)
	}
	want := []rebuildLimit{{1, 4 * time.Hour}, {7, 24 * time.Hour}, {30, 72 * time.Hour}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v (sorted by age)", got, want)
	}
	if got, err := parseRebuildLimits(""); err != nil || got != nil {
		t.Errorf("empty: got %v, %v; want no limits", got, err)
	}
	for _, bad := range []string{"1:4h", "1d", "0d:4h", "-1d:4h", "xd:4h", "1d:4", "1d:0h", "1d:-2h", "1d:4h,1d:5h", "1d:4h,,7d:1h"} {
		if _, err := parseRebuildLimits(bad); err == nil {
			t.Errorf("%q: want an error", bad)
		}
	}
}

func TestCubeFileBuildTime(t *testing.T) {
	at := time.Date(2026, 10, 12, 19, 1, 37, 123, time.UTC)
	uri := fmt.Sprintf("s3://b/_arc/rollup/posthog/events/by_event/2026-10-11_%d.parquet", at.UnixNano())
	if got, ok := cubeFileBuildTime(uri); !ok || !got.Equal(at) {
		t.Errorf("got %v %v, want %v", got, ok, at)
	}
	for _, legacy := range []string{"s3://b/x/2026-10-11.parquet", "s3://b/x/2026-10-11_abc.parquet", ""} {
		if _, ok := cubeFileBuildTime(legacy); ok {
			t.Errorf("%q: legacy/unparseable names have no build time", legacy)
		}
	}
}

// limitFixture: built recent days, with limits set and the clock under control.
func limitFixture(t *testing.T, n int, limits string) (*Manager, *localListStorage, []CubeSpec, []string) {
	t.Helper()
	m, stg, specs, dates := passFixture(t, n)
	l, err := parseRebuildLimits(limits)
	if err != nil {
		t.Fatal(err)
	}
	m.rebuildLimits = l
	m.nowFn = time.Now
	pass(t, m, stg, dates)
	return m, stg, specs, dates
}

func addLateRow(t *testing.T, m *Manager, root, date, name string) {
	t.Helper()
	addSourceFile(t, m, root, date, name, fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 23:50:00' AS "time", 'web' AS site`, date))
}

func TestRebuildLimitDefersThenRebuilds(t *testing.T) {
	m, stg, specs, dates := limitFixture(t, 2, "30d:1h")
	d := dates[0]
	before := uriSnapshot(t, m, specs, dates)
	addLateRow(t, m, stg.root, d, "late.parquet")

	if w := pass(t, m, stg, dates); w != 0 {
		t.Fatalf("changed day inside its limit: pass wrote %d manifests, want 0 (deferred)", w)
	}
	if after := uriSnapshot(t, m, specs, dates); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatal("changed day rebuilt although its cube is younger than the limit")
	}

	m.nowFn = func() time.Time { return time.Now().Add(61 * time.Minute) }
	pass(t, m, stg, dates)
	for _, s := range specs {
		if got, want := cubeCount(t, m, s, d), sourceCount(t, m, stg.root, d); got != want {
			t.Errorf("%s: after the limit passed, count %d, want %d", CubeID(s), got, want)
		}
		if entryURI(t, m, s, dates[1]) != before[CubeID(s)+"@"+dates[1]] {
			t.Errorf("%s: unchanged day rebuilt", CubeID(s))
		}
	}
}

func TestRebuildLimitTiersByAge(t *testing.T) {
	// recentDates(3) = 4, 3 and 2 days old. Days <= 2 days old: 1h; older: 5h.
	m, stg, specs, dates := limitFixture(t, 3, "2d:1h, 30d:5h")
	newest, oldest := dates[2], dates[0]
	addLateRow(t, m, stg.root, newest, "late.parquet")
	addLateRow(t, m, stg.root, oldest, "late.parquet")

	m.nowFn = func() time.Time { return time.Now().Add(2 * time.Hour) }
	pass(t, m, stg, dates)
	coarse := specs[0]
	if got, want := cubeCount(t, m, coarse, newest), sourceCount(t, m, stg.root, newest); got != want {
		t.Errorf("2-day-old day (1h limit) after 2h: count %d, want %d (rebuilt)", got, want)
	}
	if got, want := cubeCount(t, m, coarse, oldest), sourceCount(t, m, stg.root, oldest)-1; got != want {
		t.Errorf("4-day-old day (5h limit) after 2h: count %d, want the old %d (still deferred)", got, want)
	}
}

// Days without a fingerprint (entries from before fingerprints, a listing that
// failed, glob fallbacks) are limited like any other — else they would rebuild
// on every tick.
func TestRebuildLimitAppliesWithoutFingerprint(t *testing.T) {
	ctx := context.Background()
	t.Run("no fingerprint recorded", func(t *testing.T) {
		m, stg, specs, dates := limitFixture(t, 1, "30d:1h")
		for _, s := range specs {
			man, _ := m.loadManifest(ctx, s)
			for i := range man.Days {
				man.Days[i].SourceSig = ""
			}
			b, _ := json.Marshal(man)
			if err := stg.fakeStorage.Write(ctx, m.manifestKey(s), b); err != nil {
				t.Fatal(err)
			}
		}
		if w := pass(t, m, stg, dates); w != 0 {
			t.Errorf("recently built day without a fingerprint: pass wrote %d manifests, want 0 (deferred)", w)
		}
		m.nowFn = func() time.Time { return time.Now().Add(time.Hour) }
		if w := pass(t, m, stg, dates); w == 0 {
			t.Error("after the limit the unfingerprinted day must rebuild")
		}
	})
	t.Run("listing failed", func(t *testing.T) {
		m, stg, _, dates := limitFixture(t, 1, "30d:1h")
		stg.failList = true
		if w := pass(t, m, stg, dates); w != 0 {
			t.Errorf("listing failed on a recently built day: pass wrote %d manifests, want 0 (deferred)", w)
		}
	})
}

// A changed day gets one last rebuild in its final finalRebuildMargin inside the
// window, whatever its limit — late data is never left out for good.
func TestRebuildLimitFinalRebuildBeforeLeavingWindow(t *testing.T) {
	m, stg, specs, dates := limitFixture(t, 1, "30d:72h")
	d := dates[0] // 2 days old
	m.cfg.RebuildDays = 4
	addLateRow(t, m, stg.root, d, "late.parquet")
	// The day leaves the window at d + 4 days + grace; its final 24h start a day earlier.
	exit := day(d).Add(4*24*time.Hour + m.cfg.Grace)
	m.nowFn = func() time.Time { return exit.Add(-finalRebuildMargin - time.Hour) }
	if w := pass(t, m, stg, dates); w != 0 {
		t.Fatalf("before the final margin a changed day inside its 72h limit must wait (wrote %d)", w)
	}
	m.nowFn = func() time.Time { return exit.Add(-finalRebuildMargin + time.Hour) }
	pass(t, m, stg, dates)
	if got, want := cubeCount(t, m, specs[0], d), sourceCount(t, m, stg.root, d); got != want {
		t.Errorf("in the final margin: count %d, want %d (final rebuild)", got, want)
	}
}

// A build time ahead of the clock (the clock stepped back) never defers.
func TestRebuildLimitFutureBuildTimeNeverDefers(t *testing.T) {
	m, stg, specs, dates := limitFixture(t, 1, "30d:72h")
	addLateRow(t, m, stg.root, dates[0], "late.parquet")
	m.nowFn = func() time.Time { return time.Now().Add(-2 * time.Hour) }
	pass(t, m, stg, dates)
	if got, want := cubeCount(t, m, specs[0], dates[0]), sourceCount(t, m, stg.root, dates[0]); got != want {
		t.Errorf("cube built 'in the future': count %d, want %d (rebuilt, not deferred for days)", got, want)
	}
}

// The limit is compared against interval - ForwardTick/2, so a day comes due on
// the tick nearest its limit, not one tick after.
func TestRebuildLimitTickSlack(t *testing.T) {
	m, stg, specs, dates := limitFixture(t, 1, "30d:1h")
	m.cfg.ForwardTick = 30 * time.Minute
	addLateRow(t, m, stg.root, dates[0], "late.parquet")
	m.nowFn = func() time.Time { return time.Now().Add(50 * time.Minute) }
	pass(t, m, stg, dates)
	if got, want := cubeCount(t, m, specs[0], dates[0]), sourceCount(t, m, stg.root, dates[0]); got != want {
		t.Errorf("50 min into a 1h limit with 30 min ticks: count %d, want %d (due)", got, want)
	}
}

// If any cube of a day is due, every changed cube of that day is rebuilt with
// it, so the day's cubes stay on one scan and one point in time.
func TestRebuildLimitRebuildsWholeDayTogether(t *testing.T) {
	ctx := context.Background()
	m, stg, specs, dates := limitFixture(t, 1, "30d:1h")
	d := dates[0]
	// Make cube 0's file look 2h old (due); cube 1 stays fresh (deferred alone).
	man, _ := m.loadManifest(ctx, specs[0])
	for i := range man.Days {
		if man.Days[i].Date == d {
			old := man.Days[i].URI
			aged := fmt.Sprintf("%s_%d.parquet", old[:strings.LastIndex(old, "_")], time.Now().Add(-2*time.Hour).UnixNano())
			if err := os.Rename(old, aged); err != nil {
				t.Fatal(err)
			}
			man.Days[i].URI = aged
		}
	}
	b, _ := json.Marshal(man)
	if err := stg.fakeStorage.Write(ctx, m.manifestKey(specs[0]), b); err != nil {
		t.Fatal(err)
	}
	addLateRow(t, m, stg.root, d, "late.parquet")
	pass(t, m, stg, dates)
	for _, s := range specs {
		if got, want := cubeCount(t, m, s, d), sourceCount(t, m, stg.root, d); got != want {
			t.Errorf("%s: count %d, want %d — the day's cubes must rebuild together", CubeID(s), got, want)
		}
	}
}

func TestRebuildLimitNeverDelays(t *testing.T) {
	ctx := context.Background()

	t.Run("missing cube file", func(t *testing.T) {
		m, stg, specs, dates := limitFixture(t, 1, "30d:72h")
		addLateRow(t, m, stg.root, dates[0], "late.parquet")
		if err := os.Remove(entryURI(t, m, specs[1], dates[0])); err != nil {
			t.Fatal(err)
		}
		pass(t, m, stg, dates)
		if got, want := cubeCount(t, m, specs[1], dates[0]), sourceCount(t, m, stg.root, dates[0]); got != want {
			t.Errorf("lost cube file: count %d, want %d (rebuilt despite the limit)", got, want)
		}
	})

	t.Run("never built", func(t *testing.T) {
		m, stg, _, dates := limitFixture(t, 1, "30d:72h")
		extra := CubeSpec{Source: freshSrc, Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Col: "value"}}}
		mkCubeDir(t, stg.root, extra)
		man, _ := m.loadManifest(ctx, extra)
		cb := m.newCubeBuild(extra, man, nil, time.Time{})
		m.tickBuilt = nil
		m.buildExactDays(ctx, freshSrc, []*cubeBuild{cb}, daysOf(dates), time.Time{}, needPass)
		if entryURI(t, m, extra, dates[0]) == "" {
			t.Error("a day never built must not wait for a rebuild limit")
		}
	})
}

func TestFileNonceUnique(t *testing.T) {
	seen := map[int64]bool{}
	start := time.Now()
	for i := 0; i < 100000; i++ {
		n := fileNonce()
		if seen[n] {
			t.Fatalf("nonce %d handed out twice", n)
		}
		seen[n] = true
	}
	if last := time.Unix(0, fileNonce()); last.Sub(start) > time.Minute || last.Before(start) {
		t.Errorf("nonce %v drifted from the wall clock (start %v)", last, start)
	}
}
