package rollup

import (
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Rebuild limits.
//
// Source fingerprints rebuild a day whenever its files changed. Some sources get
// late files into EVERY recent day all the time — posthog.events in prod: the
// late-event reorganizer writes two ~80 KB files into each of the last ~30 days
// every 2h — so every day always looks changed and every pass rebuilds the whole
// window (~48 full-day rebuilds a day, each re-reading ~2.3 GB to absorb ~0.01%
// new rows). A rebuild limit caps how often a CHANGED day is rebuilt, by the
// day's age: "1d:4h, 7d:24h, 30d:72h" rebuilds yesterday at most every 4h, days
// up to a week old at most daily, older days at most every 3 days. Raw-mode
// queries are unaffected; cube-served queries show late rows up to the limit
// later.
//
// The limit only ever DEFERS a rebuild of a day whose cube file exists. It never
// delays a day that was never built (the fresh step's newly sealed day, a new
// cube) or whose cube file vanished. A cube's build time is the nonce in its file
// name (cubeDayURI: "<date>_<unixnano>.parquet"), so no manifest field is needed.
//
//   - Final rebuild: a changed day not rebuilt during its last finalRebuildMargin
//     in the rebuild window is rebuilt regardless, so late data is never left out
//     for good when a day ages out of the window.
//   - Per day: if any cube of a day is due, every changed cube of that day is
//     rebuilt with it (buildExactDays) — one source scan, one point in time —
//     instead of the day's ~200 cubes drifting into separate scans.
//   - The limit is compared against interval - ForwardTick/2: the build time is
//     stamped after the check, so a strict comparison would land one tick late.
//   - A build time in the future (clock stepped back) never defers.

// finalRebuildMargin is how long before a day leaves the rebuild window its last
// rebuild is allowed regardless of its limit.
const finalRebuildMargin = 24 * time.Hour

// ValidateRebuildLimits reports whether a rollup.rebuild_limits value is valid
// (startup refuses an invalid one rather than silently running without limits).
func ValidateRebuildLimits(s string) error {
	_, err := parseRebuildLimits(s)
	return err
}

// rebuildLimit: days at most maxAgeDays old are rebuilt at most every interval.
type rebuildLimit struct {
	maxAgeDays int
	interval   time.Duration
}

// parseRebuildLimits parses "1d:4h, 7d:24h, 30d:72h" (age in whole days, then a
// Go duration). Empty means no limits. Tiers come back sorted by age.
func parseRebuildLimits(s string) ([]rebuildLimit, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var out []rebuildLimit
	seen := map[int]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		age, dur, ok := strings.Cut(part, ":")
		if !ok || !strings.HasSuffix(age, "d") {
			return nil, fmt.Errorf("rebuild limit %q: want <days>d:<duration>, e.g. 7d:24h", part)
		}
		days, err := strconv.Atoi(strings.TrimSuffix(age, "d"))
		if err != nil || days < 1 {
			return nil, fmt.Errorf("rebuild limit %q: age must be a whole number of days >= 1", part)
		}
		interval, err := time.ParseDuration(strings.TrimSpace(dur))
		if err != nil || interval <= 0 {
			return nil, fmt.Errorf("rebuild limit %q: interval must be a positive duration like 4h", part)
		}
		if seen[days] {
			return nil, fmt.Errorf("rebuild limit %q: age %dd listed twice", part, days)
		}
		seen[days] = true
		out = append(out, rebuildLimit{maxAgeDays: days, interval: interval})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].maxAgeDays < out[j].maxAgeDays })
	return out, nil
}

// rebuildIntervalFor returns the limit for a day of the given date, if any:
// the first tier whose age covers it (yesterday is 1 day old).
func (m *Manager) rebuildIntervalFor(date string, now time.Time) (time.Duration, bool) {
	d, err := time.Parse("2006-01-02", date)
	if err != nil {
		return 0, false
	}
	age := int(now.UTC().Truncate(24*time.Hour).Sub(d) / (24 * time.Hour))
	for _, l := range m.rebuildLimits {
		if age <= l.maxAgeDays {
			return l.interval, true
		}
	}
	return 0, false
}

// lastNonce backs fileNonce.
var lastNonce atomic.Int64

// fileNonce is the build-time nonce in cube file names: the current Unix time in
// nanoseconds, bumped by 1 if needed so it is unique within the process. A
// wall clock with coarser resolution (macOS: 1µs) would otherwise hand two
// back-to-back builds the same name — one file overwriting the other in place.
func fileNonce() int64 {
	for {
		prev, now := lastNonce.Load(), time.Now().UnixNano()
		if now <= prev {
			now = prev + 1
		}
		if lastNonce.CompareAndSwap(prev, now) {
			return now
		}
	}
}

// cubeFileBuildTime reads a cube file's build time from its name
// ("<date>_<unixnano>.parquet"). Legacy fixed names ("<date>.parquet") have none.
func cubeFileBuildTime(uri string) (time.Time, bool) {
	name := strings.TrimSuffix(path.Base(uri), ".parquet")
	i := strings.LastIndexByte(name, '_')
	if i < 0 {
		return time.Time{}, false
	}
	n, err := strconv.ParseInt(name[i+1:], 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, n), true
}

// rebuildDeferred reports whether a CHANGED day must wait: its cube file was
// built less than the day's rebuild limit ago.
func (m *Manager) rebuildDeferred(cb *cubeBuild, date string) bool {
	if len(m.rebuildLimits) == 0 || !cb.built[date] || cb.missing[date] {
		return false
	}
	e, ok := cb.man.dailyEntry(date)
	if !ok || e.URI == "" {
		return false // an empty marker or no daily entry: nothing to keep serving
	}
	builtAt, ok := cubeFileBuildTime(e.URI)
	if !ok {
		return false
	}
	now := m.now()
	interval, ok := m.rebuildIntervalFor(date, now)
	if !ok {
		return false
	}
	if d, err := time.Parse("2006-01-02", date); err == nil {
		exit := d.Add(time.Duration(m.cfg.RebuildDays)*24*time.Hour + m.cfg.Grace)
		if final := exit.Add(-finalRebuildMargin); !now.Before(final) && builtAt.Before(final) {
			return false // last chance before the day leaves the window
		}
	}
	age := now.Sub(builtAt)
	return age >= 0 && age < interval-m.cfg.ForwardTick/2
}
