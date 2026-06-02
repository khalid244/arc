package rollup

import (
	"strings"
	"time"
)

// tsLayouts are the timestamp literal forms we accept for window bounds.
var tsLayouts = []string{
	"2006-01-02 15:04:05-07",
	"2006-01-02 15:04:05+00",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02", // bare DATE literal (midnight UTC) — Grafana/DuckDB emit these
}

func parseTS(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, l := range tsLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func fmtTS(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05+00") }

// grainDur returns the fixed UTC duration of a grain. Week/month are not fixed
// durations; alignment for those isn't needed because cubes are built at hour
// grain (the finest), and re-bucketing up uses date_trunc, not literal bounds.
func grainDur(grain string) time.Duration {
	if n, ok := grainSeconds(grain); ok && n > 0 {
		return time.Duration(n) * time.Second
	}
	return time.Hour
}

// alignUp returns the smallest grain-aligned instant >= t (ceil).
func alignUp(t time.Time, grain string) time.Time {
	d := grainDur(grain)
	tr := t.Truncate(d)
	if tr.Equal(t) {
		return tr
	}
	return tr.Add(d)
}

// alignDown returns the largest grain-aligned instant <= t (floor).
func alignDown(t time.Time, grain string) time.Time { return t.Truncate(grainDur(grain)) }
