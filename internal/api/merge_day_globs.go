package api

import (
	"context"
	"strconv"
	"strings"
	"time"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// Merged day globs (query.merge_day_globs).
//
// A converted query names each day partition as its own glob
// (db/m/YYYY/MM/DD/*.parquet) and DuckDB lists a day folder for each, one LIST
// after another. With every data block cached that is nearly all of a query's
// time on a slow object store. Measured on prod 2026-11-17, a LIST then taking
// about 1.2 s: one cached day 2.3 s with its LIST and 0.02 s without, a 60-day
// count 75 s. Listing them several at once (glob_prewarm_parallelism) stops
// helping at that speed, because the listings of a long range take longer
// than they are kept (s3_glob_cache_ttl_seconds) and are made again.
//
// mergeDayGlobs sends the days of a month that share their first digit as one
// glob, .../MM/0[1234]/*.parquet, and a whole month as .../MM/[0-3][0-9]/
// *.parquet. DuckDB lists by the text before the first wildcard, so each
// merged glob is one LIST (.../MM/0), and it matches exactly the files the day
// globs matched: 60 days take about 8 LISTs instead of 60.
// Everything else is DuckDB's as before: the file sizes come with the listing
// (naming the files instead costs a HEAD request per file), a listing is kept
// for s3_glob_cache_ttl_seconds, compaction inputs are hidden by file name and
// a file deleted meanwhile is re-listed and retried.
//
// The LIST behind a merged glob returns every key under its prefix, so a group
// is left alone when that would reach a day that may still hold raw files
// (the newest days: thousands of small files in hour folders).

// SetMergeDayGlobs turns the option on or off. recentDays is how many of the
// newest days (UTC, counting today) may hold raw files; at least two are
// assumed.
func (h *QueryHandler) SetMergeDayGlobs(on bool, recentDays int) {
	h.dayGlobMerge = on
	h.dayGlobRecentDays = max(recentDays, 2)
}

// listAhead does the listing work of q before it runs and returns the SQL to
// run: day globs are merged (mergeDayGlobsIn) and the globs then left are
// listed several at once (prewarmGlobs).
func (h *QueryHandler) listAhead(ctx context.Context, q string) string {
	q = h.mergeDayGlobsIn(q)
	h.prewarmGlobs(ctx, q)
	return q
}

// mergeDayGlobsIn is mergeDayGlobs for this handler's option and clock.
func (h *QueryHandler) mergeDayGlobsIn(sql string) string {
	if !h.dayGlobMerge {
		return sql
	}
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return mergeDayGlobs(sql, today.AddDate(0, 0, 1-h.dayGlobRecentDays))
}

// dayGlob is a day-level partition glob of a remote store.
type dayGlob struct {
	month   string // everything up to and including "YYYY/MM/"
	y, m, d int
}

// parseDayGlob reads lit as <remote root>/db/measurement/YYYY/MM/DD/*.parquet.
func parseDayGlob(lit string) (dayGlob, bool) {
	dir, ok := strings.CutSuffix(lit, "/*.parquet")
	scheme := strings.Index(dir, "://")
	if !ok || scheme < 0 || strings.ContainsAny(dir, "*?[") {
		return dayGlob{}, false
	}
	remote := false
	for _, s := range remoteGlobSchemes {
		remote = remote || strings.HasPrefix(lit, s)
	}
	parts := strings.Split(dir[scheme+3:], "/")
	n := len(parts)
	// bucket, database, measurement, year, month, day at least.
	if !remote || n < 6 || len(parts[n-3]) != 4 || len(parts[n-2]) != 2 || len(parts[n-1]) != 2 {
		return dayGlob{}, false
	}
	y, errY := strconv.Atoi(parts[n-3])
	m, errM := strconv.Atoi(parts[n-2])
	d, errD := strconv.Atoi(parts[n-1])
	if errY != nil || errM != nil || errD != nil || m < 1 || m > 12 || d < 1 || d > daysIn(y, m) {
		return dayGlob{}, false
	}
	return dayGlob{month: dir[:len(dir)-2], y: y, m: m, d: d}, true
}

func daysIn(y, m int) int {
	return time.Date(y, time.Month(m)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// mergeDayGlobs merges the day globs in the path list of every read_parquet
// call of sql, month by month: all the days of a month become one glob,
// otherwise the days that share their first digit do. The merged glob stands
// where the first of its days stood (the pruner emits the days in no fixed
// order). A group is kept as it is when it has a single day, when the last
// day its LIST would cover is on or after protectedFrom (zero: none is), or
// when its month names a day twice. Text that is not such a list is kept byte
// for byte.
func mergeDayGlobs(sql string, protectedFrom time.Time) string {
	safe := func(g dayGlob, lastDay int) bool {
		return protectedFrom.IsZero() || time.Date(g.y, time.Month(g.m), lastDay, 0, 0, 0, 0, time.UTC).Before(protectedFrom)
	}
	return sqlutil.WrapReadParquetCalls(sql, func(call string, _ int) string {
		lits, lead, rest, ok := readParquetPathList(call)
		if !ok {
			return call
		}
		globs := make([]dayGlob, len(lits))
		isDay := make([]bool, len(lits))
		days := make(map[string]map[int]bool) // month -> its days in the list
		twice := make(map[string]bool)
		for i, l := range lits {
			if globs[i], isDay[i] = parseDayGlob(l); !isDay[i] {
				continue
			}
			g := globs[i]
			if days[g.month] == nil {
				days[g.month] = make(map[int]bool)
			}
			twice[g.month] = twice[g.month] || days[g.month][g.d]
			days[g.month][g.d] = true
		}
		// pattern returns the glob that replaces g, or "" to keep it.
		pattern := func(g dayGlob) string {
			in := days[g.month]
			last := daysIn(g.y, g.m)
			if twice[g.month] {
				return ""
			}
			if len(in) == last && safe(g, last) {
				return g.month + "[0-3][0-9]/*.parquet"
			}
			tens := g.d / 10
			var units strings.Builder
			for u := 0; u <= 9; u++ {
				if in[tens*10+u] {
					units.WriteByte(byte('0' + u))
				}
			}
			if units.Len() < 2 || !safe(g, min(tens*10+9, last)) {
				return ""
			}
			return g.month + strconv.Itoa(tens) + "[" + units.String() + "]/*.parquet"
		}
		out := make([]string, 0, len(lits))
		done := make(map[string]bool)
		merged := false
		for i, l := range lits {
			p := ""
			if isDay[i] {
				p = pattern(globs[i])
			}
			switch {
			case p == "":
				out = append(out, "'"+l+"'")
			case !done[p]:
				done[p] = true
				merged = true
				out = append(out, "'"+p+"'")
			}
		}
		if !merged {
			return call
		}
		return call[:lead] + "[" + strings.Join(out, ", ") + "]" + rest
	})
}

// readParquetPathList splits a read_parquet call whose first argument is a
// list of plain string literals: the literals, the length of the text before
// the list, and the text after it. ok is false for any other first argument.
func readParquetPathList(call string) (lits []string, lead int, rest string, ok bool) {
	arg := strings.TrimLeft(call[strings.IndexByte(call, '(')+1:], " \t\n")
	lead = len(call) - len(arg)
	if !strings.HasPrefix(arg, "[") {
		return nil, 0, "", false
	}
	rest = arg[1:]
	for {
		rest = strings.TrimLeft(rest, " \t\n")
		if !strings.HasPrefix(rest, "'") {
			return nil, 0, "", false
		}
		end := strings.IndexByte(rest[1:], '\'')
		if end < 0 || strings.HasPrefix(rest[end+2:], "'") {
			return nil, 0, "", false // unterminated, or an escaped quote: not a storage path
		}
		lits = append(lits, rest[1:end+1])
		rest = strings.TrimLeft(rest[end+2:], " \t\n")
		if strings.HasPrefix(rest, "]") {
			return lits, lead, rest[1:], true
		}
		if !strings.HasPrefix(rest, ",") {
			return nil, 0, "", false
		}
		rest = rest[1:]
	}
}
