package pruning

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

var gateNow = time.Date(2026, 11, 20, 9, 30, 0, 0, time.UTC)

func gateT(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// Each of these used to get a range narrower than the rows the query reads
// (rows silently missing from a successful answer). Every one now gets no
// range, or one that covers the query.
func TestTimeRangeNeverNarrowerThanTheQuery(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	none := []string{
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' OR host = 'a'",
		"SELECT * FROM db.cpu WHERE host = 'a' OR time >= '2026-10-20 10:00:00' AND host = 'b'",
		"SELECT * FROM db.cpu WHERE downtime >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE end_time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE timestamp >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE ts::timestamp >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE NOT (time >= '2026-10-20 10:00:00')",
		"SELECT * FROM db.cpu WHERE NOT time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE time > now() - INTERVAL '10 minutes' * 3",
		"SELECT * FROM db.cpu WHERE host = 'a'\n-- AND time >= '2026-10-20 10:00:00'\n",
		"SELECT * FROM db.cpu WHERE host = 'a' /* AND time >= '2026-10-20 10:00:00' */",
		"SELECT * FROM db.cpu WHERE CASE WHEN time >= '2026-10-20 10:00:00' THEN true ELSE host = 'a' END",
		"SELECT * FROM db.cpu WHERE host = 'a' QUALIFY time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE (time >= '2026-10-20 10:00:00' OR time < '2026-10-20 08:00:00')",
		"SELECT * FROM db.cpu c JOIN db.mem m ON true WHERE c.time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu c, db.mem m WHERE c.time >= '2026-10-20 10:00:00'",
		"SELECT time FROM db.cpu WHERE time >= '2026-10-20 10:00:00' UNION ALL SELECT time FROM db.mem",
		"WITH m AS (SELECT * FROM db.mem) SELECT * FROM db.cpu, m WHERE cpu.time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.mem WHERE time < (SELECT max(time) FROM db.cpu WHERE time >= '2026-10-20 11:00:00')",
		"SELECT * FROM (SELECT time, lag(time) OVER (ORDER BY time) p FROM db.cpu) WHERE time >= '2026-10-20 10:00:00'",
		"SELECT * FROM (SELECT host, max(time) AS time FROM db.cpu GROUP BY host) s WHERE time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE host IN (SELECT host FROM db.mem WHERE time >= '2026-10-20 10:00:00')",
		"SELECT * FROM db.cpu WHERE time NOT BETWEEN '2026-10-20 10:00:00' AND '2026-10-20 11:00:00'",
		"SELECT * FROM db.cpu e SEMI JOIN db.mem m ON e.host = m.host WHERE time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu POSITIONAL JOIN db.mem WHERE time >= '2026-10-20 10:00:00'",
		// End only: no start is assumed (it was 2020-01-01, dropping older rows).
		"SELECT * FROM db.cpu WHERE time < '2024-01-01'",
		"SELECT * FROM db.cpu WHERE time <= '2024-01-01 00:00:00'",
		// Caller text that spells a masker placeholder is never read.
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' AND host = '__STR_0__'",
		// Unterminated or nested block comment.
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' /* AND",
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' /* a /* b */ c */",
	}
	for _, q := range none {
		if tr := p.timeRangeAt(q, gateNow); tr != nil {
			t.Errorf("range %v..%v for %q, want none", tr.Start, tr.End, q)
		}
	}

	// Widened to what every read's WHERE proves.
	for _, c := range []struct {
		sql, start, end string
	}{
		{"SELECT * FROM db.cpu WHERE time >= '2026-10-20 11:00:00'::timestamp - INTERVAL '3 hours'",
			"2026-10-20T08:00:00Z", ""},
		{"SELECT * FROM db.cpu WHERE time > now() - INTERVAL '1 day' - INTERVAL '6 hours'",
			"2026-11-19T03:30:00Z", ""},
		// Two subqueries over 7 and 30 days read 30 days, not 7.
		{"SELECT w.n * 100.0 / m.n FROM (SELECT count(*) n FROM events WHERE time >= NOW() - INTERVAL 7 DAY) w, " +
			"(SELECT count(*) n FROM events WHERE time >= NOW() - INTERVAL 30 DAY) m",
			"2026-10-21T09:30:00Z", ""},
		// A second table bounded only by its start keeps reading after the
		// first table's end.
		{"WITH a AS (SELECT user_id, min(time) t0 FROM events WHERE time >= '2026-11-12T00:00:00Z' AND time < '2026-11-19T00:00:00Z' GROUP BY 1), " +
			"t AS (SELECT * FROM transactions WHERE time >= '2026-11-12T00:00:00Z') SELECT * FROM t JOIN a USING (user_id)",
			"2026-11-12T00:00:00Z", ""},
		// One read without an end, the other ending after the assumed end:
		// the hull keeps the later end.
		{"SELECT * FROM cpu WHERE time >= '2026-11-01' UNION ALL SELECT * FROM mem WHERE time >= '2026-10-01' AND time < '2027-01-01'",
			"2026-10-01T00:00:00Z", "2027-01-01T00:00:00Z"},
		{"SELECT * FROM events e JOIN a ON a.user_id = e.user_id WHERE e.time >= '2026-11-12T00:00:00Z' AND " +
			"e.time >= CAST('2026-11-12T00:00:00Z' AS TIMESTAMPTZ) - INTERVAL 1 HOUR AND e.time < '2026-11-19T00:00:00Z' " +
			"AND e.time < CAST('2026-11-12T00:00:00Z' AS TIMESTAMPTZ) - INTERVAL 1 HOUR",
			"", ""},
	} {
		tr := p.timeRangeAt(c.sql, gateNow)
		if c.start == "" {
			if tr != nil {
				t.Errorf("range %v..%v for %q, want none", tr.Start, tr.End, c.sql)
			}
			continue
		}
		if tr == nil || !tr.Start.Equal(gateT(t, c.start)) {
			t.Errorf("range %+v for %q, want start %s", tr, c.sql, c.start)
			continue
		}
		if c.end == "" && (!tr.EndAssumed || !tr.End.Equal(gateNow.Add(24*time.Hour))) {
			t.Errorf("range %+v for %q, want the assumed end now+24h", tr, c.sql)
		}
		if c.end != "" && !tr.End.Equal(gateT(t, c.end)) {
			t.Errorf("range %+v for %q, want end %s", tr, c.sql, c.end)
		}
	}
}

// The shapes dashboards, the cache warmer and the Grafana macros produce keep
// exactly the range the regex reader has always taken.
func TestTimeRangeKeptForDashboardShapes(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	const lo, hi = "2026-11-12T00:00:00Z", "2026-11-19T00:00:00Z"
	filter := "time >= '" + lo + "' AND time < '" + hi + "'"
	for _, q := range []string{
		"SELECT max(hash(COLUMNS(*))) FROM downloads WHERE " + filter,
		"SELECT DISTINCT os_name FROM events WHERE " + filter + " AND os_name IS NOT NULL",
		"SELECT count(*) FROM events WHERE " + filter + " AND event = 'x'",
		"SELECT count(*) FROM posthog.events WHERE " + filter,
		"SELECT count(*) FROM \"events\" WHERE " + filter,
		"SELECT time_bucket(INTERVAL '1 hour', time) t, count(*) FROM events WHERE " + filter + " GROUP BY 1 ORDER BY 1",
		// CTE panels: the bound is in the CTE, ORs only inside CASE or parens,
		// IS DISTINCT FROM and EXTRACT(... FROM ...) are not relations.
		"WITH e AS (SELECT * FROM events WHERE " + filter + " AND (os_name = 'iOS' OR os_name = 'iPadOS') " +
			"AND is_emulator IS DISTINCT FROM ('true')) SELECT CASE WHEN a = 1 OR b = 2 THEN 'x' ELSE 'y'END AS k, " +
			"extract(hour FROM time) h, count(*) FROM e GROUP BY 1, 2",
		"WITH base AS (SELECT app_version, count(*) c FROM events WHERE time >= now() - INTERVAL 30 DAY AND time < now() " +
			"AND app_version != '' GROUP BY 1) SELECT '(none)' AS v UNION ALL SELECT app_version FROM base ORDER BY 1",
		// JOINs of a measurement with a CTE, unqualified or alias-qualified bounds.
		"WITH c AS (SELECT session_id, min(time) ct FROM events WHERE " + filter + " GROUP BY 1) " +
			"SELECT count(*) FROM events e JOIN c ON e.session_id = c.session_id WHERE " + filter + " AND e.time < c.ct",
		"WITH fp AS (SELECT distinct_id, min(time) t FROM events WHERE " + filter + " GROUP BY 1) " +
			"SELECT count(*) FROM events e JOIN fp ON e.distinct_id = fp.distinct_id WHERE e.time >= '" + lo +
			"' AND e.time < '" + hi + "' AND e.time < fp.t",
		"SELECT * FROM events WHERE time BETWEEN '" + lo + "' AND '2026-11-18T23:59:59Z'",
		"SELECT * FROM events -- last week\nWHERE " + filter + " /* panel 3 */",
		"SELECT * FROM events WHERE time > now() - INTERVAL '20 days'",
		"SELECT * FROM events WHERE time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'",
		"SELECT * FROM events WHERE time > NOW() - INTERVAL 1 HOUR",
	} {
		want := p.extractTimeRangeAt(q, gateNow)
		got := p.timeRangeAt(q, gateNow)
		if want == nil || got == nil || *got != *want {
			t.Errorf("range %+v for %q, want the regex range %+v", got, q, want)
		}
	}
}

// An inclusive end (<=, BETWEEN) on an hour boundary reads that hour.
func TestInclusiveEndReadsItsHour(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	for _, q := range []string{
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 08:00:00' AND time <= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE time BETWEEN '2026-10-20 08:00:00' AND '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20T08:00:00Z' AND time <= '2026-10-20T10:00:00Z'",
	} {
		tr := p.timeRangeAt(q, gateNow)
		if tr == nil || !tr.End.Equal(gateT(t, "2026-10-20T10:00:00.000001Z")) {
			t.Fatalf("range %+v for %q, want an end 1µs past 10:00", tr, q)
		}
		paths := p.GeneratePartitionPaths(context.Background(), "/data", "db", "cpu", tr)
		if !slices.Contains(paths, "/data/db/cpu/2026/10/20/10/*.parquet") {
			t.Errorf("paths %v for %q do not read hour 10", paths, q)
		}
	}
	tr := p.timeRangeAt("SELECT * FROM db.cpu WHERE time >= now() - INTERVAL '2 hours' AND time <= now() - INTERVAL '1 hour'", gateNow)
	if tr == nil || !tr.End.Equal(gateNow.Add(-time.Hour+time.Microsecond)) {
		t.Errorf("relative inclusive end: %+v", tr)
	}
}

// The instant each literal spelling stands for, as DuckDB reads it against
// a TIMESTAMPTZ column in a UTC session.
func TestGateLiteralSemantics(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	for _, c := range []struct{ lit, start string }{
		{"'2026-10-20 10:00:00+02:00'", "2026-10-20T08:00:00Z"},
		{"'2026-10-20T10:00:00Z'", "2026-10-20T10:00:00Z"},
		{"TIMESTAMPTZ '2026-10-20 10:00:00+02:00'", "2026-10-20T08:00:00Z"},
		{"'2026-10-20 10:00:00+02:00'::timestamptz", "2026-10-20T08:00:00Z"},
		{"TIMESTAMP '2026-10-20 10:00:00'", "2026-10-20T10:00:00Z"},
		{"'2026-10-20 10:00:00'::timestamp", "2026-10-20T10:00:00Z"},
		{"DATE '2026-10-20'", "2026-10-20T00:00:00Z"},
		{"CAST('2026-10-20 10:00:00+02:00' AS TIMESTAMPTZ) - INTERVAL 1 HOUR", "2026-10-20T07:00:00Z"},
		{"TIMESTAMPTZ '2026-03-31 10:00:00' - INTERVAL '1 month'", "2026-02-28T10:00:00Z"},
		{"'2026-10-20 10:00:00.0000005'", "2026-10-20T10:00:00Z"},
	} {
		q := "SELECT * FROM cpu WHERE time >= " + c.lit + " AND time < '2026-10-21'"
		lo, _, ok := gateBounds(t, q)
		if !ok || lo == nil || !lo.Equal(gateT(t, c.start)) {
			t.Errorf("%s: proven start %v, want %s", c.lit, lo, c.start)
		}
		// The regex reader's candidate must not be kept when it starts later
		// than DuckDB's bound (TIMESTAMP '…+02' read as 08:00 dropped 08-10).
		if tr := p.timeRangeAt(q, gateNow); tr != nil && tr.Start.After(gateT(t, c.start)) {
			t.Errorf("%s: range starts %v, after %s", c.lit, tr.Start, c.start)
		}
	}
	// TIMESTAMP drops an offset: DuckDB reads the wall clock.
	q := "SELECT * FROM cpu WHERE time >= TIMESTAMP '2026-10-20 10:00:00+02:00' AND time < '2026-10-21'"
	if lo, _, _ := gateBounds(t, q); lo == nil || !lo.Equal(gateT(t, "2026-10-20T10:00:00Z")) {
		t.Errorf("TIMESTAMP with offset: %v", lo)
	}
	// Against time::timestamp a bare string becomes a TIMESTAMP: its offset
	// is dropped (read as 10:00, not 15:00 or 08:00).
	for _, lit := range []string{"'2026-10-20 10:00:00-05:00'", "'2026-10-20 10:00:00+02:00'"} {
		q := "SELECT * FROM cpu WHERE time::timestamp >= " + lit + " AND time::timestamp <= " + lit
		lo, hi, _ := gateBounds(t, q)
		if lo == nil || hi == nil || !lo.Equal(gateT(t, "2026-10-20T10:00:00Z")) || !hi.Equal(gateT(t, "2026-10-20T10:00:00.000001Z")) {
			t.Errorf("time::timestamp vs %s: %v..%v, want 10:00..10:00.000001", lit, lo, hi)
		}
	}
	// A bare string takes no interval (DuckDB refuses VARCHAR - INTERVAL).
	if lo, _, _ := gateBounds(t, "SELECT * FROM cpu WHERE time >= '2026-10-20' - INTERVAL 1 HOUR"); lo != nil {
		t.Errorf("bare string minus interval proved %v", lo)
	}
}

func gateBounds(t *testing.T, q string) (*time.Time, *time.Time, bool) {
	t.Helper()
	g, ok := newShapeGate(q, gateNow)
	if !ok {
		t.Fatalf("not tokenized: %q", q)
	}
	refs, ok := g.measurementRefs()
	if !ok || len(refs) != 1 {
		t.Fatalf("refs %v ok=%v for %q", refs, ok, q)
	}
	return g.provenBounds(refs[0])
}

func TestAddMonthsClampedMatchesDuckDB(t *testing.T) {
	for _, c := range []struct {
		from string
		n    int
		want string
	}{
		{"2026-03-31T10:00:00Z", -1, "2026-02-28T10:00:00Z"},
		{"2024-03-31T00:00:00Z", -1, "2024-02-29T00:00:00Z"},
		{"2026-01-31T00:00:00Z", 1, "2026-02-28T00:00:00Z"},
		{"2026-11-20T09:30:00Z", -3, "2026-08-20T09:30:00Z"},
		{"2026-12-31T00:00:00Z", -12, "2025-12-31T00:00:00Z"},
	} {
		if got := addMonthsClamped(gateT(t, c.from), c.n); !got.Equal(gateT(t, c.want)) {
			t.Errorf("%s %+d months = %v, want %s", c.from, c.n, got, c.want)
		}
	}
}

func TestTokenizeShapeSplitsPlaceholders(t *testing.T) {
	toks, ok := tokenizeShape("SELECT CASE WHEN a THEN __STR_0__END AS r FROM x")
	if !ok {
		t.Fatal("unbalanced")
	}
	var words []string
	for _, tk := range toks {
		words = append(words, tk.text)
	}
	if !slices.Contains(words, "__STR_0__") || !slices.Contains(words, "END") {
		t.Errorf("tokens %q", words)
	}
	if _, ok := tokenizeShape("SELECT (1"); ok {
		t.Error("unbalanced parens accepted")
	}
}

// A JOIN in parentheses: the rewrite's JOIN pattern turns mem into a read,
// and the FROM list item "(" used to be skipped as a subquery, so mem was
// pruned by cpu's range. Such a query now gets no range; a real subquery in
// parentheses keeps its range.
func TestParenthesisedJoinGetsNoRange(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	const cte = "WITH c AS (SELECT id, v FROM cpu WHERE time >= '2026-06-02 10:00:00') "
	for _, q := range []string{
		cte + "SELECT m.id FROM (c JOIN mem m ON c.v=m.v)",
		cte + "SELECT m.id FROM ((c JOIN mem m ON c.v = m.v))",
		cte + "SELECT m.id FROM (c LEFT JOIN mem m ON c.v = m.v)",
		cte + "SELECT m.id FROM ((c JOIN mem m ON c.v = m.v) JOIN c c2 ON c2.id = c.id)",
		cte + "SELECT m.id FROM ((SELECT * FROM c) x JOIN mem m ON x.v = m.v)",
		cte + "SELECT m.id FROM c JOIN (c c2 JOIN mem m ON c2.v = m.v) ON c.id = c2.id",
		cte + "SELECT m.id FROM c, (c c2 LEFT JOIN mem m ON c2.v = m.v) WHERE c.id = c2.id",
		"SELECT m.id FROM (cpu c JOIN mem m ON c.v = m.v) WHERE c.time >= '2026-06-02 10:00:00' AND m.time >= '2026-06-02 10:00:00'",
		"SELECT * FROM (mem) WHERE time >= '2026-06-02 10:00:00'",
	} {
		if tr := p.timeRangeAt(q, gateNow); tr != nil {
			t.Errorf("range %v..%v for %q, want none", tr.Start, tr.End, q)
		}
	}
	const lo = "2026-06-02T10:00:00Z"
	for _, q := range []string{
		"SELECT * FROM (SELECT * FROM cpu WHERE time >= '2026-06-02 10:00:00') s",
		"SELECT * FROM ((SELECT * FROM cpu WHERE time >= '2026-06-02 10:00:00')) s",
		"SELECT * FROM ((SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00') UNION ALL (SELECT id FROM mem WHERE time >= '2026-06-02 10:00:00')) s",
		"SELECT * FROM ((SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00') ORDER BY id LIMIT 5) s",
		"SELECT * FROM (FROM cpu WHERE time >= '2026-06-02 10:00:00') s",
		"SELECT * FROM (WITH x AS (SELECT * FROM cpu WHERE time >= '2026-06-02 10:00:00') SELECT * FROM x) s",
		"SELECT * FROM (VALUES (1), (2)) v(n), cpu WHERE time >= '2026-06-02 10:00:00'",
		"SELECT * FROM cpu c JOIN (SELECT * FROM mem WHERE time >= '2026-06-02 10:00:00') m ON c.v = m.v WHERE c.time >= '2026-06-02 10:00:00'",
	} {
		tr := p.timeRangeAt(q, gateNow)
		if tr == nil || !tr.Start.Equal(gateT(t, lo)) {
			t.Errorf("range %+v for %q, want start %s", tr, q, lo)
		}
	}
}

// A non-ASCII space outside literals: DuckDB reads U+00A0, U+200B, U+2003
// (and others) as a space and the rewrite's `\b` sees a boundary next to it,
// so `id<U+00A0>FROM mem` reads mem, a FROM the tokenizer never saw. Such a
// query now gets no range. In a string literal or a comment it changes
// nothing.
func TestUnicodeSpaceOutsideLiteralsGetsNoRange(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	const head = "SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' UNION ALL SELECT id"
	for _, sp := range []string{"\u00a0", "\u200b", "\u2003", "\u202f", "\u3000", "\ufeff", "\u2060",
		// Identifier characters to DuckDB, but spaces to the API's
		// keyword checks (foldUnicodeSpace): never read either.
		"\u200c", "\u2028"} {
		for _, q := range []string{
			head + sp + "FROM mem",
			head + " FROM" + sp + "mem",
			head + sp + "FROM mem WHERE time >= '2026-06-02 10:00:00'",
			"SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00'" + sp + "OR host = 'a'",
			"SELECT id FROM cpu WHERE" + sp + "time >= '2026-06-02 10:00:00'",
		} {
			if tr := p.timeRangeAt(q, gateNow); tr != nil {
				t.Errorf("range %v..%v for %q, want none", tr.Start, tr.End, q)
			}
		}
	}
	// One word to the tokenizer (and an identifier to DuckDB) that the
	// rewrite's `\b` splits: `id<U+00E9>FROM mem` reads mem to the rewrite.
	for _, q := range []string{
		head + "\u00e9FROM mem",
		head + "\u0639FROM mem",
		head + "$FROM mem",
		"SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' UNION ALL SELECT id FROM mem\u00e9",
	} {
		if tr := p.timeRangeAt(q, gateNow); tr != nil {
			t.Errorf("range %v..%v for %q, want none", tr.Start, tr.End, q)
		}
	}

	// In a literal, a quoted identifier or a comment the space is not read,
	// and a word all of non-ASCII letters is one word to all three readers.
	const bound = "time >= '2026-06-02 10:00:00' AND time < '2026-06-03 00:00:00'"
	plain := p.timeRangeAt("SELECT id FROM cpu WHERE "+bound, gateNow)
	if plain == nil {
		t.Fatal("no range for the plain query")
	}
	for _, q := range []string{
		"SELECT id FROM cpu WHERE " + bound + " AND host <> 'a\u00a0FROM mem'",
		"SELECT id FROM cpu WHERE " + bound + " AND host <> 'x\u200by\u2003z'",
		"SELECT id AS \"a\u00a0FROM mem\" FROM cpu WHERE " + bound,
		"SELECT id FROM cpu WHERE " + bound + " -- \u00a0FROM mem",
		"SELECT id /* \u00a0FROM mem */ FROM cpu WHERE " + bound,
		"SELECT id AS \u0639\u062f\u062f FROM cpu WHERE " + bound,
	} {
		tr := p.timeRangeAt(q, gateNow)
		if tr == nil || !tr.Start.Equal(plain.Start) || !tr.End.Equal(plain.End) || tr.EndAssumed != plain.EndAssumed {
			t.Errorf("range %+v for %q, want %+v as without the non-ASCII text", tr, q, plain)
		}
	}
}

// DuckDB reads any word after a dot as a name: g.group, s.limit, s.case and
// s.end are columns. Read as keywords they cut a later top-level OR off the
// WHERE body (or nested a CASE), and the query was pruned by the bound
// before it. Such a query now gets no range; quoted names, numbers and text
// in literals or comments keep it.
func TestKeywordAfterDotGetsNoRange(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	const bound = "m.time >= '2026-06-02 10:00:00'"
	for _, q := range []string{
		"SELECT m.id FROM mem m, (SELECT 'b' AS \"group\") g WHERE " + bound + " AND g.group = 'b' OR m.host = 'a'",
		"SELECT m.id FROM mem m, (SELECT true AS \"limit\") s WHERE " + bound + " AND s.limit OR true",
		"SELECT m.id FROM mem m, (SELECT true AS \"order\") s WHERE " + bound + " AND s.order OR true",
		"SELECT m.id FROM mem m, (SELECT true AS \"select\") s WHERE " + bound + " AND s.select OR true",
		"SELECT m.id FROM mem m, (SELECT true AS \"union\") s WHERE " + bound + " AND s.union OR true",
		"SELECT m.id FROM mem m, (SELECT 1 AS \"case\", 2 AS \"end\") s WHERE " + bound + " AND s.case = 1 OR s.end = 2",
		"SELECT m.id FROM (SELECT true AS \"where\") c RIGHT JOIN mem m ON c.where AND " + bound,
		"SELECT m.id FROM mem m, (SELECT true AS \"limit\") s WHERE " + bound + " AND s . limit OR true",
		"SELECT m.id FROM mem m, (SELECT true AS \"limit\") s WHERE " + bound + " AND s./* c */limit OR true",
		"SELECT m.id FROM mem m, (SELECT {'limit': true} AS st) s WHERE " + bound + " AND s.st.limit OR true",
		"SELECT m.id FROM mem m, (SELECT true AS \"or\") s WHERE " + bound + " AND s.or",
		"SELECT m.id FROM mem m, (SELECT true AS \"join\") s WHERE " + bound + " AND s.join",
		"SELECT m.id FROM mem m, (SELECT true AS \"having\") s WHERE " + bound + " AND s.having OR true",
	} {
		if tr := p.timeRangeAt(q, gateNow); tr != nil {
			t.Errorf("range %v..%v for %q, want none", tr.Start, tr.End, q)
		}
	}
	const lo = "2026-06-02T10:00:00Z"
	for _, q := range []string{
		"SELECT m.id FROM mem m, (SELECT 'b' AS \"group\") g WHERE " + bound + " AND g.\"group\" = 'b'",
		"SELECT m.id FROM mem m WHERE " + bound + " AND m.host = 'a' AND m.v > 1.5",
		"SELECT m.id, m.date, m.event, m.interval FROM mem m WHERE " + bound + " AND m.v <> .5 AND m.host <> 'x.limit OR true'",
		"SELECT m.id FROM mem m WHERE " + bound + " /* AND s.limit OR true */",
		"SELECT count(*) FROM posthog.events WHERE time >= '2026-06-02 10:00:00' AND events.time < '2026-06-03'",
	} {
		tr := p.timeRangeAt(q, gateNow)
		if tr == nil || !tr.Start.Equal(gateT(t, lo)) {
			t.Errorf("range %+v for %q, want start %s", tr, q, lo)
		}
	}
}
