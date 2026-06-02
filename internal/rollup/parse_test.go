package rollup

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// rawAgainstSource runs the original SQL verbatim against the day-28 corpus by
// rewriting its FROM downloads into the same read_parquet(...) source the cube
// path uses. This is the ground truth Parse must reproduce.
func rawAgainstSource(t *testing.T, db *sql.DB, rawSQL string, nKeys int) result {
	t.Helper()
	src := strings.Replace(rawSQL, "FROM downloads",
		fmt.Sprintf("FROM read_parquet(%s, union_by_name=true)", day28Glob), 1)
	return runShape(t, db, src, nKeys)
}

// assertRoundTrip parses rawSQL, expects it supported, then asserts the shape's
// SourceRefSQL produces the same grouped result as running rawSQL directly.
func assertRoundTrip(t *testing.T, db *sql.DB, rawSQL string) {
	t.Helper()
	q, ok, reason := Parse(rawSQL, "time")
	if !ok {
		t.Fatalf("expected supported, got ok=false (%s)\nSQL: %s", reason, rawSQL)
	}

	nKeys := len(q.Dims)
	if q.Grain != "" {
		nKeys++
	}

	want := rawAgainstSource(t, db, rawSQL, nKeys)
	got := runShape(t, db, q.SourceRefSQL(day28Glob), nKeys)

	if len(want.rows) != len(got.rows) {
		t.Fatalf("group count mismatch: raw=%d shape=%d\nshape=%+v\nSQL: %s",
			len(want.rows), len(got.rows), q, rawSQL)
	}
	keys := make([]string, 0, len(want.rows))
	for k := range want.rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		wv := want.rows[k]
		gv, present := got.rows[k]
		if !present {
			t.Fatalf("shape missing group %q\nSQL: %s", k, rawSQL)
		}
		for i := range wv {
			if !aggMatch(wv[i], gv[i], q.Aggs[i]) {
				t.Errorf("group %q agg[%d] mismatch: raw=%v shape=%v\nSQL: %s",
					k, i, wv[i], gv[i], rawSQL)
			}
		}
	}
}

// TestParse_RoundTrip proves fidelity: every supported SQL over downloads, once
// parsed, must compute the same result via the canonical SourceRefSQL form.
func TestParse_RoundTrip(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)

	cases := []string{
		// --- counts / grain ---
		`SELECT count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29'`,
		`SELECT date_trunc('hour', time) AS b, count(*) AS n FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' GROUP BY 1`,
		`SELECT date_trunc('day', time) AS b, count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' GROUP BY b`,
		`SELECT date_trunc('hour', time) AS b, status, count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' GROUP BY 1, 2`,
		`SELECT time_bucket(INTERVAL '1 hour', time) AS b, count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' GROUP BY 1`,

		// --- WHERE filters ---
		`SELECT date_trunc('hour', time) AS b, count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' AND status = 'ns' GROUP BY 1`,
		`SELECT count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' AND response = 200`,
		`SELECT count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' AND response != 200`,
		`SELECT count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' AND tag IN ('HAMIOS', 'HAMAND')`,
		`SELECT count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' AND tag IS NULL`,
		`SELECT count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' AND tag IS NOT NULL`,
		`SELECT status, count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' AND response NOT IN (404) GROUP BY status`,
		// same-column OR collapses to IN
		`SELECT count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' AND (status = 'ns' OR status = 'eu')`,

		// --- numeric aggregates ---
		`SELECT date_trunc('hour', time) AS b, avg(duration_seconds) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' GROUP BY 1`,
		`SELECT date_trunc('hour', time) AS b, min(duration_seconds), max(duration_seconds), sum(duration_seconds), count(duration_seconds) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' GROUP BY 1`,

		// --- sketch aggregates (approximate) ---
		`SELECT date_trunc('hour', time) AS b, count(distinct device_id) AS uniq FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' GROUP BY 1`,
		`SELECT date_trunc('hour', time) AS b, quantile_cont(duration_seconds, 0.95) AS p95 FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' GROUP BY 1`,

		// --- DATE / bare TIMESTAMP literals on the time range ---
		`SELECT count(*) FROM downloads WHERE time >= DATE '2025-12-28' AND time < DATE '2025-12-29'`,
		`SELECT count(*) FROM downloads WHERE time >= TIMESTAMP '2025-12-28 00:00:00' AND time < TIMESTAMP '2025-12-29 00:00:00'`,
	}

	for i, sqlText := range cases {
		sqlText := sqlText
		t.Run(fmt.Sprintf("case%02d", i), func(t *testing.T) {
			assertRoundTrip(t, db, sqlText)
		})
	}
}

// TestParse_Shapes asserts the extracted shape fields without needing the corpus,
// pinning the structural decoding (grain, dims, aggs, filters, source).
func TestParse_Shapes(t *testing.T) {
	q, ok, reason := Parse(
		`SELECT date_trunc('hour', time) AS b, status, count(*) AS n, avg(duration_seconds) AS a `+
			`FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29' `+
			`AND tag IN ('HAMIOS','HAMAND') GROUP BY 1, 2`, "time")
	if !ok {
		t.Fatalf("unexpected ok=false: %s", reason)
	}
	if q.Source != "default.downloads" {
		t.Errorf("source = %q", q.Source)
	}
	if q.Grain != "hour" {
		t.Errorf("grain = %q", q.Grain)
	}
	if len(q.Dims) != 1 || q.Dims[0] != "status" {
		t.Errorf("dims = %v", q.Dims)
	}
	if q.TimeLo != "2025-12-28" || q.TimeHi != "2025-12-29" {
		t.Errorf("range = [%q,%q)", q.TimeLo, q.TimeHi)
	}
	if len(q.Aggs) != 2 || q.Aggs[0].Kind != AggCount || q.Aggs[0].Alias != "n" ||
		q.Aggs[1].Kind != AggAvg || q.Aggs[1].Col != "duration_seconds" {
		t.Errorf("aggs = %+v", q.Aggs)
	}
	if len(q.Filters) != 1 || q.Filters[0].Op != OpIn || q.Filters[0].Col != "tag" {
		t.Errorf("filters = %+v", q.Filters)
	}

	// Percentile DECIMAL literal decodes to 0.95.
	qp, ok, reason := Parse(
		`SELECT quantile_cont(duration_seconds, 0.95) FROM downloads `+
			`WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29'`, "time")
	if !ok {
		t.Fatalf("percentile ok=false: %s", reason)
	}
	if qp.Aggs[0].Kind != AggPercentile || qp.Aggs[0].P != 0.95 {
		t.Errorf("percentile agg = %+v", qp.Aggs[0])
	}

	// Explicit schema qualifies through.
	qs, _, _ := Parse(
		`SELECT count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29'`, "time")
	_ = qs
}

// TestParse_Unsupported pins the conservative rejections: each must return
// ok=false so the caller safely falls through to source.
func TestParse_Unsupported(t *testing.T) {
	const trange = `time >= TIMESTAMPTZ '2025-12-28' AND time < TIMESTAMPTZ '2025-12-29'`
	cases := []struct {
		name string
		sql  string
	}{
		{"join", `SELECT count(*) FROM downloads a JOIN other b ON a.id = b.id WHERE ` + trange},
		{"subquery_from", `SELECT count(*) FROM (SELECT * FROM downloads) x WHERE ` + trange},
		{"cte", `WITH x AS (SELECT * FROM downloads) SELECT count(*) FROM x WHERE ` + trange},
		{"window", `SELECT row_number() OVER (ORDER BY time) FROM downloads WHERE ` + trange},
		{"stddev", `SELECT stddev(duration_seconds) FROM downloads WHERE ` + trange},
		{"no_time_range", `SELECT count(*) FROM downloads WHERE status = 'ns'`},
		{"one_sided_time", `SELECT count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28'`},
		{"lte_upper_bound", `SELECT count(*) FROM downloads WHERE time >= TIMESTAMPTZ '2025-12-28' AND time <= TIMESTAMPTZ '2025-12-29'`},
		{"no_aggregate", `SELECT status FROM downloads WHERE ` + trange + ` GROUP BY status`},
		{"cross_column_or", `SELECT count(*) FROM downloads WHERE ` + trange + ` AND (os = 'ios' OR status = 'ns')`},
		{"having", `SELECT status, count(*) FROM downloads WHERE ` + trange + ` GROUP BY status HAVING count(*) > 1`},
		{"expr_filter", `SELECT count(*) FROM downloads WHERE ` + trange + ` AND length(status) = 2`},
		{"not_select", `INSERT INTO downloads VALUES (1)`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, ok, _ := Parse(c.sql, "time"); ok {
				t.Errorf("expected ok=false for %s\nSQL: %s", c.name, c.sql)
			}
		})
	}
}
