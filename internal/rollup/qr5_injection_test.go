package rollup

import (
	"path/filepath"
	"strings"
	"testing"
)

// QR-5: the rollup emitter is the security boundary — the API validates and
// RBAC-checks the ORIGINAL SQL and then executes the rewritten SQL as-is. Every
// identifier the rollup emits must therefore be quoted the DuckDB way (a double
// quote doubled), and every time bound must be a validated, re-formatted
// timestamp, never the raw literal text from the request.

// qrSecretFile writes a Parquet file the query never names. Its rows carry the
// marker 4242 in every column an injected read could pick up.
func qrSecretFile(t *testing.T, f *qrFixture) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "secret.parquet")
	mustExec(t, f.raw, "COPY (SELECT TIMESTAMPTZ '2026-06-01 00:00:00+00' AS bucket, 'secret' AS region, 4242::BIGINT AS _cnt) TO '"+p+"' (FORMAT PARQUET)")
	return p
}

func requireNoSecret(t *testing.T, res qrResult, sqlText string) {
	t.Helper()
	for _, r := range res.rows {
		if strings.Contains(r, "4242") || strings.Contains(r, "secret") {
			t.Fatalf("query read the unnamed secret file (row %q)\nSQL: %s", r, sqlText)
		}
	}
}

func TestQuoteIdent(t *testing.T) {
	cases := map[string]string{
		"region":            `"region"`,
		`say "hi"`:          `"say ""hi"""`,
		`back\slash`:        `"back\slash"`,
		`x\" FROM t --`:     `"x\"" FROM t --"`,
		"":                  `""`,
		"تطبيق":             `"تطبيق"`,
		"tab\tand\nnewline": "\"tab\tand\nnewline\"",
	}
	for in, want := range cases {
		if got := quoteIdent(in); got != want {
			t.Errorf("quoteIdent(%q) = %s, want %s", in, got, want)
		}
	}
}

// Aliases, dims and filter columns containing a double quote or a backslash
// must round-trip: the served result has the user's exact column names and the
// same rows as source — on the sealed cube read AND on the merge-on-read path
// (stored cube branch + source tail).
func TestQR5_IdentifiersWithQuotesRoundTrip(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region", qrWeirdDim},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Col: "bytes"}}}
	q := `SELECT date_trunc('day', time) AS "day ""b""", "we""ird\dim", count(*) AS "say ""hi""", sum(bytes) AS "back\slash"
		FROM events
		WHERE time >= TIMESTAMPTZ '2026-06-01 00:00:00+00' AND time < TIMESTAMPTZ '2026-06-03 00:00:00+00'
		  AND "we""ird\dim" <> 'plain'
		GROUP BY 1, 2`
	for name, wm := range map[string]string{"sealed": "", "merge": "2026-06-02 12:00:00+00"} {
		// Subtest names hold no '=': t.TempDir() embeds them in the source path,
		// where DuckDB would read key=value as a hive partition column.
		t.Run(name, func(t *testing.T) {
			f := newQRFixture(t, spec, wm)
			d := f.requireSameAsRaw(t, q)
			if !d.Served {
				t.Fatalf("want served from the cube, declined: %s", d.Reason)
			}
			if wm != "" && !strings.Contains(d.SQL, "UNION ALL BY NAME") {
				t.Fatalf("want the merge-on-read path, got:\n%s", d.SQL)
			}
		})
	}
}

// A crafted alias that escapes Go-%q quoting must not change the SQL structure:
// the served query returns the cube rows under the literal alias, and never the
// rows of a file the query does not name.
func TestQR5_AliasInjectionCannotReadUnnamedFile(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount}}}
	f := newQRFixture(t, spec, "")
	secret := qrSecretFile(t, f)
	alias := `x\" FROM read_parquet('` + secret + `') GROUP BY 1, 2 --`
	q := `SELECT date_trunc('day', time) AS b, region, count(*) AS ` + quoteIdentForTest(alias) + `
		FROM events WHERE time >= TIMESTAMPTZ '2026-06-01 00:00:00+00' AND time < TIMESTAMPTZ '2026-06-03 00:00:00+00' GROUP BY 1, 2`

	d := f.r.Route(q)
	if !d.Served {
		t.Fatalf("expected the shape to be served (the alias is legal SQL); declined: %s", d.Reason)
	}
	got, err := qrQuery(f.exec, d.SQL)
	if err != nil {
		t.Fatalf("served SQL failed: %v\nSQL: %s", err, d.SQL)
	}
	requireNoSecret(t, got, d.SQL)
	want := f.rawResult(t, q)
	if len(got.cols) != 3 || got.cols[2] != alias {
		t.Fatalf("alias not preserved verbatim: cols=%q want third %q\nSQL: %s", got.cols, alias, d.SQL)
	}
	if strings.Join(got.rows, "\n") != strings.Join(want.rows, "\n") {
		t.Fatalf("rows differ from source\n got  %v\n want %v\nSQL: %s", got.rows, want.rows, d.SQL)
	}
}

// quoteIdentForTest is the DuckDB quoting rule, spelled out independently of
// the implementation under test so a broken quoteIdent cannot hide itself.
func quoteIdentForTest(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// A time literal that does not parse as a timestamp must never reach the
// emitted SQL. The reviewer's payload closes the literal and UNIONs in a read of
// an arbitrary file; in best-effort (X-Arc-Rollup-Only) mode the raw text was
// pasted into the cube read verbatim.
func TestQR5_TimeLiteralInjectionRefused(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount}}}
	f := newQRFixture(t, spec, "")
	secret := qrSecretFile(t, f)
	payload := `2026-06-01 00:00:00+00' GROUP BY 1, 2 UNION ALL SELECT bucket, region, _cnt FROM read_parquet('` + secret + `') --`
	lit := "'" + strings.ReplaceAll(payload, "'", "''") + "'"
	for _, cast := range []string{"TIMESTAMPTZ", "TIMESTAMP", "DATE", ""} {
		q := `SELECT date_trunc('day', time) AS b, region, count(*) AS n FROM events
			WHERE time >= ` + cast + ` ` + lit + ` AND time < TIMESTAMPTZ '2026-06-03 00:00:00+00' GROUP BY 1, 2`
		for name, route := range map[string]func(string) Decision{"auto": f.r.Route, "rollup-only": f.r.RouteOnly} {
			d := route(q)
			if !d.Served {
				continue // refused: the query never runs from the rollup
			}
			if strings.Contains(d.SQL, secret) {
				t.Errorf("%s/%s: emitted SQL references the unnamed file:\n%s", name, cast, d.SQL)
				continue
			}
			res, err := qrQuery(f.exec, d.SQL)
			if err != nil {
				t.Errorf("%s/%s: served SQL failed: %v\nSQL: %s", name, cast, err, d.SQL)
				continue
			}
			requireNoSecret(t, res, d.SQL)
		}
	}
}

// Bounds are validated and re-formatted, never copied: the shape carries the
// canonical UTC form (fractional seconds kept), whatever spelling was parsed.
func TestQR5_TimeBoundsCanonical(t *testing.T) {
	cases := []struct{ lo, hi, wantLo, wantHi string }{
		{"TIMESTAMP '2026-06-01 00:00:00.250'", "DATE '2026-06-03'", "2026-06-01 00:00:00.25+00", "2026-06-03 00:00:00+00"},
		{"TIMESTAMPTZ '2026-06-01T02:00:00+02:00'", "'2026-06-02T10:15:30.123Z'", "2026-06-01 00:00:00+00", "2026-06-02 10:15:30.123+00"},
	}
	for _, c := range cases {
		q := "SELECT count(*) AS n FROM events WHERE time >= " + c.lo + " AND time < " + c.hi
		s, ok, why := Parse(q, "time")
		if !ok {
			t.Fatalf("parse %s: %s", q, why)
		}
		if s.TimeLo != c.wantLo || s.TimeHi != c.wantHi {
			t.Errorf("%s:\n got  [%s, %s)\n want [%s, %s)", q, s.TimeLo, s.TimeHi, c.wantLo, c.wantHi)
		}
	}
}

// Sub-second bounds (Grafana sends milliseconds) must not be truncated to the
// second on the merge path: the head patch [lo, first full bucket) and the tail
// [watermark, hi) are read from source with the exact bounds.
func TestQR5_MergeKeepsSubSecondBounds(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount}}}
	f := newQRFixture(t, spec, "2026-06-02 12:00:00+00")
	// Rows just before lo and just before hi, inside the same second.
	f.writeSource(t, "2026-06-01", "edge.parquet", `SELECT * REPLACE (TIMESTAMPTZ '2026-06-01 00:30:00.2+00' AS "time", 'head' AS region) FROM (`+qrSourceBody("2026-06-01")+`) LIMIT 3`)
	f.writeSource(t, "2026-06-02", "edge.parquet", `SELECT * REPLACE (TIMESTAMPTZ '2026-06-02 18:00:00.2+00' AS "time", 'tail' AS region) FROM (`+qrSourceBody("2026-06-02")+`) LIMIT 3`)
	q := `SELECT region, count(*) AS n FROM events
		WHERE time >= TIMESTAMPTZ '2026-06-01 00:30:00.5+00' AND time < TIMESTAMPTZ '2026-06-02 18:00:00.7+00' GROUP BY 1`
	d := f.requireSameAsRaw(t, q)
	if !d.Served {
		t.Fatalf("want served via merge-on-read, declined: %s", d.Reason)
	}
}

// Defense in depth: a shape that reaches the emitters without the parser's
// validation (any future caller) is still refused when a bound does not parse,
// in AUTO and best-effort mode alike.
func TestQR5_ServeShapeRefusesUnparsedBounds(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount}}}
	f := newQRFixture(t, spec, "")
	bad := `2026-06-01 00:00:00+00' UNION ALL SELECT 1 --`
	for _, bestEffort := range []bool{false, true} {
		for _, s := range []QueryShape{
			{Source: spec.Source, TimeCol: "time", Grain: "day", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount, Alias: "n"}}, TimeLo: bad, TimeHi: "2026-06-03 00:00:00+00"},
			{Source: spec.Source, TimeCol: "time", Grain: "day", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount, Alias: "n"}}, TimeLo: "2026-06-01 00:00:00+00", TimeHi: bad},
		} {
			if out, _, served, why := f.r.serveShape(s, bestEffort); served || why != "bad_time_bounds" {
				t.Fatalf("bestEffort=%v: served=%v reason=%q, want refusal bad_time_bounds\nSQL: %s", bestEffort, served, why, out)
			}
		}
	}
}
