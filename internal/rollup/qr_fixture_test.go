package rollup

import (
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// qrFixture is a small, fully local rollup deployment used by the QR-* regression
// tests: a two-day source table default.events written as Parquet under root,
// hour-grain cube day files built with the PRODUCTION build SQL
// (CubeSpec.BuildCopySQL), a manifest and a Router over them.
//
// Two DuckDB handles keep the comparison honest:
//   - raw has a view named `events` over the source files — the ground truth,
//     i.e. what the query returns when it is NOT served by the rollup;
//   - exec has NO relation named `events`. The API executor runs a rewritten
//     query as-is (no storage-path conversion), so a rewritten query that still
//     names the source table must fail here exactly as it fails in production.
type qrFixture struct {
	root    string
	srcGlob string // read_parquet argument for the whole source table
	raw     *sql.DB
	exec    *sql.DB
	spec    CubeSpec
	m       *Manifest
	r       *Router
}

// qrWeirdDim is a legal DuckDB column name holding a double quote and a
// backslash — the two characters Go's %q renders differently from DuckDB.
const qrWeirdDim = `we"ird\dim`

var qrDays = []string{"2026-06-01", "2026-06-02"}

// qrSourceBody is one source day: 240 rows spread over the day's hours, with
// number-like VARCHAR columns (app_version also holds the non-numeric '2.0.1';
// resp holds only numeric-looking strings), a weird-named VARCHAR dim, numeric
// columns, a real column named "filename" (which read_parquet's filename=true
// option would clash with) and a VARCHAR dim holding NULLs (tag).
func qrSourceBody(day string) string {
	return fmt.Sprintf(`SELECT TIMESTAMPTZ '%s 00:00:00+00' + to_hours(i %% 24) + to_minutes(i %% 60) AS "time",
		['eu','us','ap'][1 + i %% 3] AS region,
		['1.1','1.10','2.0.1','7','007','7.0'][1 + (i // 3) %% 6] AS app_version,
		['a"b','c\d','plain'][1 + (i // 2) %% 3] AS "we""ird\dim",
		(i * 10)::BIGINT AS bytes,
		[200, 404, 500][1 + (i // 5) %% 3]::INTEGER AS status,
		['5','05','5.0','4'][1 + (i // 7) %% 4] AS resp,
		'f' || (i %% 2) AS filename,
		CASE WHEN i %% 5 = 0 THEN NULL ELSE ['x','y'][1 + i %% 2] END AS tag
		FROM range(0, 240) t(i)`, day)
}

// newQRFixture writes the source days, builds spec's cube files for both days
// with the production build SQL and wires a Router whose watermark is wm ("" =
// fully sealed: cube-only reads).
func newQRFixture(t *testing.T, spec CubeSpec, wm string) *qrFixture {
	t.Helper()
	f := &qrFixture{root: t.TempDir(), spec: spec}
	f.raw = openLocalDuck(t)
	t.Cleanup(func() { f.raw.Close() })
	f.exec = openLocalDuck(t)
	t.Cleanup(func() { f.exec.Close() })

	for _, d := range qrDays {
		f.writeSource(t, d, "part.parquet", qrSourceBody(d))
	}
	f.srcGlob = "'" + filepath.Join(f.root, "default", "events") + "/**/*.parquet'"
	mustExec(t, f.raw, "CREATE VIEW events AS SELECT * FROM read_parquet("+f.srcGlob+", union_by_name=true)")

	f.m = &Manifest{}
	cubeDir := filepath.Join(f.root, "_cube")
	if err := os.MkdirAll(cubeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, d := range qrDays {
		f.m.Upsert(f.buildDay(t, cubeDir, d))
	}
	f.m.CubeID = "default.events." + cubeKind(spec)
	f.m.Source, f.m.Grain, f.m.Dims, f.m.Aggs = spec.Source, spec.Grain, spec.Dims, spec.Aggs
	f.m.SchemaHash = spec.SchemaHash()
	f.r = &Router{
		Cubes:      []CubeSpec{spec},
		Manifests:  map[string]*Manifest{cubeKeyOf(spec): f.m},
		TimeCol:    "time",
		SourceExpr: func(string) string { return f.srcGlob },
		Watermark:  func(string) string { return wm },
	}
	return f
}

func (f *qrFixture) dayDir(day string) string {
	return filepath.Join(f.root, "default", "events", day[:4], day[5:7], day[8:10])
}

// writeSource writes one more Parquet file into a source day partition.
func (f *qrFixture) writeSource(t *testing.T, day, name, body string) {
	t.Helper()
	dir := f.dayDir(day)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustExec(t, f.raw, fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", body, filepath.Join(dir, name)))
}

// buildDay materializes one cube day with the production build SQL and returns
// its manifest entry (bucket span and row count read back from the file).
func (f *qrFixture) buildDay(t *testing.T, cubeDir, day string) DayEntry {
	t.Helper()
	d, ok := parseTS(day)
	if !ok {
		t.Fatalf("bad day %q", day)
	}
	dest := filepath.Join(cubeDir, day+".parquet")
	src := "'" + f.dayDir(day) + "/**/*.parquet'"
	mustExec(t, f.raw, f.spec.BuildCopySQL(src, "time", fmtTS(d), fmtTS(d.Add(24*time.Hour)), dest))
	var lo, hi time.Time
	var rows int64
	if err := f.raw.QueryRow("SELECT min(bucket), max(bucket), count(*) FROM read_parquet('"+dest+"')").Scan(&lo, &hi, &rows); err != nil {
		t.Fatalf("read back cube day %s: %v", day, err)
	}
	return DayEntry{Date: day, URI: dest, BucketLo: fmtTS(lo), BucketHi: fmtTS(hi.Add(time.Hour)), Rows: rows}
}

// qrResult is a query result normalized for comparison: column names plus rows
// rendered as sorted "v1|v2|..." strings (numbers compared by value, so BIGINT
// vs HUGEINT vs DOUBLE representations of the same number are equal).
type qrResult struct {
	cols []string
	rows []string
}

func (r qrResult) String() string {
	return fmt.Sprintf("cols=%q rows=%q", r.cols, r.rows)
}

func qrQuery(db *sql.DB, q string) (qrResult, error) {
	rs, err := db.Query(q)
	if err != nil {
		return qrResult{}, err
	}
	defer rs.Close()
	cols, err := rs.Columns()
	if err != nil {
		return qrResult{}, err
	}
	out := qrResult{cols: cols}
	for rs.Next() {
		raw := make([]any, len(cols))
		ptr := make([]any, len(cols))
		for i := range raw {
			ptr[i] = &raw[i]
		}
		if err := rs.Scan(ptr...); err != nil {
			return qrResult{}, err
		}
		parts := make([]string, len(raw))
		for i, v := range raw {
			parts[i] = qrValue(v)
		}
		out.rows = append(out.rows, strings.Join(parts, "|"))
	}
	if err := rs.Err(); err != nil {
		return qrResult{}, err
	}
	sort.Strings(out.rows)
	return out, nil
}

func qrValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case string:
		return x
	case []byte:
		return string(x)
	case *big.Int:
		return x.String()
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 64)
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case bool:
		return strconv.FormatBool(x)
	}
	return fmt.Sprint(v)
}

// rawResult runs sql against the source (the not-served ground truth).
func (f *qrFixture) rawResult(t *testing.T, q string) qrResult {
	t.Helper()
	res, err := qrQuery(f.raw, q)
	if err != nil {
		t.Fatalf("raw query failed: %v\nSQL: %s", err, q)
	}
	return res
}

// served routes sql in AUTO mode the way the API does: a served decision runs
// the rewritten SQL on the executor DB (which has no source table), a declined
// one runs the original SQL against source.
func (f *qrFixture) served(t *testing.T, q string) (qrResult, Decision, error) {
	t.Helper()
	d := f.r.Route(q)
	if !d.Served {
		return f.rawResult(t, q), d, nil
	}
	res, err := qrQuery(f.exec, d.SQL)
	return res, d, err
}

// requireSameAsRaw asserts the API-equivalent result of q equals the source
// result (same column names, same rows) and returns the routing decision.
func (f *qrFixture) requireSameAsRaw(t *testing.T, q string) Decision {
	t.Helper()
	want := f.rawResult(t, q)
	got, d, err := f.served(t, q)
	if err != nil {
		t.Fatalf("served query failed (served=%v cube=%q): %v\nSQL: %s", d.Served, d.Cube, err, d.SQL)
	}
	if fmt.Sprint(got.cols) != fmt.Sprint(want.cols) {
		t.Fatalf("columns differ (served=%v)\n got  %q\n want %q\nSQL: %s", d.Served, got.cols, want.cols, d.SQL)
	}
	if strings.Join(got.rows, "\n") != strings.Join(want.rows, "\n") {
		t.Fatalf("rows differ from source (served=%v cube=%q)\n got  %v\n want %v\nSQL: %s", d.Served, d.Cube, got.rows, want.rows, d.SQL)
	}
	return d
}
