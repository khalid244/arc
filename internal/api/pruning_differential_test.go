package api

import (
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// TestPartitionPruningDifferential runs generated queries through
// /api/v1/query, where partition pruning leaves hour and day directories out
// of the read, and compares every answer with DuckDB reading all the files.
// The data sits on hour edges (one microsecond either side), in compacted
// day files and years before the pruner's old 2020 floor; the queries mix
// every time-literal spelling and offset, inclusive and exclusive bounds,
// OR, NOT, CASE, comments, QUALIFY, CTEs, subqueries, JOINs and UNIONs.
// A pruned answer with fewer rows than the full read is a wrong answer.
//
// ARC_PRUNE_DIFF_N sets the number of queries (default 400),
// ARC_PRUNE_DIFF_SEED the seed (default 1); ARC_PRUNE_DIFF_SQL runs one
// query instead.
func TestPartitionPruningDifferential(t *testing.T) {
	n, seed := 400, int64(1)
	if v, err := strconv.Atoi(os.Getenv("ARC_PRUNE_DIFF_N")); err == nil {
		n = v
	}
	if v, err := strconv.ParseInt(os.Getenv("ARC_PRUNE_DIFF_SEED"), 10, 64); err == nil {
		seed = v
	}
	if testing.Short() {
		n = 60
	}
	metrics.Init(zerolog.Nop())
	dataDir := filepath.Join(t.TempDir(), "data")
	truth := writePruneDiffData(t, dataDir)
	defer truth.Close()

	backend, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	duck, err := database.New(&database.Config{MemoryLimit: "512MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, zerolog.Nop())
	if err != nil {
		t.Skipf("duckdb unavailable: %v", err)
	}
	t.Cleanup(func() { duck.Close() })
	h := NewQueryHandler(duck, backend, zerolog.Nop(), 0, 0)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)

	g := &pruneDiffGen{r: rand.New(rand.NewSource(seed))}
	queries := append(pruneDiffFixed(), make([]string, 0, n)...)
	for i := 0; i < n; i++ {
		queries = append(queries, g.query())
	}
	if q := os.Getenv("ARC_PRUNE_DIFF_SQL"); q != "" {
		queries = []string{q}
	}
	compared, truthErr, failed, noFiles := 0, 0, 0, 0
	sameAs := pruneDiffSameAs()
	for _, q := range queries {
		tq := q
		if alt, ok := sameAs[q]; ok {
			tq = alt
		}
		want, err := pruneDiffRows(truth, tq)
		if err != nil {
			truthErr++
			continue
		}
		got, status, body := pruneDiffArc(t, app, q)
		if status != fiber.StatusOK {
			failed++
			t.Errorf("Arc refused a query DuckDB answers (status %d: %.300s):\n%s", status, body, q)
			continue
		}
		compared++
		if got == "" && want == "0/0/0" {
			// No file in the pruned range, and no row matches in the full
			// read either: Arc answers a read of no files with no rows
			// (46775154 does the same), where DuckDB's aggregate over the
			// full read has one all-zero row. No row is lost.
			noFiles++
			continue
		}
		if got != want {
			failed++
			t.Errorf("pruned answer differs from the full read:\n%s\n got  %s\n want %s", q, got, want)
		}
		if failed > 25 {
			t.Fatal("too many differences")
		}
	}
	t.Logf("%d queries: %d compared (%d empty with no file in range), %d invalid in DuckDB, %d differing",
		len(queries), compared, noFiles, truthErr, failed)
	if compared < (len(queries)+1)/2 {
		t.Fatalf("only %d of %d queries compared", compared, len(queries))
	}
}

// pruneDiffRows is DuckDB's answer reading every file, as canonical text.
func pruneDiffRows(db *gosql.DB, q string) (string, error) {
	rows, err := db.Query(q)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n, s, sq gosql.NullInt64
		if err := rows.Scan(&n, &s, &sq); err != nil {
			return "", err
		}
		out = append(out, fmt.Sprintf("%d/%d/%d", n.Int64, s.Int64, sq.Int64))
	}
	return strings.Join(out, ","), rows.Err()
}

func pruneDiffArc(t *testing.T, app *fiber.App, q string) (string, int, string) {
	t.Helper()
	body, _ := json.Marshal(QueryRequest{SQL: q})
	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 60000)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != fiber.StatusOK {
		return "", resp.StatusCode, string(raw)
	}
	var out QueryResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("response %s: %v", raw, err)
	}
	var rows []string
	for _, r := range out.Data {
		v := make([]int64, 3)
		for i := range v {
			if i < len(r) && r[i] != nil {
				f, _ := r[i].(float64)
				v[i] = int64(f)
			}
		}
		rows = append(rows, fmt.Sprintf("%d/%d/%d", v[0], v[1], v[2]))
	}
	return strings.Join(rows, ","), resp.StatusCode, ""
}

// The data: rows one microsecond before, on and after every hour of
// 2026-06-01..03 plus inside the hour, in hour directories; 2026-06-02 of cpu
// and 2026-06-01 of mem only as compacted day files; and old rows (1970,
// 2019, 2025). Ids are unique over both tables (cpu 1-370, mem 371-740).
func writePruneDiffData(t *testing.T, dataDir string) *gosql.DB {
	t.Helper()
	type row struct {
		id   int64
		ts   time.Time
		host string
		v    int
	}
	byDir := map[string][]row{}
	id := int64(1)
	add := func(meas string, ts time.Time) {
		ts = ts.UTC()
		dir := filepath.Join(dataDir, "default", meas, ts.Format("2006/01/02/15"))
		dayFile := (meas == "cpu" && ts.Format("2006-01-02") == "2026-06-02") ||
			(meas == "mem" && ts.Format("2006-01-02") == "2026-06-01")
		if dayFile {
			dir = filepath.Join(dataDir, "default", meas, ts.Format("2006/01/02"))
		}
		byDir[dir] = append(byDir[dir], row{id, ts, []string{"a", "b", "c"}[id%3], int(id % 7)})
		id++
	}
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	for _, meas := range []string{"cpu", "mem"} {
		for h := 0; h <= 72; h++ {
			at := start.Add(time.Duration(h) * time.Hour)
			for _, off := range []time.Duration{-time.Microsecond, 0, time.Microsecond, 17 * time.Minute, time.Hour - time.Microsecond} {
				add(meas, at.Add(off))
			}
		}
		for _, s := range []string{"1970-01-01T00:00:00.5Z", "2019-12-31T23:30:00Z", "2020-01-01T00:00:00Z", "2025-12-31T23:59:59.999999Z", "2026-05-31T23:00:00Z"} {
			ts, _ := time.Parse(time.RFC3339Nano, s)
			add(meas, ts)
		}
	}
	w, err := gosql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Exec("SET TimeZone = 'UTC'"); err != nil {
		t.Fatal(err)
	}
	for dir, rows := range byDir {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		var vals []string
		for _, r := range rows {
			vals = append(vals, fmt.Sprintf("(%d, TIMESTAMPTZ '%s', '%s', %d)", r.id, r.ts.Format("2006-01-02 15:04:05.000000Z07:00"), r.host, r.v))
		}
		q := "COPY (SELECT * FROM (VALUES " + strings.Join(vals, ",") + ") t(id, time, host, v)) TO '" +
			filepath.Join(dir, "part.parquet") + "' (FORMAT parquet)"
		if _, err := w.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	for _, meas := range []string{"cpu", "mem"} {
		if _, err := w.Exec("CREATE VIEW " + meas + " AS SELECT * FROM read_parquet('" +
			filepath.Join(dataDir, "default", meas) + "/**/*.parquet')"); err != nil {
			t.Fatal(err)
		}
	}
	w.SetMaxOpenConns(1)
	return w
}

// pruneDiffFixed are the shapes that used to answer with too few rows.
func pruneDiffFixed() []string {
	agg := func(inner string) string {
		return "SELECT count(*), sum(id), sum(id * id) FROM (" + inner + ") zz"
	}
	return []string{
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' OR host = 'a'"),
		agg("SELECT id FROM cpu WHERE host = 'a' OR time >= '2026-06-02 10:00:00' AND host = 'b'"),
		agg("SELECT id FROM cpu WHERE NOT (time >= '2026-06-02 10:00:00')"),
		agg("SELECT id FROM cpu WHERE time >= TIMESTAMPTZ '2026-06-02 11:00:00' - INTERVAL '3 hours'"),
		agg("SELECT id FROM cpu WHERE host = 'a'\n-- AND time >= '2026-06-02 10:00:00'\n"),
		agg("SELECT id FROM cpu WHERE host = 'a' /* AND time >= '2026-06-02 10:00:00' */"),
		agg("SELECT id FROM cpu WHERE CASE WHEN time >= '2026-06-02 10:00:00' THEN true ELSE host = 'a' END"),
		agg("SELECT id FROM cpu WHERE host = 'a' QUALIFY time >= '2026-06-02 10:00:00'"),
		agg("SELECT id FROM cpu WHERE (time >= '2026-06-02 10:00:00' OR time < '2026-06-01 08:00:00')"),
		agg("SELECT m.id FROM cpu c JOIN mem m ON c.v = m.v WHERE c.time >= '2026-06-03 10:00:00'"),
		agg("SELECT m.id FROM cpu c, mem m WHERE c.v = m.v AND c.time >= '2026-06-03 10:00:00'"),
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' UNION ALL SELECT id FROM mem"),
		agg("WITH m AS (SELECT * FROM mem) SELECT m.id FROM cpu, m WHERE cpu.v = m.v AND cpu.time >= '2026-06-03 10:00:00'"),
		agg("SELECT id FROM mem WHERE time < (SELECT max(time) FROM cpu WHERE time >= '2026-06-02 11:00:00')"),
		agg("SELECT id FROM (SELECT id, time, lag(time) OVER (ORDER BY time) p FROM cpu) WHERE time >= '2026-06-02 10:00:00' AND p < '2026-06-02 10:00:00'"),
		agg("SELECT id FROM (SELECT host, max(id) id, max(time) AS time FROM cpu GROUP BY host) s WHERE time >= '2026-06-02 10:00:00'"),
		agg("SELECT id FROM cpu WHERE host IN (SELECT host FROM mem WHERE time >= '2026-06-03 23:00:00')"),
		agg("SELECT id FROM cpu WHERE time >= '2026-06-01 08:00:00' AND time <= '2026-06-01 10:00:00'"),
		agg("SELECT id FROM cpu WHERE time BETWEEN '2026-06-01 08:00:00' AND '2026-06-01 10:00:00'"),
		agg("SELECT id FROM cpu WHERE time < '2024-01-01'"),
		agg("SELECT id FROM cpu WHERE time <= TIMESTAMP '2020-01-01 00:00:00'"),
		agg("SELECT id FROM cpu WHERE time >= TIMESTAMP '2026-06-02 10:00:00+02:00' AND time < '2026-06-02 12:00:00'"),
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00+02:00' AND time < '2026-06-02T09:00:00+02:00'"),
		agg("SELECT w.id FROM (SELECT id FROM cpu WHERE time >= '2026-06-03 00:00:00') w, (SELECT count(*) n FROM cpu WHERE time >= '2026-06-01 00:00:00') m"),
		agg("SELECT id FROM cpu WHERE time > TIMESTAMPTZ '2026-03-31 00:00:00' + INTERVAL '2 months'"),
		// A JOIN in parentheses: mem was pruned by cpu's range (5377 of
		// 10254 rows).
		agg("WITH c AS (SELECT id,v FROM cpu WHERE time >= '2026-06-02 10:00:00') SELECT m.id FROM (c JOIN mem m ON c.v=m.v)"),
		agg("WITH c AS (SELECT id,v FROM cpu WHERE time >= '2026-06-02 10:00:00') SELECT m.id FROM ((c JOIN mem m ON c.v=m.v))"),
		agg("WITH c AS (SELECT id,v FROM cpu WHERE time >= '2026-06-02 10:00:00') SELECT m.id FROM (c LEFT JOIN mem m ON c.v=m.v)"),
		agg("WITH c AS (SELECT id,v FROM cpu WHERE time >= '2026-06-02 10:00:00') SELECT m.id FROM ((SELECT * FROM c) x JOIN mem m ON x.v=m.v)"),
		agg("WITH c AS (SELECT id,v FROM cpu WHERE time >= '2026-06-02 10:00:00') SELECT m.id FROM c JOIN (c c2 JOIN mem m ON c2.v=m.v) ON c.id=c2.id"),
		// A non-ASCII space before FROM: mem was pruned by cpu's range (388
		// of 564 rows with U+00A0).
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' UNION ALL SELECT id\u00a0FROM mem"),
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' UNION ALL SELECT id\u200bFROM mem"),
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' UNION ALL SELECT id\u2003FROM mem"),
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00'\u00a0UNION ALL SELECT id FROM mem"),
		// In a string literal it is not a space: pruned as without it.
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' AND host <> 'a\u00a0FROM mem'"),
		// A keyword after a dot is a name to DuckDB; read as a keyword it cut
		// the top-level OR off the WHERE (194 of 252 and 194 of 370 rows).
		agg("SELECT m.id FROM mem m, (SELECT 'b' AS \"group\") g WHERE m.time >= '2026-06-02 10:00:00' AND g.group = 'b' OR m.host = 'a'"),
		agg("SELECT m.id FROM mem m, (SELECT true AS \"limit\") s WHERE m.time >= '2026-06-02 10:00:00' AND s.limit OR true"),
		agg("SELECT m.id FROM mem m, (SELECT true AS \"order\") s WHERE m.time >= '2026-06-02 10:00:00' AND s.order OR true"),
		agg("SELECT m.id FROM mem m, (SELECT true AS \"select\") s WHERE m.time >= '2026-06-02 10:00:00' AND s.select OR true"),
		agg("SELECT m.id FROM mem m, (SELECT true AS \"union\") s WHERE m.time >= '2026-06-02 10:00:00' AND s.union OR true"),
		agg("SELECT m.id FROM mem m, (SELECT 1 AS \"case\", 2 AS \"end\") s WHERE m.time >= '2026-06-02 10:00:00' AND s.case = 1 OR s.end = 2"),
		agg("SELECT m.id FROM (SELECT true AS \"where\") c RIGHT JOIN mem m ON c.where AND m.time >= '2026-06-02 10:00:00'"),
		agg("SELECT m.id FROM mem m, (SELECT true AS \"limit\") s WHERE m.time >= '2026-06-02 10:00:00' AND s . /* c */ limit OR true"),
		agg("SELECT m.id FROM mem m, (SELECT 'b' AS \"group\") g WHERE m.time >= '2026-06-02 10:00:00' AND g.\"group\" = 'b' AND m.v > 1.5"),
		// A form feed before "(": a table function to the range check, a
		// measurement read with a column alias list to the rewrite (388 of
		// 564 rows). The full read is pruneDiffSameAs's.
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' UNION ALL SELECT mem.id FROM mem\f(id)"),
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' UNION ALL SELECT mem.x AS id FROM mem\f(x)"),
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' AND\ftime < '2026-06-02 12:00:00'"),
		// A keyword after AS is an alias to DuckDB; read as CASE/END it moved
		// the nesting depth, and the top-level OR after it was missed (54 of
		// 80 and 82 of 133 rows).
		agg("SELECT id FROM mem WHERE time >= '2026-06-02 10:00:00' AND v IN (SELECT 1 AS case) OR v IN (SELECT 2 AS end)"),
		agg("SELECT m.id FROM mem m WHERE m.time >= '2026-06-02 10:00:00' AND m.v IN (SELECT 1 AS case) OR m.v IN (SELECT 2 AS \"x\" UNION ALL SELECT 3 AS end)"),
		agg("SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' AND v = (SELECT 1 AS case) UNION ALL SELECT id FROM mem WHERE v = (SELECT 2 AS end)"),
		agg("SELECT id FROM mem WHERE time >= '2026-06-02 10:00:00' AND v IN (SELECT 1 AS \"case\") AND v NOT IN (SELECT 2 AS \"end\")"),
	}
}

// pruneDiffSameAs maps fixed queries DuckDB does not read on their own the
// way Arc's rewrite runs them to SQL that reads the same rows: `mem\f(id)`
// is a call of a table function mem to DuckDB, and the rewrite runs a
// measurement read with the column alias list (id).
func pruneDiffSameAs() map[string]string {
	agg := func(inner string) string {
		return "SELECT count(*), sum(id), sum(id * id) FROM (" + inner + ") zz"
	}
	const cpu = "SELECT id FROM cpu WHERE time >= '2026-06-02 10:00:00' UNION ALL "
	return map[string]string{
		agg(cpu + "SELECT mem.id FROM mem\f(id)"):     agg(cpu + "SELECT mem.id FROM mem AS mem(id)"),
		agg(cpu + "SELECT mem.x AS id FROM mem\f(x)"): agg(cpu + "SELECT mem.x AS id FROM mem AS mem(x)"),
	}
}

// pruneDiffGen generates the queries. simple (half of them) keeps every
// WHERE to AND-ed bounds and filters, the shapes that do get pruned.
type pruneDiffGen struct {
	r      *rand.Rand
	simple bool
}

func (g *pruneDiffGen) pick(xs ...string) string { return xs[g.r.Intn(len(xs))] }

// instant is a random instant near the data: an hour of 2026-05-31..06-04,
// on it or a microsecond, second or half hour either side.
func (g *pruneDiffGen) instant() time.Time {
	base := time.Date(2026, 5, 31, 22, 0, 0, 0, time.UTC).Add(time.Duration(g.r.Intn(80)) * time.Hour)
	offs := []time.Duration{0, 0, 0, time.Microsecond, -time.Microsecond, time.Second, -time.Second, 30 * time.Minute}
	return base.Add(offs[g.r.Intn(len(offs))])
}

// literal spells ts in one of the forms DuckDB accepts, each meaning ts
// against a TIMESTAMPTZ column in a UTC session.
func (g *pruneDiffGen) literal(ts time.Time) string {
	frac := ""
	if ts.Nanosecond() != 0 {
		frac = ts.Format(".000000")
	}
	zone := time.FixedZone("", []int{0, 3 * 3600, -5 * 3600, 5*3600 + 1800}[g.r.Intn(4)])
	local := ts.In(zone)
	naive := ts.Format("2006-01-02 15:04:05") + frac
	switch g.r.Intn(14) {
	case 0:
		return "'" + naive + "'"
	case 1:
		return "'" + ts.Format("2006-01-02T15:04:05") + frac + "Z'"
	case 2:
		return "'" + local.Format("2006-01-02 15:04:05") + frac + local.Format("-07:00") + "'"
	case 3:
		return "'" + local.Format("2006-01-02T15:04:05") + frac + local.Format("Z07:00") + "'"
	case 4:
		return "TIMESTAMP '" + naive + "'"
	case 5:
		return "TIMESTAMPTZ '" + local.Format("2006-01-02 15:04:05") + frac + local.Format("-07:00") + "'"
	case 6:
		return "'" + naive + "'::timestamp"
	case 7:
		return "'" + local.Format("2006-01-02 15:04:05") + frac + local.Format("-07:00") + "'::timestamptz"
	case 8:
		if ts.Equal(ts.Truncate(24 * time.Hour)) {
			return "DATE '" + ts.Format("2006-01-02") + "'"
		}
		return "'" + naive + "'"
	case 9:
		return "CAST('" + naive + "' AS TIMESTAMPTZ)"
	case 10:
		return "TIMESTAMPTZ '" + ts.Add(time.Hour).Format("2006-01-02 15:04:05") + frac + "+00' - INTERVAL 1 HOUR"
	case 11:
		return "TIMESTAMP '" + ts.Add(-2*time.Hour).Format("2006-01-02 15:04:05") + frac + "' + INTERVAL '2 hours'"
	case 12:
		// A wall clock with an offset under TIMESTAMP: DuckDB drops the
		// offset, so this is the wall clock read as UTC.
		return "TIMESTAMP '" + naive + "+03:00'"
	default:
		days := int(time.Since(ts).Hours()/24) + 1
		return fmt.Sprintf("now() - INTERVAL '%d days'", days-g.r.Intn(3))
	}
}

// bound is one time predicate on col.
func (g *pruneDiffGen) bound(col string) string {
	a, b := g.instant(), g.instant()
	if b.Before(a) {
		a, b = b, a
	}
	switch g.r.Intn(9) {
	case 0, 1:
		return col + " " + g.pick(">=", ">") + " " + g.literal(a)
	case 2, 3:
		return col + " " + g.pick("<", "<=") + " " + g.literal(b)
	case 4:
		return col + " BETWEEN " + g.literal(a) + " AND " + g.literal(b)
	default:
		return col + " " + g.pick(">=", ">") + " " + g.literal(a) + " AND " + col + " " + g.pick("<", "<=") + " " + g.literal(b)
	}
}

// where is a WHERE body on col: bounds, other predicates and the shapes
// that hide a bound from a regex.
func (g *pruneDiffGen) where(col, host string) string {
	pick := g.r.Intn(12)
	if g.simple {
		pick = 6 + g.r.Intn(6)
	}
	switch pick {
	case 0:
		return g.bound(col) + " OR " + host + " = 'a'"
	case 1:
		return "NOT (" + g.bound(col) + ")"
	case 2:
		return "(" + g.bound(col) + " OR " + g.bound(col) + ")"
	case 3:
		return "CASE WHEN " + g.bound(col) + " THEN true ELSE " + host + " = 'b' END"
	case 4:
		return host + " <> 'c' -- AND " + g.bound(col) + "\n"
	case 5:
		return host + " <> 'c' /* AND " + g.bound(col) + " */ AND " + g.bound(col)
	case 6:
		return "(" + g.bound(col) + ") AND " + host + " IN ('a', 'b')"
	default:
		w := g.bound(col)
		if g.r.Intn(2) == 0 {
			w += " AND " + g.bound(col)
		}
		if g.r.Intn(2) == 0 {
			w += " AND " + host + " " + g.pick("=", "<>") + " '" + g.pick("a", "b", "c") + "'"
		}
		return w
	}
}

func (g *pruneDiffGen) query() string {
	g.simple = g.r.Intn(2) == 0
	t1, t2 := g.pick("cpu", "mem"), g.pick("cpu", "mem")
	var inner string
	switch g.r.Intn(13) {
	case 0, 1, 2:
		inner = "SELECT id FROM " + t1 + " WHERE " + g.where(g.pick("time", "time", "time::timestamp", "time::timestamptz"), "host")
	case 3:
		inner = "WITH x AS (SELECT * FROM " + t1 + " WHERE " + g.where("time", "host") + ") SELECT id FROM x WHERE " + g.where("time", "host")
	case 4:
		inner = "SELECT id FROM (SELECT * FROM " + t1 + " WHERE " + g.where("time", "host") + ") s WHERE " + g.where("s.time", "s.host")
	case 5:
		inner = "SELECT id FROM " + t1 + " WHERE " + g.where("time", "host") + " UNION ALL SELECT id FROM " + t2 + " WHERE " + g.where("time", "host")
	case 6:
		inner = "SELECT a.id + b.id AS id FROM cpu a JOIN mem b ON a.v = b.v AND a.host = b.host WHERE " +
			g.where("a.time", "a.host") + " AND " + g.where("b.time", "b.host")
	case 7:
		inner = "SELECT id FROM " + t1 + " WHERE time < (SELECT max(time) FROM " + t2 + " WHERE " + g.where("time", "host") + ") AND " + g.bound("time")
	case 8:
		inner = "SELECT id FROM " + t1 + " WHERE host IN (SELECT host FROM " + t2 + " WHERE " + g.where("time", "host") + ") AND " + g.where("time", "host")
	case 9:
		inner = "SELECT a.id FROM cpu a, mem b WHERE a.id = b.id - 370 AND " + g.where(g.pick("a.time", "b.time", "a.time::timestamp"), "a.host")
	case 10:
		inner = "SELECT id FROM " + t1 + " WHERE " + g.where("time", "host") + " QUALIFY row_number() OVER (PARTITION BY host ORDER BY id) " + g.pick("> 0", "< 5")
	case 11:
		inner = "WITH c AS (SELECT host, min(time) t0 FROM " + t1 + " WHERE " + g.where("time", "host") + " GROUP BY 1) " +
			"SELECT e.id FROM " + t2 + " e JOIN c ON e.host = c.host WHERE " + g.where("time", "e.host") + " AND e.time >= c.t0"
	default:
		inner = "SELECT e.id FROM " + t1 + " e LEFT JOIN " + t2 + " m ON e.id = m.id - 370 WHERE " + g.where(g.pick("e.time", "m.time", "time"), "e.host")
	}
	return "SELECT count(*), sum(id), sum(id * id) FROM (" + inner + "\n) zz"
}
