//go:build duckdb_arrow

package api

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/rollup"
	"github.com/basekick-labs/arc/internal/storage"
)

// QR-6: when partition pruning cannot VERIFY which partitions exist (a
// storage listing failed or timed out), or DuckDB finds no files on a
// partition a listing reported, the query must never be answered with a
// silently partial or empty result. It is answered 503 ("retry") and counted
// in arc_query_listing_incomplete_total; a genuinely empty result stays an
// empty 200.

// flakyStore is an S3-rooted backend (its read paths are s3://arc-qr6/...)
// whose directory and file listings are served from maps, with injectable
// failures and hangs. Everything else is the real S3 backend, which is never
// contacted: these tests fail before DuckDB would read anything.
type flakyStore struct {
	storage.Backend
	s3       *storage.S3Backend
	dirs     map[string][]string // ListDirectories(prefix) -> child names
	dayFiles map[string][]string // List(day prefix) -> direct file names
	failDirs atomic.Bool
	failList atomic.Bool
	hang     atomic.Bool
}

func (f *flakyStore) Unwrap() storage.Backend { return f.s3 }

func (f *flakyStore) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	if f.hang.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.failDirs.Load() {
		return nil, errors.New("operation error S3: ListObjectsV2, https response error StatusCode: 503, SlowDown")
	}
	return f.dirs[prefix], nil
}

func (f *flakyStore) List(ctx context.Context, prefix string) ([]string, error) {
	if f.hang.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.failList.Load() {
		return nil, errors.New("operation error S3: ListObjectsV2, context deadline exceeded")
	}
	var out []string
	for _, name := range f.dayFiles[prefix] {
		out = append(out, prefix+name)
	}
	return out, nil
}

type qr6RemoteEnv struct {
	t     *testing.T
	h     *QueryHandler
	app   *fiber.App
	store *flakyStore
	tmp   string
}

func newQR6RemoteEnv(t *testing.T) *qr6RemoteEnv {
	t.Helper()
	metrics.Init(zerolog.Nop())
	// The S3 backend only supplies the s3:// read root. Its constructor's
	// bucket probe is answered locally; nothing else ever reaches it.
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	t.Cleanup(probe.Close)
	s3, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: "arc-qr6", Region: "us-east-1", Endpoint: probe.URL,
		AccessKey: "test", SecretKey: "test", PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	store := &flakyStore{Backend: s3, s3: s3,
		dirs: map[string][]string{
			"default/events/2026/06/":    {"01", "02"},
			"default/events/2026/06/01/": {"10", "11"},
			"default/events/2026/06/02/": {},
		},
		dayFiles: map[string][]string{"default/events/2026/06/02/": {"daily.parquet"}},
	}
	tmp := t.TempDir()
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: tmp}, zerolog.Nop())
	if err != nil {
		t.Skipf("duckdb unavailable: %v", err)
	}
	t.Cleanup(func() { duck.Close() })
	h := NewQueryHandler(duck, store, zerolog.Nop(), 0, 0)
	h.Pruner().SetListingTimeout(200 * time.Millisecond)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)
	return &qr6RemoteEnv{t: t, h: h, app: app, store: store, tmp: tmp}
}

func (e *qr6RemoteEnv) post(path, sql string, headers map[string]string) (int, string, string) {
	e.t.Helper()
	body, _ := json.Marshal(QueryRequest{SQL: sql})
	req := httptest.NewRequest("POST", path, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := e.app.Test(req, 30000)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw), resp.Header.Get("Retry-After")
}

const qr6SQL = "SELECT count(*) AS n FROM events WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-03T00:00:00Z'"

func listingIncompleteTotal(t *testing.T) int64 {
	t.Helper()
	return queryMetricInt(t, "query_listing_incomplete_total")
}

// A healthy listing is unchanged: the transform prunes to exactly the listed
// hour partitions and the daily-compacted day.
func TestHealthyListingPrunesUnchanged_QR6(t *testing.T) {
	e := newQR6RemoteEnv(t)
	sql, _, err := e.h.getTransformedSQL(context.Background(), qr6SQL, "")
	if err != nil {
		t.Fatalf("healthy listing: %v", err)
	}
	for _, want := range []string{
		"s3://arc-qr6/default/events/2026/06/01/10/*.parquet",
		"s3://arc-qr6/default/events/2026/06/01/11/*.parquet",
		"s3://arc-qr6/default/events/2026/06/02/*.parquet",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("healthy transform must read %s, got %s", want, sql)
		}
	}
	if strings.Contains(sql, "/**/") || strings.Contains(sql, "2026/06/01/12/") {
		t.Fatalf("healthy transform must be pruned to the listed partitions, got %s", sql)
	}
}

// Every failure mode, on every query endpoint and on the parallel-partition
// transform: 503 with the listing message, Retry-After and the metric — never
// a partial or empty result.
func TestListingFailureAnswers503_QR6(t *testing.T) {
	modes := map[string]func(*flakyStore){
		"parent ListDirectories error": func(f *flakyStore) { f.failDirs.Store(true) },
		"day-level List error":         func(f *flakyStore) { f.failList.Store(true) },
		"listing timeout":              func(f *flakyStore) { f.hang.Store(true) },
	}
	requests := []struct {
		name, path, sql string
		headers         map[string]string
	}{
		{"JSON", "/api/v1/query", qr6SQL, nil},
		{"Arrow IPC", "/api/v1/query/arrow", qr6SQL, nil},
		{"msgpack", "/api/v1/query/msgpack", qr6SQL, nil},
		{"estimate", "/api/v1/query/estimate", qr6SQL, nil},
		// No quotes + x-arc-database: the parallel-partition transform.
		{"JSON parallel transform", "/api/v1/query", "SELECT * FROM events WHERE time > NOW() - INTERVAL 6 HOUR",
			map[string]string{"x-arc-database": "default"}},
	}
	for mode, breakIt := range modes {
		for _, r := range requests {
			t.Run(mode+"/"+r.name, func(t *testing.T) {
				e := newQR6RemoteEnv(t)
				if mode != "day-level List error" || r.name != "JSON parallel transform" {
					breakIt(e.store)
				} else {
					// The NOW()-relative window has no listed day dirs, so it
					// never reaches a day-level List; break the parent instead.
					e.store.failDirs.Store(true)
				}
				before := listingIncompleteTotal(t)
				start := time.Now()
				status, body, retryAfter := e.post(r.path, r.sql, r.headers)
				if status != fiber.StatusServiceUnavailable {
					t.Fatalf("status %d, want 503: %s", status, body)
				}
				if !strings.Contains(body, listingIncompleteMessage) {
					t.Fatalf("body must say %q: %s", listingIncompleteMessage, body)
				}
				if retryAfter == "" {
					t.Fatal("503 must carry Retry-After")
				}
				if got := listingIncompleteTotal(t); got != before+1 {
					t.Fatalf("arc_query_listing_incomplete_total = %d, want %d", got, before+1)
				}
				if time.Since(start) > 5*time.Second {
					t.Fatalf("a failed listing must fail fast, took %v", time.Since(start))
				}
				// Storage recovers: the next transform is pruned and complete
				// (nothing from the failed attempt was cached).
				e.store.failDirs.Store(false)
				e.store.failList.Store(false)
				e.store.hang.Store(false)
				if _, _, err := e.h.getTransformedSQL(context.Background(), qr6SQL, ""); err != nil {
					t.Fatalf("after recovery: %v", err)
				}
			})
		}
	}
}

// A rollup-served query whose merge read hits "No files found" on a partition
// a listing reported, while the source listing is failing: the source
// fallback cannot be built, and the answer must be 503, not an empty 200. A
// schema-drift failure that needs the same unavailable fallback is 503 too.
func TestRollupFallbackBlockedByListingIs503_QR6(t *testing.T) {
	cases := map[string]string{
		"no files on a listed tail partition": "SELECT count(*) AS n FROM read_parquet('%s/default/events/2026/06/03/05/*.parquet')",
		"schema drift":                        `SELECT count(*) AS n FROM (SELECT 1 AS x) t WHERE "site" IS NOT NULL -- %s`,
	}
	for name, tmpl := range cases {
		for _, path := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
			t.Run(name+path, func(t *testing.T) {
				e := newQR6RemoteEnv(t)
				e.h.SetRollupRouter(&fakeRollupRouter{serveSQL: fmt.Sprintf(tmpl, e.tmp)})
				e.store.failDirs.Store(true)
				status, body, _ := e.post(path, qr6SQL, nil)
				if status != fiber.StatusServiceUnavailable || !strings.Contains(body, listingIncompleteMessage) {
					t.Fatalf("status %d, want 503 listing-incomplete: %s", status, body)
				}
			})
		}
	}
}

// ---- execution-level tests on local storage with real files ----

// compactDay simulates a daily compaction on ANOTHER pod: the hour files are
// merged into one day-level file and the hour directories are deleted, while
// this query pod's listing caches still hold the hour directories.
func compactDay(t *testing.T, e *forkQueryEnv, meas, day string, hours []string, rows int) {
	t.Helper()
	dayDir := filepath.Join(e.dataDir, "default", meas, strings.ReplaceAll(day, "-", "/"))
	e.writeParquet(filepath.Join("default", meas, strings.ReplaceAll(day, "-", "/"), "daily.parquet"), fmt.Sprintf(
		`SELECT TIMESTAMPTZ '%s 10:00:00+00' + INTERVAL (i) MINUTE AS time, i AS v FROM range(%d) t(i)`, day, rows))
	for _, hh := range hours {
		if err := os.RemoveAll(filepath.Join(dayDir, hh)); err != nil {
			t.Fatal(err)
		}
	}
}

func writeHours(e *forkQueryEnv, meas, day string, hours []string, perHour int) {
	for _, hh := range hours {
		e.writeParquet(fmt.Sprintf("default/%s/%s/%s/part.parquet", meas, strings.ReplaceAll(day, "-", "/"), hh), fmt.Sprintf(
			`SELECT TIMESTAMPTZ '%s %s:00:00+00' + INTERVAL (i) MINUTE AS time, i AS v FROM range(%d) t(i)`, day, hh, perHour))
	}
}

// The QR-6 stale-cache case: after a daily compaction on another pod, the
// query pod's cached listing still names the deleted hour directories, and
// DuckDB raises "No files found". That used to be an EMPTY 200. It is now
// retried once with fresh listings and returns the complete count, on both
// endpoints.
func TestStaleListingAfterCompactionRetriesFresh_QR6(t *testing.T) {
	const q = "SELECT count(*) AS n FROM logs WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z'"
	for _, endpoint := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
		t.Run(endpoint, func(t *testing.T) {
			e := newForkQueryEnv(t)
			writeHours(e, "logs", "2026-06-01", []string{"10", "11"}, 60)
			count := func() (int, float64, string) {
				if endpoint == "/api/v1/query" {
					status, out, _ := e.post(endpoint, q, nil)
					if status != fiber.StatusOK || len(out.Data) != 1 {
						return status, -1, out.raw
					}
					return status, out.Data[0][0].(float64), out.raw
				}
				status, cols, _, raw := (&qr3Env{forkQueryEnv: e}).postArrow(q)
				if status != fiber.StatusOK || len(cols["n"]) != 1 {
					return status, -1, raw
				}
				return status, cols["n"][0].(float64), raw
			}
			if status, n, raw := count(); status != fiber.StatusOK || n != 120 {
				t.Fatalf("before compaction: status %d n=%v: %s", status, n, raw)
			}
			compactDay(t, e, "logs", "2026-06-01", []string{"10", "11"}, 120)
			if status, n, raw := count(); status != fiber.StatusOK || n != 120 {
				t.Fatalf("after compaction on another pod (stale listing): status %d n=%v, want 200 with 120: %s", status, n, raw)
			}
		})
	}
}

// Genuine empties stay empty 200s: a measurement with no files at all, and a
// time range every listing verified empty.
func TestGenuineEmptyStaysEmpty_QR6(t *testing.T) {
	e := newForkQueryEnv(t)
	writeHours(e, "logs", "2026-06-01", []string{"10"}, 5)
	before := listingIncompleteTotal(t)
	for _, q := range []string{
		"SELECT * FROM never_written WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z'",
		"SELECT * FROM logs WHERE time >= '2026-07-01T00:00:00Z' AND time < '2026-07-02T00:00:00Z'",
	} {
		for _, path := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
			if path == "/api/v1/query" {
				status, out, _ := e.post(path, q, nil)
				if status != fiber.StatusOK || !out.Success || len(out.Data) != 0 {
					t.Fatalf("%s %q: want empty 200, got %d %s", path, q, status, out.raw)
				}
				continue
			}
			status, cols, _, raw := (&qr3Env{forkQueryEnv: e}).postArrow(q)
			if status != fiber.StatusOK || len(cols["time"]) != 0 {
				t.Fatalf("%s %q: want empty 200, got %d %s", path, q, status, raw)
			}
		}
	}
	if got := listingIncompleteTotal(t); got != before {
		t.Fatalf("genuine empties must not count as listing-incomplete (%d -> %d)", before, got)
	}
}

// The parallel-partition executor follows the same rule: a partition that a
// (stale) listing reported but DuckDB cannot find fails the query with 503,
// never a result without that partition's rows; the next query re-lists and
// is complete.
func TestParallelStalePartitionIs503ThenRecovers_QR6(t *testing.T) {
	e := newForkQueryEnv(t)
	now := time.Now().UTC().Truncate(time.Hour)
	var hours []time.Time
	for i := 1; i <= 3; i++ {
		h := now.Add(-time.Duration(i) * time.Hour)
		hours = append(hours, h)
		e.writeParquet(h.Format("default/plogs/2006/01/02/15/part.parquet"),
			fmt.Sprintf("SELECT TIMESTAMPTZ '%s' + INTERVAL 10 MINUTE AS time, %d AS v", h.Format("2006-01-02 15:04:05+00"), i))
	}
	const q = "SELECT * FROM plogs WHERE time > NOW() - INTERVAL 6 HOUR"
	hdr := map[string]string{"x-arc-database": "default"}
	if _, info, _, err := e.h.getTransformedSQLForParallel(context.Background(), q, "default"); err != nil || info == nil {
		t.Fatalf("precondition: the query must take the parallel-partition path (info=%v err=%v)", info, err)
	}
	status, out, _ := e.post("/api/v1/query", q, hdr)
	if status != fiber.StatusOK || len(out.Data) != 3 {
		t.Fatalf("before compaction: status %d rows %d: %s", status, len(out.Data), out.raw)
	}

	// Compact the oldest hour into its day on another pod.
	old := hours[2]
	e.writeParquet(old.Format("default/plogs/2006/01/02/daily.parquet"),
		fmt.Sprintf("SELECT TIMESTAMPTZ '%s' + INTERVAL 10 MINUTE AS time, 3 AS v", old.Format("2006-01-02 15:04:05+00")))
	if err := os.RemoveAll(filepath.Join(e.dataDir, old.Format("default/plogs/2006/01/02/15"))); err != nil {
		t.Fatal(err)
	}
	before := listingIncompleteTotal(t)
	status, out, _ = e.post("/api/v1/query", q, hdr)
	if status == fiber.StatusOK {
		if len(out.Data) != 3 {
			t.Fatalf("a stale partition must never return a partial result: %d rows: %s", len(out.Data), out.raw)
		}
	} else {
		if status != fiber.StatusServiceUnavailable || !strings.Contains(out.raw, listingIncompleteMessage) {
			t.Fatalf("status %d, want 503 listing-incomplete: %s", status, out.raw)
		}
		if got := listingIncompleteTotal(t); got != before+1 {
			t.Fatalf("metric %d -> %d, want +1", before, got)
		}
	}
	status, out, _ = e.post("/api/v1/query", q, hdr)
	if status != fiber.StatusOK || len(out.Data) != 3 {
		t.Fatalf("after re-listing: status %d rows %d: %s", status, len(out.Data), out.raw)
	}
}

func TestUnverifiedNoFilesClassification_QR6(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	noFiles := func(p string) error {
		return fmt.Errorf("IO Error: No files found that match the pattern \"%s\"", p)
	}
	ctx := withSourceQuery(context.Background(), "SELECT * FROM read_parquet('/u/db/m/2026/06/01/00/*.parquet')", "", false, false)
	cases := []struct {
		pattern string
		want    bool
	}{
		{"s3://b/db/m/2026/06/01/10/*.parquet", true},                      // a listed partition that is gone
		{"s3://b/db/m/2026/06/01/*.parquet", true},                         // a listed daily partition
		{"s3://b/db/m/**/*.parquet", false},                                // the whole measurement: genuinely empty
		{"s3://b/db/m/2026/06/01/00/_arc_verified_empty_*.parquet", false}, // proven empty
		{"/u/db/m/2026/06/01/00/*.parquet", false},                         // the caller's own read_parquet path
		{"s3://b/_arc/rollup/x/2026-06-01.parquet", false},                 // not a partition read
	}
	for _, tc := range cases {
		if got := h.isUnverifiedNoFiles(ctx, noFiles(tc.pattern)); got != tc.want {
			t.Errorf("%s: unverified=%v, want %v", tc.pattern, got, tc.want)
		}
	}
	if h.isUnverifiedNoFiles(ctx, errors.New("Binder Error: x")) {
		t.Error("a non-no-files error is not an unverified no-files")
	}
}

// The rollup router's fresh tail follows the same rule. The cube lags a day,
// so yesterday is still read from source by the merge tail; a daily
// compaction on another pod then deletes yesterday's hour directories while
// this pod's tail listing and source transform are still cached. The merge
// tail and the source fallback both hit "No files found"; that used to end as
// an EMPTY 200. Now the fallback is re-derived with fresh listings and the
// count is complete, on both endpoints.
func TestRollupTailStaleAfterCompactionIsComplete_QR6(t *testing.T) {
	for _, endpoint := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
		t.Run(endpoint, func(t *testing.T) {
			e := newForkQueryEnv(t)
			for _, day := range []string{"2026-06-01", "2026-06-02"} {
				var hours []string
				for h := 0; h < 24; h++ {
					hours = append(hours, fmt.Sprintf("%02d", h))
				}
				writeHours(e, "events", day, hours, 60)
			}
			writeHours(e, "events", "2026-06-03", []string{"00", "01", "02", "03", "04", "05", "06", "07", "08", "09"}, 60)

			spec := rollup.CubeSpec{Source: "default.events", Grain: "hour", Aggs: []rollup.Aggregate{{Kind: rollup.AggCount}}}
			cubeDir := filepath.Join(e.dataDir, "_arc", "rollup", "events_coarse")
			if err := os.MkdirAll(cubeDir, 0o755); err != nil {
				t.Fatal(err)
			}
			builder, err := gosql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer builder.Close()
			entry, err := rollup.BuildDay(builder, spec, "'"+filepath.Join(e.dataDir, "default/events/2026/06/01/**/*.parquet")+"'",
				"time", "2026-06-01", filepath.Join(cubeDir, "2026-06-01.parquet"))
			if err != nil {
				t.Fatal(err)
			}
			m := &rollup.Manifest{CubeID: "events_coarse", Source: spec.Source, Grain: spec.Grain, Aggs: spec.Aggs, SchemaHash: spec.SchemaHash()}
			m.Upsert(entry)
			whole := filepath.Join(e.dataDir, "default", "events", "**", "*.parquet")
			r := rollup.NewRouter([]*rollup.Manifest{m}, "time",
				func(string) string { return "'" + whole + "'" },
				func(string) string { return "2026-06-03 12:00:00" })
			pruner := e.h.Pruner()
			r.SourceWindow = func(_, lo, hi string) string {
				loT, err1 := time.Parse("2006-01-02 15:04:05.999999999-07", lo)
				hiT, err2 := time.Parse("2006-01-02 15:04:05.999999999-07", hi)
				if err1 != nil || err2 != nil {
					return "'" + whole + "'"
				}
				paths, ok := pruner.ExistingPartitionPaths(whole, loT, hiT)
				if !ok {
					return "'" + whole + "'"
				}
				if len(paths) == 0 {
					return ""
				}
				return "['" + strings.Join(paths, "', '") + "']"
			}
			e.h.SetRollupRouter(r)

			const q = "SELECT count(*) AS n FROM events WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-04T00:00:00Z'"
			count := func() (int, float64, string, string) {
				if endpoint == "/api/v1/query" {
					status, out, hdr := e.post(endpoint, q, nil)
					if status != fiber.StatusOK || len(out.Data) != 1 {
						return status, -1, hdr["X-Arc-Rollup-Cube"], out.raw
					}
					return status, out.Data[0][0].(float64), hdr["X-Arc-Rollup-Cube"], out.raw
				}
				status, cols, hdr, raw := (&qr3Env{forkQueryEnv: e}).postArrow(q)
				if status != fiber.StatusOK || len(cols["n"]) != 1 {
					return status, -1, hdr["X-Arc-Rollup-Cube"], raw
				}
				return status, cols["n"][0].(float64), hdr["X-Arc-Rollup-Cube"], raw
			}
			if status, n, cube, raw := count(); status != fiber.StatusOK || n != 3480 || cube == "" {
				t.Fatalf("before compaction: status %d n=%v cube=%q: %s", status, n, cube, raw)
			}

			var hours []string
			for h := 0; h < 24; h++ {
				hours = append(hours, fmt.Sprintf("%02d", h))
			}
			compactDay(t, e, "events", "2026-06-02", hours, 1440)
			if status, n, _, raw := count(); status != fiber.StatusOK || n != 3480 {
				t.Fatalf("after compaction on another pod: status %d n=%v, want 200 with 3480: %s", status, n, raw)
			}
		})
	}
}
