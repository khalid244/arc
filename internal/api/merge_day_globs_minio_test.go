//go:build duckdb_arrow

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// query.merge_day_globs against a real S3 API (MinIO). DuckDB lists a day
// folder for every day glob of a query, one LIST after another: on a slow store
// that is most of a cached query's time (prod 2026-11-17: one cached day 2.3 s
// with its LIST and 0.02 s without, a 60-day count 75 s). With the option the
// day globs of a month that share their first digit are sent as one glob,
// which costs one LIST and matches the same files. Set ARC_TEST_MINIO_ENDPOINT
// (host:port, keys minioadmin/minioadmin) and ARC_TEST_MINIO_BUCKET to run.

type dayGlobEnv struct {
	t       *testing.T
	ctx     context.Context
	backend *storage.S3Backend
	duck    *database.DuckDB
	db      string
	// lists returns the LIST requests since the last call.
	lists func() int
}

func newDayGlobEnv(t *testing.T, name string) *dayGlobEnv {
	t.Helper()
	endpoint, bucket := os.Getenv("ARC_TEST_MINIO_ENDPOINT"), os.Getenv("ARC_TEST_MINIO_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("ARC_TEST_MINIO_ENDPOINT / ARC_TEST_MINIO_BUCKET not set")
	}
	metrics.Init(zerolog.Nop())
	target, _ := url.Parse("http://" + endpoint)
	rp := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	n := 0
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Has("list-type") {
			mu.Lock()
			n++
			mu.Unlock()
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	backend, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: proxy.URL,
		AccessKey: "minioadmin", SecretKey: "minioadmin", PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	duck, err := database.New(&database.Config{
		MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 8,
		S3Region: "us-east-1", S3AccessKey: "minioadmin", S3SecretKey: "minioadmin",
		S3Endpoint: strings.TrimPrefix(proxy.URL, "http://"), S3PathStyle: true, S3Bucket: bucket, S3IsPrimaryBackend: true,
		EnableS3Cache: true, S3CacheSize: 64 << 20, S3CacheTTLSeconds: 3600, S3GlobCacheTTLSeconds: 60,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { duck.Close() })
	return &dayGlobEnv{
		t: t, ctx: context.Background(), backend: backend, duck: duck,
		db: fmt.Sprintf("%s_%d", name, os.Getpid()),
		lists: func() int {
			mu.Lock()
			defer mu.Unlock()
			l := n
			n = 0
			return l
		},
	}
}

// put writes a parquet file under key (relative to the measurement) from
// selectSQL and removes it when the test ends.
func (e *dayGlobEnv) put(key, selectSQL string) {
	e.t.Helper()
	full := e.db + "/m/" + key
	if err := e.backend.Write(e.ctx, full, parquetBytes(e.t, selectSQL)); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = e.backend.Delete(e.ctx, full) })
}

// putRows writes rows rows timestamped on 2026-05-<day>.
func (e *dayGlobEnv) putRows(key string, day, rows int) {
	e.put(key, fmt.Sprintf("SELECT TIMESTAMPTZ '2026-05-%02d 00:00:00+00' + INTERVAL (i) SECOND AS time FROM range(%d) t(i)", day, rows))
}

// handler is a fresh query handler (its own listing and SQL caches) with the
// option on or off; DuckDB's caches are emptied too.
func (e *dayGlobEnv) handler(on bool) *forkQueryEnv {
	f := &forkQueryEnv{t: e.t, h: NewQueryHandler(e.duck, e.backend, zerolog.Nop(), 0, 0)}
	f.h.SetMergeDayGlobs(on, 2)
	f.app = fiber.New(fiber.Config{DisableStartupMessage: true})
	f.h.RegisterRoutes(f.app)
	e.duck.ClearHTTPCache()
	e.lists()
	return f
}

func (e *dayGlobEnv) where(fromDay, toDay int) string {
	return fmt.Sprintf("FROM %s.m WHERE time >= '2026-05-%02dT00:00:00Z' AND time < '2026-05-%02dT00:00:00Z'", e.db, fromDay, toDay)
}

func wantCount(t *testing.T, res queryResult, n float64) {
	t.Helper()
	if res.status != 200 || len(res.cols["n"]) != 1 || res.cols["n"][0].(float64) != n {
		t.Fatalf("status %d n=%v, want 200 n=%v: %s", res.status, res.cols["n"], n, res.raw)
	}
}

func TestMergeDayGlobsEndpointsMinIO(t *testing.T) {
	e := newDayGlobEnv(t, "mergeglobs")
	const days = 12
	for d := 1; d <= days; d++ {
		e.putRows(fmt.Sprintf("2026/05/%02d/f.parquet", d), d, 10)
		e.putRows(fmt.Sprintf("2026/05/%02d/g.parquet", d), d, 5)
	}
	// An hour folder keeps its own glob; a day outside the range but under the
	// same LIST prefix as days 10-12 must not be read.
	e.putRows("2026/05/04/07/raw.parquet", 4, 1000)
	e.put("2026/05/13/f.parquet", "SELECT TIMESTAMPTZ '2026-05-12 23:00:00+00' AS time FROM range(500)")
	q := "SELECT count(*) AS n " + e.where(1, days+1)
	for _, ep := range []string{"/api/v1/query", "/api/v1/query/arrow", "/api/v1/query/msgpack"} {
		t.Run(ep, func(t *testing.T) {
			res := postEndpoint(t, e.handler(false), ep, q, nil)
			off := e.lists()
			if res.status != 200 {
				t.Fatalf("option off: status %d: %s", res.status, res.raw)
			}
			res = postEndpoint(t, e.handler(true), ep, q, nil)
			on := e.lists()
			if res.status != 200 {
				t.Fatalf("option on: status %d: %s", res.status, res.raw)
			}
			if ep != "/api/v1/query/msgpack" {
				wantCount(t, res, days*15+1000)
			}
			// 12 day LISTs become 2: days 01-09 and days 10-12.
			if off-on != days-2 {
				t.Fatalf("%d LISTs with the option off, %d with it on; want %d fewer", off, on, days-2)
			}
		})
	}
}

// Files that differ in their columns: SELECT * returns the same columns and
// rows either way.
func TestMergeDayGlobsSameRowsAndColumnsMinIO(t *testing.T) {
	e := newDayGlobEnv(t, "mergeglobscols")
	e.put("2026/05/01/f.parquet", "SELECT TIMESTAMPTZ '2026-05-01 00:00:00+00' AS time, 1 AS a")
	e.put("2026/05/02/f.parquet", "SELECT TIMESTAMPTZ '2026-05-02 00:00:00+00' AS time, 2 AS a, 'x' AS b")
	e.put("2026/05/02/g.parquet", "SELECT TIMESTAMPTZ '2026-05-02 01:00:00+00' AS time, 'y' AS c, 3 AS a")
	e.put("2026/05/03/f.parquet", "SELECT TIMESTAMPTZ '2026-05-03 00:00:00+00' AS time, 4.5 AS d")
	q := "SELECT * " + e.where(1, 4) + " ORDER BY time"
	off := postEndpoint(t, e.handler(false), "/api/v1/query", q, nil)
	on := postEndpoint(t, e.handler(true), "/api/v1/query", q, nil)
	if off.status != 200 || on.status != 200 {
		t.Fatalf("status off %d on %d: %s", off.status, on.status, on.raw)
	}
	if on.raw != off.raw && !reflect.DeepEqual(on.cols, off.cols) {
		t.Fatalf("results differ\noff: %s\n on: %s", off.raw, on.raw)
	}
	if len(off.cols["time"]) != 4 {
		t.Fatalf("got %d rows, want 4: %s", len(off.cols["time"]), off.raw)
	}
}

// A file a cached listing still names is deleted (compaction): the query
// lists again and answers from the files left, with the merged glob too.
func TestMergeDayGlobsRetriesADeletedFileMinIO(t *testing.T) {
	e := newDayGlobEnv(t, "mergeglobsretry")
	e.putRows("2026/05/10/f1.parquet", 10, 10)
	e.putRows("2026/05/11/f2.parquet", 11, 20)
	q := "SELECT count(*) AS n " + e.where(10, 12)
	for _, ep := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
		t.Run(ep, func(t *testing.T) {
			e.putRows("2026/05/11/f3.parquet", 11, 30)
			h := e.handler(true)
			converted, _, err := h.h.getTransformedSQL(e.ctx, q, "")
			if err != nil {
				t.Fatal(err)
			}
			globs := remoteGlobsIn(h.h.mergeDayGlobsIn(converted))
			if len(globs) != 1 || !strings.HasSuffix(globs[0], "/2026/05/1[01]/*.parquet") {
				t.Fatalf("globs run for %s = %v, want the one merged glob", converted, globs)
			}
			// cache_httpfs holds a listing that names f3, and none of its bytes.
			var listed int
			if err := e.duck.DB().QueryRow("SELECT count(*) FROM glob(?)", globs[0]).Scan(&listed); err != nil || listed != 3 {
				t.Fatalf("glob listed %d files (err %v), want 3", listed, err)
			}
			if err := e.backend.Delete(e.ctx, e.db+"/m/2026/05/11/f3.parquet"); err != nil {
				t.Fatal(err)
			}
			res := postEndpoint(t, h, ep, q, nil)
			wantCount(t, res, 30)
			if got := res.header.Get("X-Arc-Retry"); got != "raw-file-missing" {
				t.Fatalf("X-Arc-Retry = %q, want raw-file-missing", got)
			}
		})
	}
}

// The inputs of a compaction whose output is uploaded stay hidden.
func TestMergeDayGlobsHidesCompactionInputsMinIO(t *testing.T) {
	e := newDayGlobEnv(t, "mergeglobscompact")
	e.putRows("2026/05/10/in1.parquet", 10, 10)
	e.putRows("2026/05/10/in2.parquet", 10, 20)
	e.putRows("2026/05/10/out.parquet", 10, 30)
	e.putRows("2026/05/11/f.parquet", 11, 7)
	q := "SELECT count(*) AS n " + e.where(10, 12)
	for _, on := range []bool{false, true} {
		h := e.handler(on)
		h.h.SetManifestManager(fakeManifests{uploaded: map[string]struct{}{
			e.db + "/m/2026/05/10/in1.parquet": {},
			e.db + "/m/2026/05/10/in2.parquet": {},
		}})
		wantCount(t, postEndpoint(t, h, "/api/v1/query", q, nil), 37)
	}
}
