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
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// A converted query names its partitions as glob literals. Only the remote
// ones are worth listing ahead: each distinct one once, in order.
func TestRemoteGlobsIn(t *testing.T) {
	sql := "SELECT * FROM read_parquet(['s3://b/db/m/2026/05/01/*.parquet', 's3://b/db/m/2026/05/02/*.parquet', 's3://b/db/m/2026/05/01/*.parquet'], union_by_name=true) " +
		"WHERE site LIKE '%x*y%' AND f NOT IN ('s3://b/db/m/2026/05/01/a.parquet') UNION ALL SELECT * FROM read_parquet('/data/db/m/**/*.parquet') " +
		"UNION ALL SELECT * FROM read_parquet('azure://c/db/m/2026/05/03/*.parquet')"
	want := []string{"s3://b/db/m/2026/05/01/*.parquet", "s3://b/db/m/2026/05/02/*.parquet", "azure://c/db/m/2026/05/03/*.parquet"}
	if got := remoteGlobsIn(sql); !reflect.DeepEqual(got, want) {
		t.Fatalf("remoteGlobsIn = %v, want %v", got, want)
	}
}

// DuckDB expands a query's partition globs one LIST after another. On a slow
// store that is most of a warm query's time (a 30-day range: 25 LISTs).
// query.glob_prewarm_parallelism lists them ahead, several at once, through
// DuckDB's own glob cache, so the query then expands every glob from the
// cache: same globs, same listing age, same result. Set ARC_TEST_MINIO_ENDPOINT
// (host:port, keys minioadmin/minioadmin) and ARC_TEST_MINIO_BUCKET to run it.
func TestGlobPrewarmMinIO(t *testing.T) {
	endpoint, bucket := os.Getenv("ARC_TEST_MINIO_ENDPOINT"), os.Getenv("ARC_TEST_MINIO_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("ARC_TEST_MINIO_ENDPOINT / ARC_TEST_MINIO_BUCKET not set")
	}
	ctx := context.Background()
	backend, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: "http://" + endpoint,
		AccessKey: "minioadmin", SecretKey: "minioadmin", PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	const days = 8
	db := fmt.Sprintf("prewarm_%d", os.Getpid())
	body := parquetBytes(t, "SELECT TIMESTAMPTZ '2026-05-01 00:00:00+00' + INTERVAL (i) SECOND AS time FROM range(10) t(i)")
	var globs []string
	for d := 1; d <= days; d++ {
		key := fmt.Sprintf("%s/m/2026/05/%02d/f.parquet", db, d)
		if err := backend.Write(ctx, key, body); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = backend.Delete(ctx, key) })
		globs = append(globs, fmt.Sprintf("'s3://%s/%s/m/2026/05/%02d/*.parquet'", bucket, db, d))
	}
	sql := "SELECT count(*) FROM read_parquet([" + strings.Join(globs, ", ") + "], union_by_name=true)"

	slowURL, counts := slowListProxy(t, endpoint)

	open := func(parallelism int) *QueryHandler {
		duck, err := database.New(&database.Config{
			MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 8,
			S3Region: "us-east-1", S3AccessKey: "minioadmin", S3SecretKey: "minioadmin",
			S3Endpoint: strings.TrimPrefix(slowURL, "http://"), S3PathStyle: true, S3Bucket: bucket, S3IsPrimaryBackend: true,
			EnableS3Cache: true, S3CacheSize: 64 << 20, S3CacheTTLSeconds: 3600, S3GlobCacheTTLSeconds: 60,
		}, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { duck.Close() })
		h := NewQueryHandler(duck, backend, zerolog.Nop(), 0, 0)
		h.SetGlobPrewarm(parallelism, 0)
		return h
	}
	count := func(h *QueryHandler) int64 {
		t.Helper()
		var n int64
		if err := h.db.DB().QueryRowContext(ctx, sql).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	off := open(0)
	off.prewarmGlobs(ctx, sql)
	if l, _ := counts(); l != 0 {
		t.Fatalf("parallelism 0 listed %d globs ahead, want none", l)
	}
	want := count(off)
	if l, m := counts(); l != days || m != 1 {
		t.Fatalf("without prewarm the query made %d LISTs, %d at once; want %d, one at a time", l, m, days)
	}

	on := open(4)
	start := time.Now()
	on.prewarmGlobs(ctx, sql)
	prewarmTook := time.Since(start)
	if l, m := counts(); l != days || m < 2 || m > 4 {
		t.Fatalf("prewarm made %d LISTs, at most %d at once; want %d LISTs, 2 to 4 at once", l, m, days)
	}
	if serial := days * 100 * time.Millisecond; prewarmTook > serial*3/4 {
		t.Fatalf("prewarm took %s, want well under the %s of %d serial LISTs", prewarmTook, serial, days)
	}
	if got := count(on); got != want {
		t.Fatalf("count after prewarm = %d, want %d", got, want)
	}
	if l, _ := counts(); l != 0 {
		t.Fatalf("the query after prewarm made %d LISTs, want 0 (every glob from the cache)", l)
	}
}

// slowListProxy is a slow object store in front of MinIO: every LIST of a day
// folder (what a partition glob costs) takes 100 ms. counts returns how many
// there were and the most in flight at once since the last call.
func slowListProxy(t *testing.T, endpoint string) (string, func() (int, int)) {
	t.Helper()
	target, _ := url.Parse("http://" + endpoint)
	rp := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	lists, inFlight, maxInFlight := 0, 0, 0
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && dayFolderRe.MatchString(r.URL.Query().Get("prefix")) {
			mu.Lock()
			lists++
			inFlight++
			maxInFlight = max(maxInFlight, inFlight)
			mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			defer func() { mu.Lock(); inFlight--; mu.Unlock() }()
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(slow.Close)
	return slow.URL, func() (int, int) {
		mu.Lock()
		defer mu.Unlock()
		l, m := lists, maxInFlight
		lists, maxInFlight = 0, 0
		return l, m
	}
}

var dayFolderRe = regexp.MustCompile(`/\d{4}/\d{2}/\d{2}/$`)

// Every query endpoint lists its partition globs ahead when
// query.glob_prewarm_parallelism is set: the day-folder LISTs of an 8-day
// query overlap instead of running one at a time, and the answer is the same.
func TestGlobPrewarmEndpointsMinIO(t *testing.T) {
	endpoint, bucket := os.Getenv("ARC_TEST_MINIO_ENDPOINT"), os.Getenv("ARC_TEST_MINIO_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("ARC_TEST_MINIO_ENDPOINT / ARC_TEST_MINIO_BUCKET not set")
	}
	metrics.Init(zerolog.Nop())
	ctx := context.Background()
	slowURL, counts := slowListProxy(t, endpoint)
	backend, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: slowURL,
		AccessKey: "minioadmin", SecretKey: "minioadmin", PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	const days = 8
	db := fmt.Sprintf("prewarmep_%d", os.Getpid())
	body := parquetBytes(t, "SELECT TIMESTAMPTZ '2026-05-01 00:00:00+00' + INTERVAL (i) SECOND AS time FROM range(10) t(i)")
	for d := 1; d <= days; d++ {
		key := fmt.Sprintf("%s/m/2026/05/%02d/f.parquet", db, d)
		if err := backend.Write(ctx, key, body); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = backend.Delete(ctx, key) })
	}
	duck, err := database.New(&database.Config{
		MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 8,
		S3Region: "us-east-1", S3AccessKey: "minioadmin", S3SecretKey: "minioadmin",
		S3Endpoint: strings.TrimPrefix(slowURL, "http://"), S3PathStyle: true, S3Bucket: bucket, S3IsPrimaryBackend: true,
		EnableS3Cache: true, S3CacheSize: 64 << 20, S3CacheTTLSeconds: 3600, S3GlobCacheTTLSeconds: 60,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { duck.Close() })
	q := "SELECT count(*) AS n FROM " + db + ".m WHERE time >= '2026-05-01T00:00:00Z' AND time < '2026-05-09T00:00:00Z'"
	for _, parallelism := range []int{0, 4} {
		for _, ep := range []string{"/api/v1/query", "/api/v1/query/arrow", "/api/v1/query/msgpack"} {
			t.Run(fmt.Sprintf("parallelism %d %s", parallelism, ep), func(t *testing.T) {
				e := &forkQueryEnv{t: t, h: NewQueryHandler(duck, backend, zerolog.Nop(), 0, 0)}
				e.h.SetGlobPrewarm(parallelism, 0)
				e.app = fiber.New(fiber.Config{DisableStartupMessage: true})
				e.h.RegisterRoutes(e.app)
				duck.ClearHTTPCache()
				counts()
				res := postEndpoint(t, e, ep, q, nil)
				if res.status != 200 {
					t.Fatalf("status %d: %s", res.status, res.raw)
				}
				if ep == "/api/v1/query" && (len(res.cols["n"]) != 1 || res.cols["n"][0].(float64) != days*10) {
					t.Fatalf("n = %v, want %d", res.cols["n"], days*10)
				}
				l, m := counts()
				if parallelism == 0 && (l < days || m != 1) {
					t.Fatalf("without prewarm: %d day LISTs, %d at once; want at least %d, one at a time", l, m, days)
				}
				if parallelism > 0 && (l != days || m < 2 || m > parallelism) {
					t.Fatalf("with prewarm: %d day LISTs, at most %d at once; want %d LISTs, 2 to %d at once", l, m, days, parallelism)
				}
			})
		}
	}
}

// Queries that arrive together must not multiply the burst on the object
// store: each lists one glob at a time by itself, as it always did, and all of
// them together run at most query.glob_prewarm_shared_limit LISTs beyond that.
func TestGlobPrewarmSharedLimitMinIO(t *testing.T) {
	endpoint, bucket := os.Getenv("ARC_TEST_MINIO_ENDPOINT"), os.Getenv("ARC_TEST_MINIO_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("ARC_TEST_MINIO_ENDPOINT / ARC_TEST_MINIO_BUCKET not set")
	}
	ctx := context.Background()
	backend, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: "http://" + endpoint,
		AccessKey: "minioadmin", SecretKey: "minioadmin", PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	const queries, days, parallelism, shared = 6, 8, 4, 4
	db := fmt.Sprintf("prewarmshared_%d", os.Getpid())
	body := parquetBytes(t, "SELECT TIMESTAMPTZ '2026-05-01 00:00:00+00' + INTERVAL (i) SECOND AS time FROM range(10) t(i)")
	sqls := make([]string, queries)
	for q := range sqls {
		var globs []string
		for d := 1; d <= days; d++ {
			key := fmt.Sprintf("%s/m%d/2026/05/%02d/f.parquet", db, q, d)
			if err := backend.Write(ctx, key, body); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = backend.Delete(ctx, key) })
			globs = append(globs, fmt.Sprintf("'s3://%s/%s/m%d/2026/05/%02d/*.parquet'", bucket, db, q, d))
		}
		sqls[q] = "SELECT count(*) FROM read_parquet([" + strings.Join(globs, ", ") + "], union_by_name=true)"
	}
	slowURL, counts := slowListProxy(t, endpoint)
	duck, err := database.New(&database.Config{
		MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 64,
		S3Region: "us-east-1", S3AccessKey: "minioadmin", S3SecretKey: "minioadmin",
		S3Endpoint: strings.TrimPrefix(slowURL, "http://"), S3PathStyle: true, S3Bucket: bucket, S3IsPrimaryBackend: true,
		EnableS3Cache: true, S3CacheSize: 64 << 20, S3CacheTTLSeconds: 3600, S3GlobCacheTTLSeconds: 60,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { duck.Close() })
	h := NewQueryHandler(duck, backend, zerolog.Nop(), 0, 0)
	h.SetGlobPrewarm(parallelism, shared)

	var wg sync.WaitGroup
	for _, sql := range sqls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.prewarmGlobs(ctx, sql)
		}()
	}
	wg.Wait()
	l, m := counts()
	if l != queries*days {
		t.Fatalf("%d queries made %d LISTs, want %d", queries, l, queries*days)
	}
	if m <= queries || m > queries+shared {
		t.Fatalf("%d queries ran %d LISTs at once; want more than %d (one each) and at most %d (plus the shared %d)", queries, m, queries, queries+shared, shared)
	}
	for _, sql := range sqls {
		var n int64
		if err := h.db.DB().QueryRowContext(ctx, sql).Scan(&n); err != nil || n != days*10 {
			t.Fatalf("count after prewarm = %d, %v; want %d", n, err, days*10)
		}
	}
	if l, _ := counts(); l != 0 {
		t.Fatalf("the queries after prewarm made %d LISTs, want 0", l)
	}
}
