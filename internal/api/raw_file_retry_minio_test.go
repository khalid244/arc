//go:build duckdb_arrow

package api

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// Review 2026-11-10 Q3 against a real S3 API (MinIO, path-style, cache_httpfs
// with a 60 s glob TTL). A raw query's partition listing is cached, then
// compaction deletes one of the files: the query re-lists that partition and
// answers from the files left. Set ARC_TEST_MINIO_ENDPOINT (host:port, keys
// minioadmin/minioadmin) and ARC_TEST_MINIO_BUCKET (an existing, empty
// bucket) to run it.
func TestRaw404RetryMinIO(t *testing.T) {
	endpoint, bucket := os.Getenv("ARC_TEST_MINIO_ENDPOINT"), os.Getenv("ARC_TEST_MINIO_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("ARC_TEST_MINIO_ENDPOINT / ARC_TEST_MINIO_BUCKET not set")
	}
	metrics.Init(zerolog.Nop())
	ctx := context.Background()
	backend, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: "http://" + endpoint,
		AccessKey: "minioadmin", SecretKey: "minioadmin", PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	duck, err := database.New(&database.Config{
		MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4,
		S3Region: "us-east-1", S3AccessKey: "minioadmin", S3SecretKey: "minioadmin",
		S3Endpoint: endpoint, S3PathStyle: true, S3Bucket: bucket, S3IsPrimaryBackend: true,
		EnableS3Cache: true, S3CacheSize: 64 << 20, S3CacheTTLSeconds: 3600, S3GlobCacheTTLSeconds: 60,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { duck.Close() })

	files := map[string]int{"f1": 10, "f2": 20, "f3": 30}
	keyOf := func(f string) string { return "db/m/2026/11/10/13/" + f + ".parquet" }
	upload := func(f string) {
		b := parquetBytes(t, fmt.Sprintf("SELECT TIMESTAMPTZ '2026-11-10 13:00:00+00' + INTERVAL (i) SECOND AS time FROM range(%d) t(i)", files[f]))
		if err := backend.Write(ctx, keyOf(f), b); err != nil {
			t.Fatal(err)
		}
	}
	for f := range files {
		upload(f)
	}

	const q = "SELECT count(*) AS n FROM db.m WHERE time >= '2026-11-10T13:00:00Z' AND time < '2026-11-10T14:00:00Z'"
	for _, endpoint := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
		t.Run(endpoint, func(t *testing.T) {
			upload("f3")
			duck.ClearHTTPCache()
			e := &forkQueryEnv{t: t, h: NewQueryHandler(duck, backend, zerolog.Nop(), 0, 0)}
			e.app = fiber.New(fiber.Config{DisableStartupMessage: true})
			e.h.RegisterRoutes(e.app)

			converted, _, err := e.h.getTransformedSQL(ctx, q, "")
			if err != nil {
				t.Fatal(err)
			}
			var globs []string
			for _, m := range sqlLiteralRe.FindAllStringSubmatch(converted, -1) {
				if strings.Contains(m[1], "*") && strings.Contains(m[1], "/2026/11/10/13/") {
					globs = append(globs, m[1])
				}
			}
			if len(globs) != 1 {
				t.Fatalf("hour globs in %s = %v, want one", converted, globs)
			}
			// List the hour without reading its files: cache_httpfs now holds
			// a listing that names f3, and none of its bytes.
			var listed int
			if err := duck.DB().QueryRow("SELECT count(*) FROM glob(?)", globs[0]).Scan(&listed); err != nil || listed != 3 {
				t.Fatalf("glob listed %d files (err %v), want 3", listed, err)
			}
			if err := backend.Delete(ctx, keyOf("f3")); err != nil {
				t.Fatal(err)
			}
			// Without the retry the query fails on the cached listing.
			var n int64
			stale := duck.DB().QueryRow(converted).Scan(&n)
			t.Logf("same SQL on the cached listing, no retry: n=%d err=%v", n, stale)
			if missingRawSourceObject(stale) == "" {
				t.Fatalf("the cached listing did not produce a raw-file 404: %v", stale)
			}

			start := time.Now()
			res := postEndpoint(t, e, endpoint, q, nil)
			t.Logf("via %s: status %d, X-Arc-Retry=%q, n=%v, %v", endpoint, res.status, res.header.Get("X-Arc-Retry"), res.cols["n"], time.Since(start))
			if res.status != 200 || len(res.cols["n"]) != 1 || res.cols["n"][0].(float64) != 30 {
				t.Fatalf("status %d n=%v, want 200 n=30 (f1+f2): %s", res.status, res.cols["n"], res.raw)
			}
			if res.header.Get("X-Arc-Retry") != "raw-file-missing" {
				t.Fatalf("X-Arc-Retry = %q, want raw-file-missing", res.header.Get("X-Arc-Retry"))
			}
		})
	}
}
