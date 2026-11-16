//go:build duckdb_arrow

package api

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// query.s3_cache_type = "on_disk" against a real S3 API (MinIO), through the
// sandboxed database: a read leaves block files in the cache folder, and a
// second database opened on the same folder (a pod restart) answers the same
// from those files without writing them again. Set ARC_TEST_MINIO_ENDPOINT
// (host:port, keys minioadmin/minioadmin) and ARC_TEST_MINIO_BUCKET to run it.
func TestS3DiskCacheMinIO(t *testing.T) {
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
	const rows = 200000
	key := fmt.Sprintf("diskcache_%d/m/2026/11/10/13/f1.parquet", os.Getpid())
	body := parquetBytes(t, fmt.Sprintf("SELECT TIMESTAMPTZ '2026-11-10 13:00:00+00' + INTERVAL (i) MILLISECOND AS time, hash(i)::VARCHAR AS a, hash(i + 1)::VARCHAR AS b FROM range(%d) t(i)", rows))
	if len(body) < 3*512*1024 {
		t.Fatalf("test file is %d bytes, want several 512 KB cache blocks", len(body))
	}
	if err := backend.Write(ctx, key, body); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Delete(ctx, key) })

	cacheDir := t.TempDir()
	open := func() *database.DuckDB {
		duck, err := database.New(&database.Config{
			MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4,
			S3Region: "us-east-1", S3AccessKey: "minioadmin", S3SecretKey: "minioadmin",
			S3Endpoint: endpoint, S3PathStyle: true, S3Bucket: bucket, S3IsPrimaryBackend: true,
			EnableS3Cache: true, S3CacheSize: 64 << 20, S3CacheTTLSeconds: 3600,
			S3CacheType: "on_disk", S3CacheDirectory: cacheDir, S3CacheMinFreeDiskBytes: 1 << 30,
		}, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		return duck
	}
	read := func(duck *database.DuckDB) (n int64, chars int64) {
		t.Helper()
		q := fmt.Sprintf("SELECT count(*), sum(length(a) + length(b))::BIGINT FROM read_parquet('s3://%s/%s')", bucket, key)
		r, err := duck.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		if !r.Next() {
			t.Fatal("no row")
		}
		if err := r.Scan(&n, &chars); err != nil {
			t.Fatal(err)
		}
		return n, chars
	}
	blockFiles := func() int {
		t.Helper()
		entries, err := os.ReadDir(cacheDir)
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}

	first := open()
	n1, chars1 := read(first)
	first.Close()
	if n1 != rows {
		t.Fatalf("first read counted %d rows, want %d", n1, rows)
	}
	written := blockFiles()
	if written < 3 {
		t.Fatalf("cache folder holds %d block files after a %d-byte read, want at least 3", written, len(body))
	}

	second := open()
	t.Cleanup(func() { second.Close() })
	if _, err := second.DB().Exec("SET GLOBAL cache_httpfs_profile_type='temp'"); err != nil {
		t.Fatal(err)
	}
	n2, chars2 := read(second)
	if n2 != n1 || chars2 != chars1 {
		t.Fatalf("read after reopen = %d rows / %d chars, want %d / %d", n2, chars2, n1, chars1)
	}
	if after := blockFiles(); after != written {
		t.Fatalf("cache folder went from %d to %d block files on the second read", written, after)
	}
	var hits, misses int64
	if err := second.DB().QueryRow("SELECT cache_hit_count, cache_miss_count FROM cache_httpfs_cache_access_info_query() WHERE cache_type = 'data'").Scan(&hits, &misses); err != nil {
		t.Fatal(err)
	}
	if hits == 0 || misses != 0 {
		t.Fatalf("data blocks after reopen: %d from the cache folder, %d fetched from S3; want all from the folder", hits, misses)
	}
}
