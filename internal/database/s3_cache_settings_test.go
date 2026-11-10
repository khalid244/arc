package database

import (
	"database/sql"
	"testing"

	"github.com/rs/zerolog"
)

// configureS3Access applies database.http_keep_alive, query.s3_glob_cache_ttl_seconds
// and query.s3_cache_max_blocks to the httpfs / cache_httpfs settings. Zero
// values keep the previous behaviour: keep-alive off, a 10 s glob TTL and
// s3_cache_size / 512 KB blocks.
func TestConfigureS3AccessHTTPAndCacheSettings(t *testing.T) {
	const gb = int64(1) << 30
	cases := []struct {
		name string
		cfg  Config
		want map[string]string
	}{
		{
			name: "defaults",
			cfg:  Config{EnableS3Cache: true, S3CacheSize: 16 * gb, S3CacheTTLSeconds: 3600},
			want: map[string]string{
				"http_keep_alive":                                "false",
				"httpfs_connection_caching":                      "false",
				"cache_httpfs_glob_cache_entry_timeout_millisec": "10000",
				"cache_httpfs_max_in_mem_cache_block_count":      "32768",
				"cache_httpfs_glob_cache_entry_size":             "1638",
			},
		},
		{
			name: "overrides",
			cfg: Config{
				EnableS3Cache: true, S3CacheSize: 16 * gb, S3CacheTTLSeconds: 3600,
				HTTPKeepAlive: true, S3GlobCacheTTLSeconds: 60, S3CacheMaxBlocks: 98304,
			},
			want: map[string]string{
				"http_keep_alive":                                "true",
				"httpfs_connection_caching":                      "true",
				"cache_httpfs_glob_cache_entry_timeout_millisec": "60000",
				"cache_httpfs_max_in_mem_cache_block_count":      "98304",
				"cache_httpfs_glob_cache_entry_size":             "4915",
			},
		},
		{
			name: "max blocks without a size",
			cfg:  Config{EnableS3Cache: true, S3CacheMaxBlocks: 1000},
			want: map[string]string{
				"cache_httpfs_max_in_mem_cache_block_count": "1000",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			if err := configureS3Access(db, &tc.cfg, zerolog.Nop()); err != nil {
				t.Skipf("httpfs unavailable: %v", err)
			}
			for name, want := range tc.want {
				var got string
				if err := db.QueryRow("SELECT current_setting('" + name + "')::VARCHAR").Scan(&got); err != nil {
					t.Skipf("%s unavailable (cache_httpfs not loaded offline?): %v", name, err)
				}
				if got != want {
					t.Errorf("%s = %s, want %s", name, got, want)
				}
			}
		})
	}
}
