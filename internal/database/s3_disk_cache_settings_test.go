package database

import (
	"database/sql"
	"testing"

	"github.com/rs/zerolog"
)

// query.s3_cache_type = "on_disk" keeps cached S3 blocks in files under
// query.s3_cache_directory instead of in process memory. The folder is
// evicted least-recently-used first once the filesystem has less than
// query.s3_cache_min_free_disk free, and s3_cache_size then sizes only the
// small in-memory layer in front of the files. An empty type keeps the
// in-memory cache exactly as before.
func TestConfigureS3AccessDiskCacheSettings(t *testing.T) {
	const gb = int64(1) << 30
	dir := t.TempDir()
	cases := []struct {
		name string
		cfg  Config
		want map[string]string
	}{
		{
			name: "on disk",
			cfg: Config{
				EnableS3Cache: true, S3CacheSize: 8 * gb, S3CacheTTLSeconds: 3600,
				S3CacheType: "on_disk", S3CacheDirectory: dir, S3CacheMinFreeDiskBytes: 900 * gb,
			},
			want: map[string]string{
				"cache_httpfs_type":                                         "on_disk",
				"cache_httpfs_cache_directory":                              dir,
				"cache_httpfs_evict_policy":                                 "lru_sp",
				"cache_httpfs_min_disk_bytes_for_cache":                     "966367641600",
				"cache_httpfs_disk_cache_reader_mem_cache_block_count":      "16384",
				"cache_httpfs_disk_cache_reader_mem_cache_timeout_millisec": "3600000",
				"cache_httpfs_glob_cache_entry_size":                        "819",
			},
		},
		{
			name: "on disk without a free-space floor keeps the extension default",
			cfg: Config{
				EnableS3Cache: true, S3CacheSize: 8 * gb,
				S3CacheType: "on_disk", S3CacheDirectory: dir,
			},
			want: map[string]string{
				"cache_httpfs_type":                     "on_disk",
				"cache_httpfs_min_disk_bytes_for_cache": "0",
			},
		},
		{
			name: "unset type stays in memory",
			cfg:  Config{EnableS3Cache: true, S3CacheSize: 16 * gb, S3CacheTTLSeconds: 3600},
			want: map[string]string{
				"cache_httpfs_type":                         "in_mem",
				"cache_httpfs_evict_policy":                 "creation_timestamp",
				"cache_httpfs_max_in_mem_cache_block_count": "32768",
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

// The on-disk cache writes its block files from inside DuckDB, so its folder
// has to be in the sandbox allowlist or nothing is cached. With the in-memory
// cache a leftover directory setting opens nothing.
func TestBuildAllowedDirectoriesDiskCache(t *testing.T) {
	has := func(dirs []string, want string) bool {
		for _, d := range dirs {
			if d == want {
				return true
			}
		}
		return false
	}
	onDisk := buildAllowedDirectories(&Config{S3CacheType: "on_disk", S3CacheDirectory: "/data/s3-cache"})
	if !has(onDisk, "/data/s3-cache/") {
		t.Errorf("on_disk allowlist = %v, want it to hold /data/s3-cache/", onDisk)
	}
	inMem := buildAllowedDirectories(&Config{S3CacheDirectory: "/data/s3-cache"})
	if has(inMem, "/data/s3-cache/") {
		t.Errorf("in_mem allowlist = %v, want no cache folder", inMem)
	}
}
