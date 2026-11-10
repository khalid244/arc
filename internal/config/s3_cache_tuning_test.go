package config

import (
	"os"
	"strings"
	"testing"
)

// query.s3_glob_cache_ttl_seconds sizes cache_httpfs's glob (LIST) cache TTL,
// hard-coded to 10 s before; query.s3_cache_max_blocks overrides the block
// count derived from s3_cache_size (0 = derive, as before).
func TestQueryS3CacheTuningConfig(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.S3GlobCacheTTLSeconds != 10 || cfg.Query.S3CacheMaxBlocks != 0 {
			t.Fatalf("defaults = %d/%d, want 10/0", cfg.Query.S3GlobCacheTTLSeconds, cfg.Query.S3CacheMaxBlocks)
		}
	})
	t.Run("toml", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		if err := os.WriteFile(dir+"/arc.toml", []byte("[query]\ns3_glob_cache_ttl_seconds = 60\ns3_cache_max_blocks = 196608\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.S3GlobCacheTTLSeconds != 60 || cfg.Query.S3CacheMaxBlocks != 196608 {
			t.Fatalf("toml = %d/%d, want 60/196608", cfg.Query.S3GlobCacheTTLSeconds, cfg.Query.S3CacheMaxBlocks)
		}
	})
	t.Run("env", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_QUERY_S3_GLOB_CACHE_TTL_SECONDS", "30")
		t.Setenv("ARC_QUERY_S3_CACHE_MAX_BLOCKS", "65536")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.S3GlobCacheTTLSeconds != 30 || cfg.Query.S3CacheMaxBlocks != 65536 {
			t.Fatalf("env = %d/%d, want 30/65536", cfg.Query.S3GlobCacheTTLSeconds, cfg.Query.S3CacheMaxBlocks)
		}
	})
	for _, tc := range []struct{ env, value, key string }{
		{"ARC_QUERY_S3_GLOB_CACHE_TTL_SECONDS", "0", "query.s3_glob_cache_ttl_seconds"},
		{"ARC_QUERY_S3_GLOB_CACHE_TTL_SECONDS", "3601", "query.s3_glob_cache_ttl_seconds"},
		{"ARC_QUERY_S3_CACHE_MAX_BLOCKS", "-1", "query.s3_cache_max_blocks"},
	} {
		t.Run("reject "+tc.env+"="+tc.value, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv(tc.env, tc.value)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("Load() err = %v, want a %s validation error", err, tc.key)
			}
		})
	}
}
