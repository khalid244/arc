package config

import (
	"os"
	"strings"
	"testing"
)

// query.s3_cache_type picks where cache_httpfs keeps S3 blocks: "in_mem"
// (default, as before) or "on_disk" under query.s3_cache_directory, evicted
// once the filesystem has less than query.s3_cache_min_free_disk free.
func TestQueryS3DiskCacheConfig(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.S3CacheType != "in_mem" || cfg.Query.S3CacheDirectory != "" || cfg.Query.S3CacheMinFreeDiskBytes != 0 {
			t.Fatalf("defaults = %q/%q/%d, want in_mem//0", cfg.Query.S3CacheType, cfg.Query.S3CacheDirectory, cfg.Query.S3CacheMinFreeDiskBytes)
		}
	})
	t.Run("toml", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		toml := "[query]\ns3_cache_type = \"on_disk\"\ns3_cache_directory = \"/data/s3-cache\"\ns3_cache_min_free_disk = \"900GB\"\n"
		if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.S3CacheType != "on_disk" || cfg.Query.S3CacheDirectory != "/data/s3-cache" || cfg.Query.S3CacheMinFreeDiskBytes != 900<<30 {
			t.Fatalf("toml = %q/%q/%d, want on_disk//data/s3-cache/%d", cfg.Query.S3CacheType, cfg.Query.S3CacheDirectory, cfg.Query.S3CacheMinFreeDiskBytes, int64(900)<<30)
		}
	})
	t.Run("env", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_QUERY_S3_CACHE_TYPE", "on_disk")
		t.Setenv("ARC_QUERY_S3_CACHE_DIRECTORY", "/cache")
		t.Setenv("ARC_QUERY_S3_CACHE_MIN_FREE_DISK", "50GB")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.S3CacheType != "on_disk" || cfg.Query.S3CacheDirectory != "/cache" || cfg.Query.S3CacheMinFreeDiskBytes != 50<<30 {
			t.Fatalf("env = %q/%q/%d", cfg.Query.S3CacheType, cfg.Query.S3CacheDirectory, cfg.Query.S3CacheMinFreeDiskBytes)
		}
	})
	for _, tc := range []struct {
		name string
		env  map[string]string
		key  string
	}{
		{"unknown type", map[string]string{"ARC_QUERY_S3_CACHE_TYPE": "ssd"}, "query.s3_cache_type"},
		{"on_disk without a directory", map[string]string{"ARC_QUERY_S3_CACHE_TYPE": "on_disk"}, "query.s3_cache_directory"},
		{"relative directory", map[string]string{"ARC_QUERY_S3_CACHE_TYPE": "on_disk", "ARC_QUERY_S3_CACHE_DIRECTORY": "cache"}, "query.s3_cache_directory"},
		{"bad floor", map[string]string{"ARC_QUERY_S3_CACHE_MIN_FREE_DISK": "lots"}, "query.s3_cache_min_free_disk"},
	} {
		t.Run("reject "+tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("Load() err = %v, want a %s validation error", err, tc.key)
			}
		})
	}
}

// query.glob_prewarm_parallelism: how many of a query's partition globs are
// listed at once ahead of it (0 = off, the default).
func TestQueryGlobPrewarmConfig(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.GlobPrewarmParallelism != 0 {
			t.Fatalf("default = %d, want 0", cfg.Query.GlobPrewarmParallelism)
		}
	})
	t.Run("env", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_QUERY_GLOB_PREWARM_PARALLELISM", "8")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.GlobPrewarmParallelism != 8 {
			t.Fatalf("env = %d, want 8", cfg.Query.GlobPrewarmParallelism)
		}
	})
	for _, bad := range []string{"-1", "33"} {
		t.Run("reject "+bad, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_QUERY_GLOB_PREWARM_PARALLELISM", bad)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), "query.glob_prewarm_parallelism") {
				t.Fatalf("Load() err = %v, want a query.glob_prewarm_parallelism validation error", err)
			}
		})
	}
}

// query.glob_prewarm_shared_limit: how many LISTs all queries together may run
// beyond one each (0 = same as glob_prewarm_parallelism, the default).
func TestQueryGlobPrewarmSharedLimitConfig(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.GlobPrewarmSharedLimit != 0 {
			t.Fatalf("default = %d, want 0", cfg.Query.GlobPrewarmSharedLimit)
		}
	})
	t.Run("env", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_QUERY_GLOB_PREWARM_SHARED_LIMIT", "12")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.GlobPrewarmSharedLimit != 12 {
			t.Fatalf("env = %d, want 12", cfg.Query.GlobPrewarmSharedLimit)
		}
	})
	for _, bad := range []string{"-1", "257"} {
		t.Run("reject "+bad, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_QUERY_GLOB_PREWARM_SHARED_LIMIT", bad)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), "query.glob_prewarm_shared_limit") {
				t.Fatalf("Load() err = %v, want a query.glob_prewarm_shared_limit validation error", err)
			}
		})
	}
}
