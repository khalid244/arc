package config

import (
	"os"
	"testing"
)

// query.glob_cache_ttl_seconds / query.partition_cache_ttl_seconds size the
// partition pruner's listing and path-set caches. Defaults keep the old
// hard-coded 30s / 60s; TOML and ARC_QUERY_* env override them.
func TestQueryPrunerCacheTTLConfig(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.GlobCacheTTLSeconds != 30 || cfg.Query.PartitionCacheTTLSeconds != 60 {
			t.Fatalf("defaults = %d/%d, want 30/60", cfg.Query.GlobCacheTTLSeconds, cfg.Query.PartitionCacheTTLSeconds)
		}
	})
	t.Run("toml", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		if err := os.WriteFile(dir+"/arc.toml", []byte("[query]\nglob_cache_ttl_seconds = 120\npartition_cache_ttl_seconds = 300\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.GlobCacheTTLSeconds != 120 || cfg.Query.PartitionCacheTTLSeconds != 300 {
			t.Fatalf("toml = %d/%d, want 120/300", cfg.Query.GlobCacheTTLSeconds, cfg.Query.PartitionCacheTTLSeconds)
		}
	})
	t.Run("env", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_QUERY_GLOB_CACHE_TTL_SECONDS", "90")
		t.Setenv("ARC_QUERY_PARTITION_CACHE_TTL_SECONDS", "180")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.GlobCacheTTLSeconds != 90 || cfg.Query.PartitionCacheTTLSeconds != 180 {
			t.Fatalf("env = %d/%d, want 90/180", cfg.Query.GlobCacheTTLSeconds, cfg.Query.PartitionCacheTTLSeconds)
		}
	})
}
