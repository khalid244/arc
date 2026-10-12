package config

import (
	"strings"
	"testing"
)

// CS-12: the in-process reorg DuckDB must get a spill cap and a memory limit.
// Unset, both inherit the compaction values, which are themselves resolved to
// concrete numbers at load.
func TestReorgSpillCapAndMemoryLimitDefaults(t *testing.T) {
	t.Run("defaults_follow_compaction", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Compaction.MaxTempDirectorySize == "" || cfg.Reorg.MaxTempDirectorySize != cfg.Compaction.MaxTempDirectorySize {
			t.Errorf("reorg spill cap = %q, want the compaction spill cap %q",
				cfg.Reorg.MaxTempDirectorySize, cfg.Compaction.MaxTempDirectorySize)
		}
		if cfg.Compaction.MemoryLimit == "" || cfg.Reorg.MemoryLimit != cfg.Compaction.MemoryLimit {
			t.Errorf("reorg memory_limit = %q, want the resolved compaction memory_limit %q",
				cfg.Reorg.MemoryLimit, cfg.Compaction.MemoryLimit)
		}
	})
	t.Run("compaction_override_is_inherited", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_COMPACTION_MAX_TEMP_DIRECTORY_SIZE", "20GiB")
		t.Setenv("ARC_COMPACTION_MEMORY_LIMIT", "3GB")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Reorg.MaxTempDirectorySize != "20GiB" {
			t.Errorf("reorg spill cap = %q, want 20GiB", cfg.Reorg.MaxTempDirectorySize)
		}
		if cfg.Reorg.MemoryLimit != "3GB" {
			t.Errorf("reorg memory_limit = %q, want 3GB", cfg.Reorg.MemoryLimit)
		}
	})
	t.Run("explicit_reorg_values_win", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_COMPACTION_MAX_TEMP_DIRECTORY_SIZE", "20GiB")
		t.Setenv("ARC_REORG_MAX_TEMP_DIRECTORY_SIZE", "15GiB")
		t.Setenv("ARC_REORG_MEMORY_LIMIT", "2GB")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Reorg.MaxTempDirectorySize != "15GiB" {
			t.Errorf("reorg spill cap = %q, want 15GiB", cfg.Reorg.MaxTempDirectorySize)
		}
		if cfg.Reorg.MemoryLimit != "2GB" {
			t.Errorf("reorg memory_limit = %q, want 2GB", cfg.Reorg.MemoryLimit)
		}
	})
	t.Run("unsettable_memory_limit_rejected_at_load", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_REORG_MEMORY_LIMIT", "50%")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "reorg.memory_limit") {
			t.Fatalf("got %v, want a reorg.memory_limit validation error", err)
		}
	})
}
