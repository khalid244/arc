package config

import (
	"os"
	"testing"
)

// IW-02 / IW-11: ingest.local_disk_max_bytes (0 = off) budgets the WAL dir and
// both stager dirs; wal.reject_on_append_failure (default false = upstream
// behaviour) refuses a write whose WAL append failed.
func TestIngestBackpressureConfig(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Ingest.LocalDiskMaxBytes != 0 || cfg.WAL.RejectOnAppendFailure {
			t.Fatalf("defaults: local_disk_max_bytes=%d reject_on_append_failure=%v, want 0/false",
				cfg.Ingest.LocalDiskMaxBytes, cfg.WAL.RejectOnAppendFailure)
		}
	})
	t.Run("toml", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		toml := "[ingest]\nlocal_disk_max_bytes = \"100GB\"\n[wal]\nreject_on_append_failure = true\n"
		if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Ingest.LocalDiskMaxBytes != 100<<30 || !cfg.WAL.RejectOnAppendFailure {
			t.Fatalf("got local_disk_max_bytes=%d reject=%v", cfg.Ingest.LocalDiskMaxBytes, cfg.WAL.RejectOnAppendFailure)
		}
	})
	t.Run("rejects garbage", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		if err := os.WriteFile(dir+"/arc.toml", []byte("[ingest]\nlocal_disk_max_bytes = \"lots\"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(); err == nil {
			t.Fatal("Load() accepted local_disk_max_bytes = \"lots\"")
		}
	})
}
