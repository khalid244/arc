package config

import (
	"os"
	"strings"
	"testing"
)

// IW-08: ingest.stager_merge_memory_limit / stager_merge_threads /
// stager_job_max_files / stager_job_max_bytes bound every stager merge job.
func TestStagerMergeLimitsConfig(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		in := cfg.Ingest
		if in.StagerMergeMemoryLimit != "1GB" || in.StagerMergeThreads != 2 ||
			in.StagerJobMaxFiles != 500 || in.StagerJobMaxBytes != 1<<30 {
			t.Fatalf("defaults = memory %q threads %d files %d bytes %d; want 1GB, 2, 500, 1GiB",
				in.StagerMergeMemoryLimit, in.StagerMergeThreads, in.StagerJobMaxFiles, in.StagerJobMaxBytes)
		}
	})

	t.Run("toml", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		toml := "[ingest]\nstager_merge_memory_limit = \"512MB\"\nstager_merge_threads = 1\n" +
			"stager_job_max_files = 200\nstager_job_max_bytes = \"256MB\"\n"
		if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		in := cfg.Ingest
		if in.StagerMergeMemoryLimit != "512MB" || in.StagerMergeThreads != 1 ||
			in.StagerJobMaxFiles != 200 || in.StagerJobMaxBytes != 256<<20 {
			t.Fatalf("got memory %q threads %d files %d bytes %d", in.StagerMergeMemoryLimit,
				in.StagerMergeThreads, in.StagerJobMaxFiles, in.StagerJobMaxBytes)
		}
	})

	for name, toml := range map[string]string{
		"percent memory":   "[ingest]\nstager_merge_memory_limit = \"50%\"\n",
		"negative threads": "[ingest]\nstager_merge_threads = -1\n",
		"zero files":       "[ingest]\nstager_job_max_files = 0\n",
		"bad bytes":        "[ingest]\nstager_job_max_bytes = \"lots\"\n",
	} {
		t.Run("rejects "+name, func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), "stager_") {
				t.Fatalf("Load() = %v, want an ingest.stager_* validation error", err)
			}
		})
	}
}
