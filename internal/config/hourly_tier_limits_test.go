package config

import (
	"os"
	"strings"
	"testing"
)

func loadCompactionTOML(t *testing.T, body string) (*Config, error) {
	t.Helper()
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.WriteFile(dir+"/arc.toml", []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load()
}

func TestCompactionHourlyTierLimitsConfig(t *testing.T) {
	t.Run("default inherits", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		c := cfg.Compaction
		if c.HourlyMaxConcurrent != 0 || c.HourlyMemoryLimit != "" || c.HourlyThreads != 0 || c.HourlyMaxTempDirectorySize != "" {
			t.Fatalf("hourly overrides default to (%d, %q, %d, %q), want all unset",
				c.HourlyMaxConcurrent, c.HourlyMemoryLimit, c.HourlyThreads, c.HourlyMaxTempDirectorySize)
		}
	})

	t.Run("toml", func(t *testing.T) {
		cfg, err := loadCompactionTOML(t, "[compaction]\nmax_concurrent = 1\nmemory_limit = \"12GB\"\nthreads = 4\n"+
			"hourly_max_concurrent = 3\nhourly_memory_limit = \"4GB\"\nhourly_threads = 2\nhourly_max_temp_directory_size = \"12GiB\"\n")
		if err != nil {
			t.Fatal(err)
		}
		c := cfg.Compaction
		if c.HourlyMaxConcurrent != 3 || c.HourlyMemoryLimit != "4GB" || c.HourlyThreads != 2 || c.HourlyMaxTempDirectorySize != "12GiB" {
			t.Fatalf("loaded (%d, %q, %d, %q), want (3, 4GB, 2, 12GiB)",
				c.HourlyMaxConcurrent, c.HourlyMemoryLimit, c.HourlyThreads, c.HourlyMaxTempDirectorySize)
		}
		if c.MaxConcurrent != 1 || c.MemoryLimit != "12GB" || c.Threads != 4 {
			t.Fatalf("daily sizing changed: (%d, %q, %d)", c.MaxConcurrent, c.MemoryLimit, c.Threads)
		}
	})

	t.Run("env", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_COMPACTION_HOURLY_MAX_CONCURRENT", "3")
		t.Setenv("ARC_COMPACTION_HOURLY_MEMORY_LIMIT", "4GB")
		t.Setenv("ARC_COMPACTION_HOURLY_THREADS", "2")
		t.Setenv("ARC_COMPACTION_HOURLY_MAX_TEMP_DIRECTORY_SIZE", "12GiB")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		c := cfg.Compaction
		if c.HourlyMaxConcurrent != 3 || c.HourlyMemoryLimit != "4GB" || c.HourlyThreads != 2 || c.HourlyMaxTempDirectorySize != "12GiB" {
			t.Fatalf("env loaded (%d, %q, %d, %q)", c.HourlyMaxConcurrent, c.HourlyMemoryLimit, c.HourlyThreads, c.HourlyMaxTempDirectorySize)
		}
	})

	t.Run("explicit compaction sizing is inherited, not re-derived", func(t *testing.T) {
		cfg, err := loadCompactionTOML(t, "[compaction]\nmax_concurrent = 1\nmemory_limit = \"12GB\"\nthreads = 4\nhourly_max_concurrent = 3\n")
		if err != nil {
			t.Fatal(err)
		}
		if c := cfg.Compaction; c.HourlyMemoryLimit != "" || c.HourlyThreads != 0 {
			t.Fatalf("hourly sizing = (%q, %d), want unset (inherits 12GB / 4)", c.HourlyMemoryLimit, c.HourlyThreads)
		}
	})

	// The auto memory_limit and threads split a budget across max_concurrent
	// jobs. An hourly pass runs hourly_max_concurrent jobs, so inheriting the
	// max_concurrent share would multiply the budget; it is derived for the
	// hourly concurrency instead.
	t.Run("auto compaction sizing is derived for the hourly concurrency", func(t *testing.T) {
		original := effectiveCoresFn
		t.Cleanup(func() { effectiveCoresFn = original })
		effectiveCoresFn = func() int { return 6 }
		cfg, err := loadCompactionTOML(t, "[database]\nmemory_limit = \"12GB\"\n[compaction]\nmax_concurrent = 1\nhourly_max_concurrent = 3\n")
		if err != nil {
			t.Fatal(err)
		}
		c := cfg.Compaction
		if c.MemoryLimit != "12GB" || c.Threads != 3 {
			t.Fatalf("daily auto sizing = (%q, %d), want (12GB, 3)", c.MemoryLimit, c.Threads)
		}
		if c.HourlyMemoryLimit != "4GB" || c.HourlyThreads != 2 {
			t.Fatalf("hourly auto sizing = (%q, %d), want (4GB, 2)", c.HourlyMemoryLimit, c.HourlyThreads)
		}
	})

	for _, tc := range []struct{ name, body, want string }{
		{"negative concurrency", "hourly_max_concurrent = -1", "compaction.hourly_max_concurrent"},
		{"negative threads", "hourly_threads = -1", "compaction.hourly_threads"},
		{"percent memory", "hourly_memory_limit = \"50%\"", "compaction.hourly_memory_limit"},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			_, err := loadCompactionTOML(t, "[compaction]\n"+tc.body+"\n")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load() error = %v, want one naming %s", err, tc.want)
			}
		})
	}
}
