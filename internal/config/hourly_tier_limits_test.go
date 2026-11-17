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

// prodDaily is the production compactor's daily sizing.
const prodDaily = "[compaction]\nmax_concurrent = 1\nmemory_limit = \"12GB\"\nthreads = 4\nmax_temp_directory_size = \"60GiB\"\n"

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
		cfg, err := loadCompactionTOML(t, prodDaily+
			"hourly_max_concurrent = 2\nhourly_memory_limit = \"6GB\"\nhourly_threads = 3\nhourly_max_temp_directory_size = \"16GiB\"\n")
		if err != nil {
			t.Fatal(err)
		}
		c := cfg.Compaction
		if c.HourlyMaxConcurrent != 2 || c.HourlyMemoryLimit != "6GB" || c.HourlyThreads != 3 || c.HourlyMaxTempDirectorySize != "16GiB" {
			t.Fatalf("loaded (%d, %q, %d, %q), want (2, 6GB, 3, 16GiB)",
				c.HourlyMaxConcurrent, c.HourlyMemoryLimit, c.HourlyThreads, c.HourlyMaxTempDirectorySize)
		}
		if c.MaxConcurrent != 1 || c.MemoryLimit != "12GB" || c.Threads != 4 {
			t.Fatalf("daily sizing changed: (%d, %q, %d)", c.MaxConcurrent, c.MemoryLimit, c.Threads)
		}
	})

	t.Run("env", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_COMPACTION_MEMORY_LIMIT", "4GB")
		t.Setenv("ARC_COMPACTION_HOURLY_MAX_CONCURRENT", "2")
		t.Setenv("ARC_COMPACTION_HOURLY_MEMORY_LIMIT", "1GB")
		t.Setenv("ARC_COMPACTION_HOURLY_THREADS", "2")
		t.Setenv("ARC_COMPACTION_HOURLY_MAX_TEMP_DIRECTORY_SIZE", "6GiB")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		c := cfg.Compaction
		if c.HourlyMaxConcurrent != 2 || c.HourlyMemoryLimit != "1GB" || c.HourlyThreads != 2 || c.HourlyMaxTempDirectorySize != "6GiB" {
			t.Fatalf("env loaded (%d, %q, %d, %q)", c.HourlyMaxConcurrent, c.HourlyMemoryLimit, c.HourlyThreads, c.HourlyMaxTempDirectorySize)
		}
	})

	// Explicit daily sizing is inherited as is, which is fine while the
	// hourly tier runs no more jobs than daily does.
	t.Run("explicit compaction sizing is inherited, not re-derived", func(t *testing.T) {
		cfg, err := loadCompactionTOML(t, "[compaction]\nmax_concurrent = 4\nmemory_limit = \"12GB\"\nthreads = 4\nhourly_max_concurrent = 2\n")
		if err != nil {
			t.Fatal(err)
		}
		if c := cfg.Compaction; c.HourlyMemoryLimit != "" || c.HourlyThreads != 0 || c.HourlyMaxTempDirectorySize != "" {
			t.Fatalf("hourly sizing = (%q, %d, %q), want unset (inherits)", c.HourlyMemoryLimit, c.HourlyThreads, c.HourlyMaxTempDirectorySize)
		}
	})

	// The hourly total (hourly_max_concurrent x per-job value) may not exceed
	// the daily one: hourly_max_concurrent alone on prod's daily sizing would
	// run 3 x 12GB and 3 x 60GiB of spill on a 100Gi disk.
	for _, tc := range []struct{ name, body, want string }{
		{"hourly_max_concurrent alone on explicit daily sizing", prodDaily + "hourly_max_concurrent = 3\n",
			"set compaction.hourly_memory_limit to at most 4GB"},
		{"memory set, spill inherited", prodDaily + "hourly_max_concurrent = 3\nhourly_memory_limit = \"4GB\"\n",
			"set compaction.hourly_max_temp_directory_size to at most 20GiB"},
		{"explicit hourly memory too big", prodDaily + "hourly_max_concurrent = 2\nhourly_memory_limit = \"6.5GB\"\nhourly_max_temp_directory_size = \"16GiB\"\n",
			"compaction.hourly_memory_limit to at most 6GB"},
		{"explicit hourly spill too big", prodDaily + "hourly_max_concurrent = 2\nhourly_memory_limit = \"6GB\"\nhourly_max_temp_directory_size = \"31GiB\"\n",
			"compaction.hourly_max_temp_directory_size to at most 30GiB"},
		{"default spill cap", "[compaction]\nmax_concurrent = 1\nmemory_limit = \"12GB\"\nhourly_max_concurrent = 2\nhourly_memory_limit = \"6GB\"\n",
			"compaction.hourly_max_temp_directory_size to at most 6GiB"},
		{"unparseable daily spill", "[compaction]\nmax_concurrent = 1\nmemory_limit = \"12GB\"\nmax_temp_directory_size = \"lots\"\nhourly_max_concurrent = 2\nhourly_memory_limit = \"6GB\"\nhourly_max_temp_directory_size = \"16GiB\"\n",
			"compaction.max_temp_directory_size = \"lots\" is not a size"},
	} {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			_, err := loadCompactionTOML(t, tc.body)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load() error = %v, want one containing %q", err, tc.want)
			}
		})
	}

	t.Run("accepts a total equal to the daily one", func(t *testing.T) {
		if _, err := loadCompactionTOML(t, prodDaily+"hourly_max_concurrent = 3\nhourly_memory_limit = \"4GB\"\nhourly_max_temp_directory_size = \"20GiB\"\n"); err != nil {
			t.Fatal(err)
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
		cfg, err := loadCompactionTOML(t, "[database]\nmemory_limit = \"12GB\"\n[compaction]\nmax_concurrent = 1\nhourly_max_concurrent = 3\nhourly_max_temp_directory_size = \"4GiB\"\n")
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
		{"garbage spill cap", "hourly_max_temp_directory_size = \"lots\"", "compaction.hourly_max_temp_directory_size"},
		{"unit-less spill cap", "hourly_max_temp_directory_size = \"16\"", "compaction.hourly_max_temp_directory_size"},
		{"percent spill cap", "hourly_max_temp_directory_size = \"90%\"", "compaction.hourly_max_temp_directory_size"},
		{"zero spill cap", "hourly_max_temp_directory_size = \"0GiB\"", "compaction.hourly_max_temp_directory_size"},
		{"negative spill cap", "hourly_max_temp_directory_size = \"-1GiB\"", "compaction.hourly_max_temp_directory_size"},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			_, err := loadCompactionTOML(t, "[compaction]\n"+tc.body+"\n")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load() error = %v, want one naming %s", err, tc.want)
			}
		})
	}
}

func TestParseDuckDBSize(t *testing.T) {
	for in, want := range map[string]float64{
		"16GiB": 16 << 30, "16gib": 16 << 30, " 1.5 GiB ": 1.5 * (1 << 30), "6GB": 6e9,
		"512MB": 512e6, "512MiB": 512 << 20, "1TiB": 1 << 40, "100B": 100,
	} {
		if got, ok := parseDuckDBSize(in); !ok || got != want {
			t.Errorf("parseDuckDBSize(%q) = (%v, %v), want %v", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "16", "lots", "90%", "-1GiB", "16 GiBs", "GiB"} {
		if _, ok := parseDuckDBSize(in); ok {
			t.Errorf("parseDuckDBSize(%q) accepted", in)
		}
	}
}
