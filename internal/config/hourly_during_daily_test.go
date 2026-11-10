package config

import (
	"os"
	"testing"
)

func TestCompactionHourlyDuringDailyConfig(t *testing.T) {
	t.Run("default off", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Compaction.HourlyDuringDaily {
			t.Fatal("hourly_during_daily defaults to true, want false")
		}
	})

	t.Run("toml", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		if err := os.WriteFile(dir+"/arc.toml", []byte("[compaction]\nhourly_during_daily = true\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if !cfg.Compaction.HourlyDuringDaily {
			t.Fatal("hourly_during_daily = true in arc.toml did not load")
		}
	})

	t.Run("env", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_COMPACTION_HOURLY_DURING_DAILY", "true")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if !cfg.Compaction.HourlyDuringDaily {
			t.Fatal("ARC_COMPACTION_HOURLY_DURING_DAILY=true did not load")
		}
	})
}
