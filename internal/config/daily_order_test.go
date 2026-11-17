package config

import (
	"os"
	"strings"
	"testing"
)

func TestCompactionDailyOrderConfig(t *testing.T) {
	load := func(t *testing.T, toml string) (*Config, error) {
		t.Helper()
		dir := t.TempDir()
		t.Chdir(dir)
		if toml != "" {
			if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return Load()
	}

	t.Run("default discovery", func(t *testing.T) {
		cfg, err := load(t, "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Compaction.DailyOrder != "discovery" {
			t.Fatalf("daily_order defaults to %q, want discovery", cfg.Compaction.DailyOrder)
		}
	})

	t.Run("toml", func(t *testing.T) {
		cfg, err := load(t, "[compaction]\ndaily_order = \"priority\"\n")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Compaction.DailyOrder != "priority" {
			t.Fatalf("daily_order = %q, want priority", cfg.Compaction.DailyOrder)
		}
	})

	t.Run("env, case and spaces", func(t *testing.T) {
		t.Setenv("ARC_COMPACTION_DAILY_ORDER", " Priority ")
		cfg, err := load(t, "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Compaction.DailyOrder != "priority" {
			t.Fatalf("ARC_COMPACTION_DAILY_ORDER loaded %q, want priority", cfg.Compaction.DailyOrder)
		}
	})

	for name, value := range map[string]string{"unknown value fails": "newest", "empty value fails": ""} {
		t.Run(name, func(t *testing.T) {
			_, err := load(t, "[compaction]\ndaily_order = \""+value+"\"\n")
			if err == nil || !strings.Contains(err.Error(), "compaction.daily_order") {
				t.Fatalf("Load() error = %v, want an invalid compaction.daily_order error", err)
			}
		})
	}
}
