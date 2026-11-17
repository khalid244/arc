package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// compaction.manifest_recovery_input_check_concurrency bounds how many input
// HEADs manifest recovery runs at once while proving a finished job's inputs
// are gone. Default 1 = the serial checks recovery always did; 0 is read as 1.
func TestManifestRecoveryInputCheckConcurrencyConfig(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    int
		wantErr bool
	}{
		{"default serial", "", 1, false},
		{"zero means serial", "0", 1, false},
		{"set", "16", 16, false},
		{"max", "64", 64, false},
		{"negative", "-1", 0, true},
		{"above max", "65", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			if tc.value != "" {
				t.Setenv("ARC_COMPACTION_MANIFEST_RECOVERY_INPUT_CHECK_CONCURRENCY", tc.value)
			}
			cfg, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "compaction.manifest_recovery_input_check_concurrency") {
					t.Fatalf("got %v, want a compaction.manifest_recovery_input_check_concurrency validation error", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.Compaction.ManifestRecoveryInputCheckConcurrency; got != tc.want {
				t.Fatalf("ManifestRecoveryInputCheckConcurrency = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestManifestRecoveryInputCheckConcurrencyFromTOML(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	toml := "[compaction]\nmanifest_recovery_input_check_concurrency = 8\n"
	if err := os.WriteFile(filepath.Join(dir, "arc.toml"), []byte(toml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Compaction.ManifestRecoveryInputCheckConcurrency; got != 8 {
		t.Fatalf("ManifestRecoveryInputCheckConcurrency = %d from arc.toml, want 8", got)
	}
}

// compaction.input_delete_retries: how many times a compaction job re-sends a
// failed batch delete of its own inputs. Default 0 = no retry (today).
func TestInputDeleteRetriesConfig(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    int
		wantErr bool
	}{
		{"default off", "", 0, false},
		{"set", "2", 2, false},
		{"max", "10", 10, false},
		{"negative", "-1", 0, true},
		{"above max", "11", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			if tc.value != "" {
				t.Setenv("ARC_COMPACTION_INPUT_DELETE_RETRIES", tc.value)
			}
			cfg, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "compaction.input_delete_retries") {
					t.Fatalf("got %v, want a compaction.input_delete_retries validation error", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.Compaction.InputDeleteRetries; got != tc.want {
				t.Fatalf("InputDeleteRetries = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestInputDeleteRetriesFromTOML(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	toml := "[compaction]\ninput_delete_retries = 2\n"
	if err := os.WriteFile(filepath.Join(dir, "arc.toml"), []byte(toml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Compaction.InputDeleteRetries; got != 2 {
		t.Fatalf("InputDeleteRetries = %d from arc.toml, want 2", got)
	}
}
