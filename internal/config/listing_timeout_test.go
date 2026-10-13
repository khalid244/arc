package config

import (
	"os"
	"testing"
)

// query.listing_timeout_seconds bounds each storage listing partition pruning
// makes. A listing that does not finish in time refuses the query with 503
// (QR-6) instead of answering from an unverified file set, so on a slow
// object store (Hetzner) the bound decides how often panels see that 503.
func TestQueryListingTimeoutConfig(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.ListingTimeoutSeconds != 5 {
			t.Fatalf("default listing timeout = %d, want 5", cfg.Query.ListingTimeoutSeconds)
		}
	})
	t.Run("toml", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		if err := os.WriteFile(dir+"/arc.toml", []byte("[query]\nlisting_timeout_seconds = 15\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Query.ListingTimeoutSeconds != 15 {
			t.Fatalf("listing timeout = %d, want 15", cfg.Query.ListingTimeoutSeconds)
		}
	})
}
