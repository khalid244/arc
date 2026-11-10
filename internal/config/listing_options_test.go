package config

import (
	"os"
	"strings"
	"testing"
)

// query.listing_recent_days / listing_single_flight /
// listing_stale_reuse_seconds tune the partition pruner's storage listings.
// All are off by default, which keeps the listings as they were.
func TestQueryListingOptionsConfig(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		q := cfg.Query
		if q.ListingRecentDays != 0 || q.ListingSingleFlight || q.ListingStaleReuseSeconds != 0 {
			t.Fatalf("defaults = %d/%v/%d, want 0/false/0", q.ListingRecentDays, q.ListingSingleFlight, q.ListingStaleReuseSeconds)
		}
	})
	t.Run("toml", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		toml := "[query]\nlisting_recent_days = 2\nlisting_single_flight = true\nlisting_stale_reuse_seconds = 1800\n"
		if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		q := cfg.Query
		if q.ListingRecentDays != 2 || !q.ListingSingleFlight || q.ListingStaleReuseSeconds != 1800 {
			t.Fatalf("toml = %d/%v/%d, want 2/true/1800", q.ListingRecentDays, q.ListingSingleFlight, q.ListingStaleReuseSeconds)
		}
	})
	t.Run("env", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_QUERY_LISTING_RECENT_DAYS", "3")
		t.Setenv("ARC_QUERY_LISTING_SINGLE_FLIGHT", "true")
		t.Setenv("ARC_QUERY_LISTING_STALE_REUSE_SECONDS", "600")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		q := cfg.Query
		if q.ListingRecentDays != 3 || !q.ListingSingleFlight || q.ListingStaleReuseSeconds != 600 {
			t.Fatalf("env = %d/%v/%d, want 3/true/600", q.ListingRecentDays, q.ListingSingleFlight, q.ListingStaleReuseSeconds)
		}
	})
	for _, bad := range []struct{ key, value string }{
		{"listing_recent_days", "-1"},
		{"listing_recent_days", "32"},
		{"listing_stale_reuse_seconds", "-1"},
		{"listing_stale_reuse_seconds", "86401"},
	} {
		t.Run("invalid "+bad.key+"="+bad.value, func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			if err := os.WriteFile(dir+"/arc.toml", []byte("[query]\n"+bad.key+" = "+bad.value+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), "query."+bad.key) {
				t.Fatalf("Load() error = %v, want one naming query.%s", err, bad.key)
			}
		})
	}
}
