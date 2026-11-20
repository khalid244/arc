package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestCompactionRequeueAndCoalescedTimeoutConfig(t *testing.T) {
	t.Run("defaults keep today's behaviour", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		c := cfg.Compaction
		if c.RequeueFailedTiers || c.CoalescedFreshTimeout {
			t.Fatalf("requeue_failed_tiers=%v coalesced_fresh_timeout=%v, want both false", c.RequeueFailedTiers, c.CoalescedFreshTimeout)
		}
		if c.RequeueMinDelay != 30*time.Minute {
			t.Fatalf("requeue_min_delay = %v, want 30m", c.RequeueMinDelay)
		}
	})

	t.Run("toml", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		toml := "[compaction]\nrequeue_failed_tiers = true\nrequeue_min_delay = \"45m\"\ncoalesced_fresh_timeout = true\n"
		if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		c := cfg.Compaction
		if !c.RequeueFailedTiers || !c.CoalescedFreshTimeout || c.RequeueMinDelay != 45*time.Minute {
			t.Fatalf("loaded requeue_failed_tiers=%v requeue_min_delay=%v coalesced_fresh_timeout=%v", c.RequeueFailedTiers, c.RequeueMinDelay, c.CoalescedFreshTimeout)
		}
	})

	t.Run("env", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_COMPACTION_REQUEUE_FAILED_TIERS", "true")
		t.Setenv("ARC_COMPACTION_REQUEUE_MIN_DELAY", "1h")
		t.Setenv("ARC_COMPACTION_COALESCED_FRESH_TIMEOUT", "true")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		c := cfg.Compaction
		if !c.RequeueFailedTiers || !c.CoalescedFreshTimeout || c.RequeueMinDelay != time.Hour {
			t.Fatalf("loaded requeue_failed_tiers=%v requeue_min_delay=%v coalesced_fresh_timeout=%v", c.RequeueFailedTiers, c.RequeueMinDelay, c.CoalescedFreshTimeout)
		}
	})

	// A delay this short would retry a failed tier at nearly every drain.
	for _, bad := range []string{"0s", "-5m", "30s", "soon"} {
		t.Run("rejects "+bad, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_COMPACTION_REQUEUE_MIN_DELAY", bad)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), "requeue_min_delay") {
				t.Fatalf("requeue_min_delay=%q: err = %v, want a requeue_min_delay error", bad, err)
			}
		})
	}
}
