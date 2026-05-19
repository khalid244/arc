package compaction

import (
	"testing"

	"github.com/rs/zerolog"
)

// Cycle 3: pin the contract that tier configs accept a MaxOutputBytes
// setting and propagate it onto the tier struct. Production wiring in
// cmd/arc/main.go will read max_output_size_mb from configmap and set
// this; Job will read it off the tier to decide between single-file
// and multi-output COPY.

func TestBaseTier_MaxOutputBytes_PropagatedFromConfig(t *testing.T) {
	cfg := &BaseTierConfig{
		MaxOutputBytes: 1024 * 1024,
		Logger:         zerolog.Nop(),
	}
	tier := NewBaseTier(cfg)
	if tier.MaxOutputBytes != 1024*1024 {
		t.Errorf("BaseTier.MaxOutputBytes: got %d, want %d", tier.MaxOutputBytes, 1024*1024)
	}
}

func TestBaseTier_MaxOutputBytes_DefaultZeroWhenUnset(t *testing.T) {
	cfg := &BaseTierConfig{Logger: zerolog.Nop()}
	tier := NewBaseTier(cfg)
	if tier.MaxOutputBytes != 0 {
		t.Errorf("BaseTier.MaxOutputBytes default: got %d, want 0 (unbounded — single-file mode)", tier.MaxOutputBytes)
	}
}

func TestHourlyTier_MaxOutputBytes_PropagatedFromConfig(t *testing.T) {
	cfg := &HourlyTierConfig{
		MaxOutputBytes: 256 * 1024 * 1024,
		Logger:         zerolog.Nop(),
	}
	tier := NewHourlyTier(cfg)
	if tier.MaxOutputBytes != 256*1024*1024 {
		t.Errorf("HourlyTier.MaxOutputBytes: got %d, want %d", tier.MaxOutputBytes, 256*1024*1024)
	}
}

func TestDailyTier_MaxOutputBytes_PropagatedFromConfig(t *testing.T) {
	cfg := &DailyTierConfig{
		MaxOutputBytes: 1024 * 1024 * 1024,
		Logger:         zerolog.Nop(),
	}
	tier := NewDailyTier(cfg)
	if tier.MaxOutputBytes != 1024*1024*1024 {
		t.Errorf("DailyTier.MaxOutputBytes: got %d, want %d", tier.MaxOutputBytes, 1024*1024*1024)
	}
}
