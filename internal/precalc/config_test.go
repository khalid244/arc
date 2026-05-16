package precalc

import (
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	c := Config{Enabled: true, TZ: "Asia/Riyadh"}
	c.Defaults()
	if c.CoverageThreshold != 0.99 {
		t.Errorf("CoverageThreshold = %v, want 0.99", c.CoverageThreshold)
	}
	if c.DimRichCap != 100 {
		t.Errorf("DimRichCap = %v, want 100", c.DimRichCap)
	}
	if c.HLLLgK != 14 {
		t.Errorf("HLLLgK = %v, want 14", c.HLLLgK)
	}
	if c.GraceWindow != 15*time.Minute {
		t.Errorf("GraceWindow = %v, want 15m", c.GraceWindow)
	}
}

func TestConfigValidate_RequiresTZ(t *testing.T) {
	c := Config{Enabled: true}
	if err := c.Validate(); err == nil {
		t.Fatal("Validate() should fail when TZ unset and Enabled=true")
	}
}

func TestConfigValidate_RejectsBadTZ(t *testing.T) {
	c := Config{Enabled: true, TZ: "Atlantis/Mu"}
	if err := c.Validate(); err == nil {
		t.Fatal("Validate() should fail on unknown TZ")
	}
}

func TestConfigValidate_AcceptsValid(t *testing.T) {
	c := Config{Enabled: true, TZ: "Asia/Riyadh", CoverageThreshold: 0.99, HLLLgK: 14}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestConfigValidate_DisabledSkipsChecks(t *testing.T) {
	c := Config{Enabled: false}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() on disabled config should pass, got %v", err)
	}
}
