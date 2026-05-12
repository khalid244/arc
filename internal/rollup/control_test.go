package rollup

import "testing"

func TestControl_PauseAndResume(t *testing.T) {
	c := NewControl()
	if c.IsPaused("r1") {
		t.Fatal("default should be unpaused")
	}
	c.Pause("r1")
	if !c.IsPaused("r1") {
		t.Error("expected paused after Pause")
	}
	c.Resume("r1")
	if c.IsPaused("r1") {
		t.Error("expected unpaused after Resume")
	}
}

func TestControl_RebuildRequestConsumed(t *testing.T) {
	c := NewControl()
	if c.PopRebuildRequest("r1") {
		t.Fatal("nothing requested yet")
	}
	c.RequestRebuild("r1")
	if !c.PopRebuildRequest("r1") {
		t.Error("expected pending rebuild")
	}
	if c.PopRebuildRequest("r1") {
		t.Error("rebuild should be consumed exactly once")
	}
}
