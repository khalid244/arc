package api

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/rollup/tiered"
	"github.com/rs/zerolog"
)

func TestHandler_TryTieredRewrite_NoTablesOptedIn(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	got, ok := h.tryTieredRewrite(context.Background(), "SELECT 1")
	if ok {
		t.Error("with no tieredDeps, tryTieredRewrite should return ok=false")
	}
	if got != "SELECT 1" {
		t.Errorf("expected original SQL, got %q", got)
	}
}

func TestHandler_SetTieredDeps_StoreAndRetrieve(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	d := &tiered.RewriteDeps{GraceWindow: 6 * time.Hour}
	h.SetTieredDeps("default.events", d)
	if got := h.tieredDepsFor("default.events"); got != d {
		t.Errorf("expected to retrieve registered deps, got %+v", got)
	}
	h.SetTieredDeps("default.events", nil)
	if got := h.tieredDepsFor("default.events"); got != nil {
		t.Errorf("expected nil after deregister, got %+v", got)
	}
}
