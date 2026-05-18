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
	got, ok := h.tryTieredRewrite(context.Background(), "SELECT 1", "")
	if ok {
		t.Error("with no tieredDeps, tryTieredRewrite should return ok=false")
	}
	if got != "SELECT 1" {
		t.Errorf("expected original SQL, got %q", got)
	}
}

// Verifies the header→table resolution: a bare `FROM downloads` with
// header `default` must look up tieredDeps["default.downloads"], not
// just iterate the map and pick the first match.
func TestHandler_TryTieredRewrite_HeaderResolvesBareTable(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	// Register deps for "other.unrelated" — should NOT be used for `FROM downloads`.
	h.SetTieredDeps("other.unrelated", &tiered.RewriteDeps{GraceWindow: 6 * time.Hour})
	sql := "SELECT * FROM downloads"
	got, ok := h.tryTieredRewrite(context.Background(), sql, "default")
	if ok {
		t.Errorf("with header=default and table=downloads, no deps for default.downloads should mean refuse; got rewritten %q", got)
	}
	if got != sql {
		t.Errorf("expected original SQL on refusal, got %q", got)
	}
}

// SQL-wins precedence: `FROM other.events` with header `production` must
// look up `other.events` (SQL's db wins), not `production.events`.
func TestHandler_TryTieredRewrite_SQLDBWinsOverHeader(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	h.SetTieredDeps("production.events", &tiered.RewriteDeps{GraceWindow: 6 * time.Hour})
	sql := "SELECT * FROM other.events"
	got, ok := h.tryTieredRewrite(context.Background(), sql, "production")
	if ok {
		t.Errorf("FROM other.events must resolve to other.events (not production.events); got rewritten %q", got)
	}
	if got != sql {
		t.Errorf("expected original SQL on refusal, got %q", got)
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
