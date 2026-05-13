package rollup

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func TestHTTP_ListReturnsAllRollups(t *testing.T) {
	app := fiber.New()
	specs := []RollupSpec{
		{Name: "d__events__1h", Database: "d", SourceTable: "events", BucketInterval: time.Hour},
	}
	wmStore := newInMemWMStore()
	_ = wmStore.Put(context.Background(), Watermark{Rollup: "d__events__1h", StoragePath: specs[0].StoragePath(), Watermark: time.Date(2026, 5, 10, 13, 0, 0, 0, time.UTC)})

	h := &HTTPHandler{
		WMReader: wmStore,
		Builder:  false,
		Logger:   zerolog.Nop(),
	}
	h.SetSpecs(specs)
	h.Register(app)

	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/rollups", nil))
	if err != nil {
		t.Fatalf("test: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	var got []map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v\nbody: %s", err, body)
	}
	if len(got) != 1 || got[0]["name"] != "d__events__1h" {
		t.Errorf("unexpected list: %v", got)
	}
}

func TestHTTP_PauseRefusedOnNonBuilder(t *testing.T) {
	app := fiber.New()
	h := &HTTPHandler{
		WMReader: newInMemWMStore(),
		Builder:  false,
		Logger:   zerolog.Nop(),
	}
	h.SetSpecs([]RollupSpec{{Name: "x"}})
	h.Register(app)
	req := httptest.NewRequest("POST", "/api/v1/rollups/x/pause", strings.NewReader(""))
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test: %v", err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("expected 409 on non-builder, got %d", resp.StatusCode)
	}
}

func TestHTTP_PauseAcceptedOnBuilder(t *testing.T) {
	app := fiber.New()
	ctrl := NewControl()
	h := &HTTPHandler{
		WMReader: newInMemWMStore(),
		Builder:  true,
		Control:  ctrl,
		Logger:   zerolog.Nop(),
	}
	h.SetSpecs([]RollupSpec{{Name: "x"}})
	h.Register(app)
	req := httptest.NewRequest("POST", "/api/v1/rollups/x/pause", strings.NewReader(""))
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if !ctrl.IsPaused("x") {
		t.Error("expected control plane to record paused=true")
	}
}
