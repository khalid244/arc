package api

// POST /api/v1/internal/stager/import (ingest.stager_import_enabled): a
// terminating ingest pod hands its staged units to this node. The route
// dispatches on X-Arc-Stager-Kind, refuses with 503 while this node cannot
// take writes, and maps the stager's verdict to a status the sender acts on
// (2xx = delete locally; anything else = keep).

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

type fakeImporter struct {
	got []byte
	res ingest.StagerImportResult
	err error
}

func (f *fakeImporter) ImportUnit(r io.Reader) (ingest.StagerImportResult, error) {
	f.got, _ = io.ReadAll(r)
	return f.res, f.err
}

func TestStagerImportRoute(t *testing.T) {
	post := func(app *fiber.App, kind string, body []byte) (int, string) {
		req := httptest.NewRequest("POST", ingest.StagerImportPath, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-tar")
		if kind != "" {
			req.Header.Set(ingest.StagerKindHeader, kind)
		}
		resp, err := app.Test(req, 5000)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	fresh := &fakeImporter{res: ingest.StagerImportResult{Stored: 2, Skipped: 1}}
	late := &fakeImporter{}
	var refuse error
	app := fiber.New()
	RegisterStagerImportRoute(app, map[string]StagerImporter{"fresh": fresh, "late": late},
		func() error { return refuse }, zerolog.Nop())

	code, body := post(app, "fresh", []byte("unit-bytes"))
	if code != fiber.StatusOK || string(fresh.got) != "unit-bytes" {
		t.Fatalf("fresh unit: %d %s (importer got %q)", code, body, fresh.got)
	}
	var res ingest.StagerImportResult
	if err := json.Unmarshal([]byte(body), &res); err != nil || res.Stored != 2 || res.Skipped != 1 {
		t.Fatalf("response %q, want the import result", body)
	}
	if code, _ := post(app, "", []byte("x")); code != fiber.StatusServiceUnavailable {
		t.Errorf("no kind header: %d, want 503", code)
	}
	if code, _ := post(app, "other", []byte("x")); code != fiber.StatusServiceUnavailable {
		t.Errorf("unknown kind: %d, want 503 (the sender keeps the unit)", code)
	}
	for err, want := range map[error]int{
		fmt.Errorf("%w: x", ingest.ErrStagerImportInvalid):  fiber.StatusBadRequest,
		fmt.Errorf("%w: x", ingest.ErrStagerImportFromSelf): fiber.StatusConflict,
		ingest.ErrStagerClosing:                             fiber.StatusServiceUnavailable,
		errors.New("disk full"):                             fiber.StatusInternalServerError,
	} {
		late.err = err
		if code, _ := post(app, "late", []byte("x")); code != want {
			t.Errorf("importer error %v: %d, want %d", err, code, want)
		}
	}
	refuse = fmt.Errorf("%w: disk", ingest.ErrIngestBackpressure)
	late.err = nil
	late.got = nil
	if code, _ := post(app, "late", []byte("x")); code != fiber.StatusServiceUnavailable || late.got != nil {
		t.Errorf("node refusing writes: %d (importer called: %v), want 503 without importing", code, late.got != nil)
	}
}
