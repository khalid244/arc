package api

// IW-02 / IW-11: a write refused for local-disk backpressure (or a failed WAL
// append with wal.reject_on_append_failure) is a 503 with Retry-After: 30 on
// every ingest route, decided before anything is appended to the WAL. Never a
// 204 for rows that were dropped, and never a 500 that clients treat as
// permanent.

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func backpressureBuffer(t *testing.T) *ingest.ArrowBuffer {
	t.Helper()
	store, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.IngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 4}
	buf := ingest.NewArrowBuffer(cfg, store, zerolog.Nop())
	t.Cleanup(func() { _ = buf.Close() })
	buf.SetBackpressureGate(func() error {
		return fmt.Errorf("%w: local disk 90%% of ingest.local_disk_max_bytes", ingest.ErrIngestBackpressure)
	})
	return buf
}

func TestIngestBackpressureIsA503WithRetryAfter30(t *testing.T) {
	buf := backpressureBuffer(t)
	app := fiber.New()
	NewMsgPackHandler(zerolog.Nop(), buf, 10<<20).RegisterRoutes(app)
	NewLineProtocolHandler(buf, zerolog.Nop()).RegisterRoutes(app)

	payload, err := msgpack.Marshal(map[string]interface{}{
		"m":       "cpu",
		"columns": map[string]interface{}{"time": []interface{}{time.Now().UnixMicro()}, "usage": []interface{}{1.5}},
	})
	if err != nil {
		t.Fatal(err)
	}
	lp := []byte(fmt.Sprintf("cpu,host=a usage=1.5 %d\n", time.Now().UnixNano()))
	reqs := map[string]struct {
		path, ctype string
		body        []byte
	}{
		"msgpack":           {"/api/v1/write/msgpack", "application/msgpack", payload},
		"line protocol v1":  {"/write?db=db", "text/plain", lp},
		"line protocol v2":  {"/api/v2/write?bucket=db", "text/plain", lp},
		"line protocol arc": {"/api/v1/write/line-protocol", "text/plain", lp},
	}
	for name, r := range reqs {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("POST", r.path, bytes.NewReader(r.body))
			req.Header.Set("Content-Type", r.ctype)
			req.Header.Set("x-arc-database", "db")
			resp, err := app.Test(req, 5000)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "30" {
				t.Fatalf("status %d Retry-After %q; want 503 with Retry-After: 30", resp.StatusCode, resp.Header.Get("Retry-After"))
			}
		})
	}
	if n := buf.GetStats()["total_records_buffered"]; n != int64(0) {
		t.Fatalf("refused writes buffered %v records", n)
	}
}

func TestIngestRefusalMappingCoversBackpressure(t *testing.T) {
	err := fmt.Errorf("%w: test", ingest.ErrIngestBackpressure)
	if got := importBufferStatus(err); got != fiber.StatusServiceUnavailable {
		t.Fatalf("import maps backpressure to %d, want 503", got)
	}
	if !isIngestRefusal(err) || !isIngestRefusal(fmt.Errorf("%w: x", ingest.ErrIngestFenced)) {
		t.Fatal("isIngestRefusal must cover both the fence and backpressure")
	}
	app := fiber.New()
	app.Post("/w", func(c *fiber.Ctx) error { return respondIngestFenced(c, fmt.Errorf("%w: x", ingest.ErrIngestFenced)) })
	resp, err2 := app.Test(httptest.NewRequest("POST", "/w", nil))
	if err2 != nil {
		t.Fatal(err2)
	}
	if resp.Header.Get("Retry-After") != "5" {
		t.Fatalf("fenced Retry-After = %q, want the fence's 5", resp.Header.Get("Retry-After"))
	}
}
