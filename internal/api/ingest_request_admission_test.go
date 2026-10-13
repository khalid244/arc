package api

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// A line-protocol request carrying two measurements is written one measurement
// at a time. If the local disk crosses the hard watermark between them, the
// request must not be answered 503 after its first measurement was accepted:
// the client's retry would write that measurement twice.
func TestLineProtocolRefusalIsAllOrNothing(t *testing.T) {
	store, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.IngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 4}
	buf := ingest.NewArrowBuffer(cfg, store, zerolog.Nop())
	t.Cleanup(func() { _ = buf.Close() })
	// Admit the up-front check and the first measurement, refuse from then on.
	var calls atomic.Int64
	buf.SetBackpressureGate(func() error {
		if calls.Add(1) > 2 {
			return fmt.Errorf("%w: local disk over the hard watermark", ingest.ErrIngestBackpressure)
		}
		return nil
	})
	app := fiber.New()
	NewLineProtocolHandler(buf, zerolog.Nop()).RegisterRoutes(app)
	now := time.Now().UnixNano()
	lp := []byte(fmt.Sprintf("cpu,host=a usage=1.5 %d\nmem,host=a used=2 %d\n", now, now))
	req := httptest.NewRequest("POST", "/api/v1/write/line-protocol", bytes.NewReader(lp))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("x-arc-database", "db")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatal(err)
	}
	n := buf.GetStats()["total_records_buffered"]
	if resp.StatusCode == fiber.StatusServiceUnavailable && n != int64(0) {
		t.Fatalf("503 (retry) after %v record(s) of the request were accepted", n)
	}
	if resp.StatusCode/100 != 2 || n != int64(2) {
		t.Fatalf("status %d, buffered %v; want the whole request accepted", resp.StatusCode, n)
	}
}
