package api

import (
	gosql "database/sql"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// Review 2026-11-10 Q3 on /api/v1/query: a raw query whose first run hits a
// raw-file 404 is re-run once (X-Arc-Retry). Without the duckdb_arrow tag
// this runs the database/sql path; with it, the Arrow JSON path.
func TestRaw404RetriedOnQueryEndpoint(t *testing.T) {
	metrics.Init(zerolog.Nop())
	dataDir := filepath.Join(t.TempDir(), "data")
	part := filepath.Join(dataDir, "default/events/2026/06/01/00")
	if err := os.MkdirAll(part, 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := gosql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Exec("COPY (SELECT TIMESTAMPTZ '2026-06-01 00:10:00+00' + INTERVAL (i) MINUTE AS time FROM range(3) t(i)) TO '" +
		filepath.Join(part, "a.parquet") + "' (FORMAT parquet)")
	w.Close()
	if err != nil {
		t.Fatal(err)
	}
	backend, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, zerolog.Nop())
	if err != nil {
		t.Skipf("duckdb unavailable: %v", err)
	}
	t.Cleanup(func() { duck.Close() })
	if _, err := duck.Exec("CREATE SEQUENCE raw_retry_sql_seq"); err != nil {
		t.Fatal(err)
	}
	h := NewQueryHandler(duck, backend, zerolog.Nop(), 0, 0)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)

	const q = "SELECT CASE WHEN nextval('raw_retry_sql_seq') = 1 THEN " +
		"error('HTTP GET error on ''s3://arc/default/events/2026/06/01/00/b.parquet'' (HTTP 404)')::BIGINT ELSE n END AS n " +
		"FROM (SELECT count(*) AS n FROM events WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z')"
	body, _ := json.Marshal(QueryRequest{SQL: q})
	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 30000)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Data [][]any `json:"data"`
	}
	_ = json.Unmarshal(raw, &out)
	if resp.StatusCode != fiber.StatusOK || len(out.Data) != 1 || out.Data[0][0] != float64(3) {
		t.Fatalf("status %d: %s, want 200 n=3 after the retry", resp.StatusCode, raw)
	}
	if got := resp.Header.Get("X-Arc-Retry"); got != "raw-file-missing" {
		t.Fatalf("X-Arc-Retry = %q, want raw-file-missing", got)
	}
}
