//go:build duckdb_arrow

package api

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Tests for the fork behaviour ported onto upstream's query path in the
// October 2026 merge (QR-3, QR-7, QR-8, QR-9, QR-12 in the merge review).

// forkQueryEnv is a query handler over a local backend rooted at a temp dir,
// with the DuckDB sandbox allowed to read it.
type forkQueryEnv struct {
	t       *testing.T
	h       *QueryHandler
	app     *fiber.App
	dataDir string
}

func newForkQueryEnv(t *testing.T) *forkQueryEnv {
	t.Helper()
	metrics.Init(zerolog.Nop())
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	backend, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	duck, err := database.New(&database.Config{
		MemoryLimit:      "256MB",
		ThreadCount:      2,
		MaxConnections:   4,
		LocalStorageRoot: dataDir,
	}, zerolog.Nop())
	if err != nil {
		t.Skipf("duckdb unavailable: %v", err)
	}
	t.Cleanup(func() { duck.Close() })
	h := NewQueryHandler(duck, backend, zerolog.Nop(), 0, 0)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)
	return &forkQueryEnv{t: t, h: h, app: app, dataDir: dataDir}
}

// writeParquet writes the rows of selectSQL as one parquet file under the
// data dir (rel is relative to it), using a separate, unsandboxed DuckDB.
func (e *forkQueryEnv) writeParquet(rel, selectSQL string) string {
	e.t.Helper()
	full := filepath.Join(e.dataDir, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		e.t.Fatal(err)
	}
	db, err := gosql.Open("duckdb", "")
	if err != nil {
		e.t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("COPY (" + selectSQL + ") TO '" + full + "' (FORMAT parquet)"); err != nil {
		e.t.Fatalf("write %s: %v", rel, err)
	}
	return full
}

func (e *forkQueryEnv) post(path, sql string, headers map[string]string) (int, http200Body, map[string]string) {
	e.t.Helper()
	body, _ := json.Marshal(QueryRequest{SQL: sql})
	req := httptest.NewRequest("POST", path, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := e.app.Test(req, 30000)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out http200Body
	_ = json.Unmarshal(raw, &out)
	out.raw = string(raw)
	hdr := map[string]string{}
	for _, k := range []string{"X-Arc-Rollup-Fallback", "X-Arc-Rollup-Cube", "X-Arc-Drift-Fill"} {
		hdr[k] = resp.Header.Get(k)
	}
	return resp.StatusCode, out, hdr
}

type http200Body struct {
	Success bool            `json:"success"`
	Columns []string        `json:"columns"`
	Data    [][]interface{} `json:"data"`
	Error   string          `json:"error"`
	raw     string
}

// QR-7: an execution error must never be reported as 499 "client
// disconnected". Cancelling the query context on the error path used to make
// ctx.Err() == Canceled for every failure.
func TestExecutionErrorIsNotClientDisconnect_QR7(t *testing.T) {
	e := newForkQueryEnv(t)
	for _, path := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
		status, out, _ := e.post(path, "SELECT CAST('not a number' AS INTEGER) AS x", nil)
		if status == statusClientClosedRequest {
			t.Fatalf("%s: execution error reported as 499: %s", path, out.raw)
		}
		if status != fiber.StatusInternalServerError {
			t.Fatalf("%s: status %d, want 500: %s", path, status, out.raw)
		}
		if !strings.Contains(out.raw, "Conversion Error") {
			t.Fatalf("%s: real error hidden: %s", path, out.raw)
		}
	}
}

// QR-7: only the disconnect watcher sets the flag the 499 decision reads; a
// cancel from an error path, a timeout or the registry does not.
func TestWatchClientGoneFlagsOnlyDisconnects_QR7(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	metrics.Init(zerolog.Nop())

	// Cancelled by the caller: not a disconnect.
	serverConn, clientConn := newTCPConnPair(t)
	base, cancel := context.WithCancel(context.Background())
	ctx, _ := h.watchClientGone(base, serverConn, "", cancel, "sql_json")
	cancel()
	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)
	if clientDisconnected(ctx) {
		t.Fatal("a caller cancel was flagged as a client disconnect")
	}
	_ = clientConn.Close()

	// Client closes the connection: flagged, and the query is cancelled.
	serverConn, clientConn = newTCPConnPair(t)
	base, cancel = context.WithCancel(context.Background())
	defer cancel()
	ctx, _ = h.watchClientGone(base, serverConn, "", cancel, "sql_json")
	if err := clientConn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("client disconnect did not cancel the query")
	}
	if !clientDisconnected(ctx) {
		t.Fatal("a client disconnect was not flagged")
	}
}

// QR-3: a rollup-served query whose merge SQL hits schema drift must be served
// from SOURCE (complete), never cube-only (which drops the fresh tail).
func TestRollupSchemaDriftServesSourceNotCubeOnly_QR3(t *testing.T) {
	e := newForkQueryEnv(t)
	e.writeParquet("default/events/2026/06/01/00/a.parquet",
		"SELECT TIMESTAMPTZ '2026-06-01 00:10:00+00' AS time, 'x' AS site UNION ALL SELECT TIMESTAMPTZ '2026-06-01 00:20:00+00', 'y'")
	e.writeParquet("default/events/2026/06/01/01/b.parquet",
		"SELECT TIMESTAMPTZ '2026-06-01 01:10:00+00' AS time")
	// The AUTO rewrite fails to bind (schema drift in the merge's fresh-tail
	// branch); routeOnlyCalls records whether the cube-only route was used.
	f := &fakeRollupRouter{
		serveSQL:     `SELECT count(*) AS n FROM (SELECT 1 AS x) t WHERE "site" IS NOT NULL`,
		serveOnlySQL: "SELECT 1 AS n",
	}
	e.h.SetRollupRouter(f)

	sql := "SELECT count(*) AS n FROM events WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z'"
	for _, path := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
		f.routeOnlyCalls = 0
		status, out, hdr := e.post(path, sql, nil)
		if status != fiber.StatusOK {
			t.Fatalf("%s: status %d: %s", path, status, out.raw)
		}
		if f.routeOnlyCalls != 0 {
			t.Fatalf("%s: schema drift fell back to the cube-only route", path)
		}
		if hdr["X-Arc-Rollup-Fallback"] != "source" {
			t.Fatalf("%s: X-Arc-Rollup-Fallback = %q, want source", path, hdr["X-Arc-Rollup-Fallback"])
		}
		if path == "/api/v1/query" && (len(out.Data) != 1 || out.Data[0][0] != float64(3)) {
			t.Fatalf("%s: source fallback lost rows: %s", path, out.raw)
		}
	}
}

// QR-12: the explain endpoint runs the validator and the RBAC check.
func TestExplainRollupValidatesAndChecksRBAC_QR12(t *testing.T) {
	rbac := &mockRBACChecker{enabled: true, deniedReason: "no read permission"}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "t"))

	post := func(sql string) int {
		body, _ := json.Marshal(QueryRequest{SQL: sql})
		req := httptest.NewRequest("POST", "/api/v1/query/explain", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, -1)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := post("SELECT 1; DROP TABLE x"); got != fiber.StatusBadRequest {
		t.Fatalf("invalid SQL: status %d, want 400", got)
	}
	if got := post("SELECT count(*) FROM secret.events"); got != fiber.StatusForbidden {
		t.Fatalf("RBAC-denied table: status %d, want 403", got)
	}
}

// fakeManifests is a compactionManifestProvider with a fixed uploaded set.
type fakeManifests struct{ uploaded map[string]struct{} }

func (f fakeManifests) GetFilesInManifests(context.Context) (map[string]struct{}, error) {
	return f.uploaded, nil
}
func (f fakeManifests) GetInputFilesForUploadedOutputs(context.Context) (map[string]struct{}, error) {
	return f.uploaded, nil
}

// QR-9 / QR-8: the compaction-exclude wrapper hides the inputs of an uploaded
// output, does not collide with a real "filename" column, does not change the
// SELECT * schema, and keeps the query off the per-partition fan-out.
func TestCompactionExcludeWrapper_QR8_QR9(t *testing.T) {
	e := newForkQueryEnv(t)
	e.writeParquet("default/logs/2026/06/01/00/input.parquet",
		"SELECT TIMESTAMPTZ '2026-06-01 00:10:00+00' AS time, 'a.txt' AS filename")
	e.writeParquet("default/logs/2026/06/01/00/output.parquet",
		"SELECT TIMESTAMPTZ '2026-06-01 00:10:00+00' AS time, 'a.txt' AS filename")
	e.h.SetManifestManager(fakeManifests{uploaded: map[string]struct{}{
		"default/logs/2026/06/01/00/input.parquet": {},
	}})

	status, out, _ := e.post("/api/v1/query", "SELECT * FROM logs WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z'", nil)
	if status != fiber.StatusOK {
		t.Fatalf("status %d: %s", status, out.raw)
	}
	if len(out.Data) != 1 {
		t.Fatalf("uploaded output's input was not hidden (rows=%d): %s", len(out.Data), out.raw)
	}
	if strings.Join(out.Columns, ",") != "time,filename" {
		t.Fatalf("SELECT * schema changed during compaction: %v", out.Columns)
	}

	path := e.h.getStoragePath(context.Background(), "default", "logs")
	if _, info := e.h.buildReadParquetExprForParallel(context.Background(), path,
		"SELECT * FROM logs WHERE time >= NOW() - INTERVAL 6 HOUR", "FROM"); info != nil {
		t.Fatal("a query with an active exclusion must not fan out per partition")
	}
}
