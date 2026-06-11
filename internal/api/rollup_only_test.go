package api

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// fakeRollupRouter is a structural test double for the RollupRouter interface,
// recording which entry point the api layer used per rollup mode.
type fakeRollupRouter struct {
	routeCalls     int // auto path (RouteHTTP)
	routeOnlyCalls int // best-effort path (RouteOnlyHTTP)
	serveSQL       string
	serveOnlySQL   string
}

func (f *fakeRollupRouter) RouteHTTP(sql, headerDB string) (string, bool, string) {
	f.routeCalls++
	if f.serveSQL != "" {
		return f.serveSQL, true, "fake.cube.auto"
	}
	return "", false, ""
}

func (f *fakeRollupRouter) RouteOnlyHTTP(sql, headerDB string) (string, bool, string) {
	f.routeOnlyCalls++
	if f.serveOnlySQL != "" {
		return f.serveOnlySQL, true, "fake.cube.best_effort"
	}
	return "", false, ""
}

func (f *fakeRollupRouter) ExplainHTTP(sql, headerDB string) (bool, string, string) {
	return false, "", "fake"
}

func rollupOnlyTestApp(h *QueryHandler) *fiber.App {
	app := fiber.New()
	app.Post("/api/v1/query", h.executeQuery)
	return app
}

func postQuery(t *testing.T, app *fiber.App, sql string, headers map[string]string) (*fiber.Response, int, []byte) {
	t.Helper()
	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(`{"sql":`+strconvQuote(sql)+`}`))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := app.Test(req, 30000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return nil, resp.StatusCode, body
}

func strconvQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// TestRollupOnly_BestEffortDecline_422 pins the shape-level decline: when the
// best-effort route ALSO declines (e.g. grain_too_fine / no covering cube),
// rollup-only mode still returns 422 with the existing message — and the api
// must have consulted the BEST-EFFORT entry point, not the auto one.
func TestRollupOnly_BestEffortDecline_422(t *testing.T) {
	f := &fakeRollupRouter{} // declines both paths
	h := &QueryHandler{rollupRouter: f, logger: zerolog.Nop()}
	app := rollupOnlyTestApp(h)

	_, status, body := postQuery(t, app,
		"SELECT date_trunc('minute', time) AS b, count(*) AS n FROM events WHERE time >= TIMESTAMPTZ '2026-06-10' AND time < TIMESTAMPTZ '2026-06-11' GROUP BY 1",
		map[string]string{"X-Arc-Rollup-Only": "true"})

	if status != fiber.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (body=%s)", status, body)
	}
	if !strings.Contains(string(body), "rollup-only mode: no covering rollup cube") {
		t.Errorf("body missing the rollup-only error message: %s", body)
	}
	if f.routeOnlyCalls != 1 || f.routeCalls != 0 {
		t.Errorf("rollup-only must use the best-effort route: routeOnlyCalls=%d routeCalls=%d, want 1/0",
			f.routeOnlyCalls, f.routeCalls)
	}
}

// TestRollupOnly_CoverageGapServes_200 pins the redefinition end-to-end: a
// coverage-shaped miss (the Jun-10 "newest day not built yet" chunk) is served
// by the best-effort route as a schema-correct zero-row rewrite, and the api
// returns 200 — not the old coverage 422.
func TestRollupOnly_CoverageGapServes_200(t *testing.T) {
	db, err := database.New(&database.Config{MaxConnections: 1}, zerolog.Nop())
	if err != nil {
		t.Skipf("in-memory duckdb unavailable: %v", err)
	}
	defer db.Close()

	f := &fakeRollupRouter{
		// What the router emits for a zero-day range: a real-file read with an
		// impossible bucket predicate. Self-contained equivalent here.
		serveOnlySQL: "SELECT TIMESTAMPTZ '2026-06-10 00:00:00+00' AS b, 'eu' AS region, 0::BIGINT AS n WHERE 1=0",
	}
	h := &QueryHandler{rollupRouter: f, db: db, logger: zerolog.Nop()}
	app := rollupOnlyTestApp(h)

	_, status, body := postQuery(t, app,
		"SELECT date_trunc('day', time) AS b, region, count(*) AS n FROM events WHERE time >= TIMESTAMPTZ '2026-06-10' AND time < TIMESTAMPTZ '2026-06-11' GROUP BY 1,2",
		map[string]string{"X-Arc-Rollup-Only": "true"})

	if status != fiber.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	var got struct {
		Success  bool            `json:"success"`
		Columns  []string        `json:"columns"`
		Data     [][]interface{} `json:"data"`
		RowCount int             `json:"row_count"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v (body=%s)", err, body)
	}
	if !got.Success {
		t.Fatalf("success=false (body=%s)", body)
	}
	if len(got.Data) != 0 {
		t.Fatalf("data rows = %d, want 0 (body=%s)", len(got.Data), body)
	}
	if len(got.Columns) != 3 {
		t.Fatalf("columns = %v, want 3 schema-correct columns (body=%s)", got.Columns, body)
	}
	if f.routeOnlyCalls != 1 || f.routeCalls != 0 {
		t.Errorf("rollup-only must use the best-effort route: routeOnlyCalls=%d routeCalls=%d, want 1/0",
			f.routeOnlyCalls, f.routeCalls)
	}
}

// TestRollupAuto_UsesAutoRoute pins that WITHOUT X-Arc-Rollup-Only the api
// keeps the auto entry point (with its coverage guards) — best-effort must not
// leak into auto mode.
func TestRollupAuto_UsesAutoRoute(t *testing.T) {
	db, err := database.New(&database.Config{MaxConnections: 1}, zerolog.Nop())
	if err != nil {
		t.Skipf("in-memory duckdb unavailable: %v", err)
	}
	defer db.Close()

	f := &fakeRollupRouter{serveSQL: "SELECT TIMESTAMPTZ '2026-06-01 00:00:00+00' AS b, 1::BIGINT AS n"}
	h := &QueryHandler{
		rollupRouter: f, db: db, logger: zerolog.Nop(),
		// The auto-served path precomputes a source-fallback rewrite, which
		// walks the storage transform — give it the same minimal fixtures the
		// other handler tests use.
		storage:    &mockLocalBackend{basePath: "./data"},
		pruner:     pruning.NewPartitionPruner(zerolog.Nop()),
		queryCache: database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
	}
	app := rollupOnlyTestApp(h)

	_, status, body := postQuery(t, app,
		"SELECT date_trunc('day', time) AS b, count(*) AS n FROM events WHERE time >= TIMESTAMPTZ '2026-06-01' AND time < TIMESTAMPTZ '2026-06-02' GROUP BY 1",
		nil)

	if status != fiber.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	if f.routeCalls != 1 || f.routeOnlyCalls != 0 {
		t.Errorf("auto mode must use RouteHTTP: routeCalls=%d routeOnlyCalls=%d, want 1/0",
			f.routeCalls, f.routeOnlyCalls)
	}
}
