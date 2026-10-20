//go:build duckdb_arrow

package api

import (
	"testing"

	"github.com/gofiber/fiber/v2"
)

// rawTail404 is the error DuckDB raises when a fresh-tail glob it expanded
// names a raw file compaction has since deleted.
const rawTail404 = `error('HTTP GET error on ''s3://arc/default/events/2026/06/01/01/b.parquet'' (HTTP 404)')`

// A rollup-served AUTO query whose fresh tail hit a raw file compaction just
// removed re-runs the same merged query once (X-Arc-Rollup-Retry) instead of
// building the whole-window source fallback. When the retry fails too, the
// existing source fallback still answers.
func TestRawTail404RetriedBeforeSourceFallback(t *testing.T) {
	const sql = "SELECT count(*) AS n FROM events WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z'"
	cases := []struct {
		name         string
		serveSQL     string
		wantN        float64
		wantFallback string
	}{
		{"retry succeeds", "SELECT CASE WHEN nextval('raw_tail_retry_seq') = 1 THEN " + rawTail404 + "::BIGINT ELSE 42 END AS n", 42, ""},
		{"retry fails again", "SELECT " + rawTail404 + "::BIGINT AS n", 3, "source"},
	}
	for _, tc := range cases {
		for _, path := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
			t.Run(tc.name+path, func(t *testing.T) {
				e := newForkQueryEnv(t)
				e.writeParquet("default/events/2026/06/01/00/a.parquet",
					"SELECT TIMESTAMPTZ '2026-06-01 00:10:00+00' AS time UNION ALL SELECT TIMESTAMPTZ '2026-06-01 00:20:00+00'")
				e.writeParquet("default/events/2026/06/01/01/c.parquet",
					"SELECT TIMESTAMPTZ '2026-06-01 01:10:00+00' AS time")
				if _, err := e.h.db.Exec("CREATE SEQUENCE raw_tail_retry_seq"); err != nil {
					t.Fatal(err)
				}
				e.h.SetRollupRouter(&fakeRollupRouter{serveSQL: tc.serveSQL})
				var retryHdr string
				e.app = fiber.New(fiber.Config{DisableStartupMessage: true})
				e.app.Use(func(c *fiber.Ctx) error {
					err := c.Next()
					retryHdr = string(c.Response().Header.Peek("X-Arc-Rollup-Retry"))
					return err
				})
				e.h.RegisterRoutes(e.app)

				before := localProbes(e.h)
				status, n, hdr, raw := postCount(e, path, sql)
				if status != fiber.StatusOK || n != tc.wantN {
					t.Fatalf("status %d n=%v, want 200 n=%v: %s", status, n, tc.wantN, raw)
				}
				if retryHdr != "raw-file-missing" {
					t.Fatalf("X-Arc-Rollup-Retry = %q, want raw-file-missing", retryHdr)
				}
				if hdr["X-Arc-Rollup-Fallback"] != tc.wantFallback {
					t.Fatalf("X-Arc-Rollup-Fallback = %q, want %q", hdr["X-Arc-Rollup-Fallback"], tc.wantFallback)
				}
				if tc.wantFallback == "" && localProbes(e.h) != before {
					t.Fatal("a successful retry still built the source fallback (listed source partitions)")
				}
			})
		}
	}
}

// Review 2026-11-10 Q3: a query no cube serves re-runs once on a raw-file 404
// (X-Arc-Retry) on every endpoint that runs raw SQL. The user's SQL raises
// the 404 on its first run only, the way a stale cached listing does.
func TestRaw404RetriedForQueriesNoCubeServes(t *testing.T) {
	const sql = "SELECT raw_retry_gate(n) AS n " +
		"FROM (SELECT count(*) AS n FROM events WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z')"
	cases := []struct {
		name    string
		router  RollupRouter
		headers map[string]string
	}{
		{"no rollup router", nil, nil},
		{"router declines", &fakeRollupRouter{}, nil},
		{"X-Arc-No-Rollup", &fakeRollupRouter{serveSQL: "SELECT 99::BIGINT AS n"}, map[string]string{"X-Arc-No-Rollup": "true"}},
	}
	for _, tc := range cases {
		for _, path := range []string{"/api/v1/query", "/api/v1/query/arrow", "/api/v1/query/msgpack"} {
			t.Run(tc.name+path, func(t *testing.T) {
				e := newForkQueryEnv(t)
				e.writeParquet("default/events/2026/06/01/00/a.parquet",
					"SELECT TIMESTAMPTZ '2026-06-01 00:10:00+00' AS time UNION ALL SELECT TIMESTAMPTZ '2026-06-01 00:20:00+00'")
				e.writeParquet("default/events/2026/06/01/01/c.parquet",
					"SELECT TIMESTAMPTZ '2026-06-01 01:10:00+00' AS time")
				if _, err := e.h.db.Exec("CREATE SEQUENCE raw_retry_seq"); err != nil {
					t.Fatal(err)
				}
				// User SQL may not call nextval, so the one-shot 404 lives in a
				// server-side macro the query only calls.
				if _, err := e.h.db.Exec("CREATE MACRO raw_retry_gate(v) AS CASE WHEN nextval('raw_retry_seq') = 1 THEN " +
					rawTail404 + "::BIGINT ELSE v END"); err != nil {
					t.Fatal(err)
				}
				if tc.router != nil {
					e.h.SetRollupRouter(tc.router)
				}
				res := postEndpoint(t, e, path, sql, tc.headers)
				if res.status != fiber.StatusOK {
					t.Fatalf("status %d, want 200 after the retry: %s", res.status, res.raw)
				}
				if got := res.header.Get("X-Arc-Retry"); got != "raw-file-missing" {
					t.Fatalf("X-Arc-Retry = %q, want raw-file-missing", got)
				}
				if res.header.Get("X-Arc-Rollup-Cube") != "" || res.header.Get("X-Arc-Rollup-Retry") != "" {
					t.Fatalf("a raw query carries rollup headers: %v", res.header)
				}
				if path != "/api/v1/query/msgpack" && (len(res.cols["n"]) != 1 || res.cols["n"][0].(float64) != 3) {
					t.Fatalf("n = %v, want 3", res.cols["n"])
				}
			})
		}
	}
}
