//go:build duckdb_arrow

package api

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

// An AUTO-mode rollup-served query builds its SOURCE fallback only when the
// cube read fails. Building it runs the partition pruner, which existence-lists
// storage for every day/hour of the window: hundreds of sequential S3 LISTs
// over a long dashboard range, for a fallback a healthy cube never uses.

// localProbes is the number of local partition existence probes the pruner
// has made (glob cache hits + misses).
func localProbes(h *QueryHandler) int64 {
	s := h.Pruner().GetGlobCacheStats()
	return s["cache_hits"].(int64) + s["cache_misses"].(int64)
}

func TestRollupServedAutoDoesNotListSource_LazyFallback(t *testing.T) {
	for _, path := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
		t.Run(path, func(t *testing.T) {
			e := newQR6RemoteEnv(t)
			e.h.SetRollupRouter(&fakeRollupRouter{serveSQL: "SELECT 42 AS n"})
			status, body, _ := e.post(path, qr6SQL, nil)
			if status != fiber.StatusOK {
				t.Fatalf("status %d: %s", status, body)
			}
			if n := e.store.lists.Load(); n != 0 {
				t.Fatalf("a cube-served AUTO query listed source storage %d times; the source fallback must be built only when the cube read fails", n)
			}
			// The counter sees the source transform's listings.
			if _, _, err := e.h.getTransformedSQL(context.Background(), qr6SQL, ""); err != nil {
				t.Fatal(err)
			}
			if e.store.lists.Load() == 0 {
				t.Fatal("source transform made no listing; the counter is not wired")
			}
		})
	}
}

// postCount runs a single-value "n" query on the JSON or Arrow endpoint.
func postCount(e *forkQueryEnv, path, sql string) (int, float64, map[string]string, string) {
	if path == "/api/v1/query" {
		status, out, hdr := e.post(path, sql, nil)
		if status != fiber.StatusOK || len(out.Data) != 1 {
			return status, -1, hdr, out.raw
		}
		return status, out.Data[0][0].(float64), hdr, out.raw
	}
	status, cols, hdr, raw := (&qr3Env{forkQueryEnv: e}).postArrow(sql)
	if status != fiber.StatusOK || len(cols["n"]) != 1 {
		return status, -1, hdr, raw
	}
	return status, cols["n"][0].(float64), hdr, raw
}

func TestRollupCubeFailureBuildsSourceFallbackThen_LazyFallback(t *testing.T) {
	cases := map[string]string{
		"missing cube file": "SELECT count(*) AS n FROM read_parquet('%s/_arc/rollup/default/events/by_site/2026-06-01.parquet')",
		"schema drift":      `SELECT count(*) AS n FROM (SELECT 1 AS x) t WHERE "site" IS NOT NULL -- %s`,
	}
	const sql = "SELECT count(*) AS n FROM events WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z'"
	for name, tmpl := range cases {
		for _, path := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
			t.Run(name+path, func(t *testing.T) {
				e := newForkQueryEnv(t)
				e.writeParquet("default/events/2026/06/01/00/a.parquet",
					"SELECT TIMESTAMPTZ '2026-06-01 00:10:00+00' AS time, 'x' AS site UNION ALL SELECT TIMESTAMPTZ '2026-06-01 00:20:00+00', 'y'")
				e.writeParquet("default/events/2026/06/01/01/b.parquet",
					"SELECT TIMESTAMPTZ '2026-06-01 01:10:00+00' AS time, 'z' AS site")
				f := &fakeRollupRouter{serveSQL: "SELECT 42 AS n"}
				e.h.SetRollupRouter(f)

				before := localProbes(e.h)
				status, n, hdr, raw := postCount(e, path, sql)
				if status != fiber.StatusOK || n != 42 || hdr["X-Arc-Rollup-Fallback"] != "" {
					t.Fatalf("healthy cube: status %d n=%v fallback=%q: %s", status, n, hdr["X-Arc-Rollup-Fallback"], raw)
				}
				if got := localProbes(e.h); got != before {
					t.Fatalf("healthy cube read probed source partitions %d times", got-before)
				}

				f.serveSQL = fmt.Sprintf(tmpl, e.dataDir)
				status, n, hdr, raw = postCount(e, path, sql)
				if status != fiber.StatusOK || n != 3 {
					t.Fatalf("failed cube: status %d n=%v, want the source count 3: %s", status, n, raw)
				}
				if hdr["X-Arc-Rollup-Fallback"] != "source" || hdr["X-Arc-Rollup-Cube"] != "fake.cube.auto" {
					t.Fatalf("failed cube: fallback=%q cube=%q, want source / fake.cube.auto", hdr["X-Arc-Rollup-Fallback"], hdr["X-Arc-Rollup-Cube"])
				}
				if localProbes(e.h) == before {
					t.Fatal("the source fallback was not built when the cube read failed")
				}
			})
		}
	}
}

// A listing failure while building the fallback at failure time is the same
// 503 the eager build gave: listing message, Retry-After, metric.
func TestRollupFallbackListingFailureAtFailureTime_LazyFallback(t *testing.T) {
	for _, path := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
		t.Run(path, func(t *testing.T) {
			e := newQR6RemoteEnv(t)
			e.h.SetRollupRouter(&fakeRollupRouter{serveSQL: fmt.Sprintf(
				"SELECT count(*) AS n FROM read_parquet('%s/default/events/2026/06/03/05/*.parquet')", e.tmp)})
			e.store.failDirs.Store(true)
			before := listingIncompleteTotal(t)
			status, body, retryAfter := e.post(path, qr6SQL, nil)
			if status != fiber.StatusServiceUnavailable || !strings.Contains(body, listingIncompleteMessage) {
				t.Fatalf("status %d, want 503 listing-incomplete: %s", status, body)
			}
			if retryAfter == "" {
				t.Fatal("503 must carry Retry-After")
			}
			if got := listingIncompleteTotal(t); got != before+1 {
				t.Fatalf("arc_query_listing_incomplete_total = %d, want %d", got, before+1)
			}
			if e.store.lists.Load() == 0 {
				t.Fatal("the fallback build never listed storage")
			}
		})
	}
}
