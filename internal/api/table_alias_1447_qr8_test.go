//go:build duckdb_arrow

package api

import (
	"context"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

// The alias goes on the finished expression, after the compaction-input
// exclusion (fork QR-8: the inputs of an uploaded compaction output are hidden
// by wrapping the read in a subquery). Aliasing the bare read_parquet call
// instead would either break the wrapper or drop the exclusion, and count a
// compaction's rows twice. Fork adaptation of upstream's open-compaction test.
func TestTableAliasAfterCompactionExclusion_Issue1447(t *testing.T) {
	e := newForkQueryEnv(t)
	e.writeParquet("default/readings/2026/06/01/00/input.parquet",
		"SELECT TIMESTAMPTZ '2026-06-01 00:10:00+00' AS time, 1 AS value")
	e.writeParquet("default/readings/2026/06/01/00/output.parquet",
		"SELECT TIMESTAMPTZ '2026-06-01 00:10:00+00' AS time, 1 AS value")
	e.h.SetManifestManager(fakeManifests{uploaded: map[string]struct{}{
		"default/readings/2026/06/01/00/input.parquet": {},
	}})
	got := e.h.convertSQLToStoragePaths(withQueryExcludes(context.Background()), "SELECT readings.value FROM readings")
	if !strings.Contains(got, compactionSourceFileColumn+" NOT IN (") {
		t.Fatalf("compaction input not excluded: %s", got)
	}
	if !strings.HasSuffix(got, ")"+aliasReadings) {
		t.Fatalf("alias not on the finished expression: %s", got)
	}
	status, out, _ := e.post("/api/v1/query",
		"SELECT count(readings.value) AS n FROM readings WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z'", nil)
	if status != fiber.StatusOK || len(out.Data) != 1 {
		t.Fatalf("status %d: %s", status, out.raw)
	}
	if n, _ := out.Data[0][0].(float64); n != 1 {
		t.Fatalf("count = %v, want 1 (compaction rows counted once): %s", out.Data[0][0], out.raw)
	}
}
