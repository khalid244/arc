//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/rollup"
)

// QR-8 on the source paths. A query hides the inputs of a compaction whose
// output is uploaded, but the SQL transform cache (60 s) kept the transform it
// built BEFORE the compaction flagged its output: for up to a minute after the
// flag, a raw query, and the source fallback of a rollup-served query (built
// through the same cache), read the inputs AND the output and counted every
// row of that partition twice.

// countingManifests is switchableManifests that counts the exclusion reads.
type countingManifests struct {
	switchableManifests
	reads atomic.Int64
}

func (c *countingManifests) GetInputFilesForUploadedOutputs(ctx context.Context) (map[string]struct{}, error) {
	c.reads.Add(1)
	return c.switchableManifests.GetInputFilesForUploadedOutputs(ctx)
}

// queryResult is one response of either query endpoint, decoded to columns.
type queryResult struct {
	status int
	cols   map[string][]any
	header http.Header
	raw    string
}

// postEndpoint posts sql with headers to endpoint and decodes a JSON or Arrow
// IPC result to columns (numbers as float64; a msgpack body is left raw).
func postEndpoint(t *testing.T, e *forkQueryEnv, endpoint, sql string, headers map[string]string) queryResult {
	t.Helper()
	res, err := postEndpointErr(e, endpoint, sql, headers)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// postEndpointErr is postEndpoint for goroutines other than the test's.
func postEndpointErr(e *forkQueryEnv, endpoint, sql string, headers map[string]string) (queryResult, error) {
	body, _ := json.Marshal(QueryRequest{SQL: sql})
	req := httptest.NewRequest("POST", endpoint, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := e.app.Test(req, 60000)
	if err != nil {
		return queryResult{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	res := queryResult{status: resp.StatusCode, cols: map[string][]any{}, header: resp.Header, raw: string(raw)}
	if resp.StatusCode != fiber.StatusOK {
		return res, nil
	}
	if strings.HasSuffix(endpoint, "/msgpack") {
		return res, nil
	}
	if !strings.HasSuffix(endpoint, "/arrow") {
		var out http200Body
		if err := json.Unmarshal(raw, &out); err != nil {
			return res, fmt.Errorf("decode JSON: %v: %s", err, raw)
		}
		for _, row := range out.Data {
			for i, c := range out.Columns {
				res.cols[c] = append(res.cols[c], row[i])
			}
		}
		return res, nil
	}
	rdr, err := ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		return res, fmt.Errorf("arrow decode: %v", err)
	}
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		for i, f := range rec.Schema().Fields() {
			col := rec.Column(i)
			for row := 0; row < col.Len(); row++ {
				switch a := col.(type) {
				case *array.Int64:
					res.cols[f.Name] = append(res.cols[f.Name], float64(a.Value(row)))
				case *array.Int32:
					res.cols[f.Name] = append(res.cols[f.Name], float64(a.Value(row)))
				case *array.Float64:
					res.cols[f.Name] = append(res.cols[f.Name], a.Value(row))
				default:
					res.cols[f.Name] = append(res.cols[f.Name], a.ValueStr(row))
				}
			}
		}
	}
	if err := rdr.Err(); err != nil {
		return res, fmt.Errorf("arrow stream: %v", err)
	}
	return res, nil
}

// countOf posts sql and returns its single value of column n.
func countOf(t *testing.T, e *forkQueryEnv, endpoint, sql string, headers map[string]string) (float64, http.Header) {
	t.Helper()
	res := postEndpoint(t, e, endpoint, sql, headers)
	if res.status != fiber.StatusOK || len(res.cols["n"]) != 1 {
		t.Fatalf("%s: status %d: %s", endpoint, res.status, res.raw)
	}
	return res.cols["n"][0].(float64), res.header
}

// writeInputsThenCompact writes two input files (60 rows each) into the hour
// partition day/hour of meas; compact() then lands the compacted output (the
// same 120 rows) beside them and flags the manifest uploaded: the upload ->
// delete window.
func writeInputsThenCompact(e *forkQueryEnv, meas, day, hour string, mm *countingManifests) (compact func()) {
	rows := func(minute string) string {
		return fmt.Sprintf("SELECT TIMESTAMPTZ '%s %s:%s:00+00' + INTERVAL (i) SECOND AS time, i AS v FROM range(60) t(i)", day, hour, minute)
	}
	rel := "default/" + meas + "/" + strings.ReplaceAll(day, "-", "/") + "/" + hour + "/"
	e.writeParquet(rel+"in1.parquet", rows("10"))
	e.writeParquet(rel+"in2.parquet", rows("20"))
	return func() {
		e.writeParquet(rel+"out.parquet", "SELECT * FROM ("+rows("10")+" UNION ALL "+rows("20")+")")
		mm.uploaded = map[string]struct{}{rel + "in1.parquet": {}, rel + "in2.parquet": {}}
	}
}

var queryEndpoints = []string{"/api/v1/query", "/api/v1/query/arrow"}

func TestCachedTransformHidesLaterCompaction_QR8(t *testing.T) {
	const q = "SELECT count(*) AS n FROM default.logs WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z'"
	for _, endpoint := range queryEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			e := newForkQueryEnv(t)
			mm := &countingManifests{switchableManifests: switchableManifests{uploaded: map[string]struct{}{}}}
			e.h.SetManifestManager(mm)
			compact := writeInputsThenCompact(e, "logs", "2026-06-01", "00", mm)
			if n, _ := countOf(t, e, endpoint, q, nil); n != 120 {
				t.Fatalf("before the compaction: n = %v, want 120", n)
			}
			compact()
			if n, _ := countOf(t, e, endpoint, q, nil); n != 120 {
				t.Fatalf("after the compaction flagged its output: n = %v, want 120 (240 = a transform cached before the flag read inputs and output)", n)
			}
		})
	}
}

// The source fallback of a rollup-served query is built through the same
// transform cache: a cube read that fails recoverably (here a stale cube
// pointer) must not be answered from a transform cached before the compaction.
func TestRollupSourceFallbackHidesLaterCompaction_QR8(t *testing.T) {
	const q = "SELECT count(*) AS n FROM events WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-04T00:00:00Z'"
	for _, endpoint := range queryEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			env := newTailEnv(t)
			mm := &countingManifests{switchableManifests: switchableManifests{uploaded: map[string]struct{}{}}}
			env.h.SetManifestManager(mm)
			compact := writeInputsThenCompact(env.forkQueryEnv, "events", "2026-06-03", "11", mm)
			const want = 3480 + 120

			// A raw run caches the source transform of q.
			if n, _ := countOf(t, env.forkQueryEnv, endpoint, q, map[string]string{"X-Arc-No-Rollup": "true"}); n != want {
				t.Fatalf("raw before the compaction: n = %v, want %d", n, want)
			}
			compact()
			if err := os.Remove(env.cubeFile); err != nil {
				t.Fatal(err)
			}
			n, hdr := countOf(t, env.forkQueryEnv, endpoint, q, nil)
			if hdr.Get("X-Arc-Rollup-Fallback") != "source" {
				t.Fatalf("precondition: the stale cube must take the source fallback, headers %v", hdr)
			}
			if n != want {
				t.Fatalf("source fallback after the compaction: n = %v, want %d (each row once)", n, want)
			}
		})
	}
}

// The exclusion is read once per query, however many times the transform asks
// for it (the parallel-partition check and the standard build both do).
func TestCompactionExclusionReadOncePerQuery_QR8(t *testing.T) {
	e := newForkQueryEnv(t)
	mm := &countingManifests{switchableManifests: switchableManifests{uploaded: map[string]struct{}{}}}
	e.h.SetManifestManager(mm)
	writeInputsThenCompact(e, "logs", "2026-06-01", "00", mm)()
	for _, r := range []struct {
		name, sql string
		headers   map[string]string
	}{
		{"parallel transform", "SELECT count(*) AS n FROM logs", map[string]string{"x-arc-database": "default"}},
		{"standard transform", "SELECT count(*) AS n FROM default.logs WHERE time >= '2026-06-01T00:00:00Z'", nil},
	} {
		t.Run(r.name, func(t *testing.T) {
			before := mm.reads.Load()
			if n := jsonCount(t, e, r.sql, r.headers); n != 120 {
				t.Fatalf("n = %v, want 120", n)
			}
			if got := mm.reads.Load() - before; got != 1 {
				t.Fatalf("exclusion read %d times for one query, want 1", got)
			}
		})
	}
}

// tailEnv is a real rollup router over a cube for 2026-06-01 (1,440 rows) with
// a source tail of 2026-06-02 (1,440 rows) and 2026-06-03 00-09h (600 rows),
// its tail windows existence-pruned through the handler's own pruner.
type tailEnv struct {
	*forkQueryEnv
	router   *rollup.Router
	cubeFile string
}

func newTailEnv(t *testing.T) *tailEnv {
	t.Helper()
	e := newForkQueryEnv(t)
	var hours []string
	for h := 0; h < 24; h++ {
		hours = append(hours, fmt.Sprintf("%02d", h))
	}
	writeHours(e, "events", "2026-06-01", hours, 60)
	writeHours(e, "events", "2026-06-02", hours, 60)
	writeHours(e, "events", "2026-06-03", hours[:10], 60)

	spec := rollup.CubeSpec{Source: "default.events", Grain: "hour", Aggs: []rollup.Aggregate{{Kind: rollup.AggCount}}}
	cubeDir := filepath.Join(e.dataDir, "_arc", "rollup", "events_coarse")
	if err := os.MkdirAll(cubeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	builder, err := gosql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	cubeFile := filepath.Join(cubeDir, "2026-06-01.parquet")
	entry, err := rollup.BuildDay(builder, spec, "'"+filepath.Join(e.dataDir, "default/events/2026/06/01/**/*.parquet")+"'",
		"time", "2026-06-01", cubeFile)
	if err != nil {
		t.Fatal(err)
	}
	m := &rollup.Manifest{CubeID: "events_coarse", Source: spec.Source, Grain: spec.Grain, Aggs: spec.Aggs, SchemaHash: spec.SchemaHash()}
	m.Upsert(entry)
	whole := filepath.Join(e.dataDir, "default", "events", "**", "*.parquet")
	r := rollup.NewRouter([]*rollup.Manifest{m}, "time",
		func(string) string { return "'" + whole + "'" },
		func(string) string { return "2026-06-03 12:00:00" })
	pruner := e.h.Pruner()
	r.SourceWindow = func(_, lo, hi string) string {
		loT, err1 := time.Parse("2006-01-02 15:04:05.999999999-07", lo)
		hiT, err2 := time.Parse("2006-01-02 15:04:05.999999999-07", hi)
		if err1 != nil || err2 != nil {
			return "'" + whole + "'"
		}
		paths, ok := pruner.ExistingPartitionPaths(whole, loT, hiT)
		if !ok {
			return "'" + whole + "'"
		}
		if len(paths) == 0 {
			return ""
		}
		return "['" + strings.Join(paths, "', '") + "']"
	}
	e.h.SetRollupRouter(r)
	return &tailEnv{forkQueryEnv: e, router: r, cubeFile: cubeFile}
}

// Unreadable manifests do not take a cached transform away: it is looked up
// under the last exclusion read, as it was before the key carried one (a
// cache miss still fails as an incomplete listing, P3Q-1).
func TestCachedTransformSurvivesUnreadableManifests_QR8(t *testing.T) {
	const q = "SELECT count(*) AS n FROM default.logs WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z'"
	e := newForkQueryEnv(t)
	mm := &countingManifests{switchableManifests: switchableManifests{uploaded: map[string]struct{}{}}}
	e.h.SetManifestManager(mm)
	writeInputsThenCompact(e, "logs", "2026-06-01", "00", mm)()
	if n := jsonCount(t, e, q, nil); n != 120 {
		t.Fatalf("n = %v, want 120", n)
	}
	mm.fail.Store(true)
	if n := jsonCount(t, e, q, nil); n != 120 {
		t.Fatalf("cached transform with unreadable manifests: n = %v, want 120", n)
	}
	if status, out, _ := e.post("/api/v1/query", q+" AND v >= 0", nil); status != fiber.StatusServiceUnavailable {
		t.Fatalf("an uncached query with unreadable manifests: status %d, want 503: %s", status, out.raw)
	}
}
