//go:build duckdb_arrow

package api

import (
	"bytes"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/rollup"
)

// qr3Env is the reviewer's QR-3 case built for real: two sealed days whose
// source carries the cube dimension "site" (1,440 rows each, 2,880 in the
// cube), and a fresh UTC day whose 600 rows do NOT carry "site" yet. The
// rollup router is a real rollup.Router over real cube files built by
// rollup.BuildDay, with its fresh-tail source window existence-pruned through
// the query handler's own PartitionPruner, the way main.go wires production.
type qr3Env struct {
	*forkQueryEnv
	router *rollup.Router
}

func newQR3Env(t *testing.T) *qr3Env {
	t.Helper()
	e := newForkQueryEnv(t)

	// Source: one file per hour partition, one row per minute.
	for _, day := range []string{"2026-06-01", "2026-06-02"} {
		for hour := 0; hour < 24; hour++ {
			rel := fmt.Sprintf("default/events/%s/%02d/part.parquet", strings.ReplaceAll(day, "-", "/"), hour)
			e.writeParquet(rel, fmt.Sprintf(
				`SELECT TIMESTAMPTZ '%s %02d:00:00+00' + INTERVAL (i) MINUTE AS "time",
				        CASE WHEN i %% 2 = 0 THEN 'web' ELSE 'ios' END AS site
				 FROM range(60) t(i)`, day, hour))
		}
	}
	// Fresh tail: today, 10 hours, no "site" column in any file.
	for hour := 0; hour < 10; hour++ {
		e.writeParquet(fmt.Sprintf("default/events/2026/06/03/%02d/part.parquet", hour), fmt.Sprintf(
			`SELECT TIMESTAMPTZ '2026-06-03 %02d:00:00+00' + INTERVAL (i) MINUTE AS "time"
			 FROM range(60) t(i)`, hour))
	}

	// Build the cubes over the sealed days with a separate, unsandboxed DuckDB:
	// one on "site", and one on "plan", a dimension NO source file carries
	// (the build writes it as a typed NULL).
	builder, err := gosql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	var manifests []*rollup.Manifest
	for _, dim := range []string{"site", "plan"} {
		spec := rollup.CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{dim},
			Aggs: []rollup.Aggregate{{Kind: rollup.AggCount}}}
		cubeDir := filepath.Join(e.dataDir, "_arc", "rollup", "events_"+dim)
		if err := os.MkdirAll(cubeDir, 0o755); err != nil {
			t.Fatal(err)
		}
		m := &rollup.Manifest{CubeID: "events_" + dim, Source: spec.Source, Grain: spec.Grain,
			Dims: spec.Dims, Aggs: spec.Aggs, SchemaHash: spec.SchemaHash()}
		for _, day := range []string{"2026-06-01", "2026-06-02"} {
			src := "'" + filepath.Join(e.dataDir, "default", "events", strings.ReplaceAll(day, "-", "/"), "**", "*.parquet") + "'"
			entry, err := rollup.BuildDay(builder, spec, src, "time", day, filepath.Join(cubeDir, day+".parquet"))
			if err != nil {
				t.Fatalf("build cube %s day %s: %v", dim, day, err)
			}
			m.Upsert(entry)
		}
		manifests = append(manifests, m)
	}

	whole := filepath.Join(e.dataDir, "default", "events", "**", "*.parquet")
	r := rollup.NewRouter(manifests, "time",
		func(string) string { return "'" + whole + "'" },
		// The seal clock is already well into today; the router clamps it to
		// the cube's real coverage (2026-06-03 00:00), so today is the tail.
		func(string) string { return "2026-06-03 12:00:00" })
	pruner := e.h.Pruner()
	r.SourceWindow = func(source, lo, hi string) string {
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
	return &qr3Env{forkQueryEnv: e, router: r}
}

// postArrow posts to the Arrow IPC endpoint and returns the status, the decoded
// rows (column name -> values, all batches) and the response headers.
func (e *qr3Env) postArrow(sql string) (int, map[string][]any, map[string]string, string) {
	e.t.Helper()
	body, _ := json.Marshal(QueryRequest{SQL: sql})
	req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.app.Test(req, 30000)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	hdr := map[string]string{}
	for _, k := range []string{"X-Arc-Rollup-Fallback", "X-Arc-Rollup-Cube", "X-Arc-Drift-Fill"} {
		hdr[k] = resp.Header.Get(k)
	}
	if resp.StatusCode != fiber.StatusOK {
		return resp.StatusCode, nil, hdr, string(raw)
	}
	rdr, err := ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		e.t.Fatalf("arrow decode: %v", err)
	}
	defer rdr.Release()
	cols := map[string][]any{}
	for rdr.Next() {
		rec := rdr.Record()
		for i, f := range rec.Schema().Fields() {
			col := rec.Column(i)
			for row := 0; row < col.Len(); row++ {
				if col.IsNull(row) {
					cols[f.Name] = append(cols[f.Name], nil)
					continue
				}
				switch a := col.(type) {
				case *array.Int64:
					cols[f.Name] = append(cols[f.Name], float64(a.Value(row)))
				case *array.Uint64:
					cols[f.Name] = append(cols[f.Name], float64(a.Value(row)))
				case *array.Int32:
					cols[f.Name] = append(cols[f.Name], float64(a.Value(row)))
				case *array.Float64:
					cols[f.Name] = append(cols[f.Name], a.Value(row))
				case *array.String:
					cols[f.Name] = append(cols[f.Name], a.Value(row))
				default:
					cols[f.Name] = append(cols[f.Name], a.ValueStr(row))
				}
			}
		}
	}
	if err := rdr.Err(); err != nil {
		e.t.Fatalf("arrow stream: %v", err)
	}
	return resp.StatusCode, cols, hdr, string(raw)
}

func sumNumbers(vals []any) float64 {
	var s float64
	for _, v := range vals {
		if f, ok := v.(float64); ok {
			s += f
		}
	}
	return s
}

// TestRollupSchemaDriftRealCubeKeepsFreshTail_QR3 is the reviewer's case end
// to end: the AUTO merge's fresh-tail branch reads only today's files, which
// lack the cube dimension, so DuckDB fails to bind. The cube-only answer the
// fork used to retry with is 2,880 (sealed days only); the complete answer,
// read from source with the absent column as NULL, is 3,480. Both the JSON and
// the Arrow IPC endpoint must return 3,480.
func TestRollupSchemaDriftRealCubeKeepsFreshTail_QR3(t *testing.T) {
	e := newQR3Env(t)
	const lo, hi = "2026-06-01T00:00:00Z", "2026-06-04T00:00:00Z"
	where := fmt.Sprintf("WHERE time >= '%s' AND time < '%s'", lo, hi)

	// Precondition: the router really serves these from the cube through the
	// merge, and that merge really hits the drift.
	total := "SELECT count(*) AS n FROM events " + where
	bySite := "SELECT site, count(*) AS n FROM events " + where + " GROUP BY site ORDER BY site"
	for _, q := range []string{total, bySite} {
		rewritten, served, _ := e.router.RouteHTTP(q, "")
		if !served {
			t.Fatalf("precondition: the router must serve %q from the cube", q)
		}
		if !strings.Contains(rewritten, "UNION ALL BY NAME") {
			t.Fatalf("precondition: expected a merge-on-read with a fresh tail, got %s", rewritten)
		}
		if _, err := e.h.db.Query(rewritten); err == nil || !strings.Contains(err.Error(), `"site" not found`) {
			t.Fatalf("precondition: the merge SQL must hit schema drift on site, got err=%v\nSQL: %s", err, rewritten)
		}
	}
	// What a cube-only answer would be: the silent undercount QR-3 is about.
	cubeOnly, served, _ := e.router.RouteOnlyHTTP(total, "")
	if !served {
		t.Fatal("precondition: RouteOnly must serve")
	}
	var cubeN int64
	if err := e.h.db.DB().QueryRow(cubeOnly).Scan(&cubeN); err != nil || cubeN != 2880 {
		t.Fatalf("precondition: cube-only count = %d (err %v), want 2880", cubeN, err)
	}

	// JSON endpoint.
	status, out, hdr := e.post("/api/v1/query", total, nil)
	if status != fiber.StatusOK {
		t.Fatalf("JSON total: status %d: %s", status, out.raw)
	}
	if len(out.Data) != 1 || out.Data[0][0] != float64(3480) {
		t.Fatalf("JSON total: want 3480 (2880 sealed + 600 fresh), got %s", out.raw)
	}
	if hdr["X-Arc-Rollup-Fallback"] != "source" {
		t.Fatalf("JSON total: X-Arc-Rollup-Fallback = %q, want source", hdr["X-Arc-Rollup-Fallback"])
	}
	status, out, _ = e.post("/api/v1/query", bySite, nil)
	if status != fiber.StatusOK {
		t.Fatalf("JSON by site: status %d: %s", status, out.raw)
	}
	got := map[string]float64{}
	var sum float64
	for _, row := range out.Data {
		key := "<null>"
		if s, ok := row[0].(string); ok {
			key = s
		}
		got[key] = row[1].(float64)
		sum += row[1].(float64)
	}
	if sum != 3480 || got["<null>"] != 600 || got["ios"] != 1440 || got["web"] != 1440 {
		t.Fatalf("JSON by site: want ios=1440 web=1440 NULL=600, got %v (%s)", got, out.raw)
	}

	// Arrow IPC endpoint.
	status, cols, ahdr, raw := e.postArrow(total)
	if status != fiber.StatusOK {
		t.Fatalf("Arrow total: status %d: %s", status, raw)
	}
	if n := sumNumbers(cols["n"]); n != 3480 || len(cols["n"]) != 1 {
		t.Fatalf("Arrow total: want one row of 3480, got %v", cols["n"])
	}
	if ahdr["X-Arc-Rollup-Fallback"] != "source" {
		t.Fatalf("Arrow total: X-Arc-Rollup-Fallback = %q, want source", ahdr["X-Arc-Rollup-Fallback"])
	}
	status, cols, _, raw = e.postArrow(bySite)
	if status != fiber.StatusOK {
		t.Fatalf("Arrow by site: status %d: %s", status, raw)
	}
	if n := sumNumbers(cols["n"]); math.Abs(n-3480) > 0 {
		t.Fatalf("Arrow by site: counts sum to %v, want 3480 (%v)", n, cols)
	}
	nulls := 0
	for i, s := range cols["site"] {
		if s == nil {
			nulls++
			if cols["n"][i] != float64(600) {
				t.Fatalf("Arrow by site: NULL group = %v, want 600", cols["n"][i])
			}
		}
	}
	if nulls != 1 {
		t.Fatalf("Arrow by site: want exactly one NULL-site group, got %v", cols)
	}
}

// TestRollupSchemaDriftSourceAlsoMissingColumn_QR3: the cube dimension is
// absent from EVERY source file in the window, so the source fallback binds
// no better than the merge did. It must then be NULL-filled (the source drift
// fill), still complete: 3,480 rows in one NULL group, on both endpoints.
func TestRollupSchemaDriftSourceAlsoMissingColumn_QR3(t *testing.T) {
	e := newQR3Env(t)
	q := "SELECT plan, count(*) AS n FROM events WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-04T00:00:00Z' GROUP BY plan"
	if _, served, cube := e.router.RouteHTTP(q, ""); !served || cube != "events_plan" {
		t.Fatalf("precondition: the plan cube must serve the query (served=%v cube=%q)", served, cube)
	}

	status, out, hdr := e.post("/api/v1/query", q, nil)
	if status != fiber.StatusOK {
		t.Fatalf("JSON: status %d: %s", status, out.raw)
	}
	if len(out.Data) != 1 || out.Data[0][0] != nil || out.Data[0][1] != float64(3480) {
		t.Fatalf("JSON: want one NULL-plan group of 3480, got %s", out.raw)
	}
	if hdr["X-Arc-Rollup-Fallback"] != "source" || hdr["X-Arc-Drift-Fill"] != "plan" {
		t.Fatalf("JSON: fallback=%q drift-fill=%q, want source / plan", hdr["X-Arc-Rollup-Fallback"], hdr["X-Arc-Drift-Fill"])
	}

	status, cols, ahdr, raw := e.postArrow(q)
	if status != fiber.StatusOK {
		t.Fatalf("Arrow: status %d: %s", status, raw)
	}
	if len(cols["n"]) != 1 || cols["n"][0] != float64(3480) || cols["plan"][0] != nil {
		t.Fatalf("Arrow: want one NULL-plan group of 3480, got %v", cols)
	}
	if ahdr["X-Arc-Rollup-Fallback"] != "source" || ahdr["X-Arc-Drift-Fill"] != "plan" {
		t.Fatalf("Arrow: fallback=%q drift-fill=%q, want source / plan", ahdr["X-Arc-Rollup-Fallback"], ahdr["X-Arc-Drift-Fill"])
	}
}
