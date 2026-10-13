//go:build duckdb_arrow

package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

// P3Q-2: DuckDB answers a read_parquet whose glob matches nothing with
// "No files found" for the WHOLE statement, and the handlers answer that as an
// empty 200. That is right for a statement that reads one relation, but when
// the statement reads several (UNION ALL, LEFT JOIN, NOT IN (subquery), ...),
// one relation without files emptied the whole result: silently wrong.
//
//   - A relation whose time range every listing verified empty used to be
//     narrowed to a no-files glob (the fork's fast empty answer). In a
//     multi-relation statement it is now read like any other relation, so the
//     statement gets its real answer.
//   - A relation with no data files at all (never written) has no schema to
//     read; in a multi-relation statement it is now a 422 naming it, never an
//     empty 200.
//
// A statement that reads exactly one relation keeps the empty 200.

const (
	p3q2Day      = "time >= '2026-06-01T00:00:00Z' AND time < '2026-06-02T00:00:00Z'"
	p3q2LeftDay  = "l.time >= '2026-06-01T00:00:00Z' AND l.time < '2026-06-02T00:00:00Z'"
	p3q2RightDay = "o.time >= '2026-06-01T00:00:00Z' AND o.time < '2026-06-02T00:00:00Z'"
)

// p3q2Env: logs holds 60 rows on 2026-06-01; other exists but has no rows that
// day (its only file is in May).
func p3q2Env(t *testing.T) *forkQueryEnv {
	t.Helper()
	e := newForkQueryEnv(t)
	e.writeParquet("default/logs/2026/06/01/10/a.parquet",
		"SELECT TIMESTAMPTZ '2026-06-01 10:10:00+00' + INTERVAL (i) SECOND AS time, i AS v FROM range(60) t(i)")
	e.writeParquet("default/other/2026/05/01/10/a.parquet",
		"SELECT TIMESTAMPTZ '2026-05-01 10:10:00+00' AS time, 1 AS v")
	return e
}

// multiRelationQueries reads `logs` (60 rows that day) together with `rel`.
// Each answer is 60 when rel has no rows that day.
func multiRelationQueries(rel string) map[string]string {
	return map[string]string{
		"union all": "SELECT count(*) AS n FROM (SELECT time FROM logs WHERE " + p3q2Day +
			" UNION ALL SELECT time FROM " + rel + " WHERE " + p3q2Day + ") u",
		"left join": "SELECT count(*) AS n FROM logs l LEFT JOIN " + rel + " o ON l.time = o.time AND " + p3q2RightDay +
			" WHERE " + p3q2LeftDay,
		"not in subquery": "SELECT count(*) AS n FROM logs WHERE " + p3q2Day +
			" AND v NOT IN (SELECT v FROM " + rel + " WHERE " + p3q2Day + ")",
	}
}

func TestMultiRelationStatementWithVerifiedEmptyRelation_P3Q2(t *testing.T) {
	e := p3q2Env(t)
	a := &qr3Env{forkQueryEnv: e}
	for name, q := range multiRelationQueries("other") {
		t.Run(name, func(t *testing.T) {
			if n := jsonCount(t, e, q, nil); n != 60 {
				t.Fatalf("JSON: n = %v, want 60", n)
			}
			if n := jsonCount(t, e, q, map[string]string{"x-arc-database": "default"}); n != 60 {
				t.Fatalf("JSON with x-arc-database: n = %v, want 60", n)
			}
			status, cols, _, raw := a.postArrow(q)
			if status != fiber.StatusOK || len(cols["n"]) != 1 || cols["n"][0].(float64) != 60 {
				t.Fatalf("Arrow IPC: status %d n=%v, want 60: %s", status, cols["n"], raw)
			}
			status, out, _ := e.post("/api/v1/query/msgpack", q, nil)
			if status != fiber.StatusOK || strings.Contains(out.raw, "No files found") {
				t.Fatalf("msgpack: status %d: %q", status, out.raw)
			}
			transformed, _, err := e.h.getTransformedSQL(context.Background(), q, "")
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(transformed, "_arc_verified_empty_") {
				t.Fatalf("a multi-relation statement must not read a no-files glob:\n%s", transformed)
			}
		})
	}

	// /query/estimate counts the rows of the statement itself.
	q := "SELECT time FROM logs WHERE " + p3q2Day + " UNION ALL SELECT time FROM other WHERE " + p3q2Day
	status, out, _ := e.post("/api/v1/query/estimate", q, nil)
	var est EstimateResponse
	_ = json.Unmarshal([]byte(out.raw), &est)
	if status != fiber.StatusOK || !est.Success || est.EstimatedRows == nil || *est.EstimatedRows != 60 {
		t.Fatalf("estimate: status %d: %s, want 60 rows", status, out.raw)
	}
}

// One relation, verified empty: the fast no-files answer and its empty 200
// are unchanged.
func TestSingleRelationVerifiedEmptyStaysEmpty_P3Q2(t *testing.T) {
	e := p3q2Env(t)
	q := "SELECT time, v FROM other WHERE " + p3q2Day
	transformed, _, err := e.h.getTransformedSQL(context.Background(), q, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(transformed, "_arc_verified_empty_") {
		t.Fatalf("a single relation keeps the verified-empty shortcut:\n%s", transformed)
	}
	status, out, _ := e.post("/api/v1/query", q, nil)
	if status != fiber.StatusOK || len(out.Data) != 0 {
		t.Fatalf("single relation, verified empty: status %d, want an empty 200: %s", status, out.raw)
	}
}

func TestMultiRelationStatementWithNeverWrittenRelation_P3Q2(t *testing.T) {
	e := p3q2Env(t)
	a := &qr3Env{forkQueryEnv: e}
	queries := multiRelationQueries("nothere")
	// No time range on the missing relation: its read is the whole-table glob.
	queries["no time range"] = "SELECT count(*) AS n FROM (SELECT time FROM logs WHERE " + p3q2Day +
		" UNION ALL SELECT time FROM nothere) u"
	for name, q := range queries {
		t.Run(name, func(t *testing.T) {
			for _, path := range []string{"/api/v1/query", "/api/v1/query/msgpack"} {
				status, out, _ := e.post(path, q, nil)
				if status != fiber.StatusUnprocessableEntity {
					t.Fatalf("%s: status %d, want 422 (an empty 200 drops the logs rows): %q", path, status, out.raw)
				}
				if !strings.Contains(out.raw, "default.nothere") {
					t.Fatalf("%s: the error must name the measurement: %q", path, out.raw)
				}
			}
			status, _, _, raw := a.postArrow(q)
			if status != fiber.StatusUnprocessableEntity || !strings.Contains(raw, "default.nothere") {
				t.Fatalf("Arrow IPC: status %d, want 422 naming default.nothere: %s", status, raw)
			}
		})
	}

	// One relation, never written: still the empty 200 (a client polling a
	// measurement before its first write).
	for _, q := range []string{"SELECT count(*) AS n FROM nothere", "SELECT * FROM nothere WHERE " + p3q2Day} {
		status, out, _ := e.post("/api/v1/query", q, nil)
		if status != fiber.StatusOK || len(out.Data) != 0 {
			t.Fatalf("%s: status %d, want an empty 200: %s", q, status, out.raw)
		}
		status, _, _, raw := a.postArrow(q)
		if status != fiber.StatusOK {
			t.Fatalf("Arrow IPC %s: status %d, want an empty 200: %s", q, status, raw)
		}
	}
}
