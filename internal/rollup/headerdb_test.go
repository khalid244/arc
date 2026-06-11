package rollup

import "testing"

// Fix for the db-blind read path: the api layer passes the x-arc-database
// header into RouteHTTP/ExplainHTTP, but the router used to discard it — an
// unqualified `FROM events` sent with headerDB=posthog was parsed, RECORDED
// and coverage-matched as default.events, so it could never route to
// posthog.events cubes and it polluted the default workload with posthog dims.

const hdrSQL = `SELECT event, count(*) FROM events ` +
	`WHERE time >= TIMESTAMPTZ '2026-06-01' AND time < TIMESTAMPTZ '2026-06-02' GROUP BY event`

// TestParseWithDB_HeaderDatabaseResolution pins the resolution precedence:
// explicit db.table in the SQL > x-arc-database header > "default".
func TestParseWithDB_HeaderDatabaseResolution(t *testing.T) {
	cases := []struct {
		name, sql, headerDB, want string
	}{
		{"unqualified resolves to headerDB", hdrSQL, "posthog", "posthog.events"},
		{"qualified wins over headerDB",
			`SELECT event, count(*) FROM posthog.events ` +
				`WHERE time >= TIMESTAMPTZ '2026-06-01' AND time < TIMESTAMPTZ '2026-06-02' GROUP BY event`,
			"otherdb", "posthog.events"},
		{"empty headerDB keeps default", hdrSQL, "", "default.events"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			q, ok, reason := ParseWithDB(c.sql, "time", c.headerDB)
			if !ok {
				t.Fatalf("parse failed: %s", reason)
			}
			if q.Source != c.want {
				t.Errorf("Source = %q, want %q", q.Source, c.want)
			}
		})
	}

	// Parse (no header) keeps its corpus behavior — existing callers unchanged.
	q, ok, reason := Parse(hdrSQL, "time")
	if !ok {
		t.Fatalf("Parse failed: %s", reason)
	}
	if q.Source != "default.events" {
		t.Errorf("Parse Source = %q, want default.events", q.Source)
	}
}

// TestRouterRouteHTTPThreadsHeaderDB proves the header reaches the recorder on
// both the plain path and the CTE-base path, and that ExplainHTTP resolves with
// the header but still never records.
func TestRouterRouteHTTPThreadsHeaderDB(t *testing.T) {
	var sources []string
	r := &Router{TimeCol: "time", OnQuery: func(q QueryShape) { sources = append(sources, q.Source) }}

	r.RouteHTTP(hdrSQL, "posthog")
	r.RouteHTTP(`WITH base AS (`+hdrSQL+`) SELECT * FROM base`, "posthog")

	if len(sources) != 2 {
		t.Fatalf("recorded %d shapes, want 2 (plain + cte base)", len(sources))
	}
	for i, src := range sources {
		if src != "posthog.events" {
			t.Errorf("recorded[%d].Source = %q, want posthog.events", i, src)
		}
	}

	r.ExplainHTTP(hdrSQL, "posthog")
	if len(sources) != 2 {
		t.Errorf("ExplainHTTP recorded the query (got %d shapes)", len(sources))
	}
}

// TestManagerRouteHTTPThreadsHeaderDB proves the Manager facade (what the api
// layer actually holds) forwards headerDB to the live router, so the workload
// learns the right source and planSpecs(posthog.events) sees the counts.
func TestManagerRouteHTTPThreadsHeaderDB(t *testing.T) {
	m := testManager(newFakeStorage())
	m.mu.Lock()
	m.rebuildRouterLocked() // wires OnQuery = m.workload.Record, as production does
	m.mu.Unlock()

	m.RouteHTTP(hdrSQL, "posthog")
	if got := m.workload.DimCounts("posthog.events")["event"]; got != 1 {
		t.Fatalf("posthog.events event count = %d, want 1 (headerDB dropped on the Manager path?)", got)
	}
	if got := m.workload.DimCounts("default.events")["event"]; got != 0 {
		t.Fatalf("default.events polluted with %d counts", got)
	}
}
