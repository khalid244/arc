package tiered

import (
	"context"
	"testing"
)

// TestRouter_GrafanaTimeGroupMacro — the Arc Grafana plugin expands
// `$__timeGroup(time, $__interval)` to epoch-math bucketing
//   to_timestamp((epoch_ns(time) // 1000000000 // 86400) * 86400)
// instead of `date_trunc(...)`. The router's GROUP BY classifier only
// recognises `date_trunc`, so EVERY panel using $__timeGroup falls back
// to a source scan. This is the real cause of slow 30-day dashboards —
// not the CASE WHEN / subquery alone.
func TestRouter_GrafanaTimeGroupMacro(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/sketch/a.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"a_0", "a_1"}, EffectiveCard: 2},
		},
	}

	// Exact expansion produced by Arc Grafana plugin v1.2.2 for 1d $__interval.
	q := `SELECT to_timestamp((epoch_ns(time) // 1000000000 // 86400) * 86400) AS time,
		dim_a,
		COUNT(*) AS value
		FROM events
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-31'
		  AND dim_a IN ('a_0','a_1')
		GROUP BY 1, 2
		ORDER BY time ASC`

	_, ok := Rewrite(ctx, q, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if ok {
		t.Fatal("unexpected: router accepted epoch-bucketing — was the parser extended?")
	}
	t.Log("router refuses epoch-bucketing — confirmed Grafana plugin bug bypasses the rollup")
}

// TestRouter_GrafanaCaseWhenPlusSubquery — reproduces the dashboard panel's
// rawSql shape (CASE WHEN normalisation in GROUP BY + IN-subquery for top-N).
// Both clauses are individually router-refuse; this test pins the production
// observation: the rewrite returns ok=false, so the query falls back to a
// 30-day source scan over the downloads table.
func TestRouter_GrafanaCaseWhenPlusSubquery(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)
	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/sketch/a.parquet",
		"_arc/rollup/default/events/1h/2025/03/01/sketch/a.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"a_0", "a_1", "a_2"}, EffectiveCard: 3},
			"dim_b": {Role: "Dim", KeptValues: []string{"b_0", "b_1"}, EffectiveCard: 2},
		},
	}

	// Mirrors the Grafana panel rawSql but against the generic events fixture.
	q := `SELECT date_trunc('day', time) AS time,
		CASE
			WHEN dim_a LIKE '%.tiktok.com' THEN 'tiktok.com'
			WHEN dim_a LIKE '%.youtube.com' THEN 'youtube.com'
			ELSE dim_a
		END AS metric,
		COUNT(*) AS value
		FROM events
		WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-31'
		  AND dim_a IN (
			SELECT dim_a FROM events
			WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-31'
			GROUP BY dim_a ORDER BY COUNT(*) DESC LIMIT 10
		)
		GROUP BY 1, 2
		ORDER BY time ASC`

	_, ok := Rewrite(ctx, q, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
	if ok {
		t.Fatal("expected router to refuse CASE WHEN + IN-subquery; got accept")
	}
	t.Log("router correctly refuses; in prod this falls back to a 30-day source scan")
}

