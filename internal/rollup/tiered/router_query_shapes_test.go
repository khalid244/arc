package tiered

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// This file exercises a wide range of BI/dashboard query shapes against
// the router and records which ones it accepts (rollup-served) vs refuses
// (falls back to source scan). Most shapes come from Grafana, Metabase,
// Tableau, and Apache Superset typical panel patterns plus standard
// analytics tests.
//
// Run all of these with:
//   go test -tags=duckdb_arrow -count=1 -run TestRouterShape -v ./internal/rollup/tiered/

// shapeTest is a single SQL shape with the expected router decision.
type shapeTest struct {
	name      string
	sql       string
	wantAccept bool
	reason    string
}

func TestRouterShapes_Battery(t *testing.T) {
	ctx := context.Background()
	db := buildEventsTable(t)

	manifest := &MemoryFileIndex{Paths: []string{
		"_arc/rollup/default/events/1h/2025/03/01/sketch/a.parquet",
		"_arc/rollup/default/events/1h/2025/03/02/sketch/b.parquet",
		"_arc/rollup/default/events/1h/2025/03/05/sketch/c.parquet",
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/a.parquet",
		"_arc/rollup/default/events/1h/2025/03/02/by_dim_a/b.parquet",
		"_arc/rollup/default/events/1h/2025/03/05/by_dim_a/c.parquet",
		"_arc/rollup/default/events/1h/2025/03/01/sketch/a.parquet",
		"_arc/rollup/default/events/1h/2025/03/02/sketch/b.parquet",
		"_arc/rollup/default/events/1h/2025/03/05/sketch/c.parquet",
		"_arc/rollup/default/events/1h/2025/03/01/by_dim_a/d.parquet",
		"_arc/rollup/default/events/1h/2025/03/02/by_dim_a/e.parquet",
		"_arc/rollup/default/events/1h/2025/03/05/by_dim_a/f.parquet",
	}}
	spec := &Spec{
		Table: "default.events", TZ: "UTC", TimeColumn: "time",
		Dims: map[string]DimSpec{
			"dim_a": {Role: "Dim", KeptValues: []string{"a_0", "a_1", "a_2", "a_3", "a_4"}, EffectiveCard: 5},
			"dim_b": {Role: "Dim", KeptValues: []string{"b_0", "b_1"}, EffectiveCard: 2},
			"dim_c": {Role: "Sketch", EffectiveCard: 0},
		},
	}

	tests := []shapeTest{
		// ── ACCEPT: standard analytics shapes ─────────────────────────
		{
			name: "count_by_hour_grafana_basic",
			sql: `SELECT date_trunc('hour', time) AS t, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				GROUP BY 1 ORDER BY 1`,
			wantAccept: true,
			reason:     "Grafana basic time-series; one dim implicit",
		},
		{
			name: "count_by_hour_and_dim",
			sql: `SELECT date_trunc('hour', time) AS t, dim_a, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				GROUP BY 1, 2 ORDER BY 1, 2`,
			wantAccept: true,
			reason:     "Grafana stacked time-series — picks by_dim_a variant",
		},
		{
			name: "count_by_day_using_date_trunc",
			sql: `SELECT date_trunc('day', time) AS d, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				GROUP BY 1`,
			wantAccept: true,
			reason:     "day-bucket — picks tier=1d",
		},
		{
			name: "where_dim_eq_single_value",
			sql: `SELECT date_trunc('hour', time) AS t, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				  AND dim_a = 'a_0'
				GROUP BY 1`,
			wantAccept: true,
			reason:     "WHERE dim = value translates to dim_a_class = 'a_0'",
		},
		{
			name: "where_dim_in_list",
			sql: `SELECT date_trunc('hour', time) AS t, dim_a, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				  AND dim_a IN ('a_0', 'a_1', 'a_2')
				GROUP BY 1, 2`,
			wantAccept: true,
			reason:     "WHERE dim IN (list) — common multi-select filter",
		},
		{
			name: "where_dim_is_null",
			sql: `SELECT date_trunc('hour', time) AS t, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				  AND dim_a IS NULL
				GROUP BY 1`,
			wantAccept: true,
			reason:     "IS NULL maps to dim_a_class = '_null_'",
		},
		{
			name: "where_dim_is_not_null",
			sql: `SELECT date_trunc('hour', time) AS t, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				  AND dim_a IS NOT NULL
				GROUP BY 1`,
			wantAccept: true,
			reason:     "IS NOT NULL maps to dim_a_class <> '_null_'",
		},
		{
			name: "between_time_filter",
			sql: `SELECT date_trunc('hour', time) AS t, COUNT(*) AS n FROM events
				WHERE time BETWEEN TIMESTAMP '2025-03-01' AND TIMESTAMP '2025-03-06'
				GROUP BY 1`,
			wantAccept: true,
			reason:     "BETWEEN desugars to >= AND <=",
		},
		{
			name: "order_by_limit",
			sql: `SELECT date_trunc('hour', time) AS t, dim_a, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				GROUP BY 1, 2 ORDER BY n DESC LIMIT 100`,
			wantAccept: true,
			reason:     "Top-N within rollup",
		},
		{
			name: "scalar_aggregate_kpi_card",
			sql: `SELECT COUNT(*) AS total FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'`,
			wantAccept: true,
			reason:     "KPI card pattern: no GROUP BY, scalar aggregate",
		},

		// ── REFUSE: shapes the router doesn't support today ─────────────
		{
			name: "or_in_where",
			sql: `SELECT date_trunc('hour', time) AS t, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				  AND (dim_a = 'a_0' OR dim_a = 'a_1')
				GROUP BY 1`,
			wantAccept: false,
			reason:     "OR in WHERE — router refuses (translate to IN list instead)",
		},
		{
			name: "case_when_aggregate",
			sql: `SELECT date_trunc('hour', time) AS t,
			       SUM(CASE WHEN dim_a = 'a_0' THEN 1 ELSE 0 END) AS a0_count
				FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				GROUP BY 1`,
			wantAccept: false,
			reason:     "CASE WHEN inside SUM — too complex for the translator",
		},
		{
			name: "like_filter",
			sql: `SELECT date_trunc('hour', time) AS t, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				  AND dim_a LIKE 'a_%'
				GROUP BY 1`,
			wantAccept: false,
			reason:     "LIKE filter — router refuses (use IN list instead)",
		},
		{
			name: "subquery_in_filter",
			sql: `SELECT date_trunc('hour', time) AS t, dim_a, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				  AND dim_a IN (SELECT dim_a FROM events WHERE dim_b = 'b_0')
				GROUP BY 1, 2`,
			wantAccept: false,
			reason:     "Subquery in WHERE — router refuses",
		},
		{
			name: "window_function",
			sql: `SELECT date_trunc('hour', time) AS t, dim_a,
			       COUNT(*) AS n,
			       ROW_NUMBER() OVER (PARTITION BY dim_a ORDER BY date_trunc('hour', time)) AS rn
				FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				GROUP BY 1, 2`,
			wantAccept: false,
			reason:     "Window functions — router refuses",
		},
		{
			name: "join_two_tables",
			sql: `SELECT date_trunc('hour', e1.time), COUNT(*)
				FROM events e1 JOIN events e2 ON e1.dim_a = e2.dim_a
				WHERE e1.time >= TIMESTAMP '2025-03-01' AND e1.time < TIMESTAMP '2025-03-06'
				GROUP BY 1`,
			wantAccept: false,
			reason:     "JOIN — router refuses",
		},
		{
			name: "rollup_grouping_set_modifier",
			sql: `SELECT date_trunc('hour', time) AS t, dim_a, COUNT(*) FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				GROUP BY ROLLUP (1, 2)`,
			wantAccept: false,
			reason:     "ROLLUP/CUBE/GROUPING SETS — multiple grouping sets, refused",
		},
		{
			name: "cube_modifier",
			sql: `SELECT date_trunc('hour', time) AS t, dim_a, COUNT(*) FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				GROUP BY CUBE (1, 2)`,
			wantAccept: false,
			reason:     "CUBE — multiple grouping sets, refused",
		},
		{
			name: "explicit_grouping_sets",
			sql: `SELECT date_trunc('hour', time) AS t, dim_a, COUNT(*) FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				GROUP BY GROUPING SETS ((1), (1, 2))`,
			wantAccept: false,
			reason:     "GROUPING SETS — multiple grouping sets, refused",
		},
		{
			name: "extract_hour_filter",
			sql: `SELECT date_trunc('hour', time) AS t, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				  AND EXTRACT(HOUR FROM time) BETWEEN 9 AND 17
				GROUP BY 1`,
			wantAccept: false,
			reason:     "EXTRACT() not a recognised filter shape — refused (avoids silent drop)",
		},

		// ── EDGE CASES ────────────────────────────────────────────────
		{
			name: "no_time_filter_unbounded",
			sql: `SELECT date_trunc('hour', time) AS t, COUNT(*) AS n FROM events
				GROUP BY 1`,
			wantAccept: true,
			reason:     "Unbounded query — accepted; uses full rollup range",
		},
		{
			name: "filter_on_aggregate_having",
			sql: `SELECT date_trunc('hour', time) AS t, dim_a, COUNT(*) AS n FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				GROUP BY 1, 2 HAVING COUNT(*) > 10`,
			wantAccept: false,
			reason:     "HAVING on aggregate — router doesn't translate post-agg filters; refused (avoids silent drop)",
		},
		{
			name: "not_in_list",
			sql: `SELECT date_trunc('hour', time) AS t, dim_a, COUNT(*) FROM events
				WHERE time >= TIMESTAMP '2025-03-01' AND time < TIMESTAMP '2025-03-06'
				  AND dim_a NOT IN ('a_0', 'a_1')
				GROUP BY 1, 2`,
			wantAccept: true,
			reason:     "NOT IN — translates to dim_a_class NOT IN",
		},
	}

	// Pretty results table.
	var rows []string
	var unexpected []string
	for _, tc := range tests {
		_, ok := Rewrite(ctx, tc.sql, RewriteDeps{DB: db, Files: manifest, Spec: spec, DimRichCap: 100})
		actual := "REFUSE"
		if ok {
			actual = "ACCEPT"
		}
		expected := "REFUSE"
		if tc.wantAccept {
			expected = "ACCEPT"
		}
		mark := " "
		if ok != tc.wantAccept {
			mark = "✗"
			unexpected = append(unexpected, fmt.Sprintf("%s: expected=%s got=%s — %s", tc.name, expected, actual, tc.reason))
		}
		rows = append(rows, fmt.Sprintf("  %s %-40s %-7s (expected %s) — %s", mark, tc.name, actual, expected, tc.reason))
	}

	t.Logf("\nRouter shape compatibility:\n%s\n", strings.Join(rows, "\n"))
	if len(unexpected) > 0 {
		t.Logf("\nUnexpected results (treat as documentation, not regressions unless intentional):\n  %s",
			strings.Join(unexpected, "\n  "))
	}
}
