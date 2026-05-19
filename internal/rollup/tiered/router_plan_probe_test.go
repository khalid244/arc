package tiered

import (
	"fmt"
	"strings"
	"testing"
)

// TestProbePlanJSON_ROLLUP — dumps the DuckDB plan JSON for a ROLLUP and an
// EXTRACT-filter query so we can identify which JSON field distinguishes
// each from a vanilla GROUP BY / simple WHERE. Used to design the router
// refusal predicates.
func TestProbePlanJSON_ROLLUP(t *testing.T) {
	db := buildEventsTable(t)
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"vanilla_group_by", `SELECT date_trunc('hour', time), COUNT(*) FROM events GROUP BY 1`},
		{"rollup_group_by", `SELECT date_trunc('hour', time), dim_a, COUNT(*) FROM events GROUP BY ROLLUP (1, 2)`},
		{"cube_group_by", `SELECT date_trunc('hour', time), dim_a, COUNT(*) FROM events GROUP BY CUBE (1, 2)`},
		{"grouping_sets", `SELECT date_trunc('hour', time), dim_a, COUNT(*) FROM events GROUP BY GROUPING SETS ((1), (1, 2))`},
		{"vanilla_filter", `SELECT COUNT(*) FROM events WHERE dim_a = 'a_0'`},
		{"extract_filter", `SELECT COUNT(*) FROM events WHERE EXTRACT(HOUR FROM time) = 9`},
	} {
		var planJSON string
		stmt := fmt.Sprintf("SELECT json_serialize_plan('%s')::VARCHAR", strings.ReplaceAll(tc.sql, "'", "''"))
		if err := db.QueryRow(stmt).Scan(&planJSON); err != nil {
			t.Logf("%-20s ERROR: %v", tc.name, err)
			continue
		}
		// Pull the value following "grouping_sets":
		idx := strings.Index(planJSON, "\"grouping_sets\":")
		if idx >= 0 {
			start := idx + len("\"grouping_sets\":")
			end := start
			depth := 0
			for end < len(planJSON) {
				c := planJSON[end]
				if c == '[' { depth++ }
				if c == ']' { depth--; if depth == 0 { end++; break } }
				end++
			}
			t.Logf("%-22s grouping_sets=%s", tc.name, planJSON[start:end])
		}
	}
}
