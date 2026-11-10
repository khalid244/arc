package api

import "testing"

// The SQL shapes the Grafana Arc datasource plugin really sends, after its
// macros are expanded ($__timeFilter, $__timeGroup, the appended ORDER BY time
// ASC) and after Grafana substitutes template variables. The validator is
// tightened by upstream picks from time to time; none of them may refuse
// dashboard SQL, so every shape here must pass ValidateSQLRequest.
func TestValidateSQLRequest_AcceptsGrafanaDatasourceSQL(t *testing.T) {
	const tf = "time >= '2026-11-10T00:00:00Z' AND time < '2026-11-11T00:00:00Z'"
	const bucket = "to_timestamp((epoch_ns(time) // 1000000000 // 300) * 300)"
	cases := []struct{ name, sql string }{
		{"connection test", "SELECT 1"},
		{"table browser", "SHOW TABLES"},
		{"database variable", "SHOW DATABASES"},
		{"column browser", "DESCRIBE cpu"},
		{"column browser, qualified", "DESCRIBE telegraf.cpu"},
		{"default panel query", "SELECT * FROM cpu WHERE " + tf + " LIMIT 100"},
		{"time filter with ORDER BY time ASC appended", "SELECT time, host, usage FROM cpu WHERE " + tf + " ORDER BY time ASC"},
		{"ORDER BY time ASC inserted before LIMIT", "SELECT time, host, usage FROM cpu WHERE " + tf + " ORDER BY time ASC LIMIT 1000"},
		{"time group macro", "SELECT " + bucket + " AS time, host, AVG(usage) AS usage FROM cpu WHERE " + tf + " GROUP BY 1, host ORDER BY 1"},
		{"time group macro with several aggregates", "SELECT " + bucket + " AS time, min(usage) AS lo, max(usage) AS hi, count(*) AS n FROM cpu WHERE " + tf + " GROUP BY 1 ORDER BY time ASC"},
		{"qualified table", "SELECT " + bucket + " AS time, AVG(value) FROM telegraf.cpu WHERE " + tf + " GROUP BY 1 ORDER BY 1"},
		{"template variable list", "SELECT time, usage FROM cpu WHERE " + tf + " AND host IN ('web-1', 'web-2', 'db-1') ORDER BY time ASC"},
		{"template variable regex", "SELECT time, usage FROM cpu WHERE " + tf + " AND regexp_matches(host, '^(web|db)-[0-9]+$') ORDER BY time ASC"},
		{"variable query", "SELECT DISTINCT host FROM telegraf.cpu ORDER BY host"},
		{"top N table panel", "SELECT host, AVG(usage) AS usage FROM cpu WHERE " + tf + " GROUP BY host ORDER BY usage DESC LIMIT 10"},
		{"ORDER BY DESC with a second key", "SELECT time, host, usage FROM cpu WHERE " + tf + " ORDER BY time DESC, host"},
		{"CTE panel", "WITH base AS (SELECT " + bucket + " AS time, host, AVG(usage) AS usage FROM cpu WHERE " + tf + " GROUP BY 1, host) SELECT time, host, usage FROM base ORDER BY time ASC"},
		{"CTE with a ranking window", "WITH ranked AS (SELECT host, usage, ROW_NUMBER() OVER (PARTITION BY host ORDER BY time DESC) AS rn FROM cpu WHERE " + tf + ") SELECT host, usage FROM ranked WHERE rn = 1"},
		{"count FILTER", "SELECT " + bucket + " AS time, count(*) FILTER (WHERE status >= 500) AS errors, count(*) FILTER (WHERE status < 500) AS ok FROM requests WHERE " + tf + " GROUP BY 1 ORDER BY time ASC"},
		{"count FILTER ratio", "SELECT " + bucket + " AS time, 100.0 * count(*) FILTER (WHERE status >= 500) / count(*) AS error_pct FROM requests WHERE " + tf + " GROUP BY 1 ORDER BY 1"},
		{"approx quantile", "SELECT " + bucket + " AS time, approx_quantile(latency_ms, 0.95) AS p95 FROM requests WHERE " + tf + " GROUP BY 1 ORDER BY 1"},
		{"histogram aggregate", "SELECT histogram(latency_ms) FROM requests WHERE " + tf},
		{"EXTRACT FROM", "SELECT EXTRACT(HOUR FROM time) AS hour, count(*) FROM requests WHERE " + tf + " GROUP BY 1 ORDER BY 1"},
		{"quoted identifiers", `SELECT "time", "host-name" FROM cpu WHERE ` + tf + ` ORDER BY "time" ASC`},
		{"string literal holding a backtick and a keyword", "SELECT time, msg FROM logs WHERE " + tf + " AND msg LIKE '%`select` from%' ORDER BY time ASC"},
		{"trailing semicolon", "SELECT 1;"},
		{"line comment", "SELECT time, usage FROM cpu WHERE " + tf + " -- panel A\nORDER BY time ASC"},
		{"join across measurements", "SELECT a.time, a.usage, b.used FROM cpu a JOIN mem b ON a.time = b.time AND a.host = b.host WHERE a.time >= '2026-11-10T00:00:00Z' AND a.time < '2026-11-11T00:00:00Z' ORDER BY a.time ASC"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateSQLRequest(tc.sql); err != nil {
				t.Fatalf("dashboard SQL refused: %v\n%s", err, tc.sql)
			}
		})
	}
}

// The accepting test above would pass against a validator that accepts
// everything, so keep one statement that must still be refused.
func TestValidateSQLRequest_StillRefusesAStatementTheDashboardNeverSends(t *testing.T) {
	if err := ValidateSQLRequest("CREATE MACRO m(a) AS a + 1"); err == nil {
		t.Fatal("a catalog change was accepted")
	}
}
