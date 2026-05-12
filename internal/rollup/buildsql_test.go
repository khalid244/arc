package rollup

import (
	"strings"
	"testing"
	"time"
)

func TestBuildWindowSQL_BasicAggregations(t *testing.T) {
	spec := RollupSpec{
		Name:           "analytics__events__1h",
		Database:       "analytics",
		SourceTable:    "events",
		BucketColumn:   "ts",
		BucketInterval: time.Hour,
		KeepDimensions: []string{"service_name", "region"},
		Aggregations: []Aggregation{
			{
				SourceColumn: "latency_ms",
				Functions:    []AggFunction{AggSum, AggMin, AggMax},
			},
			{
				SourceColumn: "bytes",
				Functions:    []AggFunction{AggSum},
			},
		},
	}
	windowStart := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 5, 10, 13, 0, 0, 0, time.UTC)

	sql, err := BuildWindowSQL(spec, "analytics.events", windowStart, windowEnd)
	if err != nil {
		t.Fatalf("BuildWindowSQL: %v", err)
	}

	wantContains := []string{
		`time_bucket(INTERVAL '3600 seconds', ts) AS bucket`,
		`service_name`,
		`region`,
		`COUNT(*) AS __row_count`,
		`SUM(latency_ms) AS latency_ms__sum`,
		`MIN(latency_ms) AS latency_ms__min`,
		`MAX(latency_ms) AS latency_ms__max`,
		`SUM(bytes) AS bytes__sum`,
		`FROM analytics.events`,
		`WHERE ts >= TIMESTAMP '2026-05-10 12:00:00'`,
		`AND ts < TIMESTAMP '2026-05-10 13:00:00'`,
		`GROUP BY 1, service_name, region`,
	}
	for _, w := range wantContains {
		if !strings.Contains(sql, w) {
			t.Errorf("SQL missing %q\nfull SQL:\n%s", w, sql)
		}
	}
}

func TestBuildWindowSQL_NoDimensions(t *testing.T) {
	spec := RollupSpec{
		Name:           "n__1m",
		Database:       "d",
		SourceTable:    "s",
		BucketColumn:   "ts",
		BucketInterval: time.Minute,
		Aggregations: []Aggregation{
			{SourceColumn: "v", Functions: []AggFunction{AggSum}},
		},
	}
	sql, err := BuildWindowSQL(spec, "d.s", time.Unix(0, 0).UTC(), time.Unix(60, 0).UTC())
	if err != nil {
		t.Fatalf("BuildWindowSQL: %v", err)
	}
	if !strings.Contains(sql, "GROUP BY 1") {
		t.Errorf("expected GROUP BY 1; got: %s", sql)
	}
	if strings.Contains(sql, "GROUP BY 1,") {
		t.Errorf("did not expect trailing dimension in GROUP BY: %s", sql)
	}
}

func TestBuildWindowSQL_RejectsInvalidIdentifier(t *testing.T) {
	spec := RollupSpec{
		Name: "n", Database: "d", SourceTable: "s",
		BucketColumn:   "ts; DROP TABLE x;",
		BucketInterval: time.Minute,
		Aggregations:   []Aggregation{{SourceColumn: "v", Functions: []AggFunction{AggSum}}},
	}
	_, err := BuildWindowSQL(spec, "d.s", time.Unix(0, 0).UTC(), time.Unix(60, 0).UTC())
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}
}
