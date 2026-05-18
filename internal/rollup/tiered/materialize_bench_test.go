package tiered

import (
	"fmt"
	"testing"
	"time"
)

// TestMaterializeOnce_BeforeVsAfter measures the wall-clock cost of building
// 12 variants for one bucket under two patterns:
//
//   BEFORE — each variant runs its own source SQL, paying the S3 fetch latency
//            12 times (simulated with a 100ms sleep per query).
//   AFTER  — source is materialized ONCE into a TEMP TABLE; all 12 variants
//            read from local memory.
//
// Run: go test -tags=duckdb_arrow -run TestMaterializeOnce_BeforeVsAfter -v ./internal/rollup/tiered/
func TestMaterializeOnce_BeforeVsAfter(t *testing.T) {
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 24h of synthetic data — small enough that local CPU isn't the bottleneck.
	if _, err := db.Exec(`CREATE TABLE source AS
		SELECT
		  TIMESTAMPTZ '2025-04-07 00:00:00+00' + INTERVAL (i) SECOND AS time,
		  ('country_' || (i % 50))  AS country,
		  ('site_'    || (i % 200)) AS site,
		  ('os_'      || (i % 5))   AS os,
		  1.0                       AS m
		FROM range(86400) t(i)`); err != nil {
		t.Fatal(err)
	}

	const variantCount = 12

	type row struct {
		delay       time.Duration
		before      time.Duration
		after       time.Duration
		speedup     float64
		secondsSaved float64
	}
	var results []row

	for _, fetchDelay := range []time.Duration{
		25 * time.Millisecond,  // warm-cache fast S3
		100 * time.Millisecond, // typical in-DC S3
		300 * time.Millisecond, // cold-cache cross-region
	} {
		// BEFORE: re-fetch source per variant
		beforeStart := time.Now()
		for v := 0; v < variantCount; v++ {
			time.Sleep(fetchDelay)
			if _, err := db.Exec(`
				SELECT date_trunc('hour', time) AS bucket,
				       country, COUNT(*) AS cnt, SUM(m) AS sum_m
				FROM source
				GROUP BY bucket, country`); err != nil {
				t.Fatalf("before variant %d: %v", v, err)
			}
			_ = v
		}
		beforeDur := time.Since(beforeStart)

		// AFTER: materialize once, all variants read from local temp
		afterStart := time.Now()
		time.Sleep(fetchDelay)
		if _, err := db.Exec(`CREATE OR REPLACE TEMP TABLE __bucket_src AS SELECT * FROM source`); err != nil {
			t.Fatalf("materialize: %v", err)
		}
		for v := 0; v < variantCount; v++ {
			if _, err := db.Exec(`
				SELECT date_trunc('hour', time) AS bucket,
				       country, COUNT(*) AS cnt, SUM(m) AS sum_m
				FROM __bucket_src
				GROUP BY bucket, country`); err != nil {
				t.Fatalf("after variant %d: %v", v, err)
			}
			_ = v
		}
		db.Exec(`DROP TABLE IF EXISTS __bucket_src`)
		afterDur := time.Since(afterStart)

		results = append(results, row{
			delay:        fetchDelay,
			before:       beforeDur,
			after:        afterDur,
			speedup:      float64(beforeDur) / float64(afterDur),
			secondsSaved: (beforeDur - afterDur).Seconds(),
		})
	}

	t.Logf("")
	t.Logf("%-12s %-15s %-15s %-10s %s", "delay", "BEFORE", "AFTER", "speedup", "saved")
	t.Logf("%s", "-------------------------------------------------------------------")
	for _, r := range results {
		t.Logf("%-12v %-15v %-15v %.1fx       %.2fs",
			r.delay, r.before.Truncate(time.Millisecond), r.after.Truncate(time.Millisecond), r.speedup, r.secondsSaved)
	}
	t.Logf("")
	t.Logf("Larger delays (= more network IO) yield bigger speedups because the")
	t.Logf("savings are exactly (variantCount-1) × delay. The CPU/materialize")
	t.Logf("cost is the same regardless of delay.")
	fmt.Println()
}
