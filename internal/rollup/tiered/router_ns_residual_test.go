package tiered

import (
	"fmt"
	"testing"
)

// TestDateTrunc_OnTimestampNS — verify the original premise the Grafana
// plugin used to justify switching away from date_trunc: that DuckDB's
// date_trunc on TIMESTAMP_NS leaves nanosecond residuals and produces
// per-second GROUP BY rows. If this returns clean hour-truncated rows,
// the plugin's epoch math is the wrong fix — date_trunc would work and
// would route through the rollup.
func TestDateTrunc_OnTimestampNS(t *testing.T) {
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Force TIMESTAMP_NS precision (DuckDB default is TIMESTAMP / microsecond)
	_, err = db.Exec(`CREATE TABLE t (time TIMESTAMP_NS, v INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO t VALUES
		('2026-01-01 05:59:59.999999999', 1),
		('2026-01-01 06:00:00.000000001', 2),
		('2026-01-01 06:00:00.123456789', 3),
		('2026-01-01 06:59:59.987654321', 4),
		('2026-01-01 07:00:00.000000001', 5)`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`SELECT date_trunc('hour', time)::VARCHAR AS bucket, COUNT(*) FROM t GROUP BY 1 ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var groups int
	for rows.Next() {
		var bucket string
		var cnt int
		if err := rows.Scan(&bucket, &cnt); err != nil {
			t.Fatal(err)
		}
		groups++
		t.Logf("bucket=%s count=%d", bucket, cnt)
	}
	if groups > 3 {
		t.Errorf("date_trunc('hour', TIMESTAMP_NS) produced %d groups; expected ≤3 — nanosecond residual confirmed", groups)
	}
}

// TestTimeBucket_OnTimestampNS — same probe for time_bucket.
func TestTimeBucket_OnTimestampNS(t *testing.T) {
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE t (time TIMESTAMP_NS, v INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES
		('2026-01-01 05:59:59.999999999', 1),
		('2026-01-01 06:00:00.000000001', 2),
		('2026-01-01 06:30:00', 3),
		('2026-01-01 07:00:00', 4)`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT time_bucket(INTERVAL '1 hour', time)::VARCHAR AS bucket, COUNT(*) FROM t GROUP BY 1 ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var groups int
	for rows.Next() {
		var bucket string
		var cnt int
		if err := rows.Scan(&bucket, &cnt); err != nil {
			t.Fatal(err)
		}
		groups++
		fmt.Printf("time_bucket bucket=%s count=%d\n", bucket, cnt)
	}
	if groups > 3 {
		t.Errorf("time_bucket(1h, TIMESTAMP_NS) produced %d groups; expected ≤3", groups)
	}
}
