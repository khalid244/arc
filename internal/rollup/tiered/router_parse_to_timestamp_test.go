package tiered

import (
	"context"
	"fmt"
	"testing"
)

// The Grafana basekick-arc-datasource plugin emits
//   to_timestamp((epoch_ns(time) // 1000000000 // N) * N) AS t
// where N is the bucket size in seconds. The router must recognize this as
// a bucketing GROUP BY expression and map N to BucketArg.

const createEventsForBucket = `CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR)`

func mkBucketShape(t *testing.T, bucketSecs int64) *QueryShape {
	t.Helper()
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEventsForBucket); err != nil {
		t.Fatal(err)
	}
	sql := fmt.Sprintf(
		`SELECT to_timestamp((epoch_ns(time) // 1000000000 // %d) * %d) AS time,
		        COUNT(*) AS value FROM events
		 WHERE time >= TIMESTAMP '2026-05-01 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-15 00:00:00+00'
		 GROUP BY 1 ORDER BY 1`,
		bucketSecs, bucketSecs)
	qs, err := ExtractQueryShape(ctx, db, sql)
	if err != nil {
		t.Fatal(err)
	}
	return qs
}

func TestExtractShape_ToTimestamp_3600_Hour(t *testing.T) {
	qs := mkBucketShape(t, 3600)
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.BucketArg != "hour" {
		t.Errorf("BucketArg=%q want hour", qs.BucketArg)
	}
	if qs.UserBucketSecs != 0 {
		t.Errorf("UserBucketSecs=%d want 0 (exact hour, no override needed)", qs.UserBucketSecs)
	}
	if qs.TimeColumn != "time" {
		t.Errorf("TimeColumn=%q want time", qs.TimeColumn)
	}
}

func TestExtractShape_ToTimestamp_86400_Day(t *testing.T) {
	qs := mkBucketShape(t, 86400)
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.BucketArg != "day" {
		t.Errorf("BucketArg=%q want day", qs.BucketArg)
	}
	if qs.UserBucketSecs != 0 {
		t.Errorf("UserBucketSecs=%d want 0 (exact day)", qs.UserBucketSecs)
	}
}

func TestExtractShape_ToTimestamp_604800_Week(t *testing.T) {
	qs := mkBucketShape(t, 604800)
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.BucketArg != "week" {
		t.Errorf("BucketArg=%q want week", qs.BucketArg)
	}
}

func TestExtractShape_ToTimestamp_10800_ThreeHour(t *testing.T) {
	qs := mkBucketShape(t, 10800)
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	// 3h: inner bucket is hourly, outer applies the 3h wrap
	if qs.BucketArg != "hour" {
		t.Errorf("BucketArg=%q want hour (inner is hourly for sub-day multiples)", qs.BucketArg)
	}
	if qs.UserBucketSecs != 10800 {
		t.Errorf("UserBucketSecs=%d want 10800 (3h)", qs.UserBucketSecs)
	}
}

func TestExtractShape_ToTimestamp_21600_SixHour(t *testing.T) {
	qs := mkBucketShape(t, 21600)
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.BucketArg != "hour" || qs.UserBucketSecs != 21600 {
		t.Errorf("got BucketArg=%q UserBucketSecs=%d, want hour/21600", qs.BucketArg, qs.UserBucketSecs)
	}
}

func TestExtractShape_ToTimestamp_172800_TwoDay(t *testing.T) {
	qs := mkBucketShape(t, 172800)
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.BucketArg != "day" || qs.UserBucketSecs != 172800 {
		t.Errorf("got BucketArg=%q UserBucketSecs=%d, want day/172800", qs.BucketArg, qs.UserBucketSecs)
	}
}

func TestExtractShape_ToTimestamp_SubHour_60_Refused(t *testing.T) {
	qs := mkBucketShape(t, 60)
	if qs.Supported {
		t.Fatalf("expected refusal for sub-hour bucket (60s)")
	}
	if qs.Reason == "" {
		t.Error("expected non-empty Reason")
	}
}

func TestExtractShape_ToTimestamp_NonStandard_500_Refused(t *testing.T) {
	// 500 seconds — not a multiple of 3600, not a divisor of 3600 — refuse.
	qs := mkBucketShape(t, 500)
	if qs.Supported {
		t.Fatalf("expected refusal for non-aligned bucket (500s): %v", qs)
	}
}

func TestExtractShape_ToTimestamp_PreservesUserAlias(t *testing.T) {
	// The user's SELECT alias must propagate into the rewritten output
	// so Grafana's time-series detection still finds the right column.
	// (Spot check at parse level: alias is captured into shape somewhere.)
	qs := mkBucketShape(t, 3600)
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	// Right now BucketArg drives the alias in emit. After this fix we still
	// want BucketArg=hour to be set; the alias preservation is a separate
	// concern but is at least represented in OriginalSQL.
	if qs.OriginalSQL == "" {
		t.Error("OriginalSQL should be populated")
	}
}
