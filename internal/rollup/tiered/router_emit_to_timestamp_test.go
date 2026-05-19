package tiered

import (
	"strings"
	"testing"
)

// When UserBucketSecs == 0 the emitter must NOT wrap _bkt — backwards
// compatibility with the existing date_trunc path.
func TestEmit_NoUserBucket_StillUsesBareBkt(t *testing.T) {
	timeHi := mustTime("2026-05-15")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/05/01/sketch/f.parquet"})
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     mustTime("2026-05-01"),
		TimeHi:     timeHi,
		BucketArg:  "hour",
		Aggregates: []Aggregate{{Kind: AggCountStar}},
	}
	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape: shape, Tier: Tier1h, TailLo: timeHi, Variant: "sketch",
		Files: idx, Spec: makeSpec("UTC", nil), SkipCoverageCheck: true,
	})
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if strings.Contains(sql, "to_timestamp(") {
		t.Fatalf("did not expect to_timestamp wrap when UserBucketSecs=0:\n%s", sql)
	}
	if !strings.Contains(sql, "_bkt AS hour") {
		t.Fatalf("expected _bkt AS hour: %s", sql)
	}
}

// When UserBucketSecs > 0 the outer SELECT must wrap _bkt in
// to_timestamp((epoch_ns(_bkt)//1e9//N)*N) and GROUP BY the same expression.
func TestEmit_UserBucket_3h_WrapsOuter(t *testing.T) {
	timeHi := mustTime("2026-05-15")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/05/01/sketch/f.parquet"})
	shape := &QueryShape{
		Table:          "db.events",
		TimeColumn:     "time",
		TimeLo:         mustTime("2026-05-01"),
		TimeHi:         timeHi,
		BucketArg:      "hour",
		UserBucketSecs: 10800, // 3h
		Aggregates:     []Aggregate{{Kind: AggCountStar}},
	}
	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape: shape, Tier: Tier1h, TailLo: timeHi, Variant: "sketch",
		Files: idx, Spec: makeSpec("UTC", nil), SkipCoverageCheck: true,
	})
	if !ok {
		t.Fatalf("expected ok=true")
	}
	// Inner CTE still hourly (the rollup grain doesn't change)
	if !strings.Contains(sql, "date_trunc('hour', bucket) AS _bkt") {
		t.Fatalf("inner CTE should keep hourly grain: %s", sql)
	}
	// Outer projection uses the wrap
	wrapExpr := "to_timestamp((epoch_ns(_bkt)//1000000000//10800)*10800)"
	if !strings.Contains(sql, wrapExpr+" AS hour") {
		t.Fatalf("expected outer wrap %q:\n%s", wrapExpr, sql)
	}
	// Outer GROUP BY uses the same expression (or a positional ref that resolves to it)
	if !strings.Contains(sql, "GROUP BY "+wrapExpr) {
		t.Fatalf("expected GROUP BY %s in outer: %s", wrapExpr, sql)
	}
	// Outer ORDER BY uses the same expression
	if !strings.Contains(sql, "ORDER BY "+wrapExpr) {
		t.Fatalf("expected ORDER BY %s in outer: %s", wrapExpr, sql)
	}
}

// 2-day bucket — inner is daily, outer wraps to 2-day boundaries.
func TestEmit_UserBucket_2d_WrapsOuter(t *testing.T) {
	timeHi := mustTime("2026-05-30")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/05/01/sketch/f.parquet"})
	shape := &QueryShape{
		Table:          "db.events",
		TimeColumn:     "time",
		TimeLo:         mustTime("2026-05-01"),
		TimeHi:         timeHi,
		BucketArg:      "day",
		UserBucketSecs: 172800, // 2 days
		Aggregates:     []Aggregate{{Kind: AggCountStar}},
	}
	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape: shape, Tier: Tier1h, TailLo: timeHi, Variant: "sketch",
		Files: idx, Spec: makeSpec("UTC", nil), SkipCoverageCheck: true,
	})
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if !strings.Contains(sql, "date_trunc('day', bucket) AS _bkt") {
		t.Fatalf("inner CTE should be daily: %s", sql)
	}
	if !strings.Contains(sql, "to_timestamp((epoch_ns(_bkt)//1000000000//172800)*172800)") {
		t.Fatalf("expected 2-day wrap: %s", sql)
	}
}

// Direct unit test of the helper.
func TestOuterBucketExpr_Helper(t *testing.T) {
	cases := []struct {
		secs int64
		want string
	}{
		{0, "_bkt"},
		{-1, "_bkt"},
		{3600, "to_timestamp((epoch_ns(_bkt)//1000000000//3600)*3600)"},
		{10800, "to_timestamp((epoch_ns(_bkt)//1000000000//10800)*10800)"},
		{86400, "to_timestamp((epoch_ns(_bkt)//1000000000//86400)*86400)"},
	}
	for _, tc := range cases {
		e := outerBucketExpr(tc.secs)
		got, err := e.sql(RollupMode)
		if err != nil {
			t.Fatalf("secs=%d sql err: %v", tc.secs, err)
		}
		if got != tc.want {
			t.Fatalf("secs=%d: got %q, want %q", tc.secs, got, tc.want)
		}
	}
}
