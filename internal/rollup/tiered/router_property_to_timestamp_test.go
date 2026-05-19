package tiered

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

// Property tests for the to_timestamp bucketing path.
//
// For each bucket size, we run the user's SQL (using the Grafana plugin's
// to_timestamp idiom) two ways:
//   1. Directly against the source table (truth)
//   2. Through the router → emitted rollup SQL
// and assert the row sets match. This is the strongest guarantee that
// the bucket-translation produces identical results.
func TestProperty_ToTimestamp_Bucketing(t *testing.T) {
	cases := []struct {
		name       string
		bucketSecs int64
	}{
		{"1h", 3600},
		{"3h", 10800},
		{"6h", 21600},
		{"12h", 43200},
		{"1d", 86400},
		{"2d", 172800},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runToTimestampProperty(t, tc.bucketSecs)
		})
	}
}

func runToTimestampProperty(t *testing.T, bucketSecs int64) {
	t.Helper()
	ctx := context.Background()
	keptSites := []string{"youtu.be", "www.instagram.com", "youtube.com"}
	db, spec, idx, tmp := setupKept3DayDataset(t, ctx, keptSites, 3)

	// 3-day window covering all three rolled-up days.
	userSQL := fmt.Sprintf(
		`SELECT to_timestamp((epoch_ns(time) // 1000000000 // %d) * %d) AS time,
		        site, COUNT(*) AS value FROM events
		 WHERE time >= TIMESTAMP '2026-05-14 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-17 00:00:00+00'
		   AND site IN (%s)
		 GROUP BY 1, 2
		 ORDER BY time ASC`,
		bucketSecs, bucketSecs, quoteList(keptSites))

	truth := runRowSet(t, db, userSQL)
	rewritten := mustRewriteAccept(t, ctx, db, idx, spec, tmp, userSQL)
	got := runRowSet(t, db, rewritten)

	if !equalRowSets(truth, got) {
		t.Errorf("row sets differ for %ds bucket.\nuser sql:\n%s\nrewritten:\n%s\ntruth (%d rows):\n%s\nrollup (%d rows):\n%s",
			bucketSecs, userSQL, rewritten, len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}

// Exact Grafana plugin SQL — the literal query that caused the user's
// dashboard panel to fall back to source-scan and time out.
func TestProperty_ToTimestamp_LiteralGrafanaQuery(t *testing.T) {
	ctx := context.Background()
	keptSites := []string{"youtu.be", "www.instagram.com", "youtube.com"}
	db, spec, idx, tmp := setupKept3DayDataset(t, ctx, keptSites, 1)

	// This SQL matches what basekick-arc-datasource sends to /api/v1/query.
	userSQL := fmt.Sprintf(
		`SELECT to_timestamp((epoch_ns(time) // 1000000000 // 3600) * 3600) AS time,
		        site, COUNT(*) AS value FROM events
		 WHERE time >= TIMESTAMP '2026-05-14 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-15 00:00:00+00'
		   AND site IN (%s)
		 GROUP BY 1, 2
		 ORDER BY time ASC`,
		quoteList(keptSites))

	truth := runRowSet(t, db, userSQL)
	rewritten := mustRewriteAccept(t, ctx, db, idx, spec, tmp, userSQL)
	got := runRowSet(t, db, rewritten)

	if !equalRowSets(truth, got) {
		t.Errorf("row sets differ.\nrewritten:\n%s\ntruth (%d rows):\n%s\nrollup (%d rows):\n%s",
			rewritten, len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}

// Sub-hour bucket (60s) must be refused; the query falls back to source.
// mustRewriteAccept would call t.Fatal, so this test directly checks Rewrite.
func TestProperty_ToTimestamp_SubHour_RefusedFallback(t *testing.T) {
	ctx := context.Background()
	keptSites := []string{"youtu.be"}
	db, spec, idx, tmp := setupKept3DayDataset(t, ctx, keptSites, 1)

	userSQL := fmt.Sprintf(
		`SELECT to_timestamp((epoch_ns(time) // 1000000000 // 60) * 60) AS time,
		        COUNT(*) AS value FROM events
		 WHERE time >= TIMESTAMP '2026-05-14 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-15 00:00:00+00'
		   AND site IN (%s)
		 GROUP BY 1
		 ORDER BY time ASC`,
		quoteList(keptSites))

	rewritten, accepted := tryRewrite(t, ctx, db, idx, spec, tmp, userSQL)
	if accepted {
		t.Fatalf("expected refusal for sub-hour bucket; got rewrite:\n%s", rewritten)
	}
}

// tryRewrite returns (rewritten, accepted) without failing the test.
func tryRewrite(t *testing.T, ctx context.Context, db *sql.DB, idx *MemoryFileIndex, spec *Spec, tmp, userSQL string) (string, bool) {
	t.Helper()
	rewritten, ok := Rewrite(ctx, userSQL, RewriteDeps{
		DB: db, Files: idx, Spec: spec, DimRichCap: 100, GraceWindow: 0,
		StoragePrefix: tmp + "/",
	})
	return rewritten, ok
}
