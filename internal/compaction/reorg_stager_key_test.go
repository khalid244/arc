package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// REVIEW-1: the LateStager (exactly-once uploads with persisted merge jobs)
// names its outputs with the job id between the late name and the timestamp:
//
//	<db>/<m>_late/<m>_late_<jobID>_<YYYYMMDD>_<HHMMSS>_<nanos>.parquet
//
// where jobID = "<podID>-<16 hex>" and podID is sanitized to [A-Za-z0-9-]
// (no '_'). In production every flush is staged, so every late file has this
// shape. The CS-11 flat-name rule rejected it, so nothing was ever drained.

// stagerLateJobKey is the LateStager's lateJobKey format, verbatim.
func stagerLateJobKey(db, measurement, jobID string, created time.Time) string {
	lateMeasurement := measurement + "_late"
	return fmt.Sprintf("%s/%s/%s_%s_%s_%09d.parquet", db, lateMeasurement, lateMeasurement, jobID,
		created.Format("20060102_150405"), created.Nanosecond())
}

func reorgSidecarUnrecognizedCount() int64 {
	v, _ := metrics.Get().Snapshot()["reorg_sidecar_keys_unrecognized_total"].(int64)
	return v
}

func TestIsSidecarSourceKey_StagerJobKeys(t *testing.T) {
	created := time.Date(2026, 5, 20, 3, 0, 0, 123456789, time.UTC)
	prefix := "posthog/events_late/"
	accept := map[string]string{
		"statefulset_pod": stagerLateJobKey("posthog", "events", "arc-ingest-0-1a2b3c4d5e6f7a8b", created),
		"deployment_pod":  stagerLateJobKey("posthog", "events", "arc-ingest-7f9c8d6b5-x2k4p-00ff00ff00ff00ff", created),
		"all_digit_hex":   stagerLateJobKey("posthog", "events", "arc-ingest-2-1234567890123456", created),
		"time_fallback":   stagerLateJobKey("posthog", "events", fmt.Sprintf("ingest-%x", created.UnixNano()), created),
		"single_char_pod": stagerLateJobKey("posthog", "events", "a-0123456789abcdef", created),
		"legacy_flat":     "posthog/events_late/events_late_20260520_030000_123456789.parquet",
	}
	for name, key := range accept {
		t.Run("accept/"+name, func(t *testing.T) {
			ft, ok := parseFilenameTime(key)
			if !ok {
				t.Fatalf("parseFilenameTime rejects %s", key)
			}
			if want := time.Date(2026, 5, 20, 3, 0, 0, 0, time.UTC); !ft.Equal(want) {
				t.Fatalf("parseFilenameTime(%s) = %v, want %v", key, ft, want)
			}
			if !isSidecarSourceKey(key, prefix, "events_late") {
				t.Fatalf("isSidecarSourceKey rejects %s: the drain would never move it", key)
			}
		})
	}
	reject := map[string]string{
		"nested_real_measurement": "posthog/events_late/2026/05/20/03/events_late_20260520_030000_12345678.parquet",
		"nested_job_key":          "posthog/events_late/2026/05/20/03/events_late_arc-ingest-0-1a2b3c4d5e6f7a8b_20260520_030000_123456789.parquet",
		"other_name":              "posthog/events_late/notes_20260520_030000_1.parquet",
		"empty_writer_segment":    "posthog/events_late/events_late__20260520_030000_1.parquet",
		"writer_with_underscore":  "posthog/events_late/events_late_arc_ingest_20260520_030000_1.parquet",
		"leading_dash_writer":     "posthog/events_late/events_late_-pod_20260520_030000_1.parquet",
		"not_parquet":             "posthog/events_late/events_late_arc-ingest-0-1a2b_20260520_030000_1.json",
		"other_prefix":            "posthog/events/events_late_20260520_030000_1.parquet",
	}
	for name, key := range reject {
		t.Run("reject/"+name, func(t *testing.T) {
			if isSidecarSourceKey(key, prefix, "events_late") {
				t.Fatalf("isSidecarSourceKey accepts %s", key)
			}
		})
	}
}

// End to end over real files and real DuckDB: stager-named late files (two
// pods, same hour) drain into the base measurement with every row, and the
// sidecar is emptied.
func TestReorg_StagerJobKeyFilesDrain(t *testing.T) {
	t.Setenv("TZ", "UTC")
	b, tmp := newHookBackend(t)
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	if err := os.MkdirAll(lateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 5, 20, 3, 0, 0, 1, time.UTC)
	names := []string{
		filepath.Base(stagerLateJobKey("posthog", "events", "arc-ingest-0-1a2b3c4d5e6f7a8b", created)),
		filepath.Base(stagerLateJobKey("posthog", "events", "arc-ingest-1-0011223344556677", created.Add(90*time.Second))),
		// A legacy inline-upload name in the same hour still drains too.
		"events_late_20260520_030500_000000003.parquet",
	}
	for i, name := range names {
		writeLateParquet(t, db, lateDir, name, fmt.Sprintf(
			`SELECT TIMESTAMP '2026-04-0%d 12:00:00' + INTERVAL (r) MINUTE AS time, 'h%d'::VARCHAR AS host, r::BIGINT AS value FROM range(%d) t(r)`,
			i+1, i, 2+i))
	}
	db.Close()
	scratch := filepath.Join(tmp, "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	r := newReorg(b, scratch, zerolog.New(io.Discard))
	before := reorgSidecarUnrecognizedCount()
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if n := countParquetFlat(t, lateDir); n != 0 {
		t.Fatalf("stager-named late files were not drained: %d left in events_late", n)
	}
	total, perDay := outputRowsByDay(t, filepath.Join(tmp, "posthog", "events"))
	if total != 2+3+4 || len(perDay) != 3 {
		t.Fatalf("drained rows = %d over %d days, want 9 over 3 (%v)", total, len(perDay), perDay)
	}
	if got := reorgSidecarUnrecognizedCount(); got != before {
		t.Errorf("recognised keys were counted as unrecognized: %d -> %d", before, got)
	}
	if keys, _ := r.ManifestManager.List(context.Background()); len(keys) != 0 {
		t.Errorf("leftover manifests %v", keys)
	}
}

// A flat file directly under a sidecar dir whose name the drain does not
// recognise (naming drift between writer and reorg) must show up on a
// dashboard, not only in a log. The nested files of a real measurement named
// *_late are an expected layout and are not counted.
func TestReorg_UnrecognizedFlatSidecarKeyIsCounted(t *testing.T) {
	t.Setenv("TZ", "UTC")
	b, tmp := newHookBackend(t)
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	if err := os.MkdirAll(lateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	row := `SELECT TIMESTAMP '2026-04-01 12:00:00' AS time, 'h'::VARCHAR AS host, 1::BIGINT AS value`
	writeLateParquet(t, db, lateDir, "events_late_20260520_030000_000000001.parquet", row)
	// Drifted writer format: unknown, flat, never drained.
	drift := filepath.Join(lateDir, "events_late_v2_arc_ingest_0_20260520_030000_000000002.parquet")
	writeLateParquet(t, db, lateDir, filepath.Base(drift), row)
	// Valid shape but an impossible date: also unrecognized.
	badDate := filepath.Join(lateDir, "events_late_20261340_030000_000000003.parquet")
	writeLateParquet(t, db, lateDir, filepath.Base(badDate), row)
	// Nested file (real-measurement layout): ignored but not counted.
	nested := filepath.Join(lateDir, "2026", "05", "20", "03", "events_late_20260520_030000_12345678.parquet")
	if err := os.MkdirAll(filepath.Dir(nested), 0o755); err != nil {
		t.Fatal(err)
	}
	writeLateParquet(t, db, filepath.Dir(nested), filepath.Base(nested), row)
	db.Close()
	scratch := filepath.Join(tmp, "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	r := newReorg(b, scratch, zerolog.New(io.Discard))
	before := reorgSidecarUnrecognizedCount()
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := reorgSidecarUnrecognizedCount() - before; got != 2 {
		t.Errorf("reorg_sidecar_keys_unrecognized_total rose by %d, want 2 (the drifted name and the bad date)", got)
	}
	for _, p := range []string{drift, badDate, nested} {
		if !fileExists(t, p) {
			t.Errorf("an unrecognized key was moved or deleted: %s", p)
		}
	}
	if total, _ := outputRowsByDay(t, filepath.Join(tmp, "posthog", "events")); total != 1 {
		t.Errorf("only the recognised file drains: got %d rows, want 1", total)
	}
}
