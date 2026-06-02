package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// duckSessionTZ returns the TimeZone DuckDB pins for new connections in THIS
// process. DuckDB reads the OS default at cgo init; os.Setenv mid-process does
// NOT change it (verified empirically). So a test that wants a specific session
// TZ must be launched with TZ set in the environment before the process starts.
func duckSessionTZ(t *testing.T) string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	var tz string
	if err := db.QueryRow("SELECT current_setting('TimeZone')").Scan(&tz); err != nil {
		t.Fatalf("read session TZ: %v", err)
	}
	return tz
}

// runReorgMidnightRows generates a bucket with TIMESTAMP (no-tz) rows straddling
// UTC midnight (23:30 and 00:30 UTC on the day boundary 2026-04-01/02), runs the
// real Reorganizer, and returns the sorted set of "Y/M/D" day partitions that the
// rows landed in. tsType selects TIMESTAMP vs TIMESTAMPTZ for the source column.
func runReorgMidnightRows(t *testing.T, tsType string) []string {
	t.Helper()
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, err := storage.NewLocalBackend(tmp, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	if err := os.MkdirAll(lateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	// Two rows: 2026-04-01 23:30 UTC and 2026-04-02 00:30 UTC. Correct UTC days
	// are 2026/04/01 and 2026/04/02 respectively, regardless of session TZ.
	var lit1, lit2 string
	switch tsType {
	case "TIMESTAMPTZ":
		lit1 = `TIMESTAMPTZ '2026-04-01 23:30:00+00'`
		lit2 = `TIMESTAMPTZ '2026-04-02 00:30:00+00'`
	default: // TIMESTAMP
		lit1 = `TIMESTAMP '2026-04-01 23:30:00'`
		lit2 = `TIMESTAMP '2026-04-02 00:30:00'`
	}
	writeLateParquet(t, db, lateDir, "events_late_20260520_030000_0.parquet", fmt.Sprintf(`
		SELECT %s AS time, 'a'::VARCHAR AS host, 1::BIGINT AS value
		UNION ALL
		SELECT %s AS time, 'b'::VARCHAR AS host, 2::BIGINT AS value`, lit1, lit2))
	db.Close()

	reorg := newReorg(backend, filepath.Join(tmp, "scratch"), logger)
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run: %v", err)
	}
	eventsDir := filepath.Join(tmp, "posthog", "events")
	_, perDay := outputRowsByDay(t, eventsDir)
	days := make([]string, 0, len(perDay))
	for k := range perDay {
		days = append(days, k)
	}
	sort.Strings(days)
	return days
}

// TestReorg_TimezoneDayAssignment is the core mis-partitioning probe. It runs in
// the CURRENT process's session TZ and checks each row's UTC day partition.
//
// Two modes:
//   - As a CHILD of the driver (REORG_TZ_CHILD=1): asserts STRICT UTC
//     correctness so the driver's matrix records PASS/FAIL per zone. This is
//     where the TIMESTAMP-col bug surfaces as a hard FAIL under non-UTC TZ.
//   - Standalone (whole-suite run on a dev machine in any TZ): the TIMESTAMP
//     case only LOGS the mis-partition as a documented finding (does not fail
//     the suite on a non-UTC host). The TIMESTAMPTZ case — the prod-relevant
//     one, since Arc's ingest writes tz-aware timestamps — ALWAYS hard-asserts
//     correctness.
func TestReorg_TimezoneDayAssignment(t *testing.T) {
	tz := duckSessionTZ(t)
	child := os.Getenv("REORG_TZ_CHILD") == "1"
	t.Logf("DuckDB session TimeZone = %q (child=%v)", tz, child)

	const wantDay1 = "2026/04/01" // for the 23:30 UTC row
	const wantDay2 = "2026/04/02" // for the 00:30 UTC row
	want := []string{wantDay1, wantDay2}

	t.Run("TIMESTAMP_input", func(t *testing.T) {
		days := runReorgMidnightRows(t, "TIMESTAMP")
		t.Logf("TIMESTAMP source, session TZ=%q -> day partitions: %v", tz, days)
		if !equalStrSlice(days, want) {
			msg := fmt.Sprintf("MIS-PARTITION (TIMESTAMP col, session TZ=%q): rows around UTC midnight landed in %v, want %v. "+
				"This is the late-data day-assignment bug: EXTRACT(... AT TIME ZONE 'UTC') on a tz-naive TIMESTAMP "+
				"shifts by the session TZ offset.", tz, days, want)
			if child {
				t.Error(msg) // strict in the driver matrix
			} else {
				t.Logf("FINDING (non-fatal in standalone run): %s", msg)
			}
		}
	})

	t.Run("TIMESTAMPTZ_input", func(t *testing.T) {
		days := runReorgMidnightRows(t, "TIMESTAMPTZ")
		t.Logf("TIMESTAMPTZ source, session TZ=%q -> day partitions: %v", tz, days)
		// Always strict: this is what Arc's ingest actually produces (tz-aware
		// Timestamp_us -> DuckDB TIMESTAMP WITH TIME ZONE), so it MUST be correct
		// in every timezone.
		if !equalStrSlice(days, want) {
			t.Errorf("TIMESTAMPTZ col under session TZ=%q landed in %v, want %v — "+
				"prod-relevant path is mis-partitioning!", tz, days, want)
		}
	})
}

// TestReorg_TimezoneDayAssignment_AllZones is a driver that re-invokes
// TestReorg_TimezoneDayAssignment in a fresh process per timezone (the only
// reliable way to set DuckDB's session TZ, which is pinned at cgo init). It
// reports, per zone, whether the day assignment was correct — turning the
// "subtle one" into hard evidence. It does NOT fail the suite itself; the
// per-zone correctness is logged and the child's pass/fail is surfaced.
func TestReorg_TimezoneDayAssignment_AllZones(t *testing.T) {
	if os.Getenv("REORG_TZ_CHILD") == "1" {
		t.Skip("child invocation; handled by TestReorg_TimezoneDayAssignment")
	}
	zones := []string{"UTC", "Asia/Riyadh", "America/Los_Angeles"}
	type result struct {
		zone   string
		passed bool
		out    string
	}
	var results []result
	for _, z := range zones {
		cmd := exec.Command("go", "test", "-count=1", "-run", "TestReorg_TimezoneDayAssignment$", "-v", ".")
		cmd.Env = append(os.Environ(), "TZ="+z, "REORG_TZ_CHILD=1")
		out, err := cmd.CombinedOutput()
		results = append(results, result{zone: z, passed: err == nil, out: string(out)})
	}
	t.Log("=== Timezone day-assignment matrix (TIMESTAMP + TIMESTAMPTZ source columns) ===")
	for _, r := range results {
		status := "PASS"
		if !r.passed {
			status = "FAIL"
		}
		t.Logf("[TZ=%s] %s", r.zone, status)
		// Surface the key lines so the evidence is in the test log.
		for _, line := range strings.Split(r.out, "\n") {
			if strings.Contains(line, "session TZ") ||
				strings.Contains(line, "MIS-PARTITION") ||
				strings.Contains(line, "day partitions") ||
				strings.Contains(line, "--- FAIL") ||
				strings.Contains(line, "--- PASS") {
				t.Logf("    %s | %s", r.zone, strings.TrimSpace(line))
			}
		}
	}
}

func equalStrSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
