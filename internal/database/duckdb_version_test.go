package database

// Regression guard for an intermittent S3 query failure:
//
//	INTERNAL Error: Information loss on integer cast: value -244 outside of
//	target range [0, 18446744073709551615]
//
// thrown from httpfs HTTPFileSystem::TryRangeRequest. The httpfs build that
// DuckDB v1.5.2 and v1.5.3 install timed every non-parallel range GET with
// the wall clock (Timestamp::GetCurrentTimestamp, i.e. system_clock) and
// NumericCast the difference to idx_t. Any backward step of CLOCK_REALTIME
// while a request is in flight (NTP step, VM/WSL time sync) made the
// duration negative, so the cast threw and the whole query failed. Scans of
// many small Parquet files issue thousands of such requests, which is where
// it surfaced. Fixed in duckdb-httpfs#339 (steady_clock); the first DuckDB
// release that installs a fixed httpfs is v1.5.4.
//
// Arc does not compile httpfs in: it runs INSTALL httpfs at startup, and
// DuckDB downloads the build published for the linked engine version. So the
// duckdb-go-bindings version in go.mod is what decides whether the fix is
// present. This test fails if a dependency change (for example an upstream
// merge taking an older pin) moves the engine back below v1.5.4.

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"testing"
)

// minHTTPFSMonotonicClockVersion is the first DuckDB release whose httpfs
// measures range-request duration with a monotonic clock.
var minHTTPFSMonotonicClockVersion = [3]int{1, 5, 4}

var duckdbVersionRe = regexp.MustCompile(`^v(\d+)\.(\d+)\.(\d+)`)

func parseDuckDBVersion(v string) ([3]int, error) {
	m := duckdbVersionRe.FindStringSubmatch(v)
	if m == nil {
		return [3]int{}, fmt.Errorf("unrecognised DuckDB version %q", v)
	}
	var out [3]int
	for i := range out {
		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return [3]int{}, fmt.Errorf("unrecognised DuckDB version %q: %w", v, err)
		}
		out[i] = n
	}
	return out, nil
}

func versionAtLeast(got, want [3]int) bool {
	for i := range got {
		if got[i] != want[i] {
			return got[i] > want[i]
		}
	}
	return true
}

func TestLinkedDuckDBInstallsHTTPFSWithMonotonicRangeTiming(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var raw string
	if err := db.QueryRow("SELECT library_version FROM pragma_version()").Scan(&raw); err != nil {
		t.Fatalf("read DuckDB version: %v", err)
	}
	got, err := parseDuckDBVersion(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !versionAtLeast(got, minHTTPFSMonotonicClockVersion) {
		w := minHTTPFSMonotonicClockVersion
		t.Fatalf("linked DuckDB is %s; Arc needs >= v%d.%d.%d so INSTALL httpfs fetches a build "+
			"without the wall-clock TryRangeRequest cast (duckdb-httpfs#339)", raw, w[0], w[1], w[2])
	}
}

func TestDuckDBVersionComparison(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"v1.5.2", false},
		{"v1.5.3", false},
		{"v1.4.9", false},
		{"v1.5.4", true},
		{"v1.5.6", true},
		{"v1.10.0", true},
		{"v2.0.0", true},
		{"v1.5.4-dev12", true},
	}
	for _, c := range cases {
		v, err := parseDuckDBVersion(c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if got := versionAtLeast(v, minHTTPFSMonotonicClockVersion); got != c.want {
			t.Errorf("versionAtLeast(%s, 1.5.4) = %v, want %v", c.in, got, c.want)
		}
	}
	if _, err := parseDuckDBVersion("1.5.4"); err == nil {
		t.Error("expected an error for a version without the leading v")
	}
}
