package api

import (
	"context"
	"errors"
	"testing"
)

// TestIsStaleCubeFileError pins the classifier that decides whether a
// rollup-served query failed because a cube parquet listed in the manifest no
// longer exists in object storage. On such an error the read path must fall back
// to the source scan rather than return a 500. The positive cases are the exact
// error strings DuckDB's httpfs emitted in production when the router pointed at a
// deleted/never-written cube object; the negative cases must NOT trigger fallback.
func TestIsStaleCubeFileError(t *testing.T) {
	stale := []struct {
		name string
		msg  string
	}{
		{
			name: "404 on explicit monthly cube file (downloads.coarse phantom)",
			msg:  `arrow query failed: HTTP Error: HTTP GET error reading 's3://arc-test/_arc/rollup/default/downloads/coarse/m_2026-05_1780279647693914132.parquet' in region 'us-east-1' (HTTP 404 Not Found)`,
		},
		{
			name: "404 on explicit daily cube file (events.by_region phantom)",
			msg:  `arrow query failed: HTTP Error: HTTP GET error reading 's3://arc-test/_arc/rollup/default/events/by_region/m_2026-04_1780282548855103753.parquet' in region 'us-east-1' (HTTP 404 Not Found)`,
		},
		{
			name: "NoSuchKey variant from a strict S3 backend",
			msg:  `IO Error: read_parquet failed: s3://arc-test/_arc/rollup/default/events/by_status/2026-05-09.parquet NoSuchKey`,
		},
		{
			name: "403 after a lifecycle/ACL change on a rollup object",
			msg:  `HTTP Error: HTTP GET error reading 's3://b/_arc/rollup/x/y/coarse/m_2026-01_1.parquet' (HTTP 403 Forbidden)`,
		},
	}
	for _, tc := range stale {
		if !isStaleCubeFileError(errors.New(tc.msg)) {
			t.Errorf("expected stale-cube-file error to trigger source fallback: %s", tc.name)
		}
	}

	notStale := []struct {
		name string
		msg  string
	}{
		{name: "nil error", msg: ""},
		{
			name: "glob matched zero files (handled separately, returns empty result)",
			msg:  `IO Error: No files found that match the pattern 's3://arc-test/default/events/**/*.parquet'`,
		},
		{
			name: "binder/planner error is a real query bug, not a missing file",
			msg:  `Binder Error: Referenced column "regio" not found in FROM clause`,
		},
		{
			name: "a 404 unrelated to a parquet read must not be swallowed",
			msg:  `HTTP 404 Not Found: GET /some/admin/endpoint`,
		},
	}
	for _, tc := range notStale {
		var err error
		if tc.msg != "" {
			err = errors.New(tc.msg)
		}
		if isStaleCubeFileError(err) {
			t.Errorf("did NOT expect source fallback for: %s", tc.name)
		}
	}
}

// TestShouldSourceFallback pins the rollup-served source-fallback gate. The
// merge-on-read source branch prunes to per-day globs, so a day that empties
// between the existence probe and the read raises "No files found"; for a
// rollup-served query (sourceFallbackSQL set) that must degrade to the whole-table
// source scan, not turn the whole panel silently empty. Non-rollup queries keep
// the legacy no-files→empty behaviour.
func TestShouldSourceFallback(t *testing.T) {
	const cube = "SELECT 1 FROM read_parquet('s3://b/_arc/rollup/x/coarse/m_1.parquet')"
	const src = "SELECT 1 FROM read_parquet('s3://b/x/**/*.parquet')"
	stale := errors.New(`HTTP Error: HTTP GET error reading 's3://b/_arc/rollup/x/coarse/m_1.parquet' (HTTP 404 Not Found)`)
	noFiles := errors.New(`IO Error: No files found that match the pattern 's3://b/x/2026/06/14/**/*.parquet'`)
	binder := errors.New(`Binder Error: Referenced column "regio" not found`)

	cases := []struct {
		name          string
		err           error
		converted, fb string
		ctxErr        error
		want          bool
	}{
		{"stale cube + fallback", stale, cube, src, nil, true},
		{"no-files + fallback (the race fix)", noFiles, cube, src, nil, true},
		{"no-files but NO fallback (non-rollup -> stays empty)", noFiles, src, "", nil, false},
		{"no-files but fallback == converted (don't retry self)", noFiles, src, src, nil, false},
		{"recoverable err but context cancelled", noFiles, cube, src, context.Canceled, false},
		{"benign binder error never falls back", binder, cube, src, nil, false},
		{"nil error", nil, cube, src, nil, false},
	}
	for _, tc := range cases {
		if got := shouldSourceFallback(tc.err, tc.converted, tc.fb, tc.ctxErr); got != tc.want {
			t.Errorf("%s: shouldSourceFallback = %v, want %v", tc.name, got, tc.want)
		}
	}
}
