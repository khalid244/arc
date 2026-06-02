package api

import (
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
