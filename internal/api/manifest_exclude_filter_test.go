package api

import (
	"testing"
)

// filterExcludesForTable should keep only the files whose storage key is
// under `<database>/<measurement>/`. Stops the cross-tenant contamination
// we hit in prod where posthog/events compaction manifests added 244
// `filename NOT IN (…)` entries to every downloads query.

func TestFilterExcludesForTable_KeepsMatchingPrefix(t *testing.T) {
	excludes := map[string]struct{}{
		"default/downloads/2026/05/19/00/raw_a.parquet": {},
		"default/downloads/2026/05/19/01/raw_b.parquet": {},
		"posthog/events/2026/05/19/00/raw_c.parquet":    {},
	}
	got := filterExcludesForTable(excludes, "default", "downloads")
	if len(got) != 2 {
		t.Errorf("expected 2 downloads files, got %d: %v", len(got), got)
	}
	for k := range got {
		if k == "posthog/events/2026/05/19/00/raw_c.parquet" {
			t.Errorf("posthog file leaked into downloads exclusion list: %s", k)
		}
	}
}

func TestFilterExcludesForTable_DropsAllWhenNoneMatch(t *testing.T) {
	excludes := map[string]struct{}{
		"posthog/events/2026/05/19/00/raw_a.parquet":  {},
		"posthog/events/2026/05/19/01/raw_b.parquet":  {},
		"crashlytics/crashes/2026/05/19/raw_c.parquet": {},
	}
	got := filterExcludesForTable(excludes, "default", "downloads")
	if len(got) != 0 {
		t.Errorf("expected 0 matches for default.downloads, got %d: %v", len(got), got)
	}
}

func TestFilterExcludesForTable_EmptyInputReturnsEmpty(t *testing.T) {
	got := filterExcludesForTable(nil, "default", "downloads")
	if len(got) != 0 {
		t.Errorf("expected empty map, got %d entries", len(got))
	}
}

func TestFilterExcludesForTable_RequiresTrailingSlash(t *testing.T) {
	// Should not match a different table that shares a prefix (e.g.
	// `default/downloads_test` should NOT be hit by `default/downloads`).
	excludes := map[string]struct{}{
		"default/downloads/2026/05/19/00/a.parquet":      {},
		"default/downloads_test/2026/05/19/00/b.parquet": {},
	}
	got := filterExcludesForTable(excludes, "default", "downloads")
	if len(got) != 1 {
		t.Errorf("expected 1 strict match, got %d: %v", len(got), got)
	}
	if _, ok := got["default/downloads/2026/05/19/00/a.parquet"]; !ok {
		t.Errorf("expected the exact-prefix file to be kept")
	}
}

func TestFilterExcludesForTable_EmptyTableIdentifiers_KeepsAll(t *testing.T) {
	// Defensive: when the caller couldn't extract db/measurement from the
	// path, return all excludes (current behaviour, pre-bug-fix) rather
	// than silently dropping everything.
	excludes := map[string]struct{}{
		"posthog/events/x.parquet": {},
		"default/downloads/y.parquet": {},
	}
	if got := filterExcludesForTable(excludes, "", "downloads"); len(got) != 2 {
		t.Errorf("empty database should fall through to all-keep: got %d", len(got))
	}
	if got := filterExcludesForTable(excludes, "default", ""); len(got) != 2 {
		t.Errorf("empty measurement should fall through to all-keep: got %d", len(got))
	}
}
