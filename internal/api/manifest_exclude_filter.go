package api

import "strings"

// filterExcludesForTable narrows the global compaction-exclude set down to
// only those files belonging to (database, measurement). Manifests are
// global across the cluster but each manifest's InputFiles all live under
// one table's storage prefix — there is no cross-table input in a single
// manifest. Filtering by prefix prevents a posthog compaction's
// `filename NOT IN (244 posthog files)` clause from being attached to
// every downloads query.
//
// Returns the input unchanged when either identifier is empty so the
// caller still gets exclusion semantics in legacy code paths that don't
// resolve a table.
func filterExcludesForTable(excludes map[string]struct{}, database, measurement string) map[string]struct{} {
	if database == "" || measurement == "" {
		out := make(map[string]struct{}, len(excludes))
		for k := range excludes {
			out[k] = struct{}{}
		}
		return out
	}
	// Trailing slash so `default/downloads/` doesn't match
	// `default/downloads_test/...`.
	prefix := database + "/" + measurement + "/"
	out := make(map[string]struct{})
	for k := range excludes {
		if strings.HasPrefix(k, prefix) {
			out[k] = struct{}{}
		}
	}
	return out
}
