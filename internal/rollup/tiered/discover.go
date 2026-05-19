package tiered

import (
	"context"
	"fmt"

	"github.com/basekick-labs/arc/internal/storage"
)

// DiscoverTables enumerates every table under each given database prefix
// in the storage backend and returns fully-qualified names (`db.table`).
// Excludes anything matching the supplied glob patterns (typically
// Config.ExcludeTables — `*_late` is the standard guard).
//
// Used by the rollup scheduler when the operator hasn't enumerated tables
// in `[rollup.tables.*]` — turning the manual list into an auto-discovered
// one so adding a new ingest doesn't require a config bump.
//
// Requires the backend to implement storage.DirectoryLister; if it doesn't,
// returns an empty list (the legacy List-walk fallback used by
// handleShowTables is heavy and not needed here — auto-discovery is
// already opt-in).
func DiscoverTables(ctx context.Context, backend storage.Backend, databases, excludePatterns []string) ([]string, error) {
	lister, ok := backend.(storage.DirectoryLister)
	if !ok {
		return nil, fmt.Errorf("storage backend %T does not support DirectoryLister", backend)
	}

	// Cheap glob check sharing Config.IsExcluded semantics so the call sites
	// stay in lock-step. A throw-away config buys us the same matching.
	excl := &Config{ExcludeTables: excludePatterns}

	var out []string
	for _, db := range databases {
		tables, err := lister.ListDirectories(ctx, db+"/")
		if err != nil {
			return nil, fmt.Errorf("list tables in %q: %w", db, err)
		}
		for _, t := range tables {
			fqn := db + "." + t
			if excl.IsExcluded(fqn) {
				continue
			}
			out = append(out, fqn)
		}
	}
	return out, nil
}

// DiscoverDatabases enumerates the top-level prefixes in the backend that
// look like Arc databases (folders that have at least one table beneath
// them). Returns ["default"] when the lister isn't supported so callers
// have a safe fallback for typical single-database deployments.
func DiscoverDatabases(ctx context.Context, backend storage.Backend) ([]string, error) {
	lister, ok := backend.(storage.DirectoryLister)
	if !ok {
		return []string{"default"}, nil
	}
	dbs, err := lister.ListDirectories(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}
	// Filter the `_arc` namespace prefix used for rollup files and any other
	// leading-underscore meta-directories — those aren't user databases.
	out := make([]string, 0, len(dbs))
	for _, d := range dbs {
		if len(d) > 0 && d[0] == '_' {
			continue
		}
		out = append(out, d)
	}
	return out, nil
}
