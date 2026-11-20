package config

import (
	"fmt"
	"strings"
)

// ParseColumnOrder parses compaction.column_order: ["measurement:col1,col2", ...].
//
// Each entry names the columns a compaction output of that measurement writes
// FIRST, in the listed order; every other column follows in the order the
// batch already had. Inside a Parquet row group the column chunks are stored
// in schema order, so listing a query's small, often-read columns next to each
// other puts them in one contiguous byte range: one cache block instead of one
// per column. The order changes the bytes, never the rows or the values.
//
// Matching is case-insensitive (DuckDB binds identifiers that way), so a
// column listed twice in any case is rejected as a typo, as is a measurement
// with two entries. A listed column a batch does not have is skipped when the
// batch is compacted (internal/compaction), so the same list can serve every
// database and every schema version of the measurement.
func ParseColumnOrder(entries []string) (map[string][]string, error) {
	out := make(map[string][]string, len(entries))
	for _, entry := range entries {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid column order %q (expected 'measurement:col1,col2')", entry)
		}
		measurement := strings.TrimSpace(parts[0])
		if measurement == "" {
			return nil, fmt.Errorf("empty measurement name in column order %q", entry)
		}
		if _, dup := out[measurement]; dup {
			return nil, fmt.Errorf("measurement %q is listed more than once", measurement)
		}
		names := strings.Split(parts[1], ",")
		cols := make([]string, 0, len(names))
		seen := make(map[string]struct{}, len(names))
		for _, name := range names {
			name = strings.TrimSpace(name)
			if name == "" {
				return nil, fmt.Errorf("empty column name in column order %q", entry)
			}
			key := strings.ToLower(name)
			if _, dup := seen[key]; dup {
				return nil, fmt.Errorf("column %q listed twice in column order %q", name, entry)
			}
			seen[key] = struct{}{}
			cols = append(cols, name)
		}
		out[measurement] = cols
	}
	return out, nil
}
