package tiered

import (
	"fmt"
	"time"
)

// VariantPath returns the canonical storage key for a single parquet file
// in the _arc/rollup/ layout. Schema mirrors source ingest layout:
//
//   _arc/rollup/<table>/<tier>/<YYYY>/<partition>/<variant>/<file_id>.parquet
//
// where <partition> depends on tier:
//   1h, 1d:   MM/DD
//   1w:       W<WW>   (ISO week, prefixed to keep distinct from a 2-digit month)
//   1mo:      MM
func VariantPath(table string, tier Tier, variant string, bucket time.Time, fileID string) string {
	prefix := fmt.Sprintf("_arc/rollup/%s/%s/%04d", table, tier, bucket.Year())
	switch tier {
	case Tier1h, Tier1d:
		return fmt.Sprintf("%s/%02d/%02d/%s/%s.parquet",
			prefix, bucket.Month(), bucket.Day(), variant, fileID)
	case Tier1w:
		_, week := bucket.ISOWeek()
		return fmt.Sprintf("%s/W%02d/%s/%s.parquet", prefix, week, variant, fileID)
	case Tier1mo:
		return fmt.Sprintf("%s/%02d/%s/%s.parquet", prefix, bucket.Month(), variant, fileID)
	}
	return ""
}

// TmpPath returns a `tmp/` path under the table prefix for an in-flight build.
// Renamed to the final VariantPath after build success.
func TmpPath(table, fileID string) string {
	return fmt.Sprintf("_arc/rollup/%s/tmp/%s.parquet", table, fileID)
}

// ManifestPath returns the manifest.json key for a table.
func ManifestPath(table string) string {
	return fmt.Sprintf("_arc/rollup/%s/manifest.json", table)
}

// SpecPath returns the spec.json key for a table.
func SpecPath(table string) string {
	return fmt.Sprintf("_arc/rollup/%s/spec.json", table)
}
