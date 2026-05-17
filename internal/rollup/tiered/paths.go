package tiered

import (
	"fmt"
	"time"
)

// VariantPath returns the canonical storage key for a single parquet file
// in the precalc hive-partitioned layout. Schema:
//
//   precalc/table=<T>/tier=<G>/year=<Y>/[partition]/<variant>/<file_id>.parquet
//
// where [partition] depends on tier:
//   1h, 1d:   month=MM/day=DD
//   1w:       week=WW (ISO week)
//   1mo:      month=MM
func VariantPath(table string, tier Tier, variant string, bucket time.Time, fileID string) string {
	prefix := fmt.Sprintf("precalc/table=%s/tier=%s/year=%04d", table, tier, bucket.Year())
	switch tier {
	case Tier1h, Tier1d:
		return fmt.Sprintf("%s/month=%02d/day=%02d/%s/%s.parquet",
			prefix, bucket.Month(), bucket.Day(), variant, fileID)
	case Tier1w:
		_, week := bucket.ISOWeek()
		return fmt.Sprintf("%s/week=%02d/%s/%s.parquet", prefix, week, variant, fileID)
	case Tier1mo:
		return fmt.Sprintf("%s/month=%02d/%s/%s.parquet", prefix, bucket.Month(), variant, fileID)
	}
	return ""
}

// TmpPath returns a `tmp/` path under the table prefix for an in-flight build.
// Renamed to the final VariantPath after build success.
func TmpPath(table, fileID string) string {
	return fmt.Sprintf("precalc/table=%s/tmp/%s.parquet", table, fileID)
}

// ManifestPath returns the manifest.json key for a table.
func ManifestPath(table string) string {
	return fmt.Sprintf("precalc/table=%s/manifest.json", table)
}

// SpecPath returns the spec.json key for a table.
func SpecPath(table string) string {
	return fmt.Sprintf("precalc/table=%s/spec.json", table)
}
