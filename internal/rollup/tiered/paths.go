package tiered

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// tablePath converts "db.table" → "db/table" so rollup paths mirror the
// source layout (s3://bucket/db/table/...). Tables without a dot pass through
// unchanged.
func tablePath(table string) string {
	return strings.Replace(table, ".", "/", 1)
}

// VariantPath returns the canonical storage key for a single parquet file
// in the _arc/rollup/ layout:
//
//	_arc/rollup/<db>/<table>/1h/<YYYY>/<MM>/<DD>/<variant>/<file_id>.parquet
//
// The 1h tier stores 24 hourly bucket rows per day inside the file; the
// directory partition is day-level.
func VariantPath(table string, tier Tier, variant string, bucket time.Time, fileID string) string {
	if tier != Tier1h {
		return ""
	}
	b := bucket.UTC()
	return fmt.Sprintf("_arc/rollup/%s/%s/%04d/%02d/%02d/%s/%s.parquet",
		tablePath(table), tier, b.Year(), b.Month(), b.Day(), variant, fileID)
}

// ParseVariantPath parses a storage key produced by VariantPath and returns
// the fully-qualified table name, tier string, variant, and the half-open
// bucket window [bucketLo, bucketHi). All times are in UTC.
//
// The table name is reconstructed as "db.table" when the path has two table
// segments, or just "table" for un-dotted single-segment table names.
//
// Returns ok=false for any path that doesn't match the expected structure
// (including legacy 1d/1w/1mo paths — those are intentionally ignored after
// the single-tier migration).
func ParseVariantPath(key string) (table, tier, variant string, bucketLo, bucketHi time.Time, ok bool) {
	if !strings.HasPrefix(key, "_arc/rollup/") {
		return
	}
	parts := strings.Split(key, "/")
	tierIdx := -1
	for i := 3; i < len(parts); i++ {
		if parts[i] == "1h" {
			tierIdx = i
			break
		}
	}
	if tierIdx < 0 {
		return
	}

	tableParts := parts[2:tierIdx]
	if len(tableParts) == 0 {
		return
	}
	if len(tableParts) == 1 {
		table = tableParts[0]
	} else {
		table = strings.Join(tableParts, ".")
	}

	after := parts[tierIdx+1:]
	if len(after) != 5 {
		return
	}
	year, err0 := strconv.Atoi(after[0])
	month, err1 := strconv.Atoi(after[1])
	day, err2 := strconv.Atoi(after[2])
	if err0 != nil || err1 != nil || err2 != nil {
		return
	}
	lo := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	tier = "1h"
	variant = after[3]
	bucketLo = lo
	bucketHi = lo.AddDate(0, 0, 1)
	ok = true
	return
}

// TmpPath returns a `tmp/` path under the table prefix for an in-flight build.
// Renamed to the final VariantPath after build success.
func TmpPath(table, fileID string) string {
	return fmt.Sprintf("_arc/rollup/%s/tmp/%s.parquet", tablePath(table), fileID)
}

// SpecPath returns the spec.json key for a table.
func SpecPath(table string) string {
	return fmt.Sprintf("_arc/rollup/%s/spec.json", tablePath(table))
}
