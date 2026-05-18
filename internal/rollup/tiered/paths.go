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
// in the _arc/rollup/ layout. Schema mirrors source ingest layout:
//
//	_arc/rollup/<db>/<table>/<tier>/<YYYY>/<partition>/<variant>/<file_id>.parquet
//
// where <partition> depends on tier:
//
//	1h:   MM/DD/HH  (hour segment in UTC)
//	1d:   MM/DD
//	1w:   W<WW>     (ISO week, prefixed to keep distinct from a 2-digit month)
//	1mo:  MM
func VariantPath(table string, tier Tier, variant string, bucket time.Time, fileID string) string {
	b := bucket.UTC()
	prefix := fmt.Sprintf("_arc/rollup/%s/%s/%04d", tablePath(table), tier, b.Year())
	switch tier {
	case Tier1h:
		return fmt.Sprintf("%s/%02d/%02d/%02d/%s/%s.parquet",
			prefix, b.Month(), b.Day(), b.Hour(), variant, fileID)
	case Tier1d:
		return fmt.Sprintf("%s/%02d/%02d/%s/%s.parquet",
			prefix, b.Month(), b.Day(), variant, fileID)
	case Tier1w:
		_, week := b.ISOWeek()
		return fmt.Sprintf("%s/W%02d/%s/%s.parquet", prefix, week, variant, fileID)
	case Tier1mo:
		return fmt.Sprintf("%s/%02d/%s/%s.parquet", prefix, b.Month(), variant, fileID)
	}
	return ""
}

// ParseVariantPath parses a storage key produced by VariantPath and returns
// the fully-qualified table name, tier string, variant, and the half-open
// bucket window [bucketLo, bucketHi). All times are in UTC.
//
// The table name is reconstructed as "db.table" when the path has two table
// segments, or just "table" for un-dotted single-segment table names.
//
// Returns ok=false for any path that doesn't match the expected structure.
func ParseVariantPath(key string) (table, tier, variant string, bucketLo, bucketHi time.Time, ok bool) {
	// Layout: _arc/rollup/<table-path>/<tier>/<year>/<partition...>/<variant>/<file>.parquet
	// The table path is one or two slash-separated segments.
	// Tier is one of: 1h, 1d, 1w, 1mo — detected by scanning after the prefix.
	if !strings.HasPrefix(key, "_arc/rollup/") {
		return
	}
	parts := strings.Split(key, "/")
	// parts[0]="_arc" parts[1]="rollup" parts[2+]=table then tier then date segments
	// Scan from index 3 onward for the tier segment (allows 1 or 2 table segments).
	tierIdx := -1
	for i := 3; i < len(parts); i++ {
		p := parts[i]
		if p == "1h" || p == "1d" || p == "1w" || p == "1mo" {
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

	tierStr := parts[tierIdx]
	// parts after the tier: [year, ...date-segments..., variant, file.parquet]
	after := parts[tierIdx+1:]

	switch tierStr {
	case "1h":
		// after: [year, MM, DD, HH, variant, file.parquet] → len=6
		if len(after) != 6 {
			return
		}
		year, err0 := strconv.Atoi(after[0])
		month, err1 := strconv.Atoi(after[1])
		day, err2 := strconv.Atoi(after[2])
		hour, err3 := strconv.Atoi(after[3])
		if err0 != nil || err1 != nil || err2 != nil || err3 != nil {
			return
		}
		lo := time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC)
		tier = tierStr
		variant = after[4]
		bucketLo = lo
		bucketHi = lo.Add(time.Hour)
		ok = true

	case "1d":
		// after: [year, MM, DD, variant, file.parquet] → len=5
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
		tier = tierStr
		variant = after[3]
		bucketLo = lo
		bucketHi = lo.AddDate(0, 0, 1)
		ok = true

	case "1w":
		// after: [year, W<WW>, variant, file.parquet] → len=4
		if len(after) != 4 {
			return
		}
		year, err0 := strconv.Atoi(after[0])
		weekSeg := after[1]
		if err0 != nil || len(weekSeg) < 2 || weekSeg[0] != 'W' {
			return
		}
		week, err := strconv.Atoi(weekSeg[1:])
		if err != nil {
			return
		}
		lo := isoWeekMonday(year, week)
		tier = tierStr
		variant = after[2]
		bucketLo = lo
		bucketHi = lo.AddDate(0, 0, 7)
		ok = true

	case "1mo":
		// after: [year, MM, variant, file.parquet] → len=4
		if len(after) != 4 {
			return
		}
		year, err0 := strconv.Atoi(after[0])
		month, err1 := strconv.Atoi(after[1])
		if err0 != nil || err1 != nil {
			return
		}
		lo := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		tier = tierStr
		variant = after[2]
		bucketLo = lo
		bucketHi = lo.AddDate(0, 1, 0)
		ok = true
	}
	return
}

// isoWeekMonday returns the Monday (UTC midnight) of the given ISO year+week.
func isoWeekMonday(year, week int) time.Time {
	// Jan 4 is always in week 1 of its ISO year.
	jan4 := time.Date(year, time.January, 4, 0, 0, 0, 0, time.UTC)
	// Find Monday of week 1.
	wd := int(jan4.Weekday())
	if wd == 0 {
		wd = 7
	}
	week1Mon := jan4.AddDate(0, 0, 1-wd)
	return week1Mon.AddDate(0, 0, (week-1)*7)
}

// TmpPath returns a `tmp/` path under the table prefix for an in-flight build.
// Renamed to the final VariantPath after build success.
func TmpPath(table, fileID string) string {
	return fmt.Sprintf("_arc/rollup/%s/tmp/%s.parquet", tablePath(table), fileID)
}

// ManifestPath returns the manifest.json key for a table.
func ManifestPath(table string) string {
	return fmt.Sprintf("_arc/rollup/%s/manifest.json", tablePath(table))
}

// SpecPath returns the spec.json key for a table.
func SpecPath(table string) string {
	return fmt.Sprintf("_arc/rollup/%s/spec.json", tablePath(table))
}
