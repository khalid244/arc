package rollup

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// AggFunction is one of the pre-computed aggregation functions stored in a rollup column.
type AggFunction string

const (
	AggSum     AggFunction = "sum"
	AggCount   AggFunction = "count"
	AggMin     AggFunction = "min"
	AggMax     AggFunction = "max"
	AggHLL     AggFunction = "hll"
	AggTDigest AggFunction = "tdigest"
)

// SketchConfig pins sketch sizing. Changing these invalidates merge compatibility
// with existing rollup data.
type SketchConfig struct {
	HLLLgK   int `json:"hll_lg_k"`  // 12 → ~16 KB per sketch, ~1% error
	TDigestK int `json:"tdigest_k"` // 100 → t-digest library default, ~tight P50–P99
}

// Aggregation describes the pre-aggregations to maintain for one source column.
type Aggregation struct {
	SourceColumn string        `json:"source_column"`
	Functions    []AggFunction `json:"functions"`
	SketchConfig *SketchConfig `json:"sketch_config,omitempty"`
}

// DroppedColumn records a column that was dropped from the rollup, for transparency.
type DroppedColumn struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// RollupSpec is a fully-resolved rollup definition.
type RollupSpec struct {
	Name           string        `json:"name"` // e.g. "analytics__events__1h"
	Database       string        `json:"database"`
	SourceTable    string        `json:"source_table"`
	BucketColumn   string        `json:"bucket_column"`
	BucketInterval time.Duration `json:"bucket_interval"`
	KeepDimensions []string      `json:"keep_dimensions,omitempty"`
	// NotNull lists kept dimension columns the operator guarantees are
	// non-null in the source. Enables COUNT(col) → SUM(__row_count) rewrites
	// (which would otherwise overcount NULLs). Opt-in only; the proposer
	// auto-populates this when 0/N samples have NULL.
	NotNull        []string        `json:"not_null,omitempty"`
	DroppedColumns []DroppedColumn `json:"dropped_columns,omitempty"`
	Aggregations   []Aggregation   `json:"aggregations,omitempty"`
	// KeyTable is the table component of the TOML section key. It drives the
	// rollup output's parquet directory name and lets multiple variants of
	// the same source table produce distinct names.
	KeyTable string `json:"key_table,omitempty"`
}

// Validate returns an error if the spec is malformed. Validation is structural
// only — it does not check that columns exist in the source table.
func (s *RollupSpec) Validate() error {
	if s.Name == "" {
		return errors.New("name is required")
	}
	if s.Database == "" {
		return errors.New("database is required")
	}
	if s.SourceTable == "" {
		return errors.New("source_table is required")
	}
	if s.BucketColumn == "" {
		return errors.New("bucket_column is required")
	}
	if s.BucketInterval <= 0 {
		return errors.New("bucket_interval must be > 0")
	}
	for i, agg := range s.Aggregations {
		if agg.SourceColumn == "" {
			return fmt.Errorf("aggregations[%d]: source_column is required", i)
		}
		if len(agg.Functions) == 0 {
			return fmt.Errorf("aggregations[%d]: at least one function is required", i)
		}
		needsSketch := false
		for _, f := range agg.Functions {
			if f == AggHLL || f == AggTDigest {
				needsSketch = true
			}
		}
		if needsSketch && agg.SketchConfig == nil {
			return fmt.Errorf("aggregations[%d]: sketch_config is required when using hll/tdigest", i)
		}
	}
	return nil
}

// RollupTableName returns the convention name for the rollup's Parquet table:
// "<key_table>__<interval-shorthand>", e.g. "events__1h".
// When KeyTable is set (populated from the TOML section key), it is used so
// that multiple variants referencing the same source table produce distinct
// rollup table names. Falls back to SourceTable for specs constructed directly
// (e.g. in tests that don't go through config parsing).
func (s *RollupSpec) RollupTableName() string {
	nameTable := s.KeyTable
	if nameTable == "" {
		nameTable = s.SourceTable
	}
	return fmt.Sprintf("%s__%s", nameTable, intervalShorthand(s.BucketInterval))
}

// Fingerprint returns a stable hash of the spec's shape: the fields that
// determine the columns the builder emits and the bucket boundary it groups
// by. A change to KeepDimensions, Aggregations, BucketColumn, or
// BucketInterval flips the fingerprint; cosmetic fields (Name, Database,
// SourceTable) are excluded — they identify *which* rollup, not what it
// looks like. Used by the scheduler to detect config drift and reset the
// watermark for variants whose shape changed.
//
// 16 hex chars (64 bits of SHA-256) — collision-resistant enough for the
// "is this the same shape" check while keeping the watermark file small.
func (s *RollupSpec) Fingerprint() string {
	h := sha256.New()
	fmt.Fprintf(h, "bucket_col=%s\n", s.BucketColumn)
	fmt.Fprintf(h, "bucket_interval=%d\n", int64(s.BucketInterval))
	dims := append([]string(nil), s.KeepDimensions...)
	sort.Strings(dims)
	fmt.Fprintf(h, "dims=%s\n", strings.Join(dims, ","))
	aggs := make([]string, 0, len(s.Aggregations))
	for _, a := range s.Aggregations {
		fns := make([]string, 0, len(a.Functions))
		for _, f := range a.Functions {
			fns = append(fns, string(f))
		}
		sort.Strings(fns)
		entry := a.SourceColumn + ":" + strings.Join(fns, "+")
		if a.SketchConfig != nil {
			entry += fmt.Sprintf("(hll=%d,td=%d)", a.SketchConfig.HLLLgK, a.SketchConfig.TDigestK)
		}
		aggs = append(aggs, entry)
	}
	sort.Strings(aggs)
	fmt.Fprintf(h, "aggs=%s\n", strings.Join(aggs, ";"))
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// rollupTableNamePattern matches directory names produced by RollupTableName:
// any string ending in `__<digits><smhdw>`. Used by callers that walk the
// storage backend to enumerate source tables; without this filter, second-
// start discovery would re-classify rollup output directories as source
// tables and spawn rollups-of-rollups.
var rollupTableNamePattern = regexp.MustCompile(`__\d+[smhdw]$`)

// IsRollupTableName reports whether name follows the rollup output naming
// convention (`<key>__<interval-shorthand>`). Used by table-discovery passes
// to exclude rollup directories from the source-table set.
func IsRollupTableName(name string) bool {
	return rollupTableNamePattern.MatchString(name)
}

// intervalShorthand renders a duration in human form: 5m, 1h, 1d, 1w.
func intervalShorthand(d time.Duration) string {
	switch {
	case d%(7*24*time.Hour) == 0:
		return fmt.Sprintf("%dw", int(d/(7*24*time.Hour)))
	case d%(24*time.Hour) == 0:
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	default:
		return fmt.Sprintf("%ds", int(d/time.Second))
	}
}
