// Package-level lifecycle:
//
// Schema inference runs after the first full ingest hour for a table. It is
// idempotent and re-runs only when the source schema changes. The output is a
// deterministic InferredSchema that drives spec generation (Task 3) and the
// builder (Task 4). This file is pure logic — no I/O, no DuckDB — so it can
// be unit-tested against synthetic ColumnStats fixtures.
//
// User hints (TableConfig.SketchColumns / IgnoreColumns / QuantileColumns /
// TimeColumn) are applied as overrides and short-circuit detection in that
// order. Hints never *add* inference behavior — they only override defaults.
package rollup

import (
	"fmt"
	"strings"
)

type ColumnRole int

const (
	// RoleUnknown is the zero value. ClassifyColumn never returns it.
	RoleUnknown ColumnRole = iota
	RoleTime
	RoleDim
	RoleMetric
	RoleSketch
	RoleDrop
)

func (r ColumnRole) String() string {
	switch r {
	case RoleTime:
		return "time"
	case RoleDim:
		return "dim"
	case RoleMetric:
		return "metric"
	case RoleSketch:
		return "sketch"
	case RoleDrop:
		return "drop"
	}
	return "unknown"
}

const (
	// metricCardinalityFloor is the exclusive threshold above which a numeric
	// column is treated as a continuous metric (gets t-digest). At or below,
	// it's an enum-like discrete metric (no t-digest).
	metricCardinalityFloor = 100

	// enumDimCardinality is the inclusive cap below which a numeric column
	// is treated as RoleDim instead of RoleMetric: HTTP status codes,
	// boolean flags as 0/1 ints, small enums where users filter `WHERE
	// col = N` rather than aggregate.
	enumDimCardinality = 32

	// Defaults for string-column classification, used when ThresholdConfig
	// is zero (test fixtures that don't set thresholds).
	defaultDimCardinalityMax    = 1024
	defaultSketchCardinalityMax = 100000
)

// ThresholdConfig carries the user-tunable string-column thresholds. Zero
// fields fall back to defaults (`defaultDimCardinalityMax`,
// `defaultSketchCardinalityMax`).
type ThresholdConfig struct {
	DimCardinalityMax    int64
	SketchCardinalityMax int64
}

func (t ThresholdConfig) dimMax() int64 {
	if t.DimCardinalityMax > 0 {
		return t.DimCardinalityMax
	}
	return defaultDimCardinalityMax
}

func (t ThresholdConfig) sketchMax() int64 {
	if t.SketchCardinalityMax > 0 {
		return t.SketchCardinalityMax
	}
	return defaultSketchCardinalityMax
}

// ColumnStats is the sample-derived statistics for one column.
type ColumnStats struct {
	Name     string
	Type     string // DuckDB type name ("VARCHAR", "DOUBLE", "TIMESTAMPTZ", ...)
	Distinct int64
}

// ClassifiedColumn is the result of inference.
type ClassifiedColumn struct {
	Name    string
	Role    ColumnRole
	HLL     bool // emit HLL sketch column
	TDigest bool // emit t-digest sketch column
	// HighCard is set when the column is RoleDim only because the user
	// force-kept it via keep_columns despite its high cardinality. Spec
	// generation excludes HighCard dims from the dim-rich cross-product
	// variant (which would explode row count) but still emits a per-dim
	// `by_<col>__1d` variant.
	HighCard bool
}

// InferredSchema is the per-table inference result.
type InferredSchema struct {
	TimeColumn string
	Columns    []ClassifiedColumn
}

// ClassifyColumn applies inference rules to a single column with optional user
// hints and thresholds. Classification is purely data-driven (no name patterns
// — *_id / *_uuid suffix matching was deliberately removed): a column's role
// is a deterministic function of type + cardinality. Users opt specific
// columns in / out via hints (sketch_columns, keep_columns, ignore_columns).
func ClassifyColumn(col ColumnStats, hints TableConfig, th ThresholdConfig) ClassifiedColumn {
	if hasString(hints.IgnoreColumns, col.Name) {
		return ClassifiedColumn{Name: col.Name, Role: RoleDrop}
	}
	if hasString(hints.SketchColumns, col.Name) {
		return ClassifiedColumn{Name: col.Name, Role: RoleSketch, HLL: true}
	}
	// keep_columns forces RoleDim regardless of cardinality. HighCard=true so
	// downstream spec generation excludes it from the multi-dim variant.
	if hasString(hints.KeepColumns, col.Name) {
		return ClassifiedColumn{Name: col.Name, Role: RoleDim, HighCard: col.Distinct > th.dimMax()}
	}
	lt := strings.ToLower(col.Type)
	// Match both TIMESTAMPTZ (tz-aware) and naive TIMESTAMP. The naive widening
	// is safe because Arc pins SET TimeZone='UTC' per DuckDB connection (see
	// internal/database/duckdb.go), so naive literals land on UTC bucket
	// boundaries regardless of the source column's type variant.
	if strings.HasPrefix(lt, "timestamp") {
		return ClassifiedColumn{Name: col.Name, Role: RoleTime}
	}
	if isNumericType(col.Type) {
		// Very low-cardinality numerics (≤ enumDimCardinality) are
		// enum-like codes (HTTP status, boolean flags, error categories)
		// where users filter `WHERE col = N` far more often than they
		// aggregate. Classifying them as RoleDim makes those filters
		// rollup-eligible; SUM/MIN/MAX of a status code is meaningless
		// anyway, so we don't lose useful aggregations.
		if col.Distinct > 0 && col.Distinct <= enumDimCardinality {
			return ClassifiedColumn{Name: col.Name, Role: RoleDim}
		}
		out := ClassifiedColumn{Name: col.Name, Role: RoleMetric}
		// t-digest only on continuous metrics, and only if user hasn't restricted via QuantileColumns
		if col.Distinct > metricCardinalityFloor {
			if len(hints.QuantileColumns) == 0 || hasString(hints.QuantileColumns, col.Name) {
				out.TDigest = true
			}
		}
		return out
	}
	if isStringType(col.Type) || isBoolType(col.Type) {
		// Three-zone string classification, all data-driven:
		//   ≤ dim_cardinality_max         → RoleDim (per-dim variant)
		//   ≤ sketch_cardinality_max      → RoleSketch HLL (COUNT DISTINCT)
		//   > sketch_cardinality_max      → RoleDrop (too high-card for HLL)
		//
		// HighCard split: when the user raises dim_cardinality_max above the
		// safe cross-product cap (defaultDimCardinalityMax = 1024), columns
		// in the (1024, dim_cardinality_max] band become RoleDim with
		// HighCard=true. They get their own per-dim `by_<col>__1d` variant
		// (cheap, ~10 MB each) but stay OUT of the dim-rich cross-product
		// variant — which would explode by their cardinality factor.
		if col.Distinct <= 0 {
			return ClassifiedColumn{Name: col.Name, Role: RoleDrop}
		}
		if col.Distinct <= th.dimMax() {
			return ClassifiedColumn{
				Name:     col.Name,
				Role:     RoleDim,
				HighCard: col.Distinct > defaultDimCardinalityMax,
			}
		}
		if col.Distinct <= th.sketchMax() {
			return ClassifiedColumn{Name: col.Name, Role: RoleSketch, HLL: true}
		}
		return ClassifiedColumn{Name: col.Name, Role: RoleDrop}
	}
	return ClassifiedColumn{Name: col.Name, Role: RoleDrop}
}

// InferSchema classifies every column and resolves the time column. th
// carries user-tunable string-cardinality thresholds; zero values fall
// back to package defaults.
func InferSchema(stats []ColumnStats, hints TableConfig, th ThresholdConfig) (InferredSchema, error) {
	var out InferredSchema
	timeCols := []string{}
	for _, c := range stats {
		cc := ClassifyColumn(c, hints, th)
		if cc.Role == RoleTime {
			timeCols = append(timeCols, c.Name)
		}
		out.Columns = append(out.Columns, cc)
	}
	if hints.TimeColumn != "" {
		out.TimeColumn = hints.TimeColumn
		return out, nil
	}
	if len(timeCols) == 1 {
		out.TimeColumn = timeCols[0]
		return out, nil
	}
	if len(timeCols) == 0 {
		return out, fmt.Errorf("schema inference: no TIMESTAMP/TIMESTAMPTZ column found; declare `time_column` in [rollup.tables.\"...\"]")
	}
	return out, fmt.Errorf("schema inference: %d TIMESTAMP/TIMESTAMPTZ columns (%v); declare `time_column` to disambiguate", len(timeCols), timeCols)
}

func isBoolType(t string) bool {
	u := strings.ToUpper(t)
	return u == "BOOLEAN" || u == "BOOL"
}

func isStringType(t string) bool {
	t = strings.ToLower(t)
	for _, k := range []string{"varchar", "text", "char", "blob", "string", "clob"} {
		if strings.Contains(t, k) {
			return true
		}
	}
	return false
}

func isNumericType(t string) bool {
	t = strings.ToLower(t)
	for _, k := range []string{"int", "double", "float", "decimal", "numeric", "real", "bigint", "smallint", "tinyint", "hugeint"} {
		if strings.Contains(t, k) {
			return true
		}
	}
	return false
}

// hasString reports whether v appears in s, case-insensitively. Used for
// matching user-supplied hint columns (sketch_columns / ignore_columns /
// quantile_columns) against DuckDB column names, which can differ in case.
func hasString(s []string, v string) bool {
	lv := strings.ToLower(v)
	for _, x := range s {
		if strings.ToLower(x) == lv {
			return true
		}
	}
	return false
}
