package compaction

import (
	"context"
	"database/sql"
	"strings"

	"github.com/rs/zerolog"
)

// Column order (compaction.column_order) — which columns a compaction output
// writes first.
//
// Inside a Parquet row group the column chunks are stored one after another in
// schema order, and DuckDB's writer follows the SELECT list. A dashboard query
// reads, per row group it cannot skip, one byte range per column it touches. A
// low-cardinality string column is 1-15 KB per 122,880-row group, while the
// query pod's disk cache (cache_httpfs) reads whole 512 KB blocks, so every
// such column costs a block of its own when the chunks are spread over the row
// group. Writing the often-read small columns next to each other puts them in
// one range, usually one block. Bloom filters are written per row group in the
// same column order, so their probes cluster the same way.
//
// The order never changes a row or a value, and queries read files by name
// (read_parquet(..., union_by_name=true)), so files in the new and the old
// order can sit in one partition.

// presentColumnOrder returns the listed columns the batch's unified schema
// has, in the listed order and spelled as the schema spells them, and the
// listed columns it does not have. Matching is case-insensitive, as DuckDB's
// binding is. A listed column absent from every input is skipped, the way an
// absent sort key is (bindableSortKeys): naming it in the SELECT list or the
// EXCLUDE list would be a Binder Error on every cycle.
func presentColumnOrder(order []string, cols map[string]struct{}) (present, absent []string) {
	if len(order) == 0 {
		return nil, nil
	}
	byLower := make(map[string]string, len(cols))
	for c := range cols {
		byLower[strings.ToLower(c)] = c
	}
	seen := make(map[string]struct{}, len(order))
	for _, name := range order {
		key := strings.ToLower(name)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		if actual, ok := byLower[key]; ok {
			present = append(present, actual)
		} else {
			absent = append(absent, name)
		}
	}
	return present, absent
}

// columnOrderSelect builds the SELECT list that writes leading first and every
// other column after it in the input's order.
//
// timeExpr, when non-empty, is compaction's normalizing expression for "time"
// (timeNormalizeReplace, ending in AS "time"). With no leading columns the
// result is exactly the projection compaction used before column_order
// existed: `* REPLACE (timeExpr)`, or `*` when timeExpr is empty. When "time"
// is listed, timeExpr takes its slot, because a star cannot REPLACE a column
// it EXCLUDEs. coversAll (every column of the batch is listed) drops the star
// altogether rather than depend on how a DuckDB version treats a star that
// excludes everything.
func columnOrderSelect(leading []string, coversAll bool, timeExpr string) string {
	if len(leading) == 0 {
		if timeExpr == "" {
			return "*"
		}
		return "* REPLACE (" + timeExpr + ")"
	}
	items := make([]string, 0, len(leading)+1)
	quoted := make([]string, len(leading))
	timeListed := false
	for i, c := range leading {
		quoted[i] = quoteIdent(c)
		if timeExpr != "" && strings.EqualFold(c, "time") {
			items = append(items, timeExpr)
			timeListed = true
			continue
		}
		items = append(items, quoted[i])
	}
	if !coversAll {
		star := "* EXCLUDE (" + strings.Join(quoted, ", ") + ")"
		if timeExpr != "" && !timeListed {
			star += " REPLACE (" + timeExpr + ")"
		}
		items = append(items, star)
	}
	return strings.Join(items, ", ")
}

// quoteIdent double-quotes a column name for DuckDB, doubling embedded quotes
// (as buildOrderByClause does).
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// leadingColumns resolves a configured column order against the batch's
// unified schema (read with union_by_name from fileListSQL, footers only).
// It returns nil — the old layout — when no order is configured or the schema
// probe fails: the order only moves bytes, so a failed probe must not fail a
// compaction that would otherwise succeed.
func leadingColumns(ctx context.Context, db *sql.DB, fileListSQL string, order []string, logger zerolog.Logger) (leading []string, coversAll bool) {
	if len(order) == 0 {
		return nil, false
	}
	cols, err := parquetFilesColumnNames(ctx, db, fileListSQL)
	if err != nil {
		logger.Warn().Err(err).Strs("column_order", order).Msg("Failed to probe parquet schema for the column order; writing the inputs' column order")
		return nil, false
	}
	present, absent := presentColumnOrder(order, cols)
	if len(absent) > 0 {
		// Debug: a measurement-wide list routinely names columns a given
		// batch (an hour, an old day, another database) does not carry.
		logger.Debug().Strs("absent_columns", absent).Strs("leading_columns", present).Msg("Column order lists columns no input file has; skipping them")
	}
	return present, len(present) > 0 && len(present) == len(cols)
}
