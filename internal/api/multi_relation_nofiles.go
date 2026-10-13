package api

import (
	"context"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/metrics"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// "No files found" in a statement that reads several relations (P3Q-2).
//
// DuckDB raises "No files found" for the WHOLE statement when any one of its
// read_parquet globs matches nothing, and the handlers answer that as an
// empty 200: the contract for a measurement that has no files yet. That is
// only right when the statement reads one relation. When it reads several (a
// UNION ALL, a LEFT JOIN, a NOT IN (subquery), ...), the empty answer drops
// every other relation's rows, silently.
//
// A relation whose time range a listing verified empty is no longer narrowed
// to a no-files glob in such a statement (getTransformedSQL). What is left is
// a relation with no data files at all (never written, or all deleted): it
// has no schema to bind, so the statement has no correct answer, and the
// handlers answer 422 naming it instead of an empty 200.

// multiRelationNoFilesStatus is the status for such a statement.
const multiRelationNoFilesStatus = fiber.StatusUnprocessableEntity

// isMultiRelationNoFiles reports whether err, a "No files found" the handler
// would otherwise answer as an empty result, came from a transformed
// statement (executedSQL) that reads more than one relation. Statements whose
// reads the caller wrote (read_parquet in the request SQL), rollup-served
// statements (their source is one table) and unknown requests keep the empty
// result.
func (h *QueryHandler) isMultiRelationNoFiles(ctx context.Context, err error, executedSQL string) bool {
	if _, ok := noFilesPattern(err); !ok {
		return false
	}
	sq := sourceQueryFrom(ctx)
	if sq == nil || sq.rollupServed || strings.Contains(strings.ToLower(sq.reqSQL), "read_parquet") {
		return false
	}
	return sqlutil.CountReadParquetCalls(executedSQL) > 1
}

// multiRelationNoFilesDetail is the client-facing error for such a statement,
// naming the measurement whose files are missing.
func (h *QueryHandler) multiRelationNoFilesDetail(err error) string {
	pattern, _ := noFilesPattern(err)
	name := "a measurement it reads"
	if db, meas := h.extractDBMeasurementFromPath(pattern); db != "" && meas != "" {
		name = "measurement " + db + "." + meas
	}
	return name + " has no data files, and this statement reads it together with other relations: " +
		"an empty result would drop their rows (e.g. a LEFT JOIN or UNION ALL), so the statement cannot be answered. " +
		"Check the measurement name, or query it on its own"
}

// noteMultiRelationNoFiles counts and logs one statement refused this way.
func (h *QueryHandler) noteMultiRelationNoFiles(err error, sql string) {
	metrics.Get().IncQueryErrors()
	h.logger.Warn().Err(err).Str("sql", sqlutil.ForLog(sql)).
		Msg("Query refused (422): a relation of a multi-relation statement has no data files; an empty result would be wrong")
}
