package rollup

import (
	"database/sql"
	"fmt"
	"time"
)

// Execer runs SQL — satisfied by *sql.DB and *sql.Tx.
type Execer interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
}

// BuildDay materializes one UTC day of a cube from source into destURI (local
// path or s3://) and returns its manifest entry. The cube file holds one row per
// (hour-bucket × dims); the day's bucket bounds and row count come straight from
// the freshly written file so the manifest is exact.
//
// date is "YYYY-MM-DD" (UTC). sourceExpr is the read_parquet(...) argument for
// the source measurement (ideally already day-pruned by the caller).
func BuildDay(db Execer, s CubeSpec, sourceExpr, timeCol, date, destURI string) (DayEntry, error) {
	day, err := time.Parse("2006-01-02", date)
	if err != nil {
		return DayEntry{}, fmt.Errorf("bad date %q: %w", date, err)
	}
	return BuildRange(db, s, sourceExpr, timeCol, date, fmtTS(day.UTC()), fmtTS(day.UTC().Add(24*time.Hour)), destURI)
}

// BuildRange materializes an arbitrary [lo,hi) span of a cube into one file and
// returns its manifest entry. Used both for per-day forward builds and for
// coarser (per-month / full-range) one-shot builds (opt #3); pruning is by bucket
// bounds, so file granularity is a build-cost choice, not a correctness one.
//
// It first probes the source's physical columns (a footer-only DESCRIBE), so a
// spec column the drifted source lacks builds as a typed NULL instead of a
// Binder Error (see buildSelectFrom). A probe failure is a build failure — it
// propagates so the caller logs and retries, never silently degrading the cube.
func BuildRange(db Execer, s CubeSpec, sourceExpr, timeCol, label, lo, hi, destURI string) (DayEntry, error) {
	srcCols, err := describeColumnSet(db, readParquetFrom(sourceExpr))
	if err != nil {
		return DayEntry{}, fmt.Errorf("probe %s: %w", label, err)
	}
	return BuildRangeCols(db, s, sourceExpr, timeCol, label, lo, hi, destURI, srcCols)
}

// BuildRangeCols is BuildRange with the source's physical columns already probed
// (lower-cased name -> type), for callers that build many cubes over the same
// span and share one probe (e.g. the per-month build loop).
func BuildRangeCols(db Execer, s CubeSpec, sourceExpr, timeCol, label, lo, hi, destURI string, srcCols map[string]string) (DayEntry, error) {
	return runBuildCopy(db, s, s.buildCopySQLCols(sourceExpr, timeCol, lo, hi, destURI, srcCols), label, lo, hi, destURI)
}

// BuildFromTable materializes [lo,hi) of a cube from an already-populated temp
// table (one day's source read once and shared across all cubes — opt #1). The
// aggregation is identical to BuildRange, so the cube file is identical. The
// table's columns are probed (a local metadata query) for the same typed-NULL
// drift handling as BuildRange; probe errors propagate.
func BuildFromTable(db Execer, s CubeSpec, table, timeCol, label, lo, hi, destURI string) (DayEntry, error) {
	srcCols, err := describeColumnSet(db, table)
	if err != nil {
		return DayEntry{}, fmt.Errorf("probe %s: %w", label, err)
	}
	return runBuildCopy(db, s, s.BuildCopyFrom(table, timeCol, lo, hi, destURI, srcCols), label, lo, hi, destURI)
}

// runBuildCopy executes a build COPY then reads the written file's bucket bounds
// and row count back, so the manifest entry is exact.
func runBuildCopy(db Execer, s CubeSpec, copySQL, label, lo, hi, destURI string) (DayEntry, error) {
	if _, err := db.Exec(copySQL); err != nil {
		return DayEntry{}, fmt.Errorf("build %s: %w", label, err)
	}
	var rows int64
	var blo, bhi sql.NullString
	stat := fmt.Sprintf(
		"SELECT count(*), min(bucket)::VARCHAR, (max(bucket) + INTERVAL '1 %s')::VARCHAR FROM read_parquet('%s')",
		s.Grain, destURI)
	if err := db.QueryRow(stat).Scan(&rows, &blo, &bhi); err != nil {
		return DayEntry{}, fmt.Errorf("stat %s: %w", label, err)
	}
	if rows == 0 {
		return DayEntry{Date: label, URI: destURI, SchemaHash: s.SchemaHash(), BucketLo: lo, BucketHi: hi}, nil
	}
	return DayEntry{
		Date:       label,
		URI:        destURI,
		SchemaHash: s.SchemaHash(),
		BucketLo:   normalizeTS(blo.String),
		BucketHi:   normalizeTS(bhi.String),
		Rows:       rows,
	}, nil
}

// normalizeTS reparses a DuckDB timestamp string into our canonical literal form.
func normalizeTS(s string) string {
	if t, ok := parseTS(s); ok {
		return fmtTS(t)
	}
	return s
}

// SchemaHash identifies the physical schema of a cube (dims + store columns). Any
// change excludes old day files from a merge — the gate against schema drift that
// caused "column referenced before defined" errors in a prior generation.
func (s CubeSpec) SchemaHash() string {
	var sb []byte
	sb = append(sb, s.Grain...)
	for _, d := range s.Dims {
		sb = append(sb, '|')
		sb = append(sb, d...)
	}
	for _, sc := range s.orderedStoreCols() {
		sb = append(sb, '#')
		sb = append(sb, sc[0]...)
	}
	return fmt.Sprintf("%08x", fnv32(sb))
}

func fnv32(b []byte) uint32 {
	const prime = 16777619
	h := uint32(2166136261)
	for _, c := range b {
		h ^= uint32(c)
		h *= prime
	}
	return h
}
