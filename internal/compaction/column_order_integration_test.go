//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

// columnOrderJob is one compaction run of fixture files with a column order.
type columnOrderJob struct {
	sortKeys    []string
	columnOrder []string
	kv          string // KV_METADATA body for every fixture ("" = none)
	selects     []string
}

// run writes one fixture per select list into an hourly partition of
// measurement "events", compacts them with Job.Run (download, compaction SQL,
// upload, input delete) and returns the output's local path.
func (c columnOrderJob) run(t *testing.T, db *sql.DB) (*Job, string) {
	t.Helper()
	ctx := context.Background()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })

	const partition = "posthog/events/2026/10/28/01"
	keys := make([]string, len(c.selects))
	for i, sel := range c.selects {
		keys[i] = fmt.Sprintf("%s/f%d.parquet", partition, i)
		fixture := filepath.Join(t.TempDir(), fmt.Sprintf("in-%d.parquet", i))
		opts := "FORMAT PARQUET"
		if c.kv != "" {
			opts += ", KV_METADATA {" + c.kv + "}"
		}
		q := fmt.Sprintf(`COPY (SELECT %s) TO '%s' (%s)`, sel, escapeSQLPath(filepath.ToSlash(fixture)), opts)
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("write fixture %d: %v", i, err)
		}
		data, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Write(ctx, keys[i], data); err != nil {
			t.Fatal(err)
		}
	}
	job := NewJob(&JobConfig{
		Measurement: "events", PartitionPath: partition, Files: keys,
		StorageBackend: backend, Database: "posthog", Tier: "hourly",
		TempDirectory: t.TempDir(), Logger: zerolog.Nop(), DB: db,
		ManifestManager: NewManifestManager(backend, zerolog.Nop()),
		JobID:           "column-order", SortKeys: c.sortKeys, ColumnOrder: c.columnOrder,
	})
	if err := job.Run(ctx); err != nil {
		t.Fatalf("Job.Run: %v", err)
	}
	if job.Status != JobStatusCompleted || job.OutputStorageKey == "" {
		t.Fatalf("status %s, output %q", job.Status, job.OutputStorageKey)
	}
	uri, err := storage.ObjectURI(backend, job.OutputStorageKey)
	if err != nil {
		t.Fatal(err)
	}
	return job, filepath.ToSlash(uri)
}

// parquetColumns returns the file's physical column order (the order of the
// column chunks inside every row group) and each column's DuckDB type.
func parquetColumns(t *testing.T, db *sql.DB, path string) ([]string, map[string]string) {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(`SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('%s'))`, escapeSQLPath(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	types := map[string]string{}
	for rows.Next() {
		var n, ty string
		if err := rows.Scan(&n, &ty); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
		types[n] = ty
	}
	return names, types
}

// unifiedColumns is the column order of read_parquet(union_by_name) over the
// given fixtures: the order compaction wrote before column_order existed.
func unifiedColumns(t *testing.T, db *sql.DB, selects []string) []string {
	t.Helper()
	dir := t.TempDir()
	paths := make([]string, len(selects))
	for i, sel := range selects {
		paths[i] = "'" + escapeSQLPath(filepath.ToSlash(filepath.Join(dir, fmt.Sprintf("u%d.parquet", i)))) + "'"
		if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT %s) TO %s (FORMAT PARQUET)`, sel, paths[i])); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet([%s], union_by_name=true))`, strings.Join(paths, ", ")))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		_ = rows.Scan(&n)
		out = append(out, n)
	}
	return out
}

func queryStrings(t *testing.T, db *sql.DB, q string) []string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(cols))
		for i, v := range vals {
			if v.Valid {
				parts[i] = v.String
			} else {
				parts[i] = "NULL"
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func openDuck(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("SET TimeZone='UTC'"); err != nil {
		t.Fatal(err)
	}
	return db
}

// Three inputs whose columns come in three different orders, one with an
// extra column and one without "action"; the order lists a column no input
// has. The output must start with the listed columns that exist, in the
// listed order, keep every other column after them in the batch's own order,
// and hold exactly the input rows.
var driftedInputs = []string{
	`TIMESTAMPTZ '2026-10-28 01:00:02Z' AS time, 'd1' AS distinct_id, 'ev_b' AS event, 'tool_failed' AS action, 'iOS' AS os_name, 1 AS n`,
	`'ev_a' AS event, 2 AS n, 'Android' AS os_name, TIMESTAMPTZ '2026-10-28 01:00:01Z' AS time, 'd2' AS distinct_id, 'opened' AS action, 'x' AS extra`,
	`'d3' AS distinct_id, TIMESTAMPTZ '2026-10-28 01:00:03Z' AS time, 'ev_a' AS event, 3 AS n`,
}

func TestJobRunColumnOrder(t *testing.T) {
	db := openDuck(t)
	order := []string{"action", "event", "error_message", "os_name", "time"}
	job, out := columnOrderJob{sortKeys: []string{"event", "time"}, columnOrder: order, selects: driftedInputs}.run(t, db)
	if job.FilesCompacted != 3 {
		t.Fatalf("files compacted = %d, want 3", job.FilesCompacted)
	}

	names, types := parquetColumns(t, db, out)
	wantLead := []string{"action", "event", "os_name", "time"}
	if !reflect.DeepEqual(names[:len(wantLead)], wantLead) {
		t.Fatalf("output columns = %v, want them to start with %v", names, wantLead)
	}
	var wantRest []string
	for _, c := range unifiedColumns(t, db, driftedInputs) {
		if c != "action" && c != "event" && c != "os_name" && c != "time" {
			wantRest = append(wantRest, c)
		}
	}
	if !reflect.DeepEqual(names[len(wantLead):], wantRest) {
		t.Fatalf("columns after the listed ones = %v, want %v (the batch's own order)", names[len(wantLead):], wantRest)
	}
	if types["time"] != "TIMESTAMP WITH TIME ZONE" {
		t.Fatalf("time type = %s, want TIMESTAMP WITH TIME ZONE", types["time"])
	}

	// Rows in file order: the sort keys still apply (event, then time).
	got := queryStrings(t, db, fmt.Sprintf(`SELECT distinct_id, event, action, os_name, n, extra, epoch_us(time) FROM read_parquet('%s')`, escapeSQLPath(out)))
	want := []string{
		"d2|ev_a|opened|Android|2|x|1793149201000000",
		"d3|ev_a|NULL|NULL|3|NULL|1793149203000000",
		"d1|ev_b|tool_failed|iOS|1|NULL|1793149202000000",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("output rows =\n%v\nwant\n%v", got, want)
	}
}

// Without a column order the output keeps the batch's own column order.
func TestJobRunColumnOrderOffKeepsBatchOrder(t *testing.T) {
	db := openDuck(t)
	_, out := columnOrderJob{sortKeys: []string{"event", "time"}, selects: driftedInputs}.run(t, db)
	names, _ := parquetColumns(t, db, out)
	if want := unifiedColumns(t, db, driftedInputs); !reflect.DeepEqual(names, want) {
		t.Fatalf("output columns = %v, want the batch's order %v", names, want)
	}
}

// Every listed column absent: the same output as no order at all.
func TestJobRunColumnOrderAllAbsent(t *testing.T) {
	db := openDuck(t)
	_, out := columnOrderJob{sortKeys: []string{"event", "time"}, columnOrder: []string{"nope", "gone"}, selects: driftedInputs}.run(t, db)
	names, _ := parquetColumns(t, db, out)
	if want := unifiedColumns(t, db, driftedInputs); !reflect.DeepEqual(names, want) {
		t.Fatalf("output columns = %v, want the batch's order %v", names, want)
	}
}

// "time" listed, every column listed, and one input carrying time as a
// VARCHAR epoch: the time normalization must still apply in time's new slot.
func TestJobRunColumnOrderTimeListedCoversAllMixedTimeType(t *testing.T) {
	db := openDuck(t)
	inputs := []string{
		`TIMESTAMPTZ '2026-10-28 01:00:02Z' AS time, 'ev' AS event, 'a1' AS action`,
		`'1793149201000000' AS time, 'ev' AS event, 'a2' AS action`,
	}
	_, out := columnOrderJob{sortKeys: []string{"time"}, columnOrder: []string{"time", "action", "event"}, selects: inputs}.run(t, db)
	names, types := parquetColumns(t, db, out)
	if want := []string{"time", "action", "event"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("output columns = %v, want %v", names, want)
	}
	if types["time"] != "TIMESTAMP WITH TIME ZONE" {
		t.Fatalf("time type = %s, want TIMESTAMP WITH TIME ZONE", types["time"])
	}
	got := queryStrings(t, db, fmt.Sprintf(`SELECT action, epoch_us(time) FROM read_parquet('%s')`, escapeSQLPath(out)))
	if want := []string{"a2|1793149201000000", "a1|1793149202000000"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("output rows = %v, want %v", got, want)
	}
}

// Inputs with an arc:tags footer take the staged (three-statement) dedup
// path; the order applies there too and the dedup result is unchanged.
func TestJobRunColumnOrderTaggedDedup(t *testing.T) {
	db := openDuck(t)
	inputs := []string{
		// a duplicate row across files, and a same-key row that differs in a field
		`TIMESTAMPTZ '2026-10-28 01:00:01Z' AS time, 'h1' AS host, 1.5 AS value, 'x' AS action`,
		`TIMESTAMPTZ '2026-10-28 01:00:01Z' AS time, 'h1' AS host, 1.5 AS value, 'x' AS action`,
		`'y' AS action, 2.5 AS value, 'h1' AS host, TIMESTAMPTZ '2026-10-28 01:00:01Z' AS time`,
	}
	_, out := columnOrderJob{columnOrder: []string{"action", "value"}, kv: `'arc:tags': 'host'`, selects: inputs}.run(t, db)
	names, _ := parquetColumns(t, db, out)
	if !reflect.DeepEqual(names[:2], []string{"action", "value"}) || len(names) != 4 {
		t.Fatalf("output columns = %v, want action, value first and 4 columns", names)
	}
	got := queryStrings(t, db, fmt.Sprintf(`SELECT host, value::VARCHAR, action FROM read_parquet('%s') ORDER BY action`, escapeSQLPath(out)))
	if want := []string{"h1|1.5|x", "h1|2.5|y"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("output rows = %v, want %v (one copy of the duplicate, both distinct rows)", got, want)
	}
}

// Queries read a partition's files with read_parquet([...], union_by_name=true),
// so files written with the new order sit next to files in the old order. A
// by-name read must line every value up with its column, including columns
// only one side has, and give the same answers as before the reorder.
func TestColumnOrderOutputsReadByNameNextToOldFiles(t *testing.T) {
	db := openDuck(t)
	_, reordered := columnOrderJob{sortKeys: []string{"event", "time"}, columnOrder: []string{"action", "event", "os_name", "time"}, selects: driftedInputs}.run(t, db)
	_, old := columnOrderJob{sortKeys: []string{"event", "time"}, selects: []string{
		`TIMESTAMPTZ '2026-10-28 01:00:04Z' AS time, 'd4' AS distinct_id, 'ev_c' AS event, 'opened' AS action, 7 AS n, 'only_old' AS old_only`,
	}}.run(t, db)

	q := `SELECT distinct_id, event, action, os_name, n, extra, old_only, epoch_us(time) FROM read_parquet([%s], union_by_name=true) ORDER BY distinct_id`
	both := fmt.Sprintf(q, fmt.Sprintf("'%s', '%s'", escapeSQLPath(reordered), escapeSQLPath(old)))
	reversed := fmt.Sprintf(q, fmt.Sprintf("'%s', '%s'", escapeSQLPath(old), escapeSQLPath(reordered)))
	want := []string{
		"d1|ev_b|tool_failed|iOS|1|NULL|NULL|1793149202000000",
		"d2|ev_a|opened|Android|2|x|NULL|1793149201000000",
		"d3|ev_a|NULL|NULL|3|NULL|NULL|1793149203000000",
		"d4|ev_c|opened|NULL|7|NULL|only_old|1793149204000000",
	}
	for name, sqlText := range map[string]string{"new first": both, "old first": reversed} {
		if got := queryStrings(t, db, sqlText); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: union_by_name rows =\n%v\nwant\n%v", name, got, want)
		}
	}
	// A filtered aggregate, the shape the dashboards send.
	agg := fmt.Sprintf(`SELECT event, count(*), count(DISTINCT distinct_id) FROM read_parquet(['%s', '%s'], union_by_name=true) WHERE action = 'opened' GROUP BY event ORDER BY event`, escapeSQLPath(old), escapeSQLPath(reordered))
	if got, want := queryStrings(t, db, agg), []string{"ev_a|1|1", "ev_c|1|1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("aggregate = %v, want %v", got, want)
	}
}

// "time" NOT listed: the select list is `<listed>, * EXCLUDE (<listed>) REPLACE
// (<time normalization>)`. One input carries time as a VARCHAR epoch: the
// REPLACE must still normalize it, tagless (DISTINCT) and tagged (staged).
func TestJobRunColumnOrderTimeNotListedMixedTimeType(t *testing.T) {
	inputs := []string{
		`TIMESTAMPTZ '2026-10-28 01:00:02Z' AS time, 'h1' AS host, 'ev' AS event, 'a1' AS action`,
		`'1793149201000000' AS time, 'h1' AS host, 'ev' AS event, 'a2' AS action`,
	}
	for name, kv := range map[string]string{"tagless": "", "tagged": `'arc:tags': 'host'`} {
		t.Run(name, func(t *testing.T) {
			db := openDuck(t)
			_, out := columnOrderJob{sortKeys: []string{"event", "time"}, columnOrder: []string{"action", "event"}, kv: kv, selects: inputs}.run(t, db)
			names, types := parquetColumns(t, db, out)
			if !reflect.DeepEqual(names[:2], []string{"action", "event"}) || len(names) != 4 {
				t.Fatalf("output columns = %v, want action, event first and 4 columns", names)
			}
			if types["time"] != "TIMESTAMP WITH TIME ZONE" {
				t.Fatalf("time type = %s, want TIMESTAMP WITH TIME ZONE (the REPLACE did not apply)", types["time"])
			}
			got := queryStrings(t, db, fmt.Sprintf(`SELECT action, epoch_us(time) FROM read_parquet('%s') ORDER BY time`, escapeSQLPath(out)))
			if want := []string{"a2|1793149201000000", "a1|1793149202000000"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("output rows = %v, want %v", got, want)
			}
		})
	}
}

// Listed names in another case than the files spell them: the output uses
// the files' spelling, in the listed order, and no column is duplicated or
// lost (EXCLUDE matches case-insensitively too).
func TestJobRunColumnOrderCaseMismatch(t *testing.T) {
	db := openDuck(t)
	inputs := []string{`TIMESTAMPTZ '2026-10-28 01:00:01Z' AS time, 'ev' AS Event, 'x' AS action, 1 AS n`}
	_, out := columnOrderJob{sortKeys: []string{"time"}, columnOrder: []string{"ACTION", "event", "Time"}, selects: inputs}.run(t, db)
	names, types := parquetColumns(t, db, out)
	if want := []string{"action", "Event", "time", "n"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("output columns = %v, want %v", names, want)
	}
	if types["time"] != "TIMESTAMP WITH TIME ZONE" {
		t.Fatalf("time type = %s", types["time"])
	}
	if got := queryStrings(t, db, fmt.Sprintf(`SELECT action, Event, n FROM read_parquet('%s')`, escapeSQLPath(out))); !reflect.DeepEqual(got, []string{"x|ev|1"}) {
		t.Fatalf("rows = %v", got)
	}
}

// Nested types and names that need quoting (space, double quote, dot) keep
// their values and types when listed, and when left to the star.
func TestJobRunColumnOrderNestedTypesAndQuotedNames(t *testing.T) {
	db := openDuck(t)
	inputs := []string{
		`TIMESTAMPTZ '2026-10-28 01:00:01Z' AS time, {'k': 1, 'v': 'a'} AS s, [1, 2] AS l, 'sp' AS "my col", 'q' AS "we""ird", 'dot' AS "a.b", 'rest' AS other, ['x'] AS l2`,
		`TIMESTAMPTZ '2026-10-28 01:00:02Z' AS time, {'k': 2, 'v': 'b'} AS s, [3] AS l, 'sp2' AS "my col", 'q2' AS "we""ird", 'dot2' AS "a.b", 'rest2' AS other, ['y', 'z'] AS l2`,
	}
	order := []string{`we"ird`, "a.b", "s", "my col", "l"}
	_, out := columnOrderJob{sortKeys: []string{"time"}, columnOrder: order, selects: inputs}.run(t, db)
	names, types := parquetColumns(t, db, out)
	if want := []string{`we"ird`, "a.b", "s", "my col", "l", "time", "other", "l2"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("output columns = %v, want %v", names, want)
	}
	if !strings.HasPrefix(types["s"], "STRUCT(") || types["l"] != "INTEGER[]" || types["l2"] != "VARCHAR[]" {
		t.Fatalf("nested types changed: s=%s l=%s l2=%s", types["s"], types["l"], types["l2"])
	}
	got := queryStrings(t, db, fmt.Sprintf(`SELECT "we""ird", "a.b", s.k, s.v, "my col", l::VARCHAR, other, l2::VARCHAR FROM read_parquet('%s') ORDER BY time`, escapeSQLPath(out)))
	want := []string{"q|dot|1|a|sp|[1, 2]|rest|[x]", "q2|dot2|2|b|sp2|[3]|rest2|[y, z]"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
}
