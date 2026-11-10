//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

// runSortKeyJob writes one single-row fixture per select list into an hourly
// partition of measurement "events", compacts it with the given sort keys, and
// returns the job plus the output's (event, distinct_id) rows in file order.
func runSortKeyJob(t *testing.T, sortKeys []string, selectLists ...string) (*Job, [][2]sql.NullString, error) {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })

	const partition = "otherdb/events/2026/10/28/01"
	keys := make([]string, len(selectLists))
	for i, sel := range selectLists {
		keys[i] = fmt.Sprintf("%s/f%d.parquet", partition, i)
		fixture := filepath.Join(t.TempDir(), fmt.Sprintf("in-%d.parquet", i))
		writeFixtureParquet(t, ctx, db, fixture, sel)
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
		StorageBackend: backend, Database: "otherdb", Tier: "hourly",
		TempDirectory: t.TempDir(), Logger: zerolog.Nop(), DB: db,
		ManifestManager: NewManifestManager(backend, zerolog.Nop()),
		JobID:           "sortkey-absent", SortKeys: sortKeys,
	})
	runErr := job.Run(ctx)
	if runErr != nil || job.OutputStorageKey == "" {
		return job, nil, runErr
	}

	uri, err := storage.ObjectURI(backend, job.OutputStorageKey)
	if err != nil {
		t.Fatal(err)
	}
	// event is projected through a DESCRIBE-safe COALESCE so the same query
	// works whether or not the output has an "event" column.
	src := fmt.Sprintf("read_parquet('%s')", escapeSQLPath(filepath.ToSlash(uri)))
	var hasEvent int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM (DESCRIBE SELECT * FROM %s) WHERE column_name = 'event'`, src)).Scan(&hasEvent); err != nil {
		t.Fatal(err)
	}
	eventExpr := "NULL::VARCHAR"
	if hasEvent > 0 {
		eventExpr = "event"
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT %s, distinct_id FROM %s`, eventExpr, src))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out [][2]sql.NullString
	for rows.Next() {
		var r [2]sql.NullString
		if err := rows.Scan(&r[0], &r[1]); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return job, out, nil
}

// TestJobRunSortKeyColumnAbsentFromEveryFile is the regression for a
// measurement-level sort key ("events:event,time") hitting an "events" table —
// in another database, or after schema drift — whose files have no "event"
// column. ORDER BY "event", "time" used to fail to bind (a Binder Error,
// classified non-recoverable), so the partition failed every cycle and was
// never compacted. The absent key must be dropped and the batch ordered by the
// remaining keys.
func TestJobRunSortKeyColumnAbsentFromEveryFile(t *testing.T) {
	job, rows, err := runSortKeyJob(t, []string{"event", "time"},
		`TIMESTAMPTZ '2026-10-28 01:00:02Z' AS time, 'b' AS distinct_id`,
		`TIMESTAMPTZ '2026-10-28 01:00:01Z' AS time, 'a' AS distinct_id`,
	)
	if err != nil {
		t.Fatalf("Job.Run with a sort key absent from every input file: %v", err)
	}
	if job.Status != JobStatusCompleted || job.FilesCompacted != 2 {
		t.Fatalf("status/files = %s/%d, want completed/2", job.Status, job.FilesCompacted)
	}
	got := make([]string, len(rows))
	for i, r := range rows {
		got[i] = r[1].String
	}
	if want := []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("output order = %v, want %v (ordered by the remaining key, time)", got, want)
	}
}

// TestJobRunSortKeyColumnInSomeFiles: when only some files carry the key,
// union_by_name NULL-fills the rest and the configured order applies as is
// (NULL event sorts last, DuckDB's default).
func TestJobRunSortKeyColumnInSomeFiles(t *testing.T) {
	job, rows, err := runSortKeyJob(t, []string{"event", "time"},
		`TIMESTAMPTZ '2026-10-28 01:00:00Z' AS time, 'noev' AS distinct_id`,
		`TIMESTAMPTZ '2026-10-28 01:00:03Z' AS time, 'z1' AS distinct_id, 'a_event' AS event`,
		`TIMESTAMPTZ '2026-10-28 01:00:01Z' AS time, 'nullev' AS distinct_id, NULL::VARCHAR AS event`,
		`TIMESTAMPTZ '2026-10-28 01:00:02Z' AS time, 'b1' AS distinct_id, 'b_event' AS event`,
	)
	if err != nil {
		t.Fatalf("Job.Run: %v", err)
	}
	if job.FilesCompacted != 4 {
		t.Fatalf("files compacted = %d, want 4", job.FilesCompacted)
	}
	got := make([]string, len(rows))
	for i, r := range rows {
		got[i] = r[1].String
	}
	if want := []string{"z1", "b1", "noev", "nullev"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("output order = %v, want %v (event NULLS LAST, then time)", got, want)
	}
}

func TestBindableSortKeys(t *testing.T) {
	cols := map[string]struct{}{"time": {}, "Event": {}, "os_name": {}}
	tests := []struct {
		name        string
		keys        []string
		wantKept    []string
		wantDropped []string
	}{
		{"all present", []string{"os_name", "time"}, []string{"os_name", "time"}, nil},
		{"absent dropped", []string{"event_name", "time"}, []string{"time"}, []string{"event_name"}},
		// DuckDB binds identifiers case-insensitively, so "event" binds to "Event".
		{"case-insensitive", []string{"event", "time"}, []string{"event", "time"}, nil},
		{"middle absent", []string{"os_name", "nope", "time"}, []string{"os_name", "time"}, []string{"nope"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kept, dropped := bindableSortKeys(tt.keys, cols)
			if !reflect.DeepEqual(kept, tt.wantKept) || !reflect.DeepEqual(dropped, tt.wantDropped) {
				t.Fatalf("bindableSortKeys(%v) = %v, %v; want %v, %v", tt.keys, kept, dropped, tt.wantKept, tt.wantDropped)
			}
		})
	}
}
