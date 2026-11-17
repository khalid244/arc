package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
)

// reorgOutputs drains two late files with different column orders (one with
// an extra column, one without "action") and returns the reorg outputs.
func reorgOutputs(t *testing.T, columnOrder map[string][]string) ([]string, *sql.DB) {
	t.Helper()
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)
	backend, _ := storage.NewLocalBackend(tmp, logger)
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	_ = os.MkdirAll(lateDir, 0o755)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	writeLateParquet(t, db, lateDir, "events_late_20260520_030000_0.parquet", `
		SELECT TIMESTAMP '2026-04-01 12:00:00' + (i * INTERVAL 1 DAY) AS time, ('d'||i)::VARCHAR AS distinct_id,
		       'ev'::VARCHAR AS event, ('a'||i)::VARCHAR AS action, i::BIGINT AS n
		FROM range(0,3) t(i)`)
	writeLateParquet(t, db, lateDir, "events_late_20260520_030000_1.parquet", `
		SELECT i::BIGINT AS n, ('x'||i)::VARCHAR AS extra, 'ev2'::VARCHAR AS event,
		       ('e'||i)::VARCHAR AS distinct_id, TIMESTAMP '2026-04-01 18:00:00' + (i * INTERVAL 1 DAY) AS time
		FROM range(0,3) t(i)`)

	reorg := newReorg(backend, filepath.Join(tmp, "scratch"), logger)
	reorg.ColumnOrder = columnOrder
	_ = os.MkdirAll(filepath.Join(tmp, "scratch"), 0o755)
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run: %v", err)
	}
	if remaining := countParquetFlat(t, lateDir); remaining != 0 {
		t.Fatalf("expected the sidecar drained, %d files remain", remaining)
	}
	var outs []string
	_ = filepath.Walk(filepath.Join(tmp, "posthog", "events"), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Ext(p) == ".parquet" {
			outs = append(outs, filepath.ToSlash(p))
		}
		return nil
	})
	sort.Strings(outs)
	if len(outs) != 3 {
		t.Fatalf("reorg outputs = %d (%v), want one per event day (3)", len(outs), outs)
	}
	return outs, db
}

func describeColumns(t *testing.T, db *sql.DB, path string) []string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(`SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('%s'))`, escapeSQLPath(path)))
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

// The reorganizer's outputs are compaction outputs too: with a column order
// for the measurement they start with the listed columns that exist (an
// absent one is skipped, never a Binder Error), keep the rest in the sources'
// order, and hold every row unchanged.
func TestReorg_ColumnOrder(t *testing.T) {
	outs, db := reorgOutputs(t, map[string][]string{"events": {"action", "error_message", "event", "time"}})
	var plain []string
	for _, o := range outs {
		cols := describeColumns(t, db, o)
		if want := []string{"action", "event", "time"}; !reflect.DeepEqual(cols[:3], want) {
			t.Fatalf("%s columns = %v, want them to start with %v", o, cols, want)
		}
		if len(cols) != 6 {
			t.Fatalf("%s columns = %v, want 6 (no partition columns, nothing lost)", o, cols)
		}
		plain = cols[3:]
	}
	if want := []string{"distinct_id", "n", "extra"}; !reflect.DeepEqual(plain, want) {
		t.Fatalf("columns after the listed ones = %v, want %v", plain, want)
	}

	list := make([]string, len(outs))
	for i, o := range outs {
		list[i] = "'" + escapeSQLPath(o) + "'"
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT distinct_id, event, coalesce(action, '-'), n, coalesce(extra, '-'), strftime(time, '%%Y-%%m-%%d %%H') FROM read_parquet([%s], union_by_name=true) ORDER BY distinct_id`, strings.Join(list, ", ")))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var d, e, a, x, ts string
		var n int64
		if err := rows.Scan(&d, &e, &a, &n, &x, &ts); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%s|%s|%s|%d|%s|%s", d, e, a, n, x, ts))
	}
	want := []string{
		"d0|ev|a0|0|-|2026-04-01 12", "d1|ev|a1|1|-|2026-04-02 12", "d2|ev|a2|2|-|2026-04-03 12",
		"e0|ev2|-|0|x0|2026-04-01 18", "e1|ev2|-|1|x1|2026-04-02 18", "e2|ev2|-|2|x2|2026-04-03 18",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reorg rows =\n%v\nwant\n%v", got, want)
	}
}

// No column order (or none for this measurement): the reorg COPY is the old
// SELECT *, so outputs keep the sources' unified column order.
func TestReorg_ColumnOrderOff(t *testing.T) {
	for name, co := range map[string]map[string][]string{"nil": nil, "other measurement": {"cpu": {"host"}}} {
		t.Run(name, func(t *testing.T) {
			outs, db := reorgOutputs(t, co)
			for _, o := range outs {
				if got, want := describeColumns(t, db, o), []string{"time", "distinct_id", "event", "action", "n", "extra"}; !reflect.DeepEqual(got, want) {
					t.Fatalf("%s columns = %v, want the sources' order %v", o, got, want)
				}
			}
		})
	}
}
