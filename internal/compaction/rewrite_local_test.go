//go:build duckdb_arrow

package compaction

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// RewriteLocal runs the compaction job's own file step (validation, schema
// probes, sort keys, column order, exact-row dedup, COPY options) on local
// files: what a history rewrite needs to write files identical in layout to
// the compactor's, without storage, manifests or input deletes.
func TestRewriteLocal(t *testing.T) {
	db := openDuck(t)
	dir := t.TempDir()
	var inputs []string
	for i, sel := range driftedInputs {
		p := filepath.ToSlash(filepath.Join(dir, fmt.Sprintf("in%d.parquet", i)))
		if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT %s) TO '%s' (FORMAT PARQUET)`, sel, escapeSQLPath(p))); err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, p)
	}
	// A duplicate of input 0: exact-row dedup applies as in compaction.
	dup := filepath.ToSlash(filepath.Join(dir, "dup.parquet"))
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT %s) TO '%s' (FORMAT PARQUET)`, driftedInputs[0], escapeSQLPath(dup))); err != nil {
		t.Fatal(err)
	}
	inputs = append(inputs, dup)

	out := filepath.ToSlash(filepath.Join(t.TempDir(), "sub", "events_rewrite_daily.parquet"))
	res, err := RewriteLocal(context.Background(), LocalRewriteConfig{
		Measurement: "events",
		Inputs:      inputs,
		Output:      out,
		SortKeys:    []string{"event"}, // "time" appended, as Manager.GetSortKeys does
		ColumnOrder: []string{"action", "event", "error_message", "os_name", "time"},
		Threads:     2,
	})
	if err != nil {
		t.Fatalf("RewriteLocal: %v", err)
	}
	if res.Output != out || res.RowsIn != 4 || res.RowsOut != 3 {
		t.Fatalf("result = %+v, want output %s, 4 rows in, 3 out", res, out)
	}
	if want := []string{"event", "time"}; !reflect.DeepEqual(res.SortKeys, want) {
		t.Fatalf("sort keys = %v, want %v", res.SortKeys, want)
	}
	if want := []string{"action", "event", "os_name", "time"}; !reflect.DeepEqual(res.LeadingColumns, want) {
		t.Fatalf("leading columns = %v, want %v", res.LeadingColumns, want)
	}
	if len(res.Statements) != 1 || !strings.Contains(res.Statements[0], `ORDER BY "event", "time"`) ||
		!strings.Contains(res.Statements[0], `"action", "event", "os_name", COALESCE(`) ||
		!strings.Contains(res.Statements[0], "DICTIONARY_SIZE_LIMIT 65536") {
		t.Fatalf("statements = %v", res.Statements)
	}

	names, _ := parquetColumns(t, db, out)
	if !reflect.DeepEqual(names[:4], []string{"action", "event", "os_name", "time"}) {
		t.Fatalf("output columns = %v", names)
	}
	got := queryStrings(t, db, fmt.Sprintf(`SELECT distinct_id, event FROM read_parquet('%s')`, escapeSQLPath(out)))
	if want := []string{"d2|ev_a", "d3|ev_a", "d1|ev_b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	for _, in := range inputs {
		if _, err := os.Stat(in); err != nil {
			t.Fatalf("input %s was touched: %v", in, err)
		}
	}
	// No temp output is left next to the final file.
	entries, _ := os.ReadDir(filepath.Dir(out))
	if len(entries) != 1 {
		t.Fatalf("output dir holds %d entries, want only the output", len(entries))
	}

	// Refuses to overwrite.
	if _, err := RewriteLocal(context.Background(), LocalRewriteConfig{Measurement: "events", Inputs: inputs, Output: out}); err == nil {
		t.Fatal("RewriteLocal overwrote an existing output")
	}
	// PrintOnly resolves the statements without writing.
	other := filepath.ToSlash(filepath.Join(t.TempDir(), "x.parquet"))
	res, err = RewriteLocal(context.Background(), LocalRewriteConfig{Measurement: "events", Inputs: inputs, Output: other, SortKeys: []string{"event", "time"}, PrintOnly: true})
	if err != nil || len(res.Statements) != 1 {
		t.Fatalf("PrintOnly: %v, %+v", err, res)
	}
	if _, err := os.Stat(other); !os.IsNotExist(err) {
		t.Fatalf("PrintOnly wrote %s", other)
	}
}
