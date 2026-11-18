//go:build duckdb_arrow

package compaction

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
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

// writeRewriteInputs writes the drifted fixtures as local Parquet inputs.
func writeRewriteInputs(t *testing.T) []string {
	t.Helper()
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
	return inputs
}

// A corrupt input must fail the run up front, in both modes. compactFiles
// skips a file that fails the magic-byte check, so without this the rewrite
// would silently drop that file's rows (and --print-sql would omit it).
func TestRewriteLocalRejectsInvalidInput(t *testing.T) {
	inputs := writeRewriteInputs(t)
	bad := filepath.Join(t.TempDir(), "bad.parquet")
	if err := os.WriteFile(bad, []byte("not a parquet file at all"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, printOnly := range []bool{false, true} {
		out := filepath.Join(t.TempDir(), "out.parquet")
		_, err := RewriteLocal(context.Background(), LocalRewriteConfig{
			Measurement: "events", Inputs: append(append([]string{}, inputs...), bad), Output: out,
			SortKeys: []string{"event", "time"}, PrintOnly: printOnly,
		})
		if err == nil || !strings.Contains(err.Error(), "bad.parquet") {
			t.Fatalf("printOnly=%v: error = %v, want one naming bad.parquet", printOnly, err)
		}
		if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
			t.Fatalf("printOnly=%v: output written despite a corrupt input", printOnly)
		}
	}
}

// Each run spills into its own directory under TempDirectory (DuckDB names
// its spill files identically in every process, so two runs sharing one
// directory corrupt each other's spill), removed when the run ends.
func TestRewriteLocalSpillDirectoryPerRun(t *testing.T) {
	inputs := writeRewriteInputs(t)
	shared := t.TempDir()
	var mu sync.Mutex
	var seen []string
	rewriteSpillDirHook = func(d string) { mu.Lock(); seen = append(seen, d); mu.Unlock() }
	t.Cleanup(func() { rewriteSpillDirHook = nil })

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = RewriteLocal(context.Background(), LocalRewriteConfig{
				Measurement: "events", Inputs: inputs, Output: filepath.Join(t.TempDir(), "out.parquet"),
				SortKeys: []string{"event", "time"}, TempDirectory: shared, Threads: 1,
			})
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 2 || seen[0] == seen[1] {
		t.Fatalf("spill directories = %v, want two distinct ones", seen)
	}
	for _, d := range seen {
		if filepath.Dir(d) != shared || !strings.HasPrefix(filepath.Base(d), "arc-rewrite-spill-") {
			t.Fatalf("spill directory %s is not a run directory under %s", d, shared)
		}
	}
	if left, _ := os.ReadDir(shared); len(left) != 0 {
		t.Fatalf("spill directories left behind: %v", left)
	}
}

// The in-progress output never carries the .parquet suffix (a killed run
// must not leave a footer-less *.parquet a reader or glob could pick up),
// a cancelled run leaves nothing behind, and a run sweeps the work and spill
// directories of earlier runs on this host whose process is gone.
func TestRewriteLocalKillSafety(t *testing.T) {
	inputs := writeRewriteInputs(t)

	t.Run("in-progress name", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "events_x_daily.parquet")
		res, err := RewriteLocal(context.Background(), LocalRewriteConfig{Measurement: "events", Inputs: inputs, Output: out, SortKeys: []string{"time"}, PrintOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		m := regexp.MustCompile(`TO '([^']+)'`).FindStringSubmatch(res.Statements[0])
		if m == nil || strings.HasSuffix(m[1], ".parquet") || !strings.Contains(m[1], "/.arc-rewrite-") {
			t.Fatalf("COPY target = %v, want a non-.parquet name in a .arc-rewrite- directory", m)
		}
	})

	t.Run("cancelled run leaves nothing", func(t *testing.T) {
		outDir := t.TempDir()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := RewriteLocal(ctx, LocalRewriteConfig{Measurement: "events", Inputs: inputs, Output: filepath.Join(outDir, "o.parquet"), SortKeys: []string{"time"}})
		if err == nil {
			t.Fatal("a cancelled run succeeded")
		}
		if left, _ := os.ReadDir(outDir); len(left) != 0 {
			t.Fatalf("cancelled run left %v", left)
		}
	})

	t.Run("sweeps stale run directories", func(t *testing.T) {
		outDir, spillBase := t.TempDir(), t.TempDir()
		dead := exec.Command("true")
		if err := dead.Run(); err != nil {
			t.Fatal(err)
		}
		host, _ := os.Hostname()
		mk := func(base, name string, pid int) string {
			d := filepath.Join(base, name)
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(d, rewriteOwnerFile), []byte(fmt.Sprintf("%s %d\n", host, pid)), 0o644); err != nil {
				t.Fatal(err)
			}
			_ = os.WriteFile(filepath.Join(d, "partial.parquet.inprogress"), []byte("PAR1"), 0o644)
			return d
		}
		staleWork := mk(outDir, ".arc-rewrite-stale", dead.Process.Pid)
		liveWork := mk(outDir, ".arc-rewrite-live", os.Getpid())
		staleSpill := mk(spillBase, "arc-rewrite-spill-stale", dead.Process.Pid)
		otherHost := filepath.Join(outDir, ".arc-rewrite-otherhost")
		_ = os.MkdirAll(otherHost, 0o755)
		_ = os.WriteFile(filepath.Join(otherHost, rewriteOwnerFile), []byte(fmt.Sprintf("%s-elsewhere %d\n", host, dead.Process.Pid)), 0o644)

		if _, err := RewriteLocal(context.Background(), LocalRewriteConfig{
			Measurement: "events", Inputs: inputs, Output: filepath.Join(outDir, "o.parquet"),
			SortKeys: []string{"time"}, TempDirectory: spillBase,
		}); err != nil {
			t.Fatal(err)
		}
		for _, d := range []string{staleWork, staleSpill} {
			if _, err := os.Stat(d); !os.IsNotExist(err) {
				t.Fatalf("stale %s not swept", d)
			}
		}
		for _, d := range []string{liveWork, otherHost} {
			if _, err := os.Stat(d); err != nil {
				t.Fatalf("%s of a live run / another host was swept: %v", d, err)
			}
		}
	})
}
