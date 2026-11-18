//go:build duckdb_arrow

package compaction

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
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
		// error_message is in no input: allowed here, and reported.
		AllowAbsentColumns: true,
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
	if !reflect.DeepEqual(res.DroppedColumns, []string{"error_message"}) || len(res.DroppedSortKeys) != 0 {
		t.Fatalf("dropped columns %v, sort keys %v", res.DroppedColumns, res.DroppedSortKeys)
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
		host, _ := os.Hostname()
		mk := func(base, name, owner string) string {
			d := filepath.Join(base, name)
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(d, rewriteOwnerFile), []byte(owner), 0o644); err != nil {
				t.Fatal(err)
			}
			_ = os.WriteFile(filepath.Join(d, "partial.parquet.inprogress"), []byte("PAR1"), 0o644)
			return d
		}
		// No process holds these owner files' locks: their runs are gone,
		// whatever PID they recorded (even one that is alive here).
		staleWork := mk(outDir, ".arc-rewrite-stale", fmt.Sprintf("%s %d\n", host, os.Getpid()))
		staleSpill := mk(spillBase, "arc-rewrite-spill-stale", fmt.Sprintf("%s 1\n", host))
		// A live run in another PID namespace with the same hostname: its PID
		// means nothing here, but it holds its owner file's lock.
		liveWork := mk(outDir, ".arc-rewrite-live", fmt.Sprintf("%s 999999\n", host))
		lock, err := os.OpenFile(filepath.Join(liveWork, rewriteOwnerFile), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		if ok, err := tryLockFile(lock); err != nil || !ok {
			t.Fatalf("test lock: %v %v", ok, err)
		}
		otherHost := mk(outDir, ".arc-rewrite-otherhost", fmt.Sprintf("%s-elsewhere 1\n", host))

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

// Cancelling while the COPY runs interrupts it: the run fails with the
// cancellation, publishes nothing and leaves no directory behind.
func TestRewriteLocalCancelMidCopy(t *testing.T) {
	db := openDuck(t)
	in := filepath.ToSlash(filepath.Join(t.TempDir(), "big.parquet"))
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT TIMESTAMPTZ '2026-10-28 00:00:00Z' + to_microseconds(i) AS time,
		md5(i::VARCHAR) AS distinct_id, 'ev' || (i %% 50) AS event, md5((i * 7)::VARCHAR) AS uuid
		FROM range(4000000) t(i)) TO '%s' (FORMAT PARQUET)`, escapeSQLPath(in))); err != nil {
		t.Fatal(err)
	}
	outDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	rewriteBeforeCopyHook = func() { close(started); go func() { time.Sleep(200 * time.Millisecond); cancel() }() }
	t.Cleanup(func() { rewriteBeforeCopyHook = nil })
	t0 := time.Now()
	_, err := RewriteLocal(ctx, LocalRewriteConfig{
		Measurement: "events", Inputs: []string{in}, Output: filepath.Join(outDir, "o.parquet"),
		SortKeys: []string{"event", "distinct_id", "time"}, Threads: 1,
	})
	select {
	case <-started:
	default:
		t.Fatal("the COPY never started")
	}
	if err == nil || !(strings.Contains(err.Error(), "context canceled") || strings.Contains(err.Error(), "nterrupt")) {
		t.Fatalf("error = %v, want the cancellation", err)
	}
	if left, _ := os.ReadDir(outDir); len(left) != 0 {
		t.Fatalf("cancelled run left %v", left)
	}
	t.Logf("cancelled after %s", time.Since(t0))
}

// A misspelled sort key or column order entry must not silently give the
// time-only layout. Absent entries fail the run unless allowed, and are
// reported either way.
func TestRewriteLocalAbsentKeysAndColumns(t *testing.T) {
	inputs := writeRewriteInputs(t)
	run := func(c LocalRewriteConfig) (*LocalRewriteResult, error) {
		c.Measurement, c.Inputs = "events", inputs
		c.Output = filepath.Join(t.TempDir(), "o.parquet")
		c.PrintOnly = true
		return RewriteLocal(context.Background(), c)
	}
	if _, err := run(LocalRewriteConfig{SortKeys: []string{"evnet", "time"}, ColumnOrder: []string{"action"}}); err == nil || !strings.Contains(err.Error(), "evnet") {
		t.Fatalf("absent sort key: error = %v, want one naming evnet", err)
	}
	if _, err := run(LocalRewriteConfig{SortKeys: []string{"event"}, ColumnOrder: []string{"acton", "event"}}); err == nil || !strings.Contains(err.Error(), "acton") {
		t.Fatalf("absent column: error = %v, want one naming acton", err)
	}
	res, err := run(LocalRewriteConfig{SortKeys: []string{"evnet", "event"}, ColumnOrder: []string{"acton", "event"}, AllowAbsentSortKeys: true, AllowAbsentColumns: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.DroppedSortKeys, []string{"evnet"}) || !reflect.DeepEqual(res.SortKeys, []string{"event", "time"}) {
		t.Fatalf("sort keys %v dropped %v", res.SortKeys, res.DroppedSortKeys)
	}
	if !reflect.DeepEqual(res.DroppedColumns, []string{"acton"}) || !reflect.DeepEqual(res.LeadingColumns, []string{"event"}) {
		t.Fatalf("leading %v dropped %v", res.LeadingColumns, res.DroppedColumns)
	}
	// Nothing absent: both lists are reported, empty.
	res, err = run(LocalRewriteConfig{SortKeys: []string{"event"}, ColumnOrder: []string{"action"}})
	if err != nil || res.DroppedSortKeys == nil || res.DroppedColumns == nil || len(res.DroppedSortKeys)+len(res.DroppedColumns) != 0 {
		t.Fatalf("clean run: %v dropped %v / %v", err, res.DroppedSortKeys, res.DroppedColumns)
	}
}

// An input that passes the magic-byte check but whose footer cannot be read
// fails the schema probe; the run must fail on it rather than fall back to
// the inputs' column order.
func TestRewriteLocalProbeFailureFails(t *testing.T) {
	inputs := writeRewriteInputs(t)
	bad := filepath.Join(t.TempDir(), "badfooter.parquet")
	if err := os.WriteFile(bad, []byte("PAR1 this is not a footer PAR1"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := RewriteLocal(context.Background(), LocalRewriteConfig{
		Measurement: "events", Inputs: append(append([]string{}, inputs...), bad),
		Output: filepath.Join(t.TempDir(), "o.parquet"), SortKeys: []string{"event"}, ColumnOrder: []string{"action"},
	})
	if err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("error = %v, want a schema probe failure", err)
	}
}

// Publishing never replaces a file that appeared at Output while the run
// was writing (a second run with the same --output): the run fails and the
// other file is untouched. The published file is complete (fsynced).
func TestRewriteLocalPublishDoesNotReplace(t *testing.T) {
	inputs := writeRewriteInputs(t)
	out := filepath.Join(t.TempDir(), "o.parquet")
	rewritePublishHook = func() {
		if err := os.WriteFile(out, []byte("the other run's output"), 0o644); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { rewritePublishHook = nil })
	_, err := RewriteLocal(context.Background(), LocalRewriteConfig{Measurement: "events", Inputs: inputs, Output: out, SortKeys: []string{"event"}})
	if err == nil || !strings.Contains(err.Error(), "exists") {
		t.Fatalf("error = %v, want the output to exist already", err)
	}
	if b, _ := os.ReadFile(out); string(b) != "the other run's output" {
		t.Fatalf("the other run's output was replaced: %q", b)
	}
	if left, _ := os.ReadDir(filepath.Dir(out)); len(left) != 1 {
		t.Fatalf("output dir holds %d entries, want only the other run's file", len(left))
	}
}
