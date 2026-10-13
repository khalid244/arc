package ingest

// IW-08: the stager merge is bounded. Every merge COPY runs on a DuckDB
// connection with an explicit memory_limit, threads, temp_directory and
// preserve_insertion_order=false, and a backlog is drained as a series of
// jobs of at most stager_job_max_files files and stager_job_max_bytes bytes,
// oldest first, until nothing is left. Before this, one COPY merged a whole
// measurement's backlog with DuckDB defaults: tens of thousands of staged
// files after a long outage reached the 16Gi pod limit on every retry tick.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// writeStagedBacklog writes nFiles staged parquet files of rowsPerFile rows
// each straight into stageDir under the stager naming convention
// (<db>__<m>__<marker><seq>.parquet), with strictly increasing mtimes so
// "oldest first" is observable. Rows carry a unique row_id; fresh rows are
// spread over three hours so the fresh merge produces several partitions.
// It returns the number of files written (each file is one staged source).
func writeStagedBacklog(t *testing.T, stageDir, marker, db, m string, nFiles, rowsPerFile int) int {
	t.Helper()
	if err := os.MkdirAll(stageDir, 0700); err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	conn, err := d.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []string{
		"SET TimeZone='UTC'",
		"SET partitioned_write_max_open_files=100000",
		"SET partitioned_write_flush_threshold=100000000",
	} {
		if _, err := conn.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	base := time.Now().UTC().Truncate(time.Hour).Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	q := fmt.Sprintf(`COPY (
  SELECT i // %d AS fid,
         TIMESTAMP '%s' - to_hours(CAST((i // %d) %% 3 AS BIGINT)) + to_microseconds(CAST(i %% 1000 AS BIGINT)) AS time,
         'f' || CAST(i // %d AS VARCHAR) || '-r' || CAST(i AS VARCHAR) AS row_id,
         CAST(i AS DOUBLE) AS value
  FROM range(%d) t(i)
) TO %s (FORMAT PARQUET, PARTITION_BY (fid))`,
		rowsPerFile, base, rowsPerFile, rowsPerFile, nFiles*rowsPerFile, sqlutil.QuoteStringLiteral(tmp))
	if _, err := conn.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("generate backlog: %v", err)
	}
	start := time.Now().Add(-time.Duration(nFiles+10) * time.Second)
	seq := 0
	for fid := 0; fid < nFiles; fid++ {
		dir := filepath.Join(tmp, "fid="+strconv.Itoa(fid))
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, e := range entries {
			seq++
			dst := filepath.Join(stageDir, fmt.Sprintf("%s__%s__%s%d.parquet", db, m, marker, seq))
			if err := os.Rename(filepath.Join(dir, e.Name()), dst); err != nil {
				t.Fatal(err)
			}
			mt := start.Add(time.Duration(seq) * time.Millisecond)
			if err := os.Chtimes(dst, mt, mt); err != nil {
				t.Fatal(err)
			}
		}
	}
	return seq
}

// countRowsUnder counts every row and distinct row_id in the parquet objects
// under root (the local "object store").
func countRowsUnder(t *testing.T, root string) (rows, distinct int64, files int) {
	t.Helper()
	var paths []string
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return nil
		}
		paths = append(paths, p)
		return nil
	})
	if len(paths) == 0 {
		return 0, 0, 0
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = sqlutil.QuoteStringLiteral(p)
	}
	q := "SELECT count(*), count(DISTINCT row_id) FROM " + sqlutil.ReadParquetList(quoted, "union_by_name=true")
	if err := d.QueryRow(q).Scan(&rows, &distinct); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return rows, distinct, len(paths)
}

type recordedJob struct {
	db, measurement string
	sources         []string
	bytes           int64
}

func recordJobs(e *stagerEngine) *[]recordedJob {
	var mu sync.Mutex
	jobs := &[]recordedJob{}
	e.hooks.onJob = func(db, m string, sources []string, bytes int64) {
		mu.Lock()
		defer mu.Unlock()
		*jobs = append(*jobs, recordedJob{db: db, measurement: m, sources: append([]string(nil), sources...), bytes: bytes})
	}
	return jobs
}

func stagedSeq(t *testing.T, path, marker string) int {
	t.Helper()
	base := filepath.Base(path)
	i := strings.Index(base, marker)
	if i < 0 {
		t.Fatalf("%s carries no %s marker", base, marker)
	}
	n, err := strconv.Atoi(strings.TrimSuffix(base[i+len(marker):], ".parquet"))
	if err != nil {
		t.Fatalf("seq of %s: %v", base, err)
	}
	return n
}

// assertOldestFirst checks that every job took the oldest files still
// waiting: the seqs of job k are all smaller than those of job k+1.
func assertOldestFirst(t *testing.T, jobs []recordedJob, marker string) {
	t.Helper()
	prevMax := 0
	for k, j := range jobs {
		seqs := make([]int, 0, len(j.sources))
		for _, s := range j.sources {
			seqs = append(seqs, stagedSeq(t, s, marker))
		}
		sort.Ints(seqs)
		if seqs[0] <= prevMax {
			t.Fatalf("job %d starts at seq %d, not after the previous job's newest file %d: not oldest first", k, seqs[0], prevMax)
		}
		if seqs[len(seqs)-1]-seqs[0] != len(seqs)-1 {
			t.Fatalf("job %d is not a contiguous run of the oldest files: seqs %d..%d for %d files", k, seqs[0], seqs[len(seqs)-1], len(seqs))
		}
		prevMax = seqs[len(seqs)-1]
	}
}

// TestStagerBounded_FreshBacklogDrainsInBoundedJobs: 5,000 staged files drain
// in one Flush as at least ten jobs of at most 500 files, oldest first, with
// every row uploaded exactly once.
func TestStagerBounded_FreshBacklogDrainsInBoundedJobs(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	store, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stageDir := filepath.Join(root, "fresh")
	const rowsPerFile = 3
	n := writeStagedBacklog(t, stageDir, "fresh_stage_", "db", "m", 5000, rowsPerFile)
	if n < 5000 {
		t.Fatalf("backlog has %d files, want >= 5000", n)
	}

	fs, err := NewFreshStager(&FreshStagerConfig{
		Storage: store, StageDir: stageDir, FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{JobMaxFiles: 500, JobMaxBytes: 1 << 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	jobs := recordJobs(fs.stagerEngine)

	if err := fs.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p, _ := fs.Pending(); p != 0 {
		t.Fatalf("one Flush left %d staged files: the loop must repeat until drained", p)
	}
	if len(*jobs) < 10 {
		t.Fatalf("%d staged files drained in %d jobs; with a 500-file cap that needs at least 10", n, len(*jobs))
	}
	total := 0
	for k, j := range *jobs {
		if len(j.sources) > 500 {
			t.Errorf("job %d merged %d files, cap is 500", k, len(j.sources))
		}
		total += len(j.sources)
	}
	if total != n {
		t.Errorf("jobs merged %d files in all, want %d", total, n)
	}
	assertOldestFirst(t, *jobs, "fresh_stage_")

	rows, distinct, _ := countRowsUnder(t, objRoot)
	want := int64(n * rowsPerFile)
	if rows != want || distinct != want {
		t.Fatalf("uploaded rows=%d distinct=%d, want exactly %d of each", rows, distinct, want)
	}
}

// TestStagerBounded_LateBacklogDrainsInBoundedJobs: the late stager obeys the
// same caps; each job is one object in <m>_late/.
func TestStagerBounded_LateBacklogDrainsInBoundedJobs(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	store, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stageDir := filepath.Join(root, "late")
	const rowsPerFile = 2
	n := writeStagedBacklog(t, stageDir, "late_stage_", "db", "m", 1200, rowsPerFile)

	ls, err := NewLateStager(&LateStagerConfig{
		Storage: store, StageDir: stageDir, FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{JobMaxFiles: 500, JobMaxBytes: 1 << 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Close()
	jobs := recordJobs(ls.stagerEngine)

	if err := ls.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p, _ := ls.Pending(); p != 0 {
		t.Fatalf("one Flush left %d staged files", p)
	}
	if len(*jobs) != 3 {
		t.Fatalf("%d files drained in %d jobs, want 3 (500+500+200)", n, len(*jobs))
	}
	assertOldestFirst(t, *jobs, "late_stage_")
	rows, distinct, objects := countRowsUnder(t, objRoot)
	if objects != 3 {
		t.Errorf("late jobs uploaded %d objects, want one per job (3)", objects)
	}
	want := int64(n * rowsPerFile)
	if rows != want || distinct != want {
		t.Fatalf("uploaded rows=%d distinct=%d, want exactly %d", rows, distinct, want)
	}
}

// TestStagerBounded_JobByteCap: the byte cap bounds a job even when the file
// cap would allow more.
func TestStagerBounded_JobByteCap(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	store, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stageDir := filepath.Join(root, "late")
	n := writeStagedBacklog(t, stageDir, "late_stage_", "db", "m", 60, 2)
	var largest int64
	entries, _ := os.ReadDir(stageDir)
	for _, e := range entries {
		if info, err := e.Info(); err == nil && info.Size() > largest {
			largest = info.Size()
		}
	}
	byteCap := largest * 10

	ls, err := NewLateStager(&LateStagerConfig{
		Storage: store, StageDir: stageDir, FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{JobMaxFiles: 500, JobMaxBytes: byteCap},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Close()
	jobs := recordJobs(ls.stagerEngine)
	if err := ls.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p, _ := ls.Pending(); p != 0 {
		t.Fatalf("Flush left %d staged files", p)
	}
	if len(*jobs) < 6 {
		t.Fatalf("%d files of <=%d bytes with a %d-byte cap drained in %d jobs, want >= 6", n, largest, byteCap, len(*jobs))
	}
	for k, j := range *jobs {
		if j.bytes > byteCap {
			t.Errorf("job %d holds %d bytes, cap is %d", k, j.bytes, byteCap)
		}
	}
	rows, distinct, _ := countRowsUnder(t, objRoot)
	if want := int64(n * 2); rows != want || distinct != want {
		t.Fatalf("uploaded rows=%d distinct=%d, want %d", rows, distinct, want)
	}
}

// TestStagerBounded_MergeRunsWithBoundedDuckDBSettings: the merge connection
// carries memory_limit, threads, temp_directory (under the stage dir) and
// preserve_insertion_order=false, for both stagers, and the defaults are
// 1GB / 2 threads.
func TestStagerBounded_MergeRunsWithBoundedDuckDBSettings(t *testing.T) {
	expectedLimit := func(t *testing.T, limit string) string {
		d, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		conn, err := d.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(context.Background(), "SET memory_limit="+sqlutil.QuoteStringLiteral(limit)); err != nil {
			t.Fatal(err)
		}
		var v string
		if err := conn.QueryRowContext(context.Background(), "SELECT current_setting('memory_limit')").Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	type got struct {
		memory, tempDir string
		threads         int64
		preserve        bool
	}
	capture := func(e *stagerEngine) *[]got {
		var mu sync.Mutex
		out := &[]got{}
		e.hooks.onMergeConn = func(ctx context.Context, conn *sql.Conn) {
			var g got
			row := conn.QueryRowContext(ctx, `SELECT current_setting('memory_limit'), current_setting('temp_directory'),
				current_setting('threads'), current_setting('preserve_insertion_order')`)
			if err := row.Scan(&g.memory, &g.tempDir, &g.threads, &g.preserve); err != nil {
				t.Errorf("read merge settings: %v", err)
				return
			}
			mu.Lock()
			*out = append(*out, g)
			mu.Unlock()
		}
		return out
	}
	check := func(t *testing.T, label string, settings []got, stageDir, limit string, threads int64) {
		t.Helper()
		if len(settings) == 0 {
			t.Fatalf("%s: no merge ran", label)
		}
		want := expectedLimit(t, limit)
		for _, g := range settings {
			if g.memory != want {
				t.Errorf("%s: merge memory_limit = %q, want %q (%s)", label, g.memory, want, limit)
			}
			if g.threads != threads {
				t.Errorf("%s: merge threads = %d, want %d", label, g.threads, threads)
			}
			if g.preserve {
				t.Errorf("%s: merge preserve_insertion_order = true, want false", label)
			}
			rel, err := filepath.Rel(stageDir, g.tempDir)
			if err != nil || strings.HasPrefix(rel, "..") {
				t.Errorf("%s: merge temp_directory %q is not under the stage dir %q", label, g.tempDir, stageDir)
			}
		}
	}

	t.Run("defaults", func(t *testing.T) {
		root := t.TempDir()
		store, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		fsDir, lsDir := filepath.Join(root, "fresh"), filepath.Join(root, "late")
		writeStagedBacklog(t, fsDir, "fresh_stage_", "db", "m", 3, 2)
		writeStagedBacklog(t, lsDir, "late_stage_", "db", "m", 3, 2)
		fs, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: fsDir, FlushAge: time.Hour, Logger: zerolog.Nop()})
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Close()
		ls, err := NewLateStager(&LateStagerConfig{Storage: store, StageDir: lsDir, FlushAge: time.Hour, Logger: zerolog.Nop()})
		if err != nil {
			t.Fatal(err)
		}
		defer ls.Close()
		fsGot, lsGot := capture(fs.stagerEngine), capture(ls.stagerEngine)
		if err := fs.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := ls.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		check(t, "fresh", *fsGot, fsDir, "1GB", 2)
		check(t, "late", *lsGot, lsDir, "1GB", 2)
	})

	t.Run("configured", func(t *testing.T) {
		root := t.TempDir()
		store, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		fsDir := filepath.Join(root, "fresh")
		writeStagedBacklog(t, fsDir, "fresh_stage_", "db", "m", 3, 2)
		fs, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: fsDir, FlushAge: time.Hour, Logger: zerolog.Nop(),
			Options: StagerOptions{MergeMemoryLimit: "512MB", MergeThreads: 1}})
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Close()
		got := capture(fs.stagerEngine)
		if err := fs.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		check(t, "fresh", *got, fsDir, "512MB", 1)
	})
}
