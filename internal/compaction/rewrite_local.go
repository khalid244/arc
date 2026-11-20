package compaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/rs/zerolog"
)

// LocalRewriteConfig is one RewriteLocal run: local Parquet inputs of one
// measurement merged into one output exactly as a daily compaction job writes
// it. It is the contract for rewriting history into the compactor's layout
// (the `arc compact-local` subcommand).
type LocalRewriteConfig struct {
	Measurement string
	Inputs      []string // local Parquet files (read only, never modified)
	Output      string   // output path; must not exist
	// SortKeys as configured in ingest.sort_keys for the measurement;
	// "time" is appended when absent, as Manager.GetSortKeys does. Keys no
	// input has are dropped, as in compaction.
	SortKeys []string
	// ColumnOrder as configured in compaction.column_order for the
	// measurement; nil keeps the inputs' column order.
	ColumnOrder []string
	// Session settings, as compaction.memory_limit, compaction.threads and
	// compaction.max_temp_directory_size give a subprocess. Empty / 0 leaves
	// DuckDB's default (MaxTempDirectorySize: 12GiB, the subprocess default).
	MemoryLimit          string
	Threads              int
	MaxTempDirectorySize string
	// TempDirectory holds DuckDB's spill; empty = a directory next to Output.
	TempDirectory string
	// PrintOnly resolves the statements against the inputs and returns them
	// without running them.
	PrintOnly bool
	// A sort key or listed column the inputs do not have fails the run (a
	// typo would otherwise give the time-only layout and succeed), unless
	// allowed here: old history can lack columns added later. Either way
	// the dropped names are reported in the result.
	AllowAbsentSortKeys bool
	AllowAbsentColumns  bool
	Logger              zerolog.Logger
}

// LocalRewriteResult reports what RewriteLocal resolved and wrote.
type LocalRewriteResult struct {
	Output          string   `json:"output"`
	RowsIn          int64    `json:"rows_in"`
	RowsOut         int64    `json:"rows_out"`
	SortKeys        []string `json:"sort_keys"`         // the ORDER BY keys used
	DroppedSortKeys []string `json:"dropped_sort_keys"` // requested sort keys no input has
	LeadingColumns  []string `json:"leading_columns"`   // the listed columns the inputs have, as written first
	DroppedColumns  []string `json:"dropped_columns"`   // listed columns no input has
	Statements      []string `json:"statements"`        // the DuckDB statements (output path as run)
}

// rewriteOwnerFile is in every RewriteLocal work and spill directory. Its
// run holds an flock on it for its whole lifetime, and its content is
// "<hostname> <pid>". A later run removes the same-host directories whose
// lock it can take (their run is gone: a SIGKILL or OOM kill releases the
// lock), and never touches a locked one (a live run, in any PID namespace)
// or another host's.
const rewriteOwnerFile = ".arc-rewrite-owner"

// Directory prefixes: the work directory next to the output (holds the
// in-progress output) and the per-run spill directory.
const (
	rewriteWorkPrefix  = ".arc-rewrite-"
	rewriteSpillPrefix = "arc-rewrite-spill-"
)

// rewriteInProgressSuffix marks the output while it is written: a killed run
// must not leave a footer-less *.parquet that a glob or reader picks up.
const rewriteInProgressSuffix = ".inprogress"

// Test seams: called with each run's spill directory, right before the
// COPY runs, and right before the output is published.
var (
	rewriteSpillDirHook   func(string)
	rewriteBeforeCopyHook func()
	rewritePublishHook    func()
)

// RewriteLocal runs a daily compaction job's file step on local files: the
// same validation, schema probes, sort keys, column order, exact-row dedup,
// COPY options and DuckDB session settings (configureCompactionDuckDB), on
// the DuckDB this binary links. It does no storage I/O, writes no manifest
// and deletes nothing; the output is published (hard-linked to its name,
// never over an existing file) only when complete and fsynced.
// The output carries the arc:dedup_exact footer entry like a daily output
// (hourly outputs also list their inputs in arc:consumed_inputs; a rewrite
// has no storage inputs to list).
//
// Unlike a compaction job, a corrupt input fails the run instead of being
// skipped: a rewrite that silently dropped a file's rows would replace data
// with less data. Each run spills into its own directory (under
// TempDirectory when set), because DuckDB gives its spill files the same
// names in every process. Cancelling ctx stops the COPY and removes the
// run's directories.
func RewriteLocal(ctx context.Context, cfg LocalRewriteConfig) (*LocalRewriteResult, error) {
	if len(cfg.Inputs) == 0 {
		return nil, errors.New("no input files")
	}
	if cfg.Output == "" || cfg.Measurement == "" {
		return nil, errors.New("output and measurement are required")
	}
	if _, err := os.Stat(cfg.Output); err == nil {
		return nil, fmt.Errorf("output %s already exists", cfg.Output)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	sortKeys := append([]string(nil), cfg.SortKeys...)
	hasTime := false
	for _, k := range sortKeys {
		if k == "time" {
			hasTime = true
		}
	}
	if !hasTime {
		sortKeys = append(sortKeys, "time")
	}

	files := make([]downloadedFile, len(cfg.Inputs))
	list := make([]string, len(cfg.Inputs))
	for i, in := range cfg.Inputs {
		st, err := os.Stat(in)
		if err != nil {
			return nil, err
		}
		// compactFiles skips (and only logs) an input that fails this check.
		if err := validateParquetFile(in); err != nil {
			return nil, fmt.Errorf("input %s is not a valid parquet file: %w", in, err)
		}
		files[i] = downloadedFile{storageKey: in, localPath: in, size: st.Size()}
		list[i] = sqlutil.QuoteStringLiteral(in)
	}

	outDir := filepath.Dir(cfg.Output)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	sweepStaleRewriteDirs(outDir, rewriteWorkPrefix, cfg.Logger)
	// The output is written into a private directory next to Output (same
	// filesystem, so the final hard link works) under a non-.parquet name.
	work, workLock, err := makeRewriteDir(outDir, rewriteWorkPrefix)
	if err != nil {
		return nil, err
	}
	defer removeRewriteDir(work, workLock)
	spillBase := work
	if cfg.TempDirectory != "" {
		spillBase = cfg.TempDirectory
		if err := os.MkdirAll(spillBase, 0o755); err != nil {
			return nil, err
		}
		sweepStaleRewriteDirs(spillBase, rewriteSpillPrefix, cfg.Logger)
	}
	spill, spillLock, err := makeRewriteDir(spillBase, rewriteSpillPrefix)
	if err != nil {
		return nil, err
	}
	defer removeRewriteDir(spill, spillLock)
	if rewriteSpillDirHook != nil {
		rewriteSpillDirHook(spill)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := configureCompactionDuckDB(db, cfg.MemoryLimit, cfg.Threads, spill, cfg.MaxTempDirectorySize, cfg.Logger); err != nil {
		return nil, err
	}

	// Strict resolution (the compaction job itself stays lenient): probe the
	// inputs' unified schema, as the job will, and refuse absent sort keys
	// and listed columns unless allowed, and any probe failure.
	cols, err := parquetFilesColumnNames(ctx, db, "["+strings.Join(list, ", ")+"]")
	if err != nil {
		return nil, fmt.Errorf("probe the inputs' schema: %w", err)
	}
	wantKeys, droppedKeys := bindableSortKeys(sortKeys, cols)
	wantLeading, droppedCols := presentColumnOrder(cfg.ColumnOrder, cols)
	if len(droppedKeys) > 0 && !cfg.AllowAbsentSortKeys {
		return nil, fmt.Errorf("sort keys %v are in no input file (allow with --allow-absent-sort-keys)", droppedKeys)
	}
	if len(droppedCols) > 0 && !cfg.AllowAbsentColumns {
		return nil, fmt.Errorf("column order entries %v are in no input file (allow with --allow-absent-columns)", droppedCols)
	}

	job := NewJob(&JobConfig{
		Measurement: cfg.Measurement, Tier: "daily", Database: "local",
		PartitionPath: "rewrite", TempDirectory: work,
		SortKeys: sortKeys, ColumnOrder: cfg.ColumnOrder,
		DB: db, Logger: cfg.Logger, JobID: "rewrite-local",
	})
	job.planOnly = cfg.PrintOnly
	job.outputName = filepath.Base(cfg.Output) + rewriteInProgressSuffix
	job.beforeExec = rewriteBeforeCopyHook
	written, err := job.compactFiles(ctx, files, work)
	if err != nil {
		return nil, err
	}
	// The job probes the schema again and falls back leniently on a probe
	// failure; its statements must use exactly what was checked above.
	if !sameStrings(job.plan.sortKeys, wantKeys) || !sameStrings(job.plan.leadingColumns, wantLeading) {
		return nil, fmt.Errorf("the compaction step resolved sort keys %v and leading columns %v, not the checked %v and %v (a schema probe failed?)",
			job.plan.sortKeys, job.plan.leadingColumns, wantKeys, wantLeading)
	}
	res := &LocalRewriteResult{
		Output:          cfg.Output,
		SortKeys:        job.plan.sortKeys,
		DroppedSortKeys: append([]string{}, droppedKeys...),
		LeadingColumns:  job.plan.leadingColumns,
		DroppedColumns:  append([]string{}, droppedCols...),
		Statements:      job.plan.statements,
	}
	if cfg.PrintOnly {
		return res, nil
	}
	if job.FilesCompacted != len(cfg.Inputs) {
		return nil, fmt.Errorf("compacted %d of %d input files", job.FilesCompacted, len(cfg.Inputs))
	}
	if res.RowsIn, err = countParquetRows(ctx, db, "["+strings.Join(list, ", ")+"]"); err != nil {
		return nil, err
	}
	if res.RowsOut, err = countParquetRows(ctx, db, "["+sqlutil.QuoteStringLiteral(written)+"]"); err != nil {
		return nil, err
	}
	if err := publishRewrite(written, cfg.Output, cfg.Logger); err != nil {
		return nil, err
	}
	return res, nil
}

// publishRewrite makes the finished output visible under its final name:
// fsync the file, hard-link it to dst (which fails if dst exists, so a run
// never replaces another run's output, unlike a rename), drop the temp
// name and fsync the directory, so the name survives a power loss with the
// complete file behind it.
func publishRewrite(src, dst string, logger zerolog.Logger) error {
	if err := syncPath(src); err != nil {
		return fmt.Errorf("sync output: %w", err)
	}
	if rewritePublishHook != nil {
		rewritePublishHook()
	}
	if err := os.Link(src, dst); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("output %s already exists (another run published it); not replacing it", dst)
		}
		return fmt.Errorf("publish output: %w", err)
	}
	// The output is published from here on: a leftover temp name is only
	// clutter, and the caller's deferred removal of the work dir takes it.
	if err := os.Remove(src); err != nil {
		logger.Warn().Err(err).Str("file", src).Msg("Could not remove the temp name of a published rewrite output")
	}
	if err := syncPath(filepath.Dir(dst)); err != nil {
		return fmt.Errorf("sync output directory: %w", err)
	}
	return nil
}

// syncPath fsyncs a file or a directory.
func syncPath(p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// makeRewriteDir creates a fresh base/prefix* directory and claims it: it
// creates the owner file, takes an flock on it, held until removeRewriteDir,
// and only THEN writes "<hostname> <pid>" into it. The lock, not the PID, is
// what marks the directory as live: it ends with the process however it
// dies, and it holds across PID namespaces on a shared filesystem. The order
// matters: a sweeper skips an owner file it cannot parse (still empty), so
// it can only ever try to lock a complete owner file, whose lock is already
// held. Writing first let a concurrent sweep lock and delete a directory
// being claimed.
func makeRewriteDir(base, prefix string) (string, *os.File, error) {
	d, err := os.MkdirTemp(base, prefix)
	if err != nil {
		return "", nil, err
	}
	host, _ := os.Hostname()
	f, err := os.OpenFile(filepath.Join(d, rewriteOwnerFile), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err == nil && rewriteLocksSupported {
		var ok bool
		if ok, err = tryLockFile(f); err == nil && !ok {
			err = errors.New("lock held by another process")
		}
	}
	if err == nil {
		_, err = fmt.Fprintf(f, "%s %d\n", host, os.Getpid())
	}
	if err != nil {
		if f != nil {
			f.Close()
		}
		os.RemoveAll(d)
		return "", nil, fmt.Errorf("claim %s: %w", d, err)
	}
	return d, f, nil
}

// removeRewriteDir removes a run directory, then releases its lock.
func removeRewriteDir(dir string, lock *os.File) {
	os.RemoveAll(dir)
	lock.Close()
}

// sweepStaleRewriteDirs removes base/prefix* directories of earlier runs
// that are gone: same hostname in the owner file and nobody holds its lock.
// A directory without a readable owner file, of another host, or whose lock
// is held (a live run, in any PID namespace) is left alone.
func sweepStaleRewriteDirs(base, prefix string, logger zerolog.Logger) {
	if !rewriteLocksSupported {
		return
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return
	}
	host, _ := os.Hostname()
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		dir := filepath.Join(base, e.Name())
		ownerPath := filepath.Join(dir, rewriteOwnerFile)
		data, err := os.ReadFile(ownerPath)
		if err != nil {
			continue
		}
		var ownerHost string
		var pid int
		if n, _ := fmt.Sscanf(string(data), "%s %d", &ownerHost, &pid); n != 2 || ownerHost != host {
			continue
		}
		f, err := os.OpenFile(ownerPath, os.O_RDWR, 0)
		if err != nil {
			continue
		}
		if ok, err := tryLockFile(f); err != nil || !ok {
			f.Close()
			continue
		}
		err = os.RemoveAll(dir)
		f.Close()
		if err != nil {
			logger.Warn().Err(err).Str("dir", dir).Msg("Could not remove a stale rewrite directory")
			continue
		}
		logger.Info().Str("dir", dir).Int("pid", pid).Msg("Removed the directory of an earlier rewrite whose process is gone")
	}
}
