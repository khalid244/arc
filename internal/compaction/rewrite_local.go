package compaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	Logger    zerolog.Logger
}

// LocalRewriteResult reports what RewriteLocal resolved and wrote.
type LocalRewriteResult struct {
	Output         string   `json:"output"`
	RowsIn         int64    `json:"rows_in"`
	RowsOut        int64    `json:"rows_out"`
	SortKeys       []string `json:"sort_keys"`       // the ORDER BY keys used
	LeadingColumns []string `json:"leading_columns"` // the listed columns the inputs have, as written first
	Statements     []string `json:"statements"`      // the DuckDB statements (output path as run)
}

// rewriteOwnerFile is written into every RewriteLocal work and spill
// directory: "<hostname> <pid>". A later run on the same host removes the
// directories whose process is gone (a SIGKILLed or OOM-killed run), and
// never touches a live run's or another host's.
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

// rewriteSpillDirHook is a test seam: called with each run's spill directory.
var rewriteSpillDirHook func(string)

// RewriteLocal runs a daily compaction job's file step on local files: the
// same validation, schema probes, sort keys, column order, exact-row dedup,
// COPY options and DuckDB session settings (configureCompactionDuckDB), on
// the DuckDB this binary links. It does no storage I/O, writes no manifest
// and deletes nothing; the output is renamed into place only when complete.
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
	// filesystem, so the final rename is atomic) under a non-.parquet name.
	work, err := makeRewriteDir(outDir, rewriteWorkPrefix)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(work)
	spillBase := work
	if cfg.TempDirectory != "" {
		spillBase = cfg.TempDirectory
		if err := os.MkdirAll(spillBase, 0o755); err != nil {
			return nil, err
		}
		sweepStaleRewriteDirs(spillBase, rewriteSpillPrefix, cfg.Logger)
	}
	spill, err := makeRewriteDir(spillBase, rewriteSpillPrefix)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(spill)
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

	job := NewJob(&JobConfig{
		Measurement: cfg.Measurement, Tier: "daily", Database: "local",
		PartitionPath: "rewrite", TempDirectory: work,
		SortKeys: sortKeys, ColumnOrder: cfg.ColumnOrder,
		DB: db, Logger: cfg.Logger, JobID: "rewrite-local",
	})
	job.planOnly = cfg.PrintOnly
	job.outputName = filepath.Base(cfg.Output) + rewriteInProgressSuffix
	written, err := job.compactFiles(ctx, files, work)
	if err != nil {
		return nil, err
	}
	res := &LocalRewriteResult{
		Output:         cfg.Output,
		SortKeys:       job.plan.sortKeys,
		LeadingColumns: job.plan.leadingColumns,
		Statements:     job.plan.statements,
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
	if err := os.Rename(written, cfg.Output); err != nil {
		return nil, err
	}
	return res, nil
}

// makeRewriteDir creates a fresh base/prefix* directory and records this
// process as its owner.
func makeRewriteDir(base, prefix string) (string, error) {
	d, err := os.MkdirTemp(base, prefix)
	if err != nil {
		return "", err
	}
	host, _ := os.Hostname()
	if err := os.WriteFile(filepath.Join(d, rewriteOwnerFile), []byte(fmt.Sprintf("%s %d\n", host, os.Getpid())), 0o644); err != nil {
		os.RemoveAll(d)
		return "", err
	}
	return d, nil
}

// sweepStaleRewriteDirs removes base/prefix* directories left by runs on
// this host whose process no longer exists. A directory without a readable
// owner, or owned by another host or a live process, is left alone.
func sweepStaleRewriteDirs(base, prefix string, logger zerolog.Logger) {
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
		data, err := os.ReadFile(filepath.Join(dir, rewriteOwnerFile))
		if err != nil {
			continue
		}
		var ownerHost string
		var pid int
		if n, _ := fmt.Sscanf(string(data), "%s %d", &ownerHost, &pid); n != 2 || ownerHost != host || processAlive(pid) {
			continue
		}
		if err := os.RemoveAll(dir); err != nil {
			logger.Warn().Err(err).Str("dir", dir).Msg("Could not remove a stale rewrite directory")
			continue
		}
		logger.Info().Str("dir", dir).Int("pid", pid).Msg("Removed the directory of an earlier rewrite whose process is gone")
	}
}
