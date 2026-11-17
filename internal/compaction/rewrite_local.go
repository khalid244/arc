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

// RewriteLocal runs a daily compaction job's file step on local files: the
// same validation, schema probes, sort keys, column order, exact-row dedup,
// COPY options and DuckDB session settings (configureCompactionDuckDB), on
// the DuckDB this binary links. It does no storage I/O, writes no manifest
// and deletes nothing; the output is renamed into place only when complete.
// The output carries the arc:dedup_exact footer entry like a daily output
// (hourly outputs also list their inputs in arc:consumed_inputs; a rewrite
// has no storage inputs to list).
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

	outDir := filepath.Dir(cfg.Output)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	// The job names its output itself; it writes into a private directory
	// next to Output (same filesystem) and is renamed when complete.
	work, err := os.MkdirTemp(outDir, ".arc-rewrite-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(work)
	spill := cfg.TempDirectory
	if spill == "" {
		spill = filepath.Join(work, "spill")
	}
	if err := os.MkdirAll(spill, 0o755); err != nil {
		return nil, err
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := configureCompactionDuckDB(db, cfg.MemoryLimit, cfg.Threads, spill, cfg.MaxTempDirectorySize, cfg.Logger); err != nil {
		return nil, err
	}

	files := make([]downloadedFile, len(cfg.Inputs))
	list := make([]string, len(cfg.Inputs))
	for i, in := range cfg.Inputs {
		st, err := os.Stat(in)
		if err != nil {
			return nil, err
		}
		files[i] = downloadedFile{storageKey: in, localPath: in, size: st.Size()}
		list[i] = sqlutil.QuoteStringLiteral(in)
	}
	job := NewJob(&JobConfig{
		Measurement: cfg.Measurement, Tier: "daily", Database: "local",
		PartitionPath: "rewrite", TempDirectory: work,
		SortKeys: sortKeys, ColumnOrder: cfg.ColumnOrder,
		DB: db, Logger: cfg.Logger, JobID: "rewrite-local",
	})
	job.planOnly = cfg.PrintOnly
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
