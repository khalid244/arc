package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// runCompactLocalSubcommand handles "arc compact-local": merge local Parquet
// files of one measurement into one output exactly as a daily compaction job
// writes it (compaction.RewriteLocal), and print the result as JSON (with the
// sort keys and leading columns actually used, so a caller can assert them).
// SIGINT/SIGTERM cancel the run, which then removes its own directories.
func runCompactLocalSubcommand(args []string) int {
	rc, err := parseCompactLocalArgs(args, config.Load)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 2
	}
	rc.Logger = zerolog.New(os.Stderr).Level(zerolog.InfoLevel).With().Timestamp().Str("component", "compact-local").Logger()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	res, err := compaction.RewriteLocal(ctx, rc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

// parseCompactLocalArgs resolves the compact-local flags into a rewrite
// config. Nothing falls back silently: a rewrite with the wrong sort keys or
// without the column order would run, exit 0 and write files in a layout the
// compactor does not write.
//
//   - --measurement, --output and at least one input are required.
//   - Without --from-config, --sort-keys is required, and the column order
//     must be chosen explicitly: --column-order or --no-column-order.
//   - With --from-config (load reads arc.toml / ARC_* like the compactor),
//     the measurement must have its own ingest.sort_keys entry (the default
//     sort keys are not used) and its own compaction.column_order entry,
//     unless --sort-keys / --column-order / --no-column-order override them.
//     compaction.memory_limit, threads and max_temp_directory_size apply
//     unless their flags are given.
func parseCompactLocalArgs(args []string, load func() (*config.Config, error)) (compaction.LocalRewriteConfig, error) {
	var rc compaction.LocalRewriteConfig
	fs := flag.NewFlagSet("compact-local", flag.ContinueOnError)
	measurement := fs.String("measurement", "", "measurement name (selects the configured sort keys and column order)")
	output := fs.String("output", "", "output parquet path (must not exist)")
	sortKeys := fs.String("sort-keys", "", "comma-separated sort keys, as in ingest.sort_keys (time appended when absent)")
	columnOrder := fs.String("column-order", "", "comma-separated columns to write first, as in compaction.column_order")
	noColumnOrder := fs.Bool("no-column-order", false, "write the inputs' column order (no compaction.column_order)")
	fromConfig := fs.Bool("from-config", false, "take sort keys, column order, memory_limit, threads and max_temp_directory_size from the Arc config")
	memoryLimit := fs.String("memory-limit", "", "DuckDB memory_limit (e.g. 12GB)")
	threads := fs.Int("threads", 0, "DuckDB threads (0 = DuckDB default)")
	maxTemp := fs.String("max-temp-directory-size", "", "DuckDB max_temp_directory_size (default 12GiB)")
	tempDir := fs.String("temp-directory", "", "base directory for this run's DuckDB spill directory (default: next to the output)")
	printSQL := fs.Bool("print-sql", false, "resolve and print the statements without running them")
	if err := fs.Parse(args); err != nil {
		return rc, err
	}
	split := func(s string) []string {
		var out []string
		for _, p := range strings.Split(s, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		return out
	}
	switch {
	case *measurement == "":
		return rc, errors.New("--measurement is required")
	case *output == "":
		return rc, errors.New("--output is required")
	case fs.NArg() == 0:
		return rc, errors.New("no input files")
	case *columnOrder != "" && *noColumnOrder:
		return rc, errors.New("give --column-order or --no-column-order, not both")
	}
	rc = compaction.LocalRewriteConfig{
		Measurement: *measurement, Inputs: fs.Args(), Output: *output,
		MemoryLimit: *memoryLimit, Threads: *threads, MaxTempDirectorySize: *maxTemp,
		TempDirectory: *tempDir, PrintOnly: *printSQL,
	}
	if *fromConfig {
		cfg, err := load()
		if err != nil {
			return rc, fmt.Errorf("load config: %w", err)
		}
		perMeasurement, _, err := config.ParseSortKeys(cfg.Ingest)
		if err != nil {
			return rc, fmt.Errorf("ingest.sort_keys: %w", err)
		}
		if keys, ok := perMeasurement[*measurement]; ok {
			rc.SortKeys = keys
		} else if *sortKeys == "" {
			return rc, fmt.Errorf("ingest.sort_keys has no entry for measurement %q (configured: %s); give --sort-keys to override", *measurement, mapKeys(perMeasurement))
		}
		orders, err := config.ParseColumnOrder(cfg.Compaction.ColumnOrder)
		if err != nil {
			return rc, fmt.Errorf("compaction.column_order: %w", err)
		}
		if order, ok := orders[*measurement]; ok {
			rc.ColumnOrder = order
		} else if *columnOrder == "" && !*noColumnOrder {
			return rc, fmt.Errorf("compaction.column_order has no entry for measurement %q (configured: %s); give --column-order or --no-column-order to override", *measurement, mapKeys(orders))
		}
		if rc.MemoryLimit == "" {
			rc.MemoryLimit = cfg.Compaction.MemoryLimit
		}
		if rc.Threads == 0 {
			rc.Threads = cfg.Compaction.Threads
		}
		if rc.MaxTempDirectorySize == "" {
			rc.MaxTempDirectorySize = cfg.Compaction.MaxTempDirectorySize
		}
	} else {
		if *sortKeys == "" {
			return rc, errors.New("--sort-keys is required without --from-config")
		}
		if *columnOrder == "" && !*noColumnOrder {
			return rc, errors.New("choose the column order: --column-order or --no-column-order (or --from-config)")
		}
	}
	if *sortKeys != "" {
		rc.SortKeys = split(*sortKeys)
	}
	if *columnOrder != "" {
		rc.ColumnOrder = split(*columnOrder)
	}
	if *noColumnOrder {
		rc.ColumnOrder = nil
	}
	return rc, nil
}

func mapKeys(m map[string][]string) string {
	if len(m) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}
