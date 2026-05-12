package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/basekick-labs/arc/internal/rollup"
)

// runRollupBuildSubcommand handles `arc rollup-build --window-stdin`.
// It reads a rollup.SubprocessConfig as JSON from stdin, runs the build
// (DuckDB COPY + backend upload), and writes a rollup.SubprocessResult as
// JSON to stdout. Subprocess stderr carries structured log output that the
// parent process forwards at info level.
func runRollupBuildSubcommand(args []string) {
	fs := flag.NewFlagSet("rollup-build", flag.ExitOnError)
	windowStdin := fs.Bool("window-stdin", false, "read SubprocessConfig JSON from stdin")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to parse flags: %v\n", err)
		os.Exit(1)
	}

	if !*windowStdin {
		fmt.Fprintln(os.Stderr, "error: --window-stdin is required")
		os.Exit(1)
	}

	configData, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: read config from stdin: %v\n", err)
		os.Exit(1)
	}

	var cfg rollup.SubprocessConfig
	if err := json.Unmarshal(configData, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid SubprocessConfig JSON: %v\n", err)
		os.Exit(1)
	}

	result, err := rollup.RunBuildJob(&cfg)
	if err != nil {
		// Write a failed result to stdout so the parent can parse it, then exit non-zero.
		_ = json.NewEncoder(os.Stdout).Encode(&rollup.SubprocessResult{
			Success: false,
			Error:   err.Error(),
		})
		os.Exit(1)
	}

	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintf(os.Stderr, "error: encode result: %v\n", err)
		os.Exit(1)
	}
}
