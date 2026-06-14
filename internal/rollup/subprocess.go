package rollup

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	"github.com/basekick-labs/arc/internal/s3util"
)

// Cube builds use the DuckDB datasketches extension, which has a known
// scale-triggered native crash on Linux (a large HLL/KLL aggregation can SIGSEGV
// the whole process). To keep the query server alive, every day-build runs in a
// short-lived subprocess: a crash kills only that child, and the Manager logs it
// and moves on. This mirrors arc's compaction subprocess isolation.

// DayWork is one day to build within a batch.
type DayWork struct {
	Date string `json:"date"`
	Glob string `json:"glob"`
	Dest string `json:"dest"`
}

// BuildJob is the stdin payload handed to the build subprocess: a batch of days
// for one cube. Each day is a separate per-day COPY (memory-bounded) inside the
// single subprocess, amortizing process-spawn cost across the batch.
type BuildJob struct {
	Spec     CubeSpec  `json:"spec"`
	TimeCol  string    `json:"time_col"`
	Days     []DayWork `json:"days"`
	S3       S3Params  `json:"s3"`
	MemLimit string    `json:"mem_limit"`
	Threads  int       `json:"threads"`
}

// BuildOutput is one streamed result line (one per day) from the subprocess.
type BuildOutput struct {
	Date  string   `json:"date"`
	Entry DayEntry `json:"entry"`
	Err   string   `json:"err"`
}

// configureBuildConn applies the standard build-connection settings. Sketches
// (datasketches) are only loaded when needed (the subprocess), keeping them out
// of the long-lived server process where a crash would be fatal.
func configureBuildConn(db *sql.DB, s3 S3Params, memLimit string, threads int, withSketches bool) error {
	for _, s := range buildConnStmts(s3, memLimit, threads, withSketches) {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("%q: %w", s, err)
		}
	}
	return nil
}

// buildConnStmts returns the DuckDB session-setup statements for a rollup build
// connection. Kept pure (no *sql.DB) so the S3/httpfs wiring is unit-testable.
func buildConnStmts(s3 S3Params, memLimit string, threads int, withSketches bool) []string {
	stmts := []string{"INSTALL httpfs", "LOAD httpfs"}
	if withSketches {
		stmts = append(stmts, "INSTALL datasketches FROM community", "LOAD datasketches")
	}
	stmts = append(stmts,
		fmt.Sprintf("SET GLOBAL s3_access_key_id='%s'", esc(s3.AccessKey)),
		fmt.Sprintf("SET GLOBAL s3_secret_access_key='%s'", esc(s3.SecretKey)),
		// s3_endpoint must be bare host[:port]; DuckDB derives http/https from
		// s3_use_ssl below. A scheme here yields a malformed "http://http://host"
		// URL that the rollup glob/build can't resolve. Shared with the query
		// engine via s3util so the two can't drift.
		fmt.Sprintf("SET GLOBAL s3_endpoint='%s'", esc(s3util.StripURLScheme(s3.Endpoint))),
		fmt.Sprintf("SET GLOBAL s3_url_style='%s'", urlStyle(s3.PathStyle)),
		fmt.Sprintf("SET GLOBAL s3_use_ssl=%v", s3.UseSSL),
		"SET TimeZone='UTC'",
		fmt.Sprintf("SET memory_limit='%s'", memLimit),
		"SET temp_directory='/tmp/duckdb_rollup_spill'",
		"SET preserve_insertion_order=false",
	)
	// Bound the DuckDB thread pool. On CPU-throttled build containers nproc reports
	// host cores, so the default thread count would be far above the real CPU budget
	// and each thread's parquet-reader buffers multiply memory under memory_limit —
	// the sketch-heavy compaction COPY OOMs as a result. <=0 keeps DuckDB's default.
	if threads > 0 {
		stmts = append(stmts, fmt.Sprintf("SET threads=%d", threads))
	}
	return stmts
}

// RunBuildDaySubcommand is the child entrypoint (arc rollup-buildday): read a
// BuildJob (batch of days) from stdin, build each day in an isolated DuckDB with
// datasketches loaded, and STREAM one BuildOutput line per day to stdout. A
// native crash on any day kills the process; the parent keeps the days already
// streamed and retries the rest.
func RunBuildDaySubcommand() int {
	var job BuildJob
	if err := json.NewDecoder(os.Stdin).Decode(&job); err != nil {
		fmt.Fprintln(os.Stderr, "rollup-buildday: decode job:", err)
		return 2
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "open duckdb:", err)
		return 1
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := configureBuildConn(db, job.S3, job.MemLimit, job.Threads, true); err != nil {
		fmt.Fprintln(os.Stderr, "configure:", err)
		return 1
	}
	enc := json.NewEncoder(os.Stdout)
	for _, d := range job.Days {
		entry, err := BuildDay(db, job.Spec, d.Glob, job.TimeCol, d.Date, d.Dest)
		out := BuildOutput{Date: d.Date, Entry: entry}
		if err != nil {
			out.Err = err.Error()
		}
		enc.Encode(out)  // one JSON line per day
		os.Stdout.Sync() // flush so the parent sees progress before any crash
	}
	return 0
}

// specHasSketch reports whether a cube materializes an approximate sketch (HLL
// distinct or KLL percentile) — i.e. whether its build must run isolated.
func specHasSketch(s CubeSpec) bool {
	for _, a := range s.Aggs {
		if a.Kind == AggCountDistinct || a.Kind == AggPercentile {
			return true
		}
	}
	return false
}

// spawnBuildBatch builds a batch of days for one sketch cube in a single
// subprocess, invoking onDay for each day as its result streams in. A subprocess
// crash mid-batch (e.g. datasketches SIGSEGV) is non-fatal: days already streamed
// are kept (and persisted by the caller) and the rest retried next tick.
func (m *Manager) spawnBuildBatch(ctx context.Context, spec CubeSpec, days []DayWork, onDay func(DayEntry)) error {
	job := BuildJob{Spec: spec, TimeCol: m.cfg.TimeCol, Days: days, S3: m.s3, MemLimit: m.cfg.MemLimit, Threads: m.cfg.BuildThreads}
	payload, err := json.Marshal(job)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, m.execPath, "rollup-buildday")
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	dec := json.NewDecoder(stdout)
	for {
		var out BuildOutput
		if err := dec.Decode(&out); err != nil {
			break // EOF or the process died
		}
		if out.Err != "" {
			m.log.Warn().Str("day", out.Date).Str("err", out.Err).Msg("Rollup sketch day build error")
			continue
		}
		// Zero-row days flow through too: Manager.persist records them as
		// coverage-only '-empty' markers so they are not rebuilt every tick.
		onDay(out.Entry)
	}
	return cmd.Wait()
}
