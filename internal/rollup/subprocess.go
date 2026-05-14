package rollup

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	// DuckDB driver — needed for the subprocess entry point.
	_ "github.com/duckdb/duckdb-go/v2"
)

// fromTableGlobRe extracts the first single-quoted path argument from a
// fromTable expression like `read_parquet('s3://...', union_by_name=true)`.
// We use this to pre-flight whether the glob matches any files so we can
// skip a window with no source instead of erroring on read_parquet.
var fromTableGlobRe = regexp.MustCompile(`'([^']+)'`)

// sourceHasFiles returns (true, nil) if the source-table glob matches at
// least one parquet file, (false, nil) if zero matches, and (true, err) on
// unexpected DuckDB errors. The "true on error" bias is intentional: we'd
// rather fall through to the existing build path and let it surface a
// proper error than skip-on-flaky-glob and silently advance a watermark.
func sourceHasFiles(ctx context.Context, db *sql.DB, fromTable string) (bool, error) {
	m := fromTableGlobRe.FindStringSubmatch(fromTable)
	if len(m) != 2 {
		// Unexpected fromTable shape (e.g. a bare table name in tests);
		// can't pre-check, fall through.
		return true, nil
	}
	globPath := m[1]
	countSQL := fmt.Sprintf("SELECT count(*) FROM glob('%s')", strings.ReplaceAll(globPath, "'", "''"))
	var n int64
	if err := db.QueryRowContext(ctx, countSQL).Scan(&n); err != nil {
		return true, fmt.Errorf("glob preflight: %w", err)
	}
	return n > 0, nil
}

// BatchedSpec pairs a RollupSpec with the storage key for its output parquet.
// When SubprocessConfig.SpecBatch is non-empty the subprocess runs the
// shared-scan path: read the source ONCE, compute every spec's aggregation
// against a temp table, emit one parquet per BatchedSpec.
type BatchedSpec struct {
	Spec      RollupSpec `json:"spec"`
	OutputKey string     `json:"output_key"`
}

// SpecSubprocessOutcome is the per-spec result inside a batched build. The
// top-level SubprocessResult.Success indicates whether the subprocess ran;
// per-spec success/failure lives here so the parent can advance watermarks
// for the specs that succeeded and leave manifests for the rest.
type SpecSubprocessOutcome struct {
	Success      bool   `json:"success"`
	BytesWritten int    `json:"bytes_written"`
	Error        string `json:"error,omitempty"`
}

// SubprocessConfig is serialized to stdin when launching a rollup-build subprocess.
// It carries everything the subprocess needs to run without touching the parent's
// DuckDB connection or storage backend instance.
//
// Two modes:
//   - Legacy single-spec: Spec/OutputKey populated, SpecBatch empty.
//   - Batched shared-scan: SpecBatch populated; Spec/OutputKey ignored.
//
// FromTable / WindowStart / WindowEnd are shared by every spec in a batch
// (the parent guarantees this by grouping on the resolved fromTable + window).
type SubprocessConfig struct {
	Spec        RollupSpec `json:"spec,omitempty"`
	FromTable   string     `json:"from_table"`
	WindowStart time.Time  `json:"window_start"`
	WindowEnd   time.Time  `json:"window_end"`
	OutputKey   string     `json:"output_key,omitempty"`

	// SpecBatch enables the shared-scan path when non-empty.
	SpecBatch []BatchedSpec `json:"spec_batch,omitempty"`

	TempDir string `json:"temp_dir,omitempty"` // if empty the subprocess picks its own

	// Storage backend serialization — mirrors compaction's pattern.
	StorageType   string `json:"storage_type"`
	StorageConfig string `json:"storage_config"`

	// MemoryLimit caps the subprocess's DuckDB at the configured value
	// (e.g. "2GB"). Empty = let DuckDB auto-detect from the host, which
	// on a k8s pod ignores cgroup limits and tries to grab ~half of the
	// node's RAM — frequently OOM-kills the pod since the parent is
	// already using memory_limit's worth. Set on the parent side from
	// cfg.Database.MemoryLimit (or a derived per-subprocess fraction).
	MemoryLimit string `json:"memory_limit,omitempty"`

	// ThreadCount pins the subprocess's DuckDB thread count. Without it,
	// DuckDB auto-detects via std::thread::hardware_concurrency() which on
	// Linux returns the host's nproc, NOT the pod's cgroup CPU quota — so
	// on a 2-core pod running on a 12-CPU node, DuckDB spawns 12 threads
	// that fight for 2 cores' worth of CFS quota and get throttled into
	// near-serial execution. Setting threads to match the cgroup quota
	// removes the throttling. Zero = no SET (preserve previous behavior).
	ThreadCount int `json:"thread_count,omitempty"`
}

// SubprocessResult is written to stdout by the subprocess.
//
// In legacy single-spec mode Success/BytesWritten describe the one spec.
// In batched mode Success indicates whether the subprocess ran end-to-end
// (i.e. it reached every spec) and PerSpec carries per-spec outcomes.
type SubprocessResult struct {
	Success      bool                             `json:"success"`
	BytesWritten int                              `json:"bytes_written"`
	DurationMS   int64                            `json:"duration_ms"`
	Error        string                           `json:"error,omitempty"`
	PerSpec      map[string]SpecSubprocessOutcome `json:"per_spec,omitempty"`
}

// RunBuildJob is the subprocess entry point: parse config from stdin, run the
// build core (DuckDB COPY + upload), write result JSON to stdout.
// Called by cmd/arc/cli_rollup_build.go.
//
// When SpecBatch is non-empty, the subprocess takes the shared-scan path:
// the source is read once into a TEMP TABLE and each spec's aggregation
// runs against that table. Otherwise the legacy single-spec path runs.
func RunBuildJob(cfg *SubprocessConfig) (*SubprocessResult, error) {
	rollupName := cfg.Spec.Name
	if len(cfg.SpecBatch) > 0 {
		rollupName = fmt.Sprintf("batch[%d]", len(cfg.SpecBatch))
	}
	subLogger := zerolog.New(os.Stderr).With().Timestamp().
		Str("component", "rollup-subprocess").
		Str("rollup", rollupName).
		Logger()

	subLogger.Info().
		Time("window_start", cfg.WindowStart).
		Time("window_end", cfg.WindowEnd).
		Int("batch_size", len(cfg.SpecBatch)).
		Msg("rollup-build subprocess started")

	backend, err := createRollupBackendFromConfig(cfg, subLogger)
	if err != nil {
		return nil, fmt.Errorf("create storage backend: %w", err)
	}
	defer backend.Close()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	// Cap subprocess DuckDB at the configured memory_limit. Without this,
	// DuckDB auto-detects from the host (not the cgroup) and tries to grab
	// ~half of node RAM — which OOM-kills the pod since the parent is
	// already at its own memory_limit. Best-effort: if SET fails, log and
	// continue (DuckDB's default still works on uncapped hosts).
	if cfg.MemoryLimit != "" {
		if _, err := db.Exec(fmt.Sprintf("SET memory_limit = '%s'", escapeSQLLit(cfg.MemoryLimit))); err != nil {
			subLogger.Warn().Err(err).Str("memory_limit", cfg.MemoryLimit).Msg("failed to set subprocess memory_limit; continuing with DuckDB default")
		}
	}

	if cfg.ThreadCount > 0 {
		if _, err := db.Exec(fmt.Sprintf("SET threads = %d", cfg.ThreadCount)); err != nil {
			subLogger.Warn().Err(err).Int("thread_count", cfg.ThreadCount).Msg("failed to set subprocess threads; continuing with DuckDB default")
		} else {
			subLogger.Info().Int("thread_count", cfg.ThreadCount).Msg("subprocess threads configured")
		}
	}

	// Load datasketches (best-effort; callers that use sketch aggregations need it).
	if _, err := db.Exec("LOAD datasketches"); err != nil {
		if _, ierr := db.Exec("INSTALL datasketches FROM community"); ierr == nil {
			db.Exec("LOAD datasketches") //nolint:errcheck
		}
	}

	// When the source/output backend is S3 the FromTable in cfg is a
	// read_parquet('s3://…') expression. DuckDB's httpfs extension needs
	// explicit credentials + endpoint set globally for this connection;
	// without it, the COPY below errors with an HTTP/auth failure that
	// the parent only sees as "exit status 1". Mirror the main-process
	// setup in internal/database/duckdb.go:configureS3Access — credentials
	// come from the inherited AWS_ACCESS_KEY_ID/SECRET env vars, the rest
	// from SubprocessConfig.StorageConfig.
	if cfg.StorageType == "s3" {
		if err := configureSubprocessS3(db, cfg.StorageConfig); err != nil {
			return nil, fmt.Errorf("configure duckdb s3: %w", err)
		}
	}

	tmpDir := cfg.TempDir
	if tmpDir == "" {
		tmpDir, err = os.MkdirTemp("", "arc-rollup-sub-")
		if err != nil {
			return nil, fmt.Errorf("temp dir: %w", err)
		}
	}
	defer os.RemoveAll(tmpDir)

	// Point DuckDB's spill directory at the same tmpDir so a large TEMP
	// TABLE materialization (batch path) doesn't fall back to /tmp inside
	// the container and surprise us with ENOSPC.
	if _, err := db.Exec(fmt.Sprintf("SET temp_directory='%s'", escapeSQLLit(tmpDir))); err != nil {
		subLogger.Warn().Err(err).Str("dir", tmpDir).Msg("failed to set DuckDB temp_directory; continuing")
	}

	started := time.Now()

	// Pre-flight: skip the heavy build entirely when the source glob
	// matches no parquet files. Otherwise DuckDB's read_parquet errors
	// with "No files found" and the scheduler retries the window forever.
	// On a skip the parent advances the watermark just as if the build
	// succeeded; queries against the empty day fall through to source
	// (also empty) and return zero rows — the same answer either way.
	hasFiles, err := sourceHasFiles(context.Background(), db, cfg.FromTable)
	if err != nil {
		subLogger.Warn().Err(err).Msg("source glob preflight failed; falling through to build path")
	} else if !hasFiles {
		subLogger.Info().
			Str("from_table", cfg.FromTable).
			Msg("source glob matched no parquet files; skipping window")
		if len(cfg.SpecBatch) > 0 {
			outcomes := make(map[string]SpecSubprocessOutcome, len(cfg.SpecBatch))
			for _, bs := range cfg.SpecBatch {
				outcomes[bs.Spec.Name] = SpecSubprocessOutcome{Success: true, BytesWritten: 0}
			}
			return &SubprocessResult{
				Success:    true,
				PerSpec:    outcomes,
				DurationMS: time.Since(started).Milliseconds(),
			}, nil
		}
		return &SubprocessResult{
			Success:      true,
			BytesWritten: 0,
			DurationMS:   time.Since(started).Milliseconds(),
		}, nil
	}

	// Batch path: shared TEMP TABLE, N per-spec COPYs.
	if len(cfg.SpecBatch) > 0 {
		result, err := runBuildJobBatch(db, backend, cfg, tmpDir, subLogger)
		if err != nil {
			return nil, err
		}
		result.DurationMS = time.Since(started).Milliseconds()
		subLogger.Info().
			Int("batch_size", len(cfg.SpecBatch)).
			Dur("duration", time.Since(started)).
			Msg("rollup-build subprocess (batch) completed")
		return result, nil
	}

	// Legacy single-spec path.
	selectSQL, err := BuildWindowSQL(cfg.Spec, cfg.FromTable, cfg.WindowStart, cfg.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("build sql: %w", err)
	}

	tmpFile := filepath.Join(tmpDir, "window.parquet")
	copyStmt := fmt.Sprintf(
		"COPY (%s) TO '%s' (FORMAT PARQUET)",
		selectSQL,
		strings.ReplaceAll(tmpFile, "'", "''"),
	)

	if _, err := db.ExecContext(context.Background(), copyStmt); err != nil {
		return nil, fmt.Errorf("execute copy: %w", err)
	}

	data, err := os.ReadFile(tmpFile)
	if err != nil {
		return nil, fmt.Errorf("read temp parquet: %w", err)
	}

	if err := backend.Write(context.Background(), cfg.OutputKey, data); err != nil {
		return nil, fmt.Errorf("write to backend: %w", err)
	}

	dur := time.Since(started)
	subLogger.Info().
		Int("bytes", len(data)).
		Dur("duration", dur).
		Msg("rollup-build subprocess completed")

	return &SubprocessResult{
		Success:      true,
		BytesWritten: len(data),
		DurationMS:   dur.Milliseconds(),
	}, nil
}

// runBuildJobBatch is the shared-scan path. It materializes the source once
// into a TEMP TABLE day_src and runs one COPY per BatchedSpec against the
// temp table. Per-spec failures are recorded but do not abort the batch —
// other specs still get a chance to write their parquet. The caller
// (Builder.BuildWindowBatch) uses PerSpec outcomes to advance watermarks
// only for specs that succeeded.
func runBuildJobBatch(
	db *sql.DB,
	backend storage.Backend,
	cfg *SubprocessConfig,
	tmpDir string,
	logger zerolog.Logger,
) (*SubprocessResult, error) {
	if len(cfg.SpecBatch) == 0 {
		return nil, fmt.Errorf("runBuildJobBatch called with empty SpecBatch")
	}

	// All specs in a batch must share BucketColumn — the temp-table WHERE
	// scoping uses it. Grouping logic in the scheduler enforces this, but
	// validate here so a misuse fails loudly instead of silently corrupting.
	bucketCol := cfg.SpecBatch[0].Spec.BucketColumn
	if !validIdentifier.MatchString(bucketCol) {
		return nil, fmt.Errorf("invalid bucket_column %q in batch[0]", bucketCol)
	}
	for i, bs := range cfg.SpecBatch {
		if bs.Spec.BucketColumn != bucketCol {
			return nil, fmt.Errorf("batched specs disagree on bucket_column: batch[0]=%q batch[%d]=%q", bucketCol, i, bs.Spec.BucketColumn)
		}
	}

	// Materialize the day's source once. The WHERE pushes the predicate into
	// the parquet scan via DuckDB's row-group statistics so rows outside the
	// window are never decoded.
	createStmt := fmt.Sprintf(
		"CREATE TEMP TABLE day_src AS SELECT * FROM %s WHERE %s >= TIMESTAMP '%s' AND %s < TIMESTAMP '%s'",
		cfg.FromTable,
		bucketCol,
		cfg.WindowStart.UTC().Format("2006-01-02 15:04:05"),
		bucketCol,
		cfg.WindowEnd.UTC().Format("2006-01-02 15:04:05"),
	)
	logger.Info().Str("from_table", cfg.FromTable).Msg("materializing shared TEMP TABLE day_src")
	if _, err := db.ExecContext(context.Background(), createStmt); err != nil {
		return nil, fmt.Errorf("create temp table: %w", err)
	}
	defer func() {
		// Best-effort; TEMP TABLEs vanish on connection close anyway.
		_, _ = db.Exec("DROP TABLE IF EXISTS day_src")
	}()

	// Optionally log the row count for telemetry.
	if row := db.QueryRow("SELECT count(*) FROM day_src"); row != nil {
		var rowCount int64
		if err := row.Scan(&rowCount); err == nil {
			logger.Info().Int64("rows", rowCount).Msg("day_src materialized")
		}
	}

	outcomes := make(map[string]SpecSubprocessOutcome, len(cfg.SpecBatch))
	for _, bs := range cfg.SpecBatch {
		outcome := buildOneSpecFromDaySrc(db, backend, bs, cfg.WindowStart, cfg.WindowEnd, tmpDir, logger)
		outcomes[bs.Spec.Name] = outcome
	}

	return &SubprocessResult{
		Success: true,
		PerSpec: outcomes,
	}, nil
}

// buildOneSpecFromDaySrc runs one spec's COPY against day_src and uploads
// the result. Returns a SpecSubprocessOutcome capturing success/failure;
// errors are never returned (caller continues with remaining specs).
func buildOneSpecFromDaySrc(
	db *sql.DB,
	backend storage.Backend,
	bs BatchedSpec,
	windowStart, windowEnd time.Time,
	tmpDir string,
	logger zerolog.Logger,
) SpecSubprocessOutcome {
	specLogger := logger.With().Str("spec", bs.Spec.Name).Logger()

	selectSQL, err := BuildWindowSQL(bs.Spec, "day_src", windowStart, windowEnd)
	if err != nil {
		specLogger.Warn().Err(err).Msg("build sql failed")
		return SpecSubprocessOutcome{Success: false, Error: err.Error()}
	}

	// One file per spec inside the shared tmpDir.
	safeName := strings.ReplaceAll(bs.Spec.Name, "/", "_")
	tmpFile := filepath.Join(tmpDir, "window_"+safeName+".parquet")
	copyStmt := fmt.Sprintf(
		"COPY (%s) TO '%s' (FORMAT PARQUET)",
		selectSQL,
		strings.ReplaceAll(tmpFile, "'", "''"),
	)
	if _, err := db.ExecContext(context.Background(), copyStmt); err != nil {
		specLogger.Warn().Err(err).Msg("execute copy failed")
		return SpecSubprocessOutcome{Success: false, Error: err.Error()}
	}

	data, err := os.ReadFile(tmpFile)
	if err != nil {
		specLogger.Warn().Err(err).Msg("read temp parquet failed")
		return SpecSubprocessOutcome{Success: false, Error: err.Error()}
	}

	if err := backend.Write(context.Background(), bs.OutputKey, data); err != nil {
		specLogger.Warn().Err(err).Msg("write to backend failed")
		return SpecSubprocessOutcome{Success: false, Error: err.Error()}
	}

	specLogger.Info().Int("bytes", len(data)).Msg("spec parquet uploaded")
	return SpecSubprocessOutcome{Success: true, BytesWritten: len(data)}
}

// RunBuildSubprocess launches `<self> rollup-build --window-stdin`, feeds it the
// serialized config on stdin, collects the result from stdout, and forwards
// subprocess stderr to logger at info level.
func RunBuildSubprocess(ctx context.Context, cfg *SubprocessConfig, logger zerolog.Logger) (*SubprocessResult, error) {
	configJSON, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("serialize subprocess config: %w", err)
	}

	execPath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("get executable path: %w", err)
	}

	cmd := exec.CommandContext(ctx, execPath, "rollup-build", "--window-stdin")
	cmd.Stdin = bytes.NewReader(configJSON)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	logger.Info().
		Str("rollup", cfg.Spec.Name).
		Time("window_start", cfg.WindowStart).
		Time("window_end", cfg.WindowEnd).
		Int("config_bytes", len(configJSON)).
		Msg("starting rollup-build subprocess")

	err = cmd.Run()

	// Forward subprocess stderr line-by-line. The subprocess emits structured
	// zerolog JSON to stderr (see RunBuildJob's subLogger); each line carries
	// its own level. Parse the level and forward at the matching parent level
	// so DuckDB warnings and errors don't get buried under successful builds
	// at the same Info level. When the subprocess exited non-zero, escalate
	// every line to at least Warn so the failure is visible even if the line
	// itself was logged at Info inside the subprocess.
	if stderr.Len() > 0 {
		exitFailed := err != nil
		for _, line := range strings.Split(strings.TrimSpace(stderr.String()), "\n") {
			if line == "" {
				continue
			}
			forwardSubprocessLogLine(logger, line, exitFailed)
		}
	}

	if err != nil {
		// On non-zero exit the subprocess writes a SubprocessResult{Error}
		// to stdout before exiting (cli_rollup_build.go). Surface that
		// error message instead of just "exit status 1" so operators can
		// see the actual DuckDB/storage failure that caused the exit.
		detail := ""
		if stdout.Len() > 0 {
			var partial SubprocessResult
			if jerr := json.Unmarshal(stdout.Bytes(), &partial); jerr == nil && partial.Error != "" {
				detail = " (subprocess error: " + partial.Error + ")"
			}
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("rollup-build subprocess cancelled: %w", ctx.Err())
		}
		return nil, fmt.Errorf("rollup-build subprocess failed: %w%s", err, detail)
	}

	var result SubprocessResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("parse subprocess result: %w (stdout: %s)", err, stdout.String())
	}

	logger.Debug().
		Bool("success", result.Success).
		Int("bytes_written", result.BytesWritten).
		Int64("duration_ms", result.DurationMS).
		Msg("rollup-build subprocess result")

	return &result, nil
}

// createRollupBackendFromConfig recreates a storage.Backend from the JSON
// config embedded in SubprocessConfig. Mirrors compaction's createStorageBackendFromConfig.
func createRollupBackendFromConfig(cfg *SubprocessConfig, logger zerolog.Logger) (storage.Backend, error) {
	switch cfg.StorageType {
	case "local":
		var localCfg struct {
			BasePath string `json:"base_path"`
		}
		if err := json.Unmarshal([]byte(cfg.StorageConfig), &localCfg); err != nil {
			return nil, fmt.Errorf("parse local storage config: %w", err)
		}
		return storage.NewLocalBackend(localCfg.BasePath, logger)

	case "s3":
		var s3Cfg struct {
			Bucket    string `json:"bucket"`
			Region    string `json:"region"`
			Endpoint  string `json:"endpoint"`
			PathStyle bool   `json:"path_style"`
			UseSSL    bool   `json:"use_ssl"`
		}
		if err := json.Unmarshal([]byte(cfg.StorageConfig), &s3Cfg); err != nil {
			return nil, fmt.Errorf("parse s3 storage config: %w", err)
		}
		return storage.NewS3Backend(&storage.S3Config{
			Bucket:    s3Cfg.Bucket,
			Region:    s3Cfg.Region,
			Endpoint:  s3Cfg.Endpoint,
			PathStyle: s3Cfg.PathStyle,
			UseSSL:    s3Cfg.UseSSL,
			// Credentials come from environment (parent inherits them).
		}, logger)

	case "azure":
		var azCfg struct {
			Container   string `json:"container"`
			AccountName string `json:"account_name"`
			Endpoint    string `json:"endpoint"`
		}
		if err := json.Unmarshal([]byte(cfg.StorageConfig), &azCfg); err != nil {
			return nil, fmt.Errorf("parse azure storage config: %w", err)
		}
		accountKey := os.Getenv("AZURE_STORAGE_KEY")
		return storage.NewAzureBlobBackend(&storage.AzureBlobConfig{
			ContainerName:      azCfg.Container,
			AccountName:        azCfg.AccountName,
			AccountKey:         accountKey,
			Endpoint:           azCfg.Endpoint,
			UseManagedIdentity: accountKey == "",
		}, logger)

	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.StorageType)
	}
}

// configureSubprocessS3 installs httpfs and sets the S3 settings DuckDB
// needs to read source/write rollup parquet via `read_parquet('s3://…')`.
// Credentials come from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (inherited
// from the parent process). Mirrors internal/database/duckdb.go's
// configureS3Access — kept inline so the subprocess doesn't depend on the
// database package's full Config struct.
func configureSubprocessS3(db *sql.DB, storageConfigJSON string) error {
	var s3 struct {
		Bucket    string `json:"bucket"`
		Region    string `json:"region"`
		Endpoint  string `json:"endpoint"`
		PathStyle bool   `json:"path_style"`
		UseSSL    bool   `json:"use_ssl"`
	}
	if err := json.Unmarshal([]byte(storageConfigJSON), &s3); err != nil {
		return fmt.Errorf("parse s3 storage config: %w", err)
	}
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		return fmt.Errorf("install httpfs: %w", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		return fmt.Errorf("load httpfs: %w", err)
	}
	access := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if access == "" || secret == "" {
		return fmt.Errorf("AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY not set in subprocess environment")
	}
	if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_access_key_id='%s'", escapeSQLLit(access))); err != nil {
		return fmt.Errorf("set s3_access_key_id: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_secret_access_key='%s'", escapeSQLLit(secret))); err != nil {
		return fmt.Errorf("set s3_secret_access_key: %w", err)
	}
	if s3.Region != "" {
		if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_region='%s'", escapeSQLLit(s3.Region))); err != nil {
			return fmt.Errorf("set s3_region: %w", err)
		}
	}
	if s3.Endpoint != "" {
		if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_endpoint='%s'", escapeSQLLit(s3.Endpoint))); err != nil {
			return fmt.Errorf("set s3_endpoint: %w", err)
		}
	}
	urlStyle := "vhost"
	if s3.PathStyle {
		urlStyle = "path"
	}
	if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_url_style='%s'", urlStyle)); err != nil {
		return fmt.Errorf("set s3_url_style: %w", err)
	}
	useSSL := "true"
	if !s3.UseSSL {
		useSSL = "false"
	}
	if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_use_ssl=%s", useSSL)); err != nil {
		return fmt.Errorf("set s3_use_ssl: %w", err)
	}
	return nil
}

func escapeSQLLit(s string) string { return strings.ReplaceAll(s, "'", "''") }

// forwardSubprocessLogLine forwards one stderr line from the subprocess to
// the parent logger at the appropriate level. The subprocess writes
// structured zerolog JSON (level, message, fields); we parse the level so
// warnings and errors aren't silently demoted to Info on the parent side.
// Non-JSON lines (or lines without a level field) are forwarded as Info,
// unless the subprocess exited non-zero in which case we escalate to Error
// so the failure cause is visible without grepping.
func forwardSubprocessLogLine(logger zerolog.Logger, line string, exitFailed bool) {
	var fields map[string]any
	level := ""
	message := line
	if err := json.Unmarshal([]byte(line), &fields); err == nil {
		if lv, ok := fields["level"].(string); ok {
			level = strings.ToLower(lv)
		}
		if m, ok := fields["message"].(string); ok && m != "" {
			message = m
		}
	}
	if exitFailed && (level == "" || level == "debug" || level == "info") {
		level = "error"
	}
	var ev *zerolog.Event
	switch level {
	case "fatal", "panic", "error":
		ev = logger.Error()
	case "warn", "warning":
		ev = logger.Warn()
	case "debug":
		ev = logger.Debug()
	default:
		ev = logger.Info()
	}
	ev.Str("subprocess", "rollup-build").Msg(message)
}
