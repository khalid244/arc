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
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	// DuckDB driver — needed for the subprocess entry point.
	_ "github.com/duckdb/duckdb-go/v2"
)

// SubprocessConfig is serialized to stdin when launching a rollup-build subprocess.
// It carries everything the subprocess needs to run without touching the parent's
// DuckDB connection or storage backend instance.
type SubprocessConfig struct {
	Spec        RollupSpec `json:"spec"`
	FromTable   string     `json:"from_table"`
	WindowStart time.Time  `json:"window_start"`
	WindowEnd   time.Time  `json:"window_end"`
	OutputKey   string     `json:"output_key"`
	TempDir     string     `json:"temp_dir,omitempty"` // if empty the subprocess picks its own

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
type SubprocessResult struct {
	Success      bool   `json:"success"`
	BytesWritten int    `json:"bytes_written"`
	DurationMS   int64  `json:"duration_ms"`
	Error        string `json:"error,omitempty"`
}

// RunBuildJob is the subprocess entry point: parse config from stdin, run the
// build core (DuckDB COPY + upload), write result JSON to stdout.
// Called by cmd/arc/cli_rollup_build.go.
func RunBuildJob(cfg *SubprocessConfig) (*SubprocessResult, error) {
	subLogger := zerolog.New(os.Stderr).With().Timestamp().
		Str("component", "rollup-subprocess").
		Str("rollup", cfg.Spec.Name).
		Logger()

	subLogger.Info().
		Time("window_start", cfg.WindowStart).
		Time("window_end", cfg.WindowEnd).
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

	selectSQL, err := BuildWindowSQL(cfg.Spec, cfg.FromTable, cfg.WindowStart, cfg.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("build sql: %w", err)
	}

	tmpDir := cfg.TempDir
	if tmpDir == "" {
		tmpDir, err = os.MkdirTemp("", "arc-rollup-sub-")
		if err != nil {
			return nil, fmt.Errorf("temp dir: %w", err)
		}
	}
	defer os.RemoveAll(tmpDir)

	tmpFile := filepath.Join(tmpDir, "window.parquet")
	copyStmt := fmt.Sprintf(
		"COPY (%s) TO '%s' (FORMAT PARQUET)",
		selectSQL,
		strings.ReplaceAll(tmpFile, "'", "''"),
	)

	started := time.Now()
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

	// Forward subprocess stderr line-by-line at info level.
	if stderr.Len() > 0 {
		for _, line := range strings.Split(strings.TrimSpace(stderr.String()), "\n") {
			if line != "" {
				logger.Info().Str("subprocess", "rollup-build").Msg(line)
			}
		}
	}

	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("rollup-build subprocess cancelled: %w", ctx.Err())
		}
		return nil, fmt.Errorf("rollup-build subprocess failed: %w", err)
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
