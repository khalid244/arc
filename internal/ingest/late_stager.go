package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// LateStager batches late-event parquet writes locally and uploads a single
// merged file every flushAge. Per-pod component.
//
// Without this, every schema-change in the ingest stream produces its own
// events_late parquet — at ~120 files/min in prod that's ~7K objects/hour.
// The stager merges them via DuckDB COPY(union_by_name=true) so events_late
// gets ~one file per pod per flushAge interval regardless of how many
// distinct event-type schemas appeared.
//
// Durability note: between Stage() returning success and the periodic
// flush uploading to S3, staged files live on local disk only. If the pod
// crashes in that window and the stager directory is on emptyDir, those
// records are lost. Arc's WAL replay covers the case only if WAL retention
// extends past the in-buffer flush ack — which today it doesn't. Acceptable
// for late events (mobile retry semantics absorb up to flushAge of loss);
// switch the stager dir to a PVC for stronger durability.
type LateStager struct {
	storage  storage.Backend
	stageDir string
	flushAge time.Duration
	logger   zerolog.Logger

	mu  sync.Mutex // serializes Stage + Flush against each other and against itself
	seq atomic.Int64

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

// LateStagerConfig holds configuration for constructing a LateStager.
type LateStagerConfig struct {
	Storage  storage.Backend
	StageDir string
	FlushAge time.Duration
	Logger   zerolog.Logger
}

// NewLateStager creates a stager and starts its background flush loop.
// Caller is responsible for calling Close() on shutdown so the final flush
// has a chance to drain remaining staged files.
func NewLateStager(cfg *LateStagerConfig) (*LateStager, error) {
	if cfg.Storage == nil {
		return nil, fmt.Errorf("late stager: storage backend is required")
	}
	if cfg.StageDir == "" {
		cfg.StageDir = "./data/ingest/late-stager"
	}
	if cfg.FlushAge <= 0 {
		cfg.FlushAge = 60 * time.Second
	}
	if err := os.MkdirAll(cfg.StageDir, 0700); err != nil {
		return nil, fmt.Errorf("late stager: mkdir %s: %w", cfg.StageDir, err)
	}
	s := &LateStager{
		storage:  cfg.Storage,
		stageDir: cfg.StageDir,
		flushAge: cfg.FlushAge,
		logger:   cfg.Logger.With().Str("component", "late-stager").Logger(),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	go s.runLoop()
	s.logger.Info().
		Str("stage_dir", s.stageDir).
		Dur("flush_age", s.flushAge).
		Msg("LateStager started")
	return s, nil
}

// Stage writes the given parquet bytes to local scratch keyed by
// (database, measurement). The periodic flush picks it up and merges with
// any other staged files for the same (db, measurement) pair.
//
// Filename embeds db + measurement + a monotonic seq so the flush can
// group files without re-parsing parquet metadata. The "_late_stage_"
// marker prevents directory listings from confusing these with real
// events_late S3 outputs.
func (s *LateStager) Stage(database, measurement string, parquetData []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.stageDir, 0700); err != nil {
		return fmt.Errorf("late stager: mkdir: %w", err)
	}
	seq := s.seq.Add(1)
	// db and measurement are operator-controlled; sanitize defensively.
	safeDB := sanitizeStagerSegment(database)
	safeMeas := sanitizeStagerSegment(measurement)
	name := fmt.Sprintf("%s__%s__late_stage_%d.parquet", safeDB, safeMeas, seq)
	path := filepath.Join(s.stageDir, name)
	if err := os.WriteFile(path, parquetData, 0600); err != nil {
		return fmt.Errorf("late stager: write %s: %w", path, err)
	}
	return nil
}

// Flush merges all staged parquets per (db, measurement) via DuckDB
// COPY ... union_by_name=true and uploads the merged file to events_late/.
// Source staged files are deleted only after upload succeeds.
func (s *LateStager) Flush(ctx context.Context) error {
	s.mu.Lock()
	entries, err := os.ReadDir(s.stageDir)
	if err != nil {
		s.mu.Unlock()
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("late stager: read stage dir: %w", err)
	}

	// Group staged files by (db, measurement). Pre-built filename pattern
	// makes this a string split rather than a parquet metadata read.
	type group struct {
		database    string
		measurement string
		files       []string
	}
	groups := make(map[string]*group)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		// Skip merged-output temp files (leading "." per mergeAndUpload below).
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		// Filename format: <db>__<measurement>__late_stage_<seq>.parquet
		parts := strings.SplitN(e.Name(), "__", 3)
		if len(parts) != 3 {
			continue
		}
		key := parts[0] + "/" + parts[1]
		g, ok := groups[key]
		if !ok {
			g = &group{database: parts[0], measurement: parts[1]}
			groups[key] = g
		}
		g.files = append(g.files, filepath.Join(s.stageDir, e.Name()))
	}
	s.mu.Unlock()

	if len(groups) == 0 {
		return nil
	}

	for _, g := range groups {
		if err := s.mergeAndUpload(ctx, g.database, g.measurement, g.files); err != nil {
			s.logger.Error().Err(err).
				Str("database", g.database).
				Str("measurement", g.measurement).
				Int("staged_files", len(g.files)).
				Msg("LateStager: merge failed; sources remain staged for retry")
			continue
		}
	}
	return nil
}

// mergeAndUpload runs DuckDB COPY across the staged files (union_by_name=true
// so schema differences resolve to a single output schema with NULLs for
// missing columns), uploads the result, and deletes the source files.
//
// On any failure between merge and upload-success, source files stay
// in place so the next flush retries them. Idempotent: re-running the same
// inputs produces a new output filename (wallclock+nanos) — duplicates get
// folded by the reorganizer / daily tag-dedup.
func (s *LateStager) mergeAndUpload(ctx context.Context, db, measurement string, paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	mergedLocal := filepath.Join(s.stageDir, fmt.Sprintf(".merged_%s_%s_%d.parquet",
		db, measurement, time.Now().UnixNano()))
	defer os.Remove(mergedLocal)

	d, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer d.Close()

	// Build a DuckDB array literal of all staged file paths. Each path is
	// single-quote escaped. Stager dir paths are operator-controlled so
	// we don't need extreme escaping, but be defensive.
	var b strings.Builder
	b.WriteByte('[')
	for i, p := range paths {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('\'')
		b.WriteString(strings.ReplaceAll(strings.ReplaceAll(p, "\\", "\\\\"), "'", "''"))
		b.WriteByte('\'')
	}
	b.WriteByte(']')
	fileList := b.String()

	q := fmt.Sprintf(
		`COPY (SELECT * FROM read_parquet(%s, union_by_name = true)) TO '%s' (FORMAT PARQUET)`,
		fileList,
		strings.ReplaceAll(strings.ReplaceAll(mergedLocal, "\\", "\\\\"), "'", "''"),
	)
	if _, err := d.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("duckdb merge: %w", err)
	}

	// Upload merged file. Path matches the existing events_late/ convention
	// in arrow_writer.go's flatLatePath so the reorganizer picks it up.
	lateMeasurement := measurement + "_late"
	now := time.Now().UTC()
	timestamp := now.Format("20060102_150405")
	nanos := now.UnixNano() % 1_000_000_000
	storagePath := fmt.Sprintf("%s/%s/%s_%s_%09d.parquet",
		db, lateMeasurement, lateMeasurement, timestamp, nanos)

	f, err := os.Open(mergedLocal)
	if err != nil {
		return fmt.Errorf("open merged %s: %w", mergedLocal, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat merged: %w", err)
	}
	if err := s.storage.WriteReader(ctx, storagePath, f, fi.Size()); err != nil {
		return fmt.Errorf("upload %s: %w", storagePath, err)
	}

	// Delete source staged files only after upload succeeded. Per-file
	// best-effort; a stragglers file just gets re-merged into the next
	// upload (DuckDB dedup at downstream compaction folds duplicates).
	for _, p := range paths {
		if err := os.Remove(p); err != nil {
			s.logger.Warn().Err(err).Str("path", p).Msg("LateStager: failed to delete staged file post-upload")
		}
	}

	s.logger.Info().
		Str("database", db).
		Str("measurement", measurement).
		Int("staged_files", len(paths)).
		Str("storage_path", storagePath).
		Int64("size_bytes", fi.Size()).
		Msg("LateStager merged + uploaded")
	return nil
}

// runLoop fires Flush() on flushAge intervals. Exits on Close().
func (s *LateStager) runLoop() {
	defer close(s.doneCh)
	ticker := time.NewTicker(s.flushAge)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			if err := s.Flush(ctx); err != nil {
				s.logger.Error().Err(err).Msg("LateStager: scheduled flush failed")
			}
			cancel()
		case <-s.stopCh:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := s.Flush(ctx); err != nil {
				s.logger.Error().Err(err).Msg("LateStager: shutdown flush failed (staged files remain on disk)")
			}
			cancel()
			return
		}
	}
}

// Close stops the background loop. Blocks until the final flush completes.
func (s *LateStager) Close() error {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	<-s.doneCh
	return nil
}

// sanitizeStagerSegment strips path separators and other dangerous characters
// from db/measurement names used in stager filenames. Identifiers in Arc are
// already validated by the ingest API; this is defense in depth.
func sanitizeStagerSegment(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, "..", "_")
	s = strings.ReplaceAll(s, "__", "_") // collapse double underscores so we can use __ as a separator
	return s
}
