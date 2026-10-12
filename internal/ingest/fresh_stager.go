package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	_ "github.com/duckdb/duckdb-go/v2" // register database/sql driver
	"github.com/rs/zerolog"
)

// FreshStager mirrors LateStager but for the FRESH ingest path. Each
// schema-change-driven flush stages its parquet locally; periodically the
// stager merges all staged files via DuckDB COPY ... union_by_name=true
// AND re-partitions the merged rows by event-time hour (Y/M/D/H) before
// uploading. The result: one upload per (pod, partition, flush-window)
// regardless of how many distinct event schemas appeared.
//
// Compared to LateStager:
//   - Output goes to events/Y/M/D/H/ (the live query target), not the flat
//     events_late/ sidecar.
//   - The merge SQL adds PARTITION_BY (_y, _m, _d, _h) so the unified rows
//     are written to many partition-correct output files in one COPY.
//
// Durability semantics match LateStager: a staged file is fsynced before
// Stage returns, and from then on it is the ONLY copy of its rows — the
// buffer checkpoints their WAL entries on a nil Stage. It survives a process
// crash on the stage volume (the next process uploads it) but not the loss of
// the volume (pod deletion of an emptyDir). Close reports files that are still
// staged after the final drain (ErrStagedNotUploaded) so shutdown can keep the
// WAL and alert instead of reporting a clean drain.
type FreshStager struct {
	storage  storage.Backend
	stageDir string
	flushAge time.Duration
	logger   zerolog.Logger

	mu  sync.Mutex
	seq atomic.Int64

	// closeBudget bounds the final drain in Close (nanoseconds; 0 = default).
	closeBudget atomic.Int64

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

type FreshStagerConfig struct {
	Storage  storage.Backend
	StageDir string
	FlushAge time.Duration
	Logger   zerolog.Logger
}

func NewFreshStager(cfg *FreshStagerConfig) (*FreshStager, error) {
	if cfg.Storage == nil {
		return nil, fmt.Errorf("fresh stager: storage backend required")
	}
	if cfg.StageDir == "" {
		cfg.StageDir = "./data/ingest/fresh-stager"
	}
	if cfg.FlushAge <= 0 {
		cfg.FlushAge = 60 * time.Second
	}
	if err := os.MkdirAll(cfg.StageDir, 0700); err != nil {
		return nil, fmt.Errorf("fresh stager: mkdir %s: %w", cfg.StageDir, err)
	}
	s := &FreshStager{
		storage:  cfg.Storage,
		stageDir: cfg.StageDir,
		flushAge: cfg.FlushAge,
		logger:   cfg.Logger.With().Str("component", "fresh-stager").Logger(),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	// A surviving stage dir (emptyDir outlives a container restart) may already
	// hold files; restarting the counter at 1 would overwrite them.
	s.seq.Store(seedStagerSeq(s.stageDir, "fresh_stage_"))
	go s.runLoop()
	s.logger.Info().
		Str("stage_dir", s.stageDir).
		Dur("flush_age", s.flushAge).
		Int64("seq_seed", s.seq.Load()).
		Msg("FreshStager started")
	return s, nil
}

// Stage durably writes parquetData to the stage dir (see writeStageFileAtomic).
// A nil return means the file is fsynced and will be uploaded by a later flush.
func (s *FreshStager) Stage(database, measurement string, parquetData []byte) error {
	_, err := s.stageFile(database, measurement, parquetData)
	return err
}

// stageFile is Stage that also returns the staged path, so a multi-hour flush
// that fails after staging can take the file back (unstage).
func (s *FreshStager) stageFile(database, measurement string, parquetData []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(s.stageDir, 0700); err != nil {
		return "", fmt.Errorf("fresh stager: mkdir: %w", err)
	}
	seq := s.seq.Add(1)
	safeDB := encodeStagerSegment(database)
	safeMeas := encodeStagerSegment(measurement)
	name := fmt.Sprintf("%s__%s__fresh_stage_%d.parquet", safeDB, safeMeas, seq)
	path := filepath.Join(s.stageDir, name)
	if err := writeStageFileAtomic(path, parquetData); err != nil {
		return "", fmt.Errorf("fresh stager: write %s: %w", path, err)
	}
	return path, nil
}

// unstage removes a file returned by stageFile. Serialized with Stage and the
// directory scan in Flush; a file a running Flush already listed is reported
// as an error by removeStagedFile (it may be uploaded).
func (s *FreshStager) unstage(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return removeStagedFile(path)
}

// Pending reports how many staged files are waiting for upload. The shutdown
// WAL purge must not run while this is non-zero: those rows are no longer in
// the WAL.
func (s *FreshStager) Pending() (int, error) {
	return countStagedFiles(s.stageDir, "fresh_stage_")
}

// SetCloseBudget bounds the final drain performed by Close. main.go passes the
// share of server.shutdown_timeout it reserves for the stagers; without it the
// drain gets defaultStagerCloseBudget (30s).
func (s *FreshStager) SetCloseBudget(d time.Duration) {
	if d > 0 {
		s.closeBudget.Store(int64(d))
	}
}

func (s *FreshStager) closeDrainBudget() time.Duration {
	if d := time.Duration(s.closeBudget.Load()); d > 0 {
		return d
	}
	return defaultStagerCloseBudget
}

func (s *FreshStager) Flush(ctx context.Context) error {
	s.mu.Lock()
	entries, err := os.ReadDir(s.stageDir)
	if err != nil {
		s.mu.Unlock()
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("fresh stager: read stage dir: %w", err)
	}
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
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		// Filename: <db>__<measurement>__fresh_stage_<seq>.parquet
		parts := strings.SplitN(e.Name(), "__", 3)
		if len(parts) != 3 {
			continue
		}
		// reject anything that's not our fresh_stage marker
		if !strings.HasPrefix(parts[2], "fresh_stage_") {
			continue
		}
		key := parts[0] + "/" + parts[1]
		g, ok := groups[key]
		if !ok {
			g = &group{database: decodeStagerSegment(parts[0]), measurement: decodeStagerSegment(parts[1])}
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
				Msg("FreshStager: merge failed; sources remain for retry")
			continue
		}
	}
	return nil
}

// mergeAndUpload merges all staged parquets for (db, measurement) using
// DuckDB COPY ... PARTITION_BY (_y,_m,_d,_h), then walks the output tree
// and uploads each per-partition file to events/Y/M/D/H/.
func (s *FreshStager) mergeAndUpload(ctx context.Context, db, measurement string, paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	// Drop sources that cannot possibly be parquet before DuckDB ever sees
	// them. A single unreadable file used to fail the whole group's merge
	// forever, because the retry re-submitted the same poisoned list.
	paths = quarantineStructurallyBad(s.stageDir, paths, s.logger)
	if len(paths) == 0 {
		return nil
	}

	outDir, err := os.MkdirTemp(s.stageDir, ".merge_out_")
	if err != nil {
		return fmt.Errorf("mktmpdir: %w", err)
	}
	defer os.RemoveAll(outDir)

	merged, err := execMergeWithQuarantine(ctx, s.stageDir, paths, s.logger,
		func(ctx context.Context, attempt []string) error {
			return s.runMergeCopy(ctx, measurement, attempt, outDir)
		})
	if err != nil {
		return err
	}
	if len(merged) == 0 {
		return nil
	}
	paths = merged

	// Walk outDir: structure is <outDir>/_y=YYYY/_m=M/_d=D/_h=H/<file>.parquet
	// (DuckDB emits integer values without zero-pad; we pad to match Arc's
	// generateStoragePath format YYYY/MM/DD/HH.)
	uploaded := 0
	now := time.Now().UTC()
	// Track every key we've successfully written to S3 in THIS flush. If
	// the walk fails partway, we roll those back so the next retry doesn't
	// double-upload. Without this, a partial walk + a successful retry
	// produces duplicates for the partitions that the first attempt
	// already uploaded.
	uploadedKeys := []string{}
	walkErr := filepath.Walk(outDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(p, ".parquet") {
			return nil
		}
		rel, err := filepath.Rel(outDir, p)
		if err != nil {
			return nil
		}
		// rel looks like "_y=2026/_m=5/_d=19/_h=7/events_<uuid>.parquet"
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 5 {
			s.logger.Warn().Str("rel", rel).Msg("fresh stager: unexpected output path shape; skipping")
			return nil
		}
		yi, mi, di, hi, ok := parsePartitionDirs(parts[0], parts[1], parts[2], parts[3])
		if !ok {
			s.logger.Warn().Str("rel", rel).Msg("fresh stager: bad partition dirs; skipping")
			return nil
		}
		// Build the same path shape Arc's generateStoragePath uses: zero-padded.
		// Use a wall-clock timestamp + a fresh nano so uploaded names don't
		// collide with concurrent direct-S3 writes.
		ts := now.Format("20060102_150405")
		nanos := now.UnixNano()%1_000_000_000 + int64(s.seq.Add(1))%1_000_000_000
		storagePath := fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d/%s_%s_%09d.parquet",
			db, measurement, yi, mi, di, hi, measurement, ts, nanos)
		f, err := os.Open(p)
		if err != nil {
			return fmt.Errorf("open %s: %w", p, err)
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return fmt.Errorf("stat %s: %w", p, err)
		}
		if err := s.storage.WriteReader(ctx, storagePath, f, fi.Size()); err != nil {
			f.Close()
			if !uploadedDespiteError(s.storage, storagePath) {
				return fmt.Errorf("upload %s -> %s: %w", p, storagePath, err)
			}
			s.logger.Warn().Err(err).Str("storage_path", storagePath).
				Msg("FreshStager: upload returned an error but the object exists - treating as uploaded")
		} else {
			f.Close()
		}
		uploaded++
		uploadedKeys = append(uploadedKeys, storagePath)
		return nil
	})
	if walkErr != nil {
		// Roll back any uploads that succeeded in this aborted attempt so the
		// next flush retry won't duplicate them. Best-effort delete; if some
		// of these still hang around after rollback fails, downstream
		// dedup at compaction will resolve them, but the retry-as-clean
		// path is the primary protection.
		if len(uploadedKeys) > 0 {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			s.logger.Warn().
				Err(walkErr).
				Int("partial_uploads", len(uploadedKeys)).
				Msg("FreshStager: walk failed; rolling back partial uploads")
			for _, k := range uploadedKeys {
				if delErr := s.storage.Delete(rollbackCtx, k); delErr != nil {
					s.logger.Warn().Err(delErr).Str("key", k).
						Msg("FreshStager: rollback delete failed; downstream dedup will resolve")
				}
			}
			cancel()
		}
		return walkErr
	}

	// Delete source staged files only after all uploads succeed.
	for _, p := range paths {
		if err := os.Remove(p); err != nil {
			s.logger.Warn().Err(err).Str("path", p).Msg("FreshStager: failed to delete staged source")
		}
	}

	s.logger.Info().
		Str("database", db).
		Str("measurement", measurement).
		Int("staged_files", len(paths)).
		Int("uploaded_partition_files", uploaded).
		Msg("FreshStager merged + partitioned + uploaded")
	return nil
}

// runMergeCopy merges paths into outDir, partitioned by event-time hour.
// outDir is cleared first: COPY ... OVERWRITE_OR_IGNORE would otherwise skip
// partitions left behind by a previous attempt, so a retry must start clean.
func (s *FreshStager) runMergeCopy(ctx context.Context, measurement string, paths []string, outDir string) error {
	if err := os.RemoveAll(outDir); err != nil {
		return fmt.Errorf("clear merge outdir: %w", err)
	}
	if err := os.MkdirAll(outDir, 0700); err != nil {
		return fmt.Errorf("recreate merge outdir: %w", err)
	}

	d, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer d.Close()

	// Quoted list of input file paths, read through sqlutil so Hive partition
	// inference stays off (#1005).
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = sqlutil.QuoteStringLiteral(p)
	}
	source := sqlutil.ReadParquetList(quoted, "union_by_name=true")

	outDirLit := sqlutil.QuoteStringLiteral(outDir)
	mergedFilenamePattern := sqlutil.QuoteStringLiteral(fmt.Sprintf("%s_{uuid}", encodeStagerSegment(measurement)))

	q := fmt.Sprintf(`COPY (
  SELECT *,
         CAST(extract(year  FROM time) AS INTEGER) AS _y,
         CAST(extract(month FROM time) AS INTEGER) AS _m,
         CAST(extract(day   FROM time) AS INTEGER) AS _d,
         CAST(extract(hour  FROM time) AS INTEGER) AS _h
  FROM %s
  WHERE time IS NOT NULL
) TO %s (
  FORMAT PARQUET,
  PARTITION_BY (_y, _m, _d, _h),
  COMPRESSION ZSTD,
  OVERWRITE_OR_IGNORE,
  FILENAME_PATTERN %s
)`, source, outDirLit, mergedFilenamePattern)

	// One connection for both statements: SET is per connection, and the pool
	// may hand the COPY a different one.
	conn, err := d.Conn(ctx)
	if err != nil {
		return fmt.Errorf("duckdb conn: %w", err)
	}
	defer conn.Close()
	// The partition hour is extract(hour FROM time). Arc writes time as a
	// UTC-adjusted timestamp, which DuckDB reads as TIMESTAMPTZ and extracts
	// in the SESSION time zone, so on a host whose TZ is not UTC every row
	// would be filed under the wrong hour (or day). Pin UTC like the main
	// DuckDB and reorg do, and refuse to merge if that fails: a misfiled
	// partition is silent, a failed merge keeps the sources for a retry.
	if _, err := conn.ExecContext(ctx, "SET TimeZone='UTC'"); err != nil {
		return fmt.Errorf("pin duckdb TimeZone to UTC: %w", err)
	}
	if _, err := conn.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("duckdb merge: %w", err)
	}
	return nil
}

func (s *FreshStager) runLoop() {
	defer close(s.doneCh)
	ticker := time.NewTicker(s.flushAge)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			if err := s.Flush(ctx); err != nil {
				s.logger.Error().Err(err).Msg("FreshStager: scheduled flush failed")
			}
			cancel()
		case <-s.stopCh:
			ctx, cancel := context.WithTimeout(context.Background(), s.closeDrainBudget())
			if err := s.Flush(ctx); err != nil {
				s.logger.Error().Err(err).Msg("FreshStager: shutdown flush failed")
			}
			cancel()
			return
		}
	}
}

// Close stops the background loop after one final drain bounded by the close
// budget. It returns an error wrapping ErrStagedNotUploaded when staged files
// remain: their rows are no longer in the WAL, so the caller must not report a
// clean shutdown (and must not purge the WAL on the strength of one).
func (s *FreshStager) Close() error {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	<-s.doneCh
	n, err := s.Pending()
	if err != nil {
		return fmt.Errorf("fresh stager: cannot verify %s is drained: %w", s.stageDir, err)
	}
	if n > 0 {
		s.logger.Error().Int("staged_files", n).Str("stage_dir", s.stageDir).
			Msg("FreshStager: staged files were not uploaded before shutdown; they are the only copy of their rows")
		return fmt.Errorf("fresh stager: %d staged file(s) in %s: %w", n, s.stageDir, ErrStagedNotUploaded)
	}
	return nil
}

// parsePartitionDirs accepts the four "_x=N" dir name components DuckDB
// emits and returns y,m,d,h as ints.
func parsePartitionDirs(y, m, d, h string) (int, int, int, int, bool) {
	yi, ok := parseKVInt(y, "_y=")
	if !ok {
		return 0, 0, 0, 0, false
	}
	mi, ok := parseKVInt(m, "_m=")
	if !ok {
		return 0, 0, 0, 0, false
	}
	di, ok := parseKVInt(d, "_d=")
	if !ok {
		return 0, 0, 0, 0, false
	}
	hi, ok := parseKVInt(h, "_h=")
	if !ok {
		return 0, 0, 0, 0, false
	}
	return yi, mi, di, hi, true
}

func parseKVInt(s, prefix string) (int, bool) {
	if !strings.HasPrefix(s, prefix) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(s, prefix))
	if err != nil {
		return 0, false
	}
	return n, true
}
