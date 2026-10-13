package ingest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/rs/zerolog"
)

// quarantineDirName is the subdirectory of a stager's stage dir where
// unreadable source files are parked.
//
// Both stagers skip dot-prefixed entries and directories when scanning for
// work, so quarantined files are naturally excluded from future flushes.
const quarantineDirName = ".corrupt"

// parquetMagic bookends every parquet file. A file missing either copy was
// never completely written — which is exactly what a node ENOSPC burst
// leaves behind.
const parquetMagic = "PAR1"

// minParquetSize is 4 bytes of leading magic + 4 bytes of footer length +
// 4 bytes of trailing magic. Anything smaller cannot be a parquet file.
const minParquetSize = 12

// sourceVerdict is deliberately three-valued. Collapsing "I could not check
// this file" into "this file is bad" is how a quarantine mechanism turns into
// a data-loss mechanism: an EIO or EMFILE on a node under disk pressure — the
// very condition that produces corrupt files in the first place — would
// condemn perfectly good data.
type sourceVerdict int

const (
	sourceOK sourceVerdict = iota
	// sourceCorrupt: we read the bytes successfully and they are not parquet.
	sourceCorrupt
	// sourceUnknown: an I/O error stopped us from deciding. Never quarantine.
	sourceUnknown
)

// quarantineFile moves path into the stage dir's quarantine directory. The
// bytes are preserved, never deleted: a corrupt stage file is the only
// remaining evidence of whatever damaged it.
func quarantineFile(stageDir, path string) (string, error) {
	qdir := filepath.Join(stageDir, quarantineDirName)
	if err := os.MkdirAll(qdir, 0700); err != nil {
		return "", fmt.Errorf("quarantine mkdir %s: %w", qdir, err)
	}
	// Suffix with a nanosecond stamp so repeat quarantines of the same name
	// never clobber an earlier specimen.
	dst := filepath.Join(qdir, fmt.Sprintf("%s.%d", filepath.Base(path), time.Now().UnixNano()))
	if err := os.Rename(path, dst); err != nil {
		return "", fmt.Errorf("quarantine rename %s -> %s: %w", path, dst, err)
	}
	return dst, nil
}

// classifyParquet decides whether path is worth handing to DuckDB. It is
// deliberately cheap — a stat plus two 4-byte reads — because a wedged stager
// can accumulate tens of thousands of sources and this runs on every one of
// them, every flush.
//
// Only a SUCCESSFUL read that produces non-parquet bytes returns
// sourceCorrupt. Every I/O failure returns sourceUnknown.
func classifyParquet(path string) sourceVerdict {
	fi, err := os.Stat(path)
	if err != nil {
		return sourceUnknown
	}
	if fi.Size() < minParquetSize {
		return sourceCorrupt
	}
	f, err := os.Open(path)
	if err != nil {
		return sourceUnknown
	}
	defer f.Close()

	head := make([]byte, 4)
	if _, err := f.ReadAt(head, 0); err != nil {
		return sourceUnknown
	}
	if string(head) != parquetMagic {
		return sourceCorrupt
	}
	tail := make([]byte, 4)
	if _, err := f.ReadAt(tail, fi.Size()-4); err != nil {
		return sourceUnknown
	}
	if string(tail) != parquetMagic {
		return sourceCorrupt
	}
	return sourceOK
}

// quarantineStructurallyBad partitions paths into those worth merging and
// those already known to be unreadable, moving the latter aside.
//
// A file we could not classify is dropped from THIS merge but left on disk, so
// the next flush retries it. Same for a file we failed to move: it stays where
// it is and gets re-attempted rather than poisoning the merge again.
func quarantineStructurallyBad(stageDir string, paths []string, logger zerolog.Logger) []string {
	good := make([]string, 0, len(paths))
	quarantined, skipped := 0, 0
	for _, p := range paths {
		switch classifyParquet(p) {
		case sourceOK:
			good = append(good, p)
		case sourceCorrupt:
			if quarantineSource(stageDir, p, "not a parquet file (truncated or empty)", logger) {
				quarantined++
			} else {
				skipped++
			}
		default: // sourceUnknown
			skipped++
			logger.Warn().Str("source", filepath.Base(p)).
				Msg("stager: could not read staged source to classify it; leaving it in place for the next flush")
		}
	}
	if quarantined > 0 || skipped > 0 {
		logger.Error().
			Int("quarantined", quarantined).
			Int("left_in_place", skipped).
			Int("healthy", len(good)).
			Msg("stager: unreadable staged sources excluded from merge")
	}
	return good
}

// classifyProbeError decides what a DuckDB failure says about the FILE.
//
// DuckDB separates the two cases cleanly: a parse-class failure ("Invalid
// Input Error: File '...' too small to be a Parquet file", "Footer length
// error in file '...'") is a verdict about the bytes; an "IO Error" is a
// verdict about our ability to reach them. Anything unrecognised is treated
// as undecidable on purpose, so error text we have never seen can never
// condemn healthy data.
func classifyProbeError(err error) sourceVerdict {
	if err == nil {
		return sourceOK
	}
	if strings.Contains(err.Error(), "Invalid Input Error") {
		return sourceCorrupt
	}
	return sourceUnknown
}

// probeUnreadable splits paths into those DuckDB rejects as non-parquet and
// those it could not read for some other reason. It is only called after a
// merge has already failed.
//
// It aborts on a dead context rather than reporting every file as unreadable:
// database/sql returns ctx.Err() before the driver is ever reached, so without
// this guard an expired flush deadline would classify the ENTIRE backlog as
// corrupt. On abort the partial results are discarded, so there is no
// partial-quarantine window.
func probeUnreadable(ctx context.Context, paths []string, logger zerolog.Logger) (corrupt, unknown []string, err error) {
	d, openErr := sql.Open("duckdb", "")
	if openErr != nil {
		return nil, nil, fmt.Errorf("open duckdb: %w", openErr)
	}
	defer d.Close()

	for _, p := range paths {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, nil, fmt.Errorf("probe aborted: %w", ctxErr)
		}
		q := "SELECT 1 FROM " + sqlutil.ReadParquet(sqlutil.QuoteStringLiteral(p)) + " LIMIT 0"
		_, execErr := d.ExecContext(ctx, q)
		if execErr == nil {
			continue
		}
		// Re-check: a cancellation landing mid-query surfaces as a driver
		// INTERRUPT error, not a wrapped context error.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, nil, fmt.Errorf("probe aborted: %w", ctxErr)
		}
		switch classifyProbeError(execErr) {
		case sourceCorrupt:
			corrupt = append(corrupt, p)
		default:
			unknown = append(unknown, p)
			logger.Warn().Err(execErr).Str("source", filepath.Base(p)).
				Msg("stager: could not read staged source; leaving it in place for the next flush")
		}
	}
	return corrupt, unknown, nil
}

// execMergeWithQuarantine runs exec over paths. If the merge fails, it probes
// each source individually; any source DuckDB cannot read is quarantined and
// the merge is retried once with the survivors.
//
// This is the guarantee that matters: one unreadable file costs exactly
// itself. Before this existed, a single zero-length parquet failed the whole
// group's merge, and because the retry re-submitted the same source list the
// group could never drain — one ingest pod published nothing for 11 hours
// while 1154 healthy staged files queued behind the poison (2026-09-17).
//
// A merge that fails for a reason OTHER than an unreadable source — a type
// collision across schemas, or a cancelled context — is left alone: nothing is
// quarantined and the error propagates so the sources stay put for a genuine
// retry.
func execMergeWithQuarantine(
	ctx context.Context,
	stageDir string,
	paths []string,
	logger zerolog.Logger,
	exec func(ctx context.Context, paths []string) error,
) ([]string, error) {
	mergeErr := exec(ctx, paths)
	if mergeErr == nil {
		return paths, nil
	}

	// Never let a dead deadline be mistaken for corruption.
	if ctxErr := ctx.Err(); ctxErr != nil {
		logger.Warn().Err(mergeErr).
			Msg("stager: merge failed with an expired context; sources kept for the next flush")
		return nil, mergeErr
	}

	bad, undecidable, probeErr := probeUnreadable(ctx, paths, logger)
	if probeErr != nil {
		logger.Warn().Err(probeErr).Msg("stager: could not probe sources after merge failure; sources kept")
		return nil, mergeErr
	}
	if len(bad) == 0 && len(undecidable) == 0 {
		// Every source reads fine on its own; the merge failed for some other
		// reason. Leave the sources in place for a real retry.
		return nil, mergeErr
	}

	// Excluded = quarantined (corrupt) + skipped (undecidable). Undecidable
	// sources must leave the retry list too: keeping them would re-wedge the
	// whole group on every flush, which is the failure this function exists to
	// end. They stay on disk and are re-attempted next time.
	excluded := make(map[string]struct{}, len(bad)+len(undecidable))
	quarantined := 0
	for _, p := range bad {
		excluded[p] = struct{}{}
		if quarantineSource(stageDir, p, "DuckDB cannot read this source", logger) {
			quarantined++
		}
	}
	for _, p := range undecidable {
		excluded[p] = struct{}{}
	}
	survivors := make([]string, 0, len(paths)-len(excluded))
	for _, p := range paths {
		if _, skip := excluded[p]; !skip {
			survivors = append(survivors, p)
		}
	}
	logger.Error().
		Int("quarantined", quarantined).
		Int("left_in_place", len(undecidable)).
		Int("survivors", len(survivors)).
		Int("sources", len(paths)).
		Msg("stager: unreadable sources excluded after merge failure")

	if len(survivors) == 0 {
		// Loud on purpose: the whole group was excluded and nothing was
		// uploaded. Without this the only trace is per-file lines.
		logger.Error().
			Int("quarantined", quarantined).
			Int("left_in_place", len(undecidable)).
			Msg("stager: NO readable sources in this group; nothing uploaded")
		if len(undecidable) > 0 || quarantined == 0 {
			// Either some sources may still be good, or nothing actually moved
			// (e.g. a read-only stage dir). Surface the original failure so the
			// caller does not record a clean flush over data still on disk.
			return nil, mergeErr
		}
		return nil, nil
	}
	if err := exec(ctx, survivors); err != nil {
		return nil, err
	}
	return survivors, nil
}

// quarantineSource moves one file aside, reporting whether it was actually
// moved. A file that cannot be moved stays on disk and is retried next flush.
func quarantineSource(stageDir, path, reason string, logger zerolog.Logger) bool {
	dst, err := quarantineFile(stageDir, path)
	if err != nil {
		logger.Error().Err(err).Str("path", path).
			Msg("stager: failed to quarantine unreadable source; leaving it in place")
		return false
	}
	logger.Error().
		Str("source", filepath.Base(path)).
		Str("quarantined_to", dst).
		Str("reason", reason).
		Msg("stager: quarantined unreadable staged source")
	return true
}

// seedStagerSeq returns the highest sequence number already present in
// stageDir for the given marker, so a restarted process does not hand out
// sequence 1 again and overwrite a staged file that survived on the volume.
func seedStagerSeq(stageDir, marker string) int64 {
	entries, err := os.ReadDir(stageDir)
	if err != nil {
		return 0
	}
	var highest int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		parts := strings.SplitN(e.Name(), "__", 3)
		if len(parts) != 3 || !strings.HasPrefix(parts[2], marker) {
			continue
		}
		numeric := strings.TrimSuffix(strings.TrimPrefix(parts[2], marker), ".parquet")
		n, err := strconv.ParseInt(numeric, 10, 64)
		if err == nil && n > highest {
			highest = n
		}
	}
	return highest
}

// writeStageFileAtomic writes data to path via a dot-prefixed temp file and a
// rename, so a reader can never observe a partially written source. Both
// stagers skip dot-prefixed entries, so the temp file is invisible to scans.
//
// It returns only once the file is DURABLE: the temp file is fsynced before the
// rename and the directory after it. That is load-bearing since the merge with
// upstream's WAL checkpoints: a nil Stage lets the buffer mark the rows' WAL
// entries flushed, after which the WAL may be purged and this file is the only
// copy until it is uploaded. Without the fsyncs a node crash could leave an
// empty or missing file behind a checkpoint that already released the WAL.
func writeStageFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp := filepath.Join(dir, "."+filepath.Base(path)+".tmp")
	if err := writeFileSynced(tmp, data); err != nil {
		// Clean up the partial temp: this path is reached on ENOSPC, which is
		// exactly when leaving orphans behind is least affordable.
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := syncDir(dir); err != nil {
		// The rename may not survive a crash, so the stage is not durable.
		// Take the file back: the caller fails the flush and the WAL replays
		// these rows, and a file left behind would upload them a second time.
		_ = os.Remove(path)
		return fmt.Errorf("fsync stage dir %s: %w", dir, err)
	}
	return nil
}

// writeFileSynced writes data to path and fsyncs it before closing.
func writeFileSynced(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// syncDir fsyncs a directory so a rename or unlink in it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// removeStagedFile takes back a file this process staged moments ago, for a
// multi-hour flush that failed after staging part of its rows. A file that is
// already gone was picked up by a concurrent stager flush and may be uploaded:
// that is reported as an error so the caller logs the possible duplicate.
func removeStagedFile(path string) error {
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("staged file %s was already taken by a stager flush: %w", path, err)
		}
		return err
	}
	return syncDir(filepath.Dir(path))
}

// ErrStagedNotUploaded is returned by a stager's Close when staged files are
// still on the stage volume after the final drain. Their rows were already
// checkpointed out of the WAL, so the shutdown WAL purge must not treat the
// process as drained, and the operator has to know the files are the only copy.
var ErrStagedNotUploaded = errors.New("staged files not uploaded")

// defaultStagerCloseBudget is the final-drain budget when SetCloseBudget was
// never called (the pre-merge fixed value).
const defaultStagerCloseBudget = 30 * time.Second

// countStagedFiles reports how many staged sources carrying marker are waiting
// in stageDir. Quarantined files (.corrupt/) and dot-prefixed temp files are
// not counted: the former can never be uploaded, the latter are not yet
// staged.
func countStagedFiles(stageDir, marker string) (int, error) {
	entries, err := os.ReadDir(stageDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	n := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".parquet") {
			continue
		}
		parts := strings.SplitN(name, "__", 3)
		if len(parts) == 3 && strings.HasPrefix(parts[2], marker) {
			n++
		}
	}
	return n, nil
}

// stagerUploadExistsTimeout bounds the HEAD issued after a failed upload.
const stagerUploadExistsTimeout = 10 * time.Second
