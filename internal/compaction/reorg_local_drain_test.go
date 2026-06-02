package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestReorgLocalDrain_OutputAmplification reproduces, locally, the prod
// hammel-arc reorg behaviour that makes backlog buckets so expensive to drain:
// a single late ingest-hour bucket whose rows (iOS clock-skew) span many
// EVENT-hours is rewritten via COPY ... PARTITION_BY(_y,_m,_d,_h), producing one
// output parquet PER event-hour. So a handful of input files explode into
// hundreds of tiny outputs — and in prod those thousands of output uploads are
// the upload-bound bottleneck (download_workers can't speed them up).
//
// The test also asserts the drain is lossless (the row-count audit passes) and
// that it commits (source files are deleted), exercising the real Reorganizer.
func TestReorgLocalDrain_OutputAmplification(t *testing.T) {
	t.Setenv("TZ", "UTC") // partitioning extracts UTC Y/M/D; pin the session TZ so day boundaries are deterministic (prod containers run UTC)
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)

	backend, err := storage.NewLocalBackend(tmp, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}

	const (
		numInputFiles = 4
		daysPerFile   = 3                           // distinct event-DAYS per input file
		totalDays     = numInputFiles * daysPerFile // 12 distinct event-days
		hoursPerDay   = 12                          // rows at 06:00..17:00 UTC — midday, so a session-TZ shift never moves a row across a day boundary (deterministic day count)
		totalRows     = totalDays * hoursPerDay     // 144
	)
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	if err := os.MkdirAll(lateDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Generate synthetic late parquet via DuckDB. All files share ingest-hour
	// 2026-05-20 03:00 (=> one closed bucket). Each file holds daysPerFile
	// distinct event-DAYS, each day with hoursPerDay midday rows — the exact
	// shape (rows spanning many event-hours across many days) that made the old
	// hourly PARTITION_BY explode (one output per event-hour) and that day
	// PARTITION_BY collapses to one output per event-day.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	for f := 0; f < numInputFiles; f++ {
		path := filepath.Join(lateDir, fmt.Sprintf("events_late_20260520_030000_%d.parquet", f))
		q := fmt.Sprintf(`COPY (
			SELECT TIMESTAMP '2026-04-01 06:00:00' + (d * INTERVAL 1 DAY) + (h * INTERVAL 1 HOUR) AS time,
			       'host-' || (h %% 3) AS host,
			       (d * 100 + h)::BIGINT AS value
			FROM range(%d, %d) days(d), range(0, %d) hrs(h)
		) TO '%s' (FORMAT PARQUET)`, f*daysPerFile, (f+1)*daysPerFile, hoursPerDay, escapeSQLPath(path))
		if _, err := db.ExecContext(ctx, q); err != nil {
			db.Close()
			t.Fatalf("generate parquet %d: %v", f, err)
		}
	}
	db.Close()

	if got := countParquetFlat(t, lateDir); got != numInputFiles {
		t.Fatalf("setup: expected %d input files, got %d", numInputFiles, got)
	}

	scratch := filepath.Join(tmp, "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}

	reorg := &Reorganizer{
		Backend:          backend,
		Databases:        []string{"posthog"},
		Measurements:     []string{"events"},
		MinAgeSeconds:    3600,
		TempDirectory:    filepath.Join(tmp, "scratch"),
		MaxConcurrent:    1,
		MaxFilesPerBatch: 2000,
		DownloadWorkers:  4,
		ManifestManager:  NewReorgManifestManager(backend, logger),
		Logger:           logger,
	}
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run: %v", err)
	}

	// Drain committed: events_late/ emptied.
	if remaining := countParquetFlat(t, lateDir); remaining != 0 {
		t.Errorf("expected events_late/ drained to 0, still %d files", remaining)
	}

	eventsDir := filepath.Join(tmp, "posthog", "events")
	outputFiles := countParquetTree(t, eventsDir)
	outputRows := sumParquetRowsTree(t, eventsDir)

	t.Logf("DAILY REORG: %d input late files (%d rows spanning %d event-days x %d midday hours) -> %d output files (hourly partitioning would have been %d; lossless)",
		numInputFiles, totalRows, totalDays, hoursPerDay, outputFiles, totalRows)

	// Lossless: the reorg row-count audit already enforces this; verify the rows
	// actually landed in the output partitions.
	if outputRows != int64(totalRows) {
		t.Errorf("row loss/dup: input %d rows, output %d rows", totalRows, outputRows)
	}
	// Day-granularity: exactly one output file per distinct event-DAY (not per
	// hour) — the ~24x output reduction vs the old hourly partitioning, and
	// deterministic regardless of session TZ (midday rows don't cross day lines).
	if outputFiles != totalDays {
		t.Errorf("expected %d daily output files (one per event-day), got %d", totalDays, outputFiles)
	}
}

func countParquetFlat(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("readdir %s: %v", dir, err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".parquet" {
			n++
		}
	}
	return n
}

func countParquetTree(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Ext(p) == ".parquet" {
			n++
		}
		return nil
	})
	return n
}

func sumParquetRowsTree(t *testing.T, dir string) int64 {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	glob := escapeSQLPath(filepath.Join(dir, "**", "*.parquet"))
	var n int64
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s', union_by_name=true)`, glob)).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}
