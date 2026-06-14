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

// TestReorgDiscoverAllSidecars verifies the default-on drain: with an EMPTY
// Measurements list, the reorganizer DISCOVERS every <db>/<m>_late/ sidecar
// (via storage.DirectoryLister) and drains each into its normal Y/M/D/H
// partitions. This covers all default-on tables plus future ones without an
// opt-in list.
func TestReorgDiscoverAllSidecars(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := context.Background()
	tmp := t.TempDir()
	logger := zerolog.New(io.Discard)

	backend, err := storage.NewLocalBackend(tmp, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}

	const db = "posthog"
	// Seed two DIFFERENT sidecars with parquet, each with a closed ingest-hour
	// bucket. Rows are at midday on a fixed day -> one output partition each.
	sidecars := []struct {
		base string // base measurement (no _late)
	}{
		{base: "events"},
		{base: "lifecycle"},
	}

	dbConn, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	for _, s := range sidecars {
		lateName := s.base + "_late"
		lateDir := filepath.Join(tmp, db, lateName)
		if err := os.MkdirAll(lateDir, 0o755); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(lateDir, fmt.Sprintf("%s_20260520_030000_0.parquet", lateName))
		q := fmt.Sprintf(`COPY (
			SELECT TIMESTAMP '2026-04-01 06:00:00' + (h * INTERVAL 1 HOUR) AS time,
			       'host-' || (h %% 3) AS host,
			       h::BIGINT AS value
			FROM range(0, 6) hrs(h)
		) TO '%s' (FORMAT PARQUET)`, escapeSQLPath(path))
		if _, err := dbConn.ExecContext(ctx, q); err != nil {
			dbConn.Close()
			t.Fatalf("generate parquet for %s: %v", lateName, err)
		}
	}
	dbConn.Close()

	for _, s := range sidecars {
		lateDir := filepath.Join(tmp, db, s.base+"_late")
		if got := countParquetFlat(t, lateDir); got != 1 {
			t.Fatalf("setup: expected 1 input file in %s_late, got %d", s.base, got)
		}
	}

	scratch := filepath.Join(tmp, "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}

	reorg := &Reorganizer{
		Backend:          backend,
		Databases:        []string{db},
		Measurements:     []string{}, // EMPTY: must discover sidecars itself
		MinAgeSeconds:    3600,
		TempDirectory:    scratch,
		MaxConcurrent:    1,
		MaxFilesPerBatch: 2000,
		DownloadWorkers:  4,
		ManifestManager:  NewReorgManifestManager(backend, logger),
		Logger:           logger,
	}
	if err := reorg.Run(ctx); err != nil {
		t.Fatalf("reorg.Run: %v", err)
	}

	// Both sidecars discovered & drained: each <m>_late/ emptied and the rows
	// landed under the normal <m>/Y/M/D/H partition tree.
	for _, s := range sidecars {
		lateDir := filepath.Join(tmp, db, s.base+"_late")
		if remaining := countParquetFlat(t, lateDir); remaining != 0 {
			t.Errorf("%s_late not drained: still %d files", s.base, remaining)
		}
		normalDir := filepath.Join(tmp, db, s.base)
		if got := countParquetTree(t, normalDir); got == 0 {
			t.Errorf("%s: expected drained output under %s, got 0 files", s.base, normalDir)
		}
		if rows := sumParquetRowsTree(t, normalDir); rows != 6 {
			t.Errorf("%s: expected 6 rows drained, got %d", s.base, rows)
		}
	}
}
