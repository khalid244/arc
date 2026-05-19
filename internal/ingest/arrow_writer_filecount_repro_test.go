package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

// TestFileCountRepro reproduces the production file-explosion seen on
// hammel-arc: for the posthog.events measurement, ingest produces tens
// of thousands of small parquet files per hour partition. The two known
// multipliers we want to attribute and measure are:
//
//  1. Schema-change flushes: every distinct property set (event name)
//     forces a flush via flushOnSchemaChangeLocked. With ~20 distinct
//     event names from the iOS SDK, every batch triggers schema flushes.
//
//  2. Multi-hour split: each flush whose rows span more than one
//     event-time hour writes one parquet per hour (flushPartitionedData
//     "Splitting batch across multiple hour partitions" path).
//
// Run with: go test -run TestFileCountRepro -timeout 120s -v ./internal/ingest/
func TestFileCountRepro(t *testing.T) {
	if testing.Short() {
		t.Skip("file-count repro is long-running; skipped under -short")
	}

	tmpDir, err := os.MkdirTemp("", "arc-filecount-repro-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("storage dir: %s", tmpDir)
	defer os.RemoveAll(tmpDir)

	// Prod-equivalent config (matches ingest-config.yaml on cluster).
	cfg := &config.IngestConfig{
		MaxBufferSize:   600000,
		MaxBufferAgeMS:  30000,
		FlushWorkers:    16,
		FlushQueueSize:  1500,
		ShardCount:      32,
		Compression:     "zstd",
		DataPageVersion: "2.0",
	}

	logger := zerolog.New(os.Stdout).Level(zerolog.WarnLevel)
	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	buf := NewArrowBuffer(cfg, store, logger)
	defer buf.Close()

	// Optional FreshStager: when REPRO_USE_FRESH_STAGER=1 is set, route
	// fresh ingest through the per-pod local-stager → DuckDB-merge →
	// partition-by-hour → upload pipeline.
	if os.Getenv("REPRO_USE_FRESH_STAGER") == "1" {
		stageDir := filepath.Join(tmpDir, "_fresh_stager")
		fs, err := NewFreshStager(&FreshStagerConfig{
			Storage:  store,
			StageDir: stageDir,
			FlushAge: 5 * time.Second, // short for test
			Logger:   logger,
		})
		if err != nil {
			t.Fatalf("FreshStager: %v", err)
		}
		defer fs.Close()
		buf.SetFreshStager(fs)
		t.Logf("FreshStager ENABLED at %s (flush every 5s)", stageDir)
	}

	// Production-shaped synthetic schemas: ~20 distinct event names,
	// each with its own properties (no overlap across events) so every
	// new event name triggers a schema-change flush.
	numSchemas := envInt("REPRO_SCHEMAS", 20)
	schemas := makeSyntheticSchemas(numSchemas)

	// Each "batch" represents one client batch arriving at the ingest pod.
	// We model iOS clock-skew by making each record claim a `time` chosen
	// from one of `hoursSpread` hours in the past.
	duration := time.Duration(envInt("REPRO_DURATION_SEC", 30)) * time.Second
	writeRate := envInt("REPRO_RATE", 30)
	batchSize := envInt("REPRO_BATCH_SIZE", 25)
	hoursSpread := envInt("REPRO_HOURS_SPREAD", 4)

	t.Logf("config: shards=%d  max_buffer_age=%dms  max_buffer_size=%d",
		cfg.ShardCount, cfg.MaxBufferAgeMS, cfg.MaxBufferSize)
	t.Logf("load:   duration=%v  rate=%d batches/sec  batch_size=%d  schemas=%d  hours_spread=%d",
		duration, writeRate, batchSize, numSchemas, hoursSpread)

	ctx, cancel := context.WithTimeout(context.Background(), duration+10*time.Second)
	defer cancel()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	const writers = 3 // mimic 3 ingest pods
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(wid) * 7919))
			period := time.Second / time.Duration(writeRate)
			next := time.Now()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if d := time.Until(next); d > 0 {
					time.Sleep(d)
				}
				next = next.Add(period)

				// pick a schema randomly
				sch := schemas[r.Intn(len(schemas))]
				rec := makeRecord(sch, batchSize, hoursSpread, r)
				if err := buf.writeColumnar(ctx, "posthog", rec); err != nil {
					// transient errors expected under saturation
					_ = err
				}
			}
		}(w)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	// Final flush
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Logf("FlushAll error: %v", err)
	}
	// Give pending workers a moment to drain.
	time.Sleep(2 * time.Second)
	// Force any FreshStager to drain everything before we count files.
	if buf.freshStager != nil {
		_ = buf.freshStager.Flush(context.Background())
	}

	// Walk the storage tree and count parquet files.
	type pathCount struct{ path string; count int }
	counts := map[string]int{}
	totalFiles := 0
	totalBytes := int64(0)
	_ = filepath.Walk(tmpDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if strings.HasSuffix(p, ".parquet") {
			totalFiles++
			totalBytes += info.Size()
			// Bucket key: posthog/events/Y/M/D/H/
			rel := strings.TrimPrefix(p, tmpDir+"/")
			parts := strings.Split(rel, "/")
			if len(parts) >= 6 {
				bucket := strings.Join(parts[:6], "/")
				counts[bucket]++
			}
		}
		return nil
	})

	t.Logf("=== RESULT ===")
	t.Logf("  total parquet files written: %d", totalFiles)
	t.Logf("  total bytes:                  %d", totalBytes)
	if totalFiles > 0 {
		t.Logf("  avg file size:                %d B", totalBytes/int64(totalFiles))
	}
	t.Logf("  distinct partition buckets:   %d", len(counts))

	// Sample top-10 partitions
	type kv struct {
		k string
		v int
	}
	rows := make([]kv, 0, len(counts))
	for k, v := range counts {
		rows = append(rows, kv{k, v})
	}
	// simple selection-sort for top 10
	for i := 0; i < len(rows) && i < 10; i++ {
		best := i
		for j := i + 1; j < len(rows); j++ {
			if rows[j].v > rows[best].v {
				best = j
			}
		}
		rows[i], rows[best] = rows[best], rows[i]
		t.Logf("  %-50s  %d files", rows[i].k, rows[i].v)
	}

	t.Logf("=== counters ===")
	t.Logf("  totalFlushes:                 %d", buf.totalFlushes.Load())
	t.Logf("  totalRecordsWritten:          %d", buf.totalRecordsWritten.Load())
	t.Logf("  totalSchemaChurnExceeded:     %d", buf.totalSchemaChurnExceeded.Load())

	// Verify data integrity: count rows across all output parquet files.
	parquetPaths := []string{}
	_ = filepath.Walk(tmpDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(p, ".parquet") {
			return nil
		}
		// Skip fresh-stager scratch files
		if strings.Contains(p, "_fresh_stager") {
			return nil
		}
		parquetPaths = append(parquetPaths, p)
		return nil
	})
	if len(parquetPaths) == 0 {
		t.Logf("  (no output parquet files found for row-count verification)")
		return
	}
	rowsFound := 0
	errCount := 0
	var firstErr error
	for _, p := range parquetPaths {
		n, err := countRowsInParquet(p)
		if err != nil {
			errCount++
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		rowsFound += n
	}
	t.Logf("  rows actually present in output: %d  (read errors: %d, first: %v)",
		rowsFound, errCount, firstErr)
	t.Logf("  sample output path: %s", parquetPaths[0])
}

// countRowsInParquet reads a parquet file's metadata-level row count
// without loading the data into memory. Used by the test for integrity
// verification only.
func countRowsInParquet(path string) (int, error) {
	d, err := sql.Open("duckdb", "")
	if err != nil {
		return 0, err
	}
	defer d.Close()
	var n int
	row := d.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')",
		strings.ReplaceAll(strings.ReplaceAll(path, "\\", "\\\\"), "'", "''")))
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func envInt(k string, dflt int) int {
	v := os.Getenv(k)
	if v == "" {
		return dflt
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return dflt
	}
	return n
}

// makeSyntheticSchemas returns N distinct schemas with non-overlapping
// property names, mimicking the posthog iOS SDK pattern of one property
// set per event_name.
func makeSyntheticSchemas(n int) [][]string {
	out := make([][]string, 0, n)
	for i := 0; i < n; i++ {
		nProps := 3 + (i % 6) // 3..8 properties per schema
		cols := make([]string, 0, nProps+2)
		cols = append(cols, "time")
		for j := 0; j < nProps; j++ {
			cols = append(cols, fmt.Sprintf("prop_%02d_%02d", i, j))
		}
		out = append(out, cols)
	}
	return out
}

// makeRecord builds a ColumnarRecord with `n` rows for this schema.
// Each row's `time` is randomly chosen from one of `hoursSpread` hours
// in the past, so the multi-hour-split branch of flushPartitionedData
// is exercised the same way iOS clock-skew exercises it in prod.
func makeRecord(cols []string, n, hoursSpread int, r *rand.Rand) *models.ColumnarRecord {
	data := make(map[string][]interface{}, len(cols))
	now := time.Now().UTC()
	for _, c := range cols {
		col := make([]interface{}, n)
		if c == "time" {
			for i := range col {
				offset := time.Duration(r.Intn(hoursSpread)) * time.Hour
				col[i] = now.Add(-offset).UnixMicro()
			}
		} else {
			for i := range col {
				col[i] = "v"
			}
		}
		data[c] = col
	}
	return &models.ColumnarRecord{
		Measurement: "events",
		Columns:     data,
	}
}
