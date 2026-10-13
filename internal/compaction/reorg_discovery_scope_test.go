package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// CS-11: database discovery must skip Arc's reserved root directories, and a
// "<x>_late" directory is drained only for what the ingest's late routing
// writes there: flat files named <x>_late_YYYYMMDD_HHMMSS_<digits>.parquet.

func writeParquetFile(t *testing.T, db *sql.DB, full string, selectBody string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET)`, selectBody, escapeSQLPath(full))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write %s: %v", full, err)
	}
}

func discoveryReorg(t *testing.T, tmp string, backend storage.Backend) *Reorganizer {
	t.Helper()
	scratch := filepath.Join(tmp, "scratch-reorg")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	logger := zerolog.New(io.Discard)
	return &Reorganizer{
		Backend:          backend,
		Databases:        nil, // discover
		MinAgeSeconds:    3600,
		TempDirectory:    scratch,
		MaxConcurrent:    1,
		MaxFilesPerBatch: 2000,
		DownloadWorkers:  2,
		ManifestManager:  NewReorgManifestManager(backend, logger),
		Logger:           logger,
	}
}

func fileExists(t *testing.T, p string) bool {
	t.Helper()
	_, err := os.Stat(p)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	t.Fatalf("stat %s: %v", p, err)
	return false
}

func TestReorgDiscovery_SkipsReservedRootDirs(t *testing.T) {
	t.Setenv("TZ", "UTC")
	tmp := t.TempDir()
	backend, err := storage.NewLocalBackend(tmp, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := `SELECT TIMESTAMP '2026-04-01 12:00:00' AS time, 'h'::VARCHAR AS host, 1::BIGINT AS value`
	// Sidecar-shaped files under reserved roots: Arc state, never a database.
	reserved := []string{
		filepath.Join(tmp, "_arc", "foo_late", "foo_late_20260520_030000_000000001.parquet"),
		filepath.Join(tmp, "_schema", "bar_late", "bar_late_20260520_030000_000000001.parquet"),
		filepath.Join(tmp, ".hidden", "baz_late", "baz_late_20260520_030000_000000001.parquet"),
	}
	for _, p := range reserved {
		writeParquetFile(t, db, p, row)
	}
	// A real database next to them still drains.
	real := filepath.Join(tmp, "posthog", "events_late", "events_late_20260520_030000_000000001.parquet")
	writeParquetFile(t, db, real, row)

	if err := discoveryReorg(t, tmp, backend).Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, p := range reserved {
		if !fileExists(t, p) {
			t.Errorf("a file under a reserved root was drained/deleted: %s", p)
		}
	}
	for _, d := range []string{filepath.Join(tmp, "_arc", "foo"), filepath.Join(tmp, "_schema", "bar"), filepath.Join(tmp, ".hidden", "baz")} {
		if fileExists(t, d) {
			t.Errorf("reorg wrote outputs under a reserved root: %s", d)
		}
	}
	if fileExists(t, real) {
		t.Error("the real database's sidecar must still drain")
	}
}

// A real measurement whose name happens to end in "_late" keeps its normal
// Y/M/D/H layout. Its files must never be moved into another measurement, even
// when the base name exists and a file's uid8 is all digits (so its basename
// matches the late-file time pattern).
func TestReorgDiscovery_RealMeasurementEndingInLateIsNotDrained(t *testing.T) {
	t.Setenv("TZ", "UTC")
	tmp := t.TempDir()
	backend, err := storage.NewLocalBackend(tmp, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := `SELECT TIMESTAMP '2026-04-01 12:00:00' AS time, 'h'::VARCHAR AS host, 7::BIGINT AS value`
	// Base measurement "payment" exists.
	writeParquetFile(t, db, filepath.Join(tmp, "shop", "payment", "2026", "04", "01", "12", "payment_20260401_120000_abcd1234.parquet"), row)
	// Real measurement "payment_late" (ordinary nested ingest + compaction files).
	nested := []string{
		filepath.Join(tmp, "shop", "payment_late", "2026", "05", "20", "03", "payment_late_20260520_030000_12345678.parquet"),
		filepath.Join(tmp, "shop", "payment_late", "2026", "05", "20", "03", "payment_late_20260520_031500_87654321.parquet"),
		filepath.Join(tmp, "shop", "payment_late", "2026", "05", "20", "payment_late_20260521_080000_11112222_daily.parquet"),
	}
	for _, p := range nested {
		writeParquetFile(t, db, p, row)
	}
	// A stray flat object that is not sidecar-shaped is not drained either.
	stray := filepath.Join(tmp, "shop", "payment_late", "notes_20260520_030000_1.parquet")
	writeParquetFile(t, db, stray, row)

	if err := discoveryReorg(t, tmp, backend).Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, p := range append(nested, stray) {
		if !fileExists(t, p) {
			t.Errorf("DATA MOVED: %s was drained out of measurement payment_late", p)
		}
	}
	if got := countParquetTree(t, filepath.Join(tmp, "shop", "payment")); got != 1 {
		t.Errorf("measurement payment must be untouched (1 file), got %d", got)
	}
}

// The genuine flat sidecar of the same name still drains, alongside the real
// measurement's nested files, which stay put.
func TestReorgDiscovery_FlatSidecarFilesDrainNestedFilesStay(t *testing.T) {
	t.Setenv("TZ", "UTC")
	tmp := t.TempDir()
	backend, err := storage.NewLocalBackend(tmp, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := `SELECT TIMESTAMP '2026-04-01 12:00:00' AS time, 'h'::VARCHAR AS host, 7::BIGINT AS value`
	flat := filepath.Join(tmp, "shop", "payment_late", "payment_late_20260520_030000_000000042.parquet")
	nested := filepath.Join(tmp, "shop", "payment_late", "2026", "05", "20", "03", "payment_late_20260520_030000_12345678.parquet")
	writeParquetFile(t, db, flat, row)
	writeParquetFile(t, db, nested, row)

	if err := discoveryReorg(t, tmp, backend).Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if fileExists(t, flat) {
		t.Error("the flat sidecar file must drain")
	}
	if !fileExists(t, nested) {
		t.Error("DATA MOVED: the nested file of measurement payment_late was drained")
	}
	if rows := sumParquetRowsTree(t, filepath.Join(tmp, "shop", "payment")); rows != 1 {
		t.Errorf("payment must receive exactly the 1 sidecar row, got %d", rows)
	}
}
