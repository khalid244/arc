package compaction

import (
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// CS-12: the reorg runs DuckDB in the compactor process, next to compaction
// subprocesses that can spill tens of GiB. Without a spill cap its temp files
// can push the pod over its ephemeral-storage limit (eviction mid-job).

func duckSetting(t *testing.T, d *sql.DB, name string) string {
	t.Helper()
	var v string
	if err := d.QueryRow("SELECT current_setting('" + name + "')").Scan(&v); err != nil {
		t.Fatalf("current_setting(%s): %v", name, err)
	}
	return v
}

func TestReorg_ConfigureDuckDB_AppliesSpillCapAndMemoryLimit(t *testing.T) {
	ctx := context.Background()
	spill := filepath.Join(t.TempDir(), "spill")
	if err := os.MkdirAll(spill, 0o700); err != nil {
		t.Fatal(err)
	}
	r := &Reorganizer{MemoryLimit: "512MB", MaxTempDirectorySize: "3GiB", Logger: zerolog.New(io.Discard)}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.SetMaxOpenConns(1)
	if err := r.configureDuckDB(ctx, d, spill); err != nil {
		t.Fatalf("configureDuckDB: %v", err)
	}

	// Compare against a reference instance given the same values: DuckDB
	// renders sizes in its own units.
	ref, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ref.Close()
	ref.SetMaxOpenConns(1)
	for _, q := range []string{"SET memory_limit='512MB'", "SET max_temp_directory_size='3GiB'"} {
		if _, err := ref.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"memory_limit", "max_temp_directory_size"} {
		if got, want := duckSetting(t, d, name), duckSetting(t, ref, name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if got := duckSetting(t, d, "temp_directory"); got != spill {
		t.Errorf("temp_directory = %q, want %q", got, spill)
	}
	if got := duckSetting(t, d, "TimeZone"); got != "UTC" {
		t.Errorf("TimeZone = %q, want UTC", got)
	}
}

// A limit DuckDB refuses must fail the chunk BEFORE any manifest or output
// exists, rather than run it with an unbounded spill.
func TestReorg_UnsettableSpillCapFailsChunkBeforeManifest(t *testing.T) {
	const days = 2
	b, tmp := newHookBackend(t)
	t.Setenv("TZ", "UTC")
	lateDir := filepath.Join(tmp, "posthog", "events_late")
	if err := os.MkdirAll(lateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	makeBucketManyDays(t, db, lateDir, "events_late_20260520_030000_0.parquet", days)
	db.Close()
	scratch := filepath.Join(tmp, "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	r := newReorg(b, scratch, zerolog.New(io.Discard))
	r.MaxTempDirectorySize = "lots"
	_ = r.Run(context.Background())
	if n := countParquetFlat(t, lateDir); n != 1 {
		t.Fatalf("source must be untouched, got %d", n)
	}
	if total, _ := outputRowsByDay(t, filepath.Join(tmp, "posthog", "events")); total != 0 {
		t.Fatalf("no output may be written without the spill cap, got %d rows", total)
	}
	if keys, _ := r.ManifestManager.List(context.Background()); len(keys) != 0 {
		t.Fatalf("no manifest may exist, got %v", keys)
	}

	// With a valid cap the same chunk drains.
	r.MaxTempDirectorySize = "1GiB"
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertNoDuplicateDays(t, filepath.Join(tmp, "posthog", "events"), days)
}
