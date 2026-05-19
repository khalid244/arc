package tiered

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// setupDiscoverFixture creates a local-backend tree that mimics a typical
// Arc storage layout: <db>/<table>/<partition>/file.parquet. Returns the
// backend ready for discovery calls.
func setupDiscoverFixture(t *testing.T) storage.Backend {
	t.Helper()
	base := t.TempDir()
	be, err := storage.NewLocalBackend(base, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Two databases, four tables, with one *_late that should be excluded.
	files := []string{
		"default/downloads/2026/05/19/00/a.parquet",
		"default/devices/2026/05/19/00/a.parquet",
		"default/downloads_late/2026/05/19/00/a.parquet",
		"posthog/events/2026/05/19/00/a.parquet",
		// Meta-prefixes that should NOT count as databases.
		"_arc/rollup/default/downloads/spec.json",
	}
	for _, p := range files {
		if err := be.Write(ctx, filepath.ToSlash(p), []byte("x")); err != nil {
			t.Fatalf("seed %s: %v", p, err)
		}
	}
	return be
}

func TestDiscoverDatabases_SkipsUnderscorePrefixes(t *testing.T) {
	be := setupDiscoverFixture(t)
	dbs, err := DiscoverDatabases(context.Background(), be)
	if err != nil {
		t.Fatal(err)
	}
	if !sliceHas(dbs, "default") || !sliceHas(dbs, "posthog") {
		t.Errorf("expected 'default' and 'posthog' in dbs, got %v", dbs)
	}
	for _, d := range dbs {
		if d == "_arc" {
			t.Errorf("_arc must be filtered out, got %v", dbs)
		}
	}
}

func TestDiscoverTables_ReturnsAllTablesExcept_Late(t *testing.T) {
	be := setupDiscoverFixture(t)
	tables, err := DiscoverTables(
		context.Background(), be,
		[]string{"default", "posthog"},
		[]string{"*_late"},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"default.downloads", "default.devices", "posthog.events"}
	for _, w := range want {
		if !sliceHas(tables, w) {
			t.Errorf("missing %q in discovered tables %v", w, tables)
		}
	}
	if sliceHas(tables, "default.downloads_late") {
		t.Errorf("*_late table must be excluded; got %v", tables)
	}
}

func TestDiscoverTables_NoExcludePatternsReturnsEverything(t *testing.T) {
	be := setupDiscoverFixture(t)
	tables, err := DiscoverTables(context.Background(), be, []string{"default"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !sliceHas(tables, "default.downloads_late") {
		t.Errorf("with no exclude patterns, *_late should be present; got %v", tables)
	}
}

func TestDiscoverTables_MultipleExcludes(t *testing.T) {
	be := setupDiscoverFixture(t)
	tables, err := DiscoverTables(
		context.Background(), be,
		[]string{"default"},
		[]string{"*_late", "devices"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if sliceHas(tables, "default.downloads_late") || sliceHas(tables, "default.devices") {
		t.Errorf("both *_late and explicit 'devices' should be excluded; got %v", tables)
	}
	if !sliceHas(tables, "default.downloads") {
		t.Errorf("downloads should remain; got %v", tables)
	}
}
