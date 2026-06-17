package rollup

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestScan_DelimiterWalk verifies source/day discovery walks the partition tree via
// the backend's directory listing (db -> meas -> YYYY/MM/DD), discovers the right
// days, and skips internal (_arc), _late, and config-excluded measurements — without
// the old recursive "**" glob over every object.
func TestScan_DelimiterWalk(t *testing.T) {
	root := t.TempDir()
	mk := func(parts ...string) {
		full := filepath.Join(append([]string{root}, parts...)...)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mk("default", "downloads", "2025", "12", "26", "a.parquet")
	mk("default", "downloads", "2025", "12", "27", "b.parquet")
	mk("default", "downloads", "2025", "12", "27", "b2.parquet") // 2nd file, same day -> one day
	mk("default", "downloads", "2026", "01", "02", "c.parquet")
	mk("default", "events", "2026", "01", "01", "d.parquet")
	mk("default", "foo_late", "2026", "01", "01", "e.parquet")          // skip: _late
	mk("posthog", "events", "2026", "01", "03", "f.parquet")            // another db
	mk("_arc", "rollup", "default", "downloads", "coarse", "g.parquet") // skip: _ db
	mk("default", "skipme", "2026", "01", "01", "h.parquet")            // skip: excluded
	mk("default", "downloads", "_tmp", "junk", "x", "i.parquet")        // skip: non-date dirs

	lb, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	m := &Manager{stg: lb, log: zerolog.Nop(), cfg: Config{ExcludeMeasurements: []string{"skipme"}}}

	got := m.scan(context.Background())

	asDates := func(src string) []string {
		var ds []string
		for _, d := range got[src] {
			ds = append(ds, d.Format("2006-01-02"))
		}
		sort.Strings(ds)
		return ds
	}
	eq := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	if d := asDates("default.downloads"); !eq(d, []string{"2025-12-26", "2025-12-27", "2026-01-02"}) {
		t.Errorf("default.downloads days = %v", d)
	}
	if d := asDates("default.events"); !eq(d, []string{"2026-01-01"}) {
		t.Errorf("default.events days = %v", d)
	}
	if d := asDates("posthog.events"); !eq(d, []string{"2026-01-03"}) {
		t.Errorf("posthog.events days = %v", d)
	}
	for _, skipped := range []string{"default.foo_late", "_arc.rollup", "default.skipme"} {
		if _, ok := got[skipped]; ok {
			t.Errorf("expected %q to be skipped, but it was discovered: %v", skipped, got[skipped])
		}
	}
}
