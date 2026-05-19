package tiered

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Property test for proposal F: the rollup-rewritten query must produce
// the same result as the equivalent direct-source query for a given
// (Spec, dataset, query). This locks in end-to-end correctness so any
// future regression in the rewrite pipeline (parser, variant pick, tier
// pick, emit, dim/agg helpers, schema-hash filter) fails CI rather than
// surfacing in a Grafana panel.
//
// First property: for the user's panel shape — count-by-hour-and-dim
// with a kept-values filter — rollup-served == source-served.

func TestProperty_CountByHourSite_KeptValuesOnly(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Source: 24 hours, 3 kept sites, varying per-hour counts.
	if _, err := db.Exec(`CREATE TABLE events (time TIMESTAMPTZ, site VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	keptSites := []string{"youtu.be", "www.instagram.com", "youtube.com"}
	day := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	for hour := 0; hour < 24; hour++ {
		t0 := day.Add(time.Duration(hour) * time.Hour)
		for i, s := range keptSites {
			n := 5 + hour + i*3 // deterministic count per (hour, site)
			for j := 0; j < n; j++ {
				ts := t0.Add(time.Duration(j) * time.Minute)
				if _, err := db.Exec(
					`INSERT INTO events VALUES (?, ?)`, ts, s); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	// Build the by_site rollup for the day. Place files under a
	// directory structure that ParseVariantPath recognises so PickTier
	// can compute coverage.
	tmp := t.TempDir()
	tierDir := filepath.Join(tmp, "_arc/rollup/test/events/1h/2026/05/15")
	if err := os.MkdirAll(tierDir, 0o755); err != nil {
		t.Fatal(err)
	}
	spec := &Spec{
		Table:      "test.events",
		TZ:         "UTC",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"site": {Role: "PerDim", KeptValues: keptSites, EffectiveCard: 3},
		},
	}
	specHash, err := spec.SchemaHash()
	if err != nil {
		t.Fatal(err)
	}
	bld := &Builder{
		DB:             db,
		HLLLgK:         14,
		KLLk:           200,
		SchemaHash:     specHash,
		TierTZ:         "UTC",
		BuilderVersion: "test",
		BucketLo:       day,
		BucketHi:       day.Add(24 * time.Hour),
	}
	args := BuildArgs{
		Tier:   Tier1h,
		Source: "events",
	}
	produced, err := bld.BuildAllVariants(ctx, args, spec, 100, tierDir)
	if err != nil {
		t.Fatalf("BuildAllVariants: %v", err)
	}
	bySrc, ok := produced["by_site"]
	if !ok {
		t.Fatalf("expected by_site variant; got %v", produced)
	}
	// Builder writes to "<tierDir>/<variant>.parquet"; ParseVariantPath
	// expects "<tierDir>/<variant>/<file>.parquet". Move into a variant
	// subdir so the path layout matches production.
	variantDir := filepath.Join(tierDir, "by_site")
	if err := os.MkdirAll(variantDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bySiteFinal := filepath.Join(variantDir, "f1.parquet")
	if err := os.Rename(bySrc, bySiteFinal); err != nil {
		t.Fatal(err)
	}
	// MemoryFileIndex stores the storage key (relative to bucket); the
	// emit will prepend StoragePrefix so DuckDB sees the full local path.
	relKey := "_arc/rollup/test/events/1h/2026/05/15/by_site/f1.parquet"
	idx := &MemoryFileIndex{Paths: []string{relKey}}
	storagePrefix := tmp + "/"

	// User query: panel shape — count by hour and site with kept filter.
	userSQL := fmt.Sprintf(
		`SELECT date_trunc('hour', time) AS h, site, COUNT(*) AS c FROM events
		 WHERE time >= TIMESTAMP '2026-05-15 00:00:00+00'
		   AND time <  TIMESTAMP '2026-05-16 00:00:00+00'
		   AND site IN (%s)
		 GROUP BY 1, 2`,
		quoteList(keptSites))

	// Direct-source result (ground truth).
	truth := runRowSet(t, db, userSQL)

	// Rewritten via the router.
	var refusalLog strings.Builder
	logger := zerolog.New(&refusalLog).With().Timestamp().Logger()
	rewritten, ok := Rewrite(ctx, userSQL, RewriteDeps{
		DB:            db,
		Files:         idx,
		Spec:          spec,
		DimRichCap:    100,
		GraceWindow:   0, // closed window so the open-tail fresh CTE doesn't trigger
		Logger:        logger,
		StoragePrefix: storagePrefix,
	})
	if !ok {
		t.Fatalf("router refused; expected acceptance.\nrefusal log: %s", refusalLog.String())
	}
	got := runRowSet(t, db, rewritten)

	if !equalRowSets(truth, got) {
		t.Errorf("row sets differ.\nsource truth (%d rows):\n%s\nrollup result (%d rows):\n%s",
			len(truth), formatRowSet(truth), len(got), formatRowSet(got))
	}
}

// --- helpers ---

func quoteList(vals []string) string {
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	return strings.Join(out, ",")
}

// runRowSet executes the SQL (handles multi-statement scripts like the
// emit's "SET TimeZone = …; WITH …") and returns rows as []string keys
// joined by "|" so they can be compared by set equality.
func runRowSet(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	// Multi-statement: split by ';' on top level and run all but the last as Exec.
	stmts := splitTopLevelStmts(query)
	for i := 0; i < len(stmts)-1; i++ {
		if strings.TrimSpace(stmts[i]) == "" {
			continue
		}
		if _, err := db.Exec(stmts[i]); err != nil {
			t.Fatalf("exec stmt %q: %v", stmts[i], err)
		}
	}
	last := stmts[len(stmts)-1]
	rows, err := db.Query(last)
	if err != nil {
		t.Fatalf("query %q: %v", last, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	out := []string{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range ptrs {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = fmt.Sprintf("%v", v)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	sort.Strings(out)
	return out
}

func splitTopLevelStmts(sql string) []string {
	// Simple split — fine for our generated SQL (no nested ; inside
	// quoted strings here).
	parts := strings.Split(sql, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			out = append(out, p)
		}
	}
	return out
}

func equalRowSets(a, b []string) bool {
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

func formatRowSet(s []string) string {
	return "  " + strings.Join(s, "\n  ")
}
