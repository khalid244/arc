package rollup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// QR-8: between a compaction's upload and the deletion of its inputs, a source
// partition holds BOTH the inputs and the compacted output. The API's source
// path hides the inputs through the compaction manifests; the rollup's cube
// builder (exact day listing) and its merge-on-read tail (source glob) read
// every file, so a cube day built in that window, and every tail read during
// it, counted each row twice.

// qr8Manifests is a real compaction.ManifestManager over a local backend rooted
// at root — the same reader production wires, writing manifests where the
// compactor does (_arc/compaction/...).
func qr8Manifests(t *testing.T, root string) *compaction.ManifestManager {
	t.Helper()
	be, err := storage.NewLocalBackend(root, zerolog.New(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	return compaction.NewManifestManager(be, zerolog.New(io.Discard))
}

// qr8Compact simulates a compaction of one source day of the passFixture
// layout: the day's part.parquet (the input) is rewritten into
// compacted.parquet (the output, same rows) and a manifest is recorded with the
// compactor's own ManifestManager, OutputUploaded per uploaded. The input stays
// on storage: this is the upload -> delete window.
func qr8Compact(t *testing.T, m *Manager, mm *compaction.ManifestManager, root, date string, uploaded bool) (input, output, manifestPath string) {
	t.Helper()
	rel := "default/events/" + strings.ReplaceAll(date, "-", "/")
	input, output = rel+"/part.parquet", rel+"/compacted.parquet"
	mustExec(t, m.db, fmt.Sprintf("COPY (SELECT * FROM read_parquet('%s')) TO '%s' (FORMAT PARQUET)",
		filepath.Join(root, input), filepath.Join(root, output)))
	return input, output, qr8WriteManifest(t, mm, rel, []string{input}, output, uploaded)
}

func qr8WriteManifest(t *testing.T, mm *compaction.ManifestManager, partition string, inputs []string, output string, uploaded bool) string {
	t.Helper()
	ctx := context.Background()
	p, err := mm.WriteManifest(ctx, &compaction.Manifest{
		OutputPath: output, InputFiles: inputs, Database: "default", Measurement: "events",
		PartitionPath: partition, Tier: "hourly", Status: compaction.ManifestStatusPending, JobID: fmt.Sprintf("job%d", len(inputs)+len(output)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if uploaded {
		if err := mm.MarkOutputUploaded(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

// The cube builder must count each row once while a compaction of the day is
// between upload and delete — whether or not the manifest has been flagged
// OutputUploaded yet (the output is uploaded before the manifest is written).
func TestQR8_CubeBuildCountsCompactedDayOnce(t *testing.T) {
	for _, uploaded := range []bool{true, false} {
		name := "flagged_uploaded"
		if !uploaded {
			name = "not_yet_flagged" // no '=' in the name: it would become a hive partition column of the temp path
		}
		t.Run(name, func(t *testing.T) {
			m, stg, specs, dates := passFixture(t, 2)
			mm := qr8Manifests(t, stg.root)
			m.SetCompactionManifests(mm)
			want := sourceCount(t, m, stg.root, dates[0]) // before the output lands
			qr8Compact(t, m, mm, stg.root, dates[0], uploaded)

			pass(t, m, stg, dates)
			for _, s := range specs {
				for _, d := range dates {
					if got := cubeCount(t, m, s, d); got != want {
						t.Errorf("%s@%s: cube counts %d rows, want %d (each row once)", CubeID(s), d, got, want)
					}
				}
			}
		})
	}
}

// The month build reads the same exact per-day listings (monthSourceOf), so it
// must drop the compacted inputs too.
func TestQR8_MonthSourceExcludesCompactedInputs(t *testing.T) {
	m, stg, _, dates := passFixture(t, 2)
	mm := qr8Manifests(t, stg.root)
	m.SetCompactionManifests(mm)
	input, output, _ := qr8Compact(t, m, mm, stg.root, dates[0], true)

	src := m.monthSourceOf(context.Background(), freshSrc, dates[0][:7], daysOf(dates[:1]))
	if strings.Contains(src.expr, input) || !strings.Contains(src.expr, output) {
		t.Fatalf("month source must read the output and not the compacted input:\n%s", src.expr)
	}
}

// A day built while a compaction touched it is recorded as unsettled: once the
// compaction finishes with the same effective file set nothing is rebuilt, but
// when the day later changes its rebuild is not held back by rebuild limits (an
// unsettled build may have raced the compactor). A day built settled keeps the
// limit.
func TestQR8_UnsettledBuildIsNotDeferredByRebuildLimits(t *testing.T) {
	m, stg, specs, dates := passFixture(t, 2)
	mm := qr8Manifests(t, stg.root)
	m.SetCompactionManifests(mm)
	unsettled, settled := dates[0], dates[1]
	input, _, manifestPath := qr8Compact(t, m, mm, stg.root, unsettled, true)
	pass(t, m, stg, dates)
	for _, s := range specs {
		if sig := m.manifests[cubeKeyOf(s)].DailySourceSig(unsettled); !strings.HasPrefix(sig, unsettledSig) {
			t.Fatalf("%s@%s: sig %q, want the unsettled marker", CubeID(s), unsettled, sig)
		}
		if sig := m.manifests[cubeKeyOf(s)].DailySourceSig(settled); sig == "" || strings.HasPrefix(sig, unsettledSig) {
			t.Fatalf("%s@%s: sig %q, want a plain fingerprint", CubeID(s), settled, sig)
		}
	}

	// The compaction finishes: input and manifest deleted, same effective files.
	if err := os.Remove(filepath.Join(stg.root, input)); err != nil {
		t.Fatal(err)
	}
	if err := mm.DeleteManifest(context.Background(), manifestPath); err != nil {
		t.Fatal(err)
	}
	if w := pass(t, m, stg, dates); w != 0 {
		t.Fatalf("settled day with an unchanged file set: pass wrote %d manifests, want 0", w)
	}

	// Both days change under a 24h limit: only the unsettled build is redone.
	l, err := parseRebuildLimits("30d:24h")
	if err != nil {
		t.Fatal(err)
	}
	m.rebuildLimits = l
	m.nowFn = time.Now // cube build times come from the real clock (file nonce)
	before := uriSnapshot(t, m, specs, dates)
	addLateRow(t, m, stg.root, unsettled, "late.parquet")
	addLateRow(t, m, stg.root, settled, "late.parquet")
	pass(t, m, stg, dates)
	after := uriSnapshot(t, m, specs, dates)
	for _, s := range specs {
		if k := CubeID(s) + "@" + unsettled; after[k] == before[k] {
			t.Errorf("%s: changed day built while unsettled was deferred by the rebuild limit", k)
		}
		if k := CubeID(s) + "@" + settled; after[k] != before[k] {
			t.Errorf("%s: changed day built settled must keep its rebuild limit", k)
		}
	}
}

// The merge-on-read tail and head patch must hide compacted inputs exactly like
// the API's source path: a served query returns the same numbers before and
// during the compaction window. The source also holds a REAL column named
// "filename" (QR-9: the helper column is _arc_src_file, never filename=true).
func TestQR8_MergeTailExcludesCompactedInputs(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region", "filename"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Col: "bytes"}}}
	f := newQRFixture(t, spec, "2026-06-02 12:00:00+00")
	q := `SELECT date_trunc('hour', time) AS h, region, filename, count(*) AS n, sum(bytes) AS b FROM events
		WHERE time >= TIMESTAMPTZ '2026-06-01 00:00:00+00' AND time < TIMESTAMPTZ '2026-06-03 00:00:00+00' GROUP BY 1, 2, 3`
	// A head patch [lo, first full hour) on the compacted day, too.
	qHead := strings.Replace(q, "2026-06-01 00:00:00+00", "2026-06-02 00:30:00+00", 1)
	want := map[string]qrResult{q: f.rawResult(t, q), qHead: f.rawResult(t, qHead)} // before the output lands

	// Compact the tail day: the output is written next to its still-present input.
	in := filepath.Join(f.dayDir("2026-06-02"), "part.parquet")
	f.writeSource(t, "2026-06-02", "compacted.parquet", "SELECT * FROM read_parquet('"+in+"')")
	f.r.SourceExclude = func(source string) ([]string, error) {
		if source != "default.events" {
			t.Errorf("SourceExclude asked for %q", source)
		}
		return []string{in}, nil
	}
	for _, sqlText := range []string{q, qHead} {
		d := f.r.Route(sqlText)
		if !d.Served || !strings.Contains(d.SQL, "UNION ALL BY NAME") {
			t.Fatalf("want a served merge-on-read query, got served=%v reason=%q\n%s", d.Served, d.Reason, d.SQL)
		}
		got, err := qrQuery(f.exec, d.SQL)
		if err != nil {
			t.Fatalf("served SQL failed: %v\nSQL: %s", err, d.SQL)
		}
		if strings.Join(got.rows, "\n") != strings.Join(want[sqlText].rows, "\n") {
			t.Fatalf("source branches counted compacted rows twice\n got  %v\n want %v\nSQL: %s", got.rows, want[sqlText].rows, d.SQL)
		}
		if strings.Contains(d.SQL, "filename=true") || !strings.Contains(d.SQL, "_arc_src_file") {
			t.Fatalf("exclusion must use the _arc_src_file helper column, not filename=true:\n%s", d.SQL)
		}
	}
}

// The Manager's exclusion list is exactly the uploaded-output inputs of the
// queried table, as full source URIs (what DuckDB reports as the file name).
func TestQR8_CompactionExcludesPerTable(t *testing.T) {
	m, stg, _, dates := passFixture(t, 1)
	mm := qr8Manifests(t, stg.root)
	m.SetCompactionManifests(mm)
	if got, err := m.compactionExcludes(freshSrc); err != nil || len(got) != 0 {
		t.Fatalf("no manifests: got %v, %v", got, err)
	}
	in, _, _ := qr8Compact(t, m, mm, stg.root, dates[0], true)
	qr8WriteManifest(t, mm, "default/events_other/2026/01/01", []string{"default/events_other/2026/01/01/a.parquet"}, "default/events_other/2026/01/01/b.parquet", true)
	qr8WriteManifest(t, mm, "default/events/2026/01/02", []string{"default/events/2026/01/02/pending.parquet"}, "default/events/2026/01/02/out.parquet", false)

	got, err := m.compactionExcludes(freshSrc)
	want := []string{m.rootURI() + "/" + in}
	if err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("excludes = %v, want %v (uploaded inputs of default.events only)", got, want)
	}

	// The live router is wired with it.
	m.mu.Lock()
	m.rebuildRouterLocked()
	r := m.router
	m.mu.Unlock()
	if r.SourceExclude == nil {
		t.Fatal("router SourceExclude is not wired to the compaction manifests")
	}
	if got, err := r.SourceExclude(freshSrc); err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("router SourceExclude is not wired to the compaction manifests: %v, %v", got, err)
	}
}
