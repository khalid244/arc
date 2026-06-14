package rollup

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// ---- pure unit tests (no DB): the rollup->pruner adapter --------------------

type fakeSourcePruner struct {
	paths       []string
	optimizable bool
}

func (f fakeSourcePruner) ExistingPartitionPaths(string, time.Time, time.Time) ([]string, bool) {
	return f.paths, f.optimizable
}

// prunedSourceGlob is the never-lossy adapter from the shared pruner's existing
// partition paths to a read_parquet list arg.
func TestPrunedSourceGlob(t *testing.T) {
	const wt = "['s3://b/default/downloads/**/*.parquet']"
	const lo, hi = "2025-12-28 18:00:00+00", "2025-12-29 00:00:00+00"
	full := []string{
		"s3://b/default/downloads/2025/12/28/18/*.parquet",
		"s3://b/default/downloads/2025/12/28/*.parquet",
	}

	t.Run("nil pruner -> whole-table", func(t *testing.T) {
		if got := prunedSourceGlob(wt, lo, hi, nil); got != wt {
			t.Errorf("got %s want %s", got, wt)
		}
	})
	t.Run("not optimizable -> whole-table", func(t *testing.T) {
		if got := prunedSourceGlob(wt, lo, hi, fakeSourcePruner{nil, false}); got != wt {
			t.Errorf("got %s want %s", got, wt)
		}
	})
	t.Run("empty (no data in range) -> '' so caller skips branch", func(t *testing.T) {
		if got := prunedSourceGlob(wt, lo, hi, fakeSourcePruner{[]string{}, true}); got != "" {
			t.Errorf("got %q want empty", got)
		}
	})
	t.Run("existing paths -> read_parquet list", func(t *testing.T) {
		want := "['s3://b/default/downloads/2025/12/28/18/*.parquet', 's3://b/default/downloads/2025/12/28/*.parquet']"
		if got := prunedSourceGlob(wt, lo, hi, fakeSourcePruner{full, true}); got != want {
			t.Errorf("got %s want %s", got, want)
		}
	})
	t.Run("non-canonical whole glob -> unchanged", func(t *testing.T) {
		const multi = "['a/b/**/*.parquet', 'c/d/**/*.parquet']"
		if got := prunedSourceGlob(multi, lo, hi, fakeSourcePruner{full, true}); got != multi {
			t.Errorf("got %s want %s", got, multi)
		}
	})
	t.Run("unparseable bounds -> whole-table", func(t *testing.T) {
		if got := prunedSourceGlob(wt, "garbage", hi, fakeSourcePruner{full, true}); got != wt {
			t.Errorf("got %s want %s", got, wt)
		}
	})
}

// MergeReadSQL omits a source branch when the resolver returns "" (no files in the
// window) — a zero-match per-day glob would otherwise error "No files found".
func TestMergeReadSQL_SkipsEmptySourceBranch(t *testing.T) {
	q := QueryShape{
		Source: "default.downloads", TimeCol: "time", Grain: "hour",
		Aggs:   []Aggregate{{Kind: AggCount, Alias: "n"}},
		TimeLo: "2025-12-28 00:00:00+00", TimeHi: "2025-12-29 00:00:00+00",
	}
	spec := PickNarrowest(allCubes, q)
	if spec == nil {
		t.Fatalf("no cube covers shape")
	}
	empty := func(string, string) string { return "" }
	sql, ok := q.MergeReadSQL(*spec, "'cube.parquet'", empty, "2025-12-28 18:00:00+00")
	if !ok {
		t.Fatalf("merge emit failed")
	}
	if strings.Contains(sql, "downloads") {
		t.Errorf("empty resolver must omit the source branch entirely, got:\n%s", sql)
	}
	present := func(string, string) string { return "['s3://arc-test/default/downloads/2025/12/28/18/*.parquet']" }
	sql2, _ := q.MergeReadSQL(*spec, "'cube.parquet'", present, "2025-12-28 18:00:00+00")
	if !strings.Contains(sql2, "downloads/2025/12/28/18/*.parquet") {
		t.Errorf("present resolver must include the partition path, got:\n%s", sql2)
	}
}

// ---- corpus-backed tests through the REAL pruner (local MinIO) --------------

// corpusPruner builds a real PartitionPruner wired to the MinIO corpus, so the
// merge source pruning exercises the same hour/day partition generation + S3
// DirectoryLister existence filtering the normal query path uses.
func corpusPruner(t *testing.T) *pruning.PartitionPruner {
	t.Helper()
	be, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: testBucket, Region: "us-east-1", Endpoint: "http://" + testEndpoint,
		AccessKey: testKey, SecretKey: testSecret, UseSSL: false, PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("s3 backend: %v", err)
	}
	p := pruning.NewPartitionPruner(zerolog.Nop())
	p.SetStorageBackend(be)
	return p
}

// End-to-end through the real pruner: the merge must prune to existing partitions
// (no whole-table glob), execute without "No files found", and match the
// whole-corpus ground truth over the window.
func TestCompare_MergeOnRead_ExistencePruned_Corpus(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)
	cubes := buildSealedCubes(t, db)

	const wholeTable = "['s3://arc-test/default/downloads/**/*.parquet']"
	pruner := corpusPruner(t)
	resolver := func(lo, hi string) string { return prunedSourceGlob(wholeTable, lo, hi, pruner) }

	shapes := []QueryShape{
		{Source: "default.downloads", TimeCol: "time", Grain: "hour",
			Aggs: []Aggregate{{Kind: AggCount, Alias: "n"}}, TimeLo: day28Lo, TimeHi: day28Hi},
		{Source: "default.downloads", TimeCol: "time", Grain: "hour", Dims: []string{"status"},
			Aggs: []Aggregate{{Kind: AggCount, Alias: "n"}}, TimeLo: day28Lo, TimeHi: day28Hi},
		// mid-bucket leading edge -> head patch [00:30,01:00) from source, plus tail.
		{Source: "default.downloads", TimeCol: "time", Grain: "hour",
			Aggs: []Aggregate{{Kind: AggCount, Alias: "n"}}, TimeLo: "2025-12-28 00:30:00+00", TimeHi: day28Hi},
	}
	for i, q := range shapes {
		q := q
		t.Run(fmt.Sprintf("shape%02d_dims=%v_lo=%s", i, q.Dims, q.TimeLo[11:16]), func(t *testing.T) {
			spec := PickNarrowest(allCubes, q)
			if spec == nil {
				t.Fatalf("no cube covers shape")
			}
			mergeSQL, ok := q.MergeReadSQL(*spec, cubes[cubeKey(*spec)], resolver, watermark28)
			if !ok {
				t.Fatalf("merge emit failed")
			}
			if strings.Contains(mergeSQL, "downloads/**/*.parquet") {
				t.Fatalf("merge still scans the whole table:\n%s", mergeSQL)
			}
			nKeys := len(q.Dims)
			if q.Grain != "" {
				nKeys++
			}
			src := runShape(t, db, q.SourceRefSQL(wholeTable), nKeys)
			cube := runShape(t, db, mergeSQL, nKeys)
			if len(src.rows) != len(cube.rows) {
				t.Fatalf("group count mismatch: source=%d merge=%d\nSQL: %s", len(src.rows), len(cube.rows), mergeSQL)
			}
			for k, sv := range src.rows {
				cv, ok := cube.rows[k]
				if !ok {
					t.Fatalf("merge missing group %q", k)
				}
				for j := range sv {
					if !aggMatch(sv[j], cv[j], q.Aggs[j]) {
						t.Errorf("group %q agg[%s]: source=%v merge=%v", k, q.Aggs[j].Alias, sv[j], cv[j])
					}
				}
			}
		})
	}
}

// The definitive empty-day regression through the real pruner: the corpus ends
// 2026-05-23, so a fresh tail spanning 05-23 (present) + 05-24 (empty) must have
// 05-24 dropped by the pruner's existence filter — executing cleanly (no
// "No files found") and matching ground truth.
func TestCompare_MergeOnRead_EmptyTailDay_Corpus(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)

	const (
		lo = "2026-05-23 00:00:00+00"
		wm = "2026-05-23 12:00:00+00" // cube sealed [00:00,12:00); tail [12:00,hi)
		hi = "2026-05-25 00:00:00+00" // tail spans 05-23 (present) + 05-24 (EMPTY)
	)
	const wholeTable = "['s3://arc-test/default/downloads/**/*.parquet']"
	const day23Glob = "['s3://arc-test/default/downloads/2026/05/23/**/*.parquet']"

	q := QueryShape{Source: "default.downloads", TimeCol: "time", Grain: "hour",
		Aggs: []Aggregate{{Kind: AggCount, Alias: "n"}}, TimeLo: lo, TimeHi: hi}
	spec := PickNarrowest(allCubes, q)
	if spec == nil {
		t.Fatalf("no cube covers shape")
	}
	dir := t.TempDir()
	dest := filepath.Join(dir, "cube.parquet")
	if _, err := db.Exec(spec.BuildCopySQL(day23Glob, "time", lo, wm, dest)); err != nil {
		t.Fatalf("build sealed cube: %v", err)
	}
	cubeExpr := fmt.Sprintf("'%s'", dest)

	pruner := corpusPruner(t)
	resolver := func(l, h string) string { return prunedSourceGlob(wholeTable, l, h, pruner) }

	mergeSQL, ok := q.MergeReadSQL(*spec, cubeExpr, resolver, wm)
	if !ok {
		t.Fatalf("merge emit failed")
	}
	if strings.Contains(mergeSQL, "2026/05/24") {
		t.Fatalf("empty day 2026/05/24 was NOT pruned out by the existence filter:\n%s", mergeSQL)
	}
	// runShape t.Fatalf's on a query error — also asserts the pruned list executes
	// WITHOUT "No files found".
	src := runShape(t, db, q.SourceRefSQL(wholeTable), 1)
	cube := runShape(t, db, mergeSQL, 1)
	if len(src.rows) != len(cube.rows) {
		t.Fatalf("group count mismatch: source=%d merge=%d", len(src.rows), len(cube.rows))
	}
	for k, sv := range src.rows {
		cv, ok := cube.rows[k]
		if !ok {
			t.Fatalf("merge missing group %q", k)
		}
		if !aggMatch(sv[0], cv[0], q.Aggs[0]) {
			t.Errorf("group %q: source=%v merge=%v", k, sv[0], cv[0])
		}
	}
}
