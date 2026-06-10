package rollup

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// TestStrandedManifestEvictedOnReload (F3) — when re-classification changes a
// source's cube set, the OLD cubes' directories (with valid manifests) remain on
// disk; nothing deletes them. The reload path must NOT keep routing to a cube
// that is absent from the source's CURRENT plan: a stranded cube's coverage is
// frozen (the builder never extends it), so a covHi-spanning query served from it
// silently drops every row in [frozen covHi, watermark).
//
// Sources WITHOUT a plan this boot (not yet profiled — e.g. a route-only pod)
// must keep serving their loaded manifests.
func TestStrandedManifestEvictedOnReload(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()
	root := t.TempDir()
	stg := newFakeStorage()
	m := driftManager(t, db, root, stg)
	var buf bytes.Buffer
	m.log = zerolog.New(&buf)
	ctx := context.Background()

	// The source as it exists TODAY: time + site. The current plan for it is
	// coarse + by_site — no "email" cube.
	writeSourceDay(t, db, root, "2026/06/01",
		`SELECT TIMESTAMPTZ '2026-06-01 10:00:00' AS "time", 'web' AS site`)

	// Pre-upgrade leftover: a by_email cube whose directory + manifest survived a
	// classification change. Simulate the startup reload having loaded it (the
	// merge path), exactly as reloadRouter does from a globbed manifest.json.
	stranded := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"email"}, Aggs: []Aggregate{{Kind: AggCount}}}
	manK := m.freshManifest(stranded)
	manK.Upsert(DayEntry{Date: "2026-06-01", URI: "stale.parquet", SchemaHash: manK.SchemaHash,
		BucketLo: "2026-06-01 00:00:00", BucketHi: "2026-06-02 00:00:00", Rows: 5})
	m.updateRouter(manK)

	// A different source with NO plan this boot: must keep serving.
	unplanned := CubeSpec{Source: "other.metrics", Grain: "hour", Aggs: []Aggregate{{Kind: AggCount}}}
	manO := m.freshManifest(unplanned)
	manO.Upsert(DayEntry{Date: "2026-06-01", URI: "other.parquet", SchemaHash: manO.SchemaHash,
		BucketLo: "2026-06-01 00:00:00", BucketHi: "2026-06-02 00:00:00", Rows: 3})
	m.updateRouter(manO)

	// Plan the source (as buildSource does each tick), then run the reload path.
	if _, err := m.planSpecs("default.events"); err != nil {
		t.Fatalf("planSpecs: %v", err)
	}
	m.reloadRouter(ctx)

	r := m.Router()
	if _, ok := r.Manifests[cubeKeyOf(stranded)]; ok {
		t.Fatal("F3: stranded cube (not in the source's current plan) is still in the router after reload — its frozen coverage silently undercounts covHi-spanning queries")
	}
	if _, ok := r.Manifests[cubeKeyOf(unplanned)]; !ok {
		t.Fatal("F3: manifest of a source WITHOUT a current plan must keep serving (do not evict unprofiled sources)")
	}

	// Operators must learn the directory is dead — warn, once per cube.
	if got := strings.Count(buf.String(), "stranded"); got != 1 {
		t.Fatalf("F3: want exactly 1 stranded-cube warning after first reload, got %d; logs:\n%s", got, buf.String())
	}
	m.reloadRouter(ctx)
	if got := strings.Count(buf.String(), "stranded"); got != 1 {
		t.Fatalf("F3: stranded-cube warning must be once per cube, got %d occurrences", got)
	}
}

// TestSchemaHashMismatchWarns (F3) — loadManifest silently discards ALL existing
// manifest coverage when the cube's SchemaHash changes (full rebuild from
// scratch). That is the intended behavior, but it must be operator-visible.
func TestSchemaHashMismatchWarns(t *testing.T) {
	stg := newFakeStorage()
	m := testManager(stg)
	var buf bytes.Buffer
	m.log = zerolog.New(&buf)
	ctx := context.Background()

	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"site"}, Aggs: []Aggregate{{Kind: AggCount}}}
	old := m.freshManifest(spec)
	old.SchemaHash = "stale-hash-0507bb9"
	old.Upsert(DayEntry{Date: "2026-05-01", URI: "x.parquet", SchemaHash: old.SchemaHash,
		BucketLo: "2026-05-01 00:00:00", BucketHi: "2026-05-02 00:00:00", Rows: 7})
	b, err := old.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := stg.Write(ctx, m.manifestKey(spec), b); err != nil {
		t.Fatal(err)
	}

	man, ok := m.loadManifest(ctx, spec)
	if !ok || len(man.Days) != 0 {
		t.Fatalf("schema-changed cube must load a fresh manifest (ok=%v days=%d)", ok, len(man.Days))
	}
	out := buf.String()
	if !strings.Contains(out, "stale-hash-0507bb9") || !strings.Contains(out, spec.SchemaHash()) || !strings.Contains(out, "rebuild") {
		t.Fatalf("F3: SchemaHash mismatch must warn with old+new hash and announce the full rebuild; logs:\n%s", out)
	}
}
