package rollup

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// P3Q-1 on the rollup side: the merge source branches (fresh tail, head patch)
// hide the inputs of an uploaded compaction through the compaction manifests,
// like the API's source path. When the manifests cannot be read (an S3 slow
// period) the exclusion used to be dropped silently, and a query served in the
// compaction's upload -> delete window counted the tail rows twice. The router
// now declines such a query, so the API runs it from source, which answers 503
// while the manifests stay unreadable.

// unreadableManifests is a local backend whose manifest reads fail on demand.
type unreadableManifests struct {
	*storage.LocalBackend
	failRead atomic.Bool
}

func (u *unreadableManifests) Read(ctx context.Context, p string) ([]byte, error) {
	if u.failRead.Load() && strings.HasSuffix(p, ".json") {
		return nil, errors.New("operation error S3: GetObject, https response error StatusCode: 503, SlowDown")
	}
	return u.LocalBackend.Read(ctx, p)
}

func TestRouterDeclinesWhenCompactionManifestsUnreadable_P3Q1(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount}}}
	f := newQRFixture(t, spec, "2026-06-02 12:00:00+00")
	// The tail [2026-06-02 12:00, 2026-06-03 00:00) is read from source.
	q := sprintfWhere("SELECT region, count(*) AS n FROM events WHERE %s GROUP BY 1",
		"2026-06-01 00:00:00+00", "2026-06-03 00:00:00+00")
	want := f.rawResult(t, q) // before the compaction output lands

	// Compact the tail day: the output lands beside its still-present input
	// and the manifest is flagged uploaded (the upload -> delete window).
	const in = "default/events/2026/06/02/part.parquet"
	f.writeSource(t, "2026-06-02", "compacted.parquet", "SELECT * FROM read_parquet('"+filepath.Join(f.root, in)+"')")
	lb, err := storage.NewLocalBackend(f.root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	be := &unreadableManifests{LocalBackend: lb}
	qr8WriteManifest(t, compaction.NewManifestManager(be, zerolog.Nop()), "default/events/2026/06/02",
		[]string{in}, "default/events/2026/06/02/compacted.parquet", true)

	// The query side reads the manifests through its own manager (nothing cached).
	m := &Manager{log: zerolog.Nop(), uriRoot: f.root}
	m.SetCompactionManifests(compaction.NewManifestManager(be, zerolog.Nop()))
	f.r.SourceExclude = m.compactionExcludes

	be.failRead.Store(true)
	d := f.r.Route(q)
	if d.Served {
		got, _ := qrQuery(f.exec, d.SQL)
		t.Fatalf("a merge whose compaction exclusion is unknown must not be served (got %v, want %v)\nSQL: %s",
			got.rows, want.rows, d.SQL)
	}
	if d.Reason != "compaction_manifests_unreadable" {
		t.Fatalf("reason = %q, want compaction_manifests_unreadable", d.Reason)
	}
	if !strings.Contains(humanizeReason(d.Reason), "manifest") {
		t.Fatalf("explain text must name the manifests: %q", humanizeReason(d.Reason))
	}
	if _, served, _ := f.r.RouteHTTP(q, ""); served {
		t.Fatal("RouteHTTP must decline too")
	}

	// A sealed window has no source branch, so no exclusion: still served.
	sealed := sprintfWhere("SELECT region, count(*) AS n FROM events WHERE %s GROUP BY 1",
		"2026-06-01 00:00:00+00", "2026-06-02 00:00:00+00")
	if d := f.r.Route(sealed); !d.Served {
		t.Fatalf("a sealed window must still be served: %s", d.Reason)
	}

	// Storage recovers: served again, each tail row counted once.
	be.failRead.Store(false)
	d = f.r.Route(q)
	if !d.Served {
		t.Fatalf("after recovery the query must be served again: %s", d.Reason)
	}
	got, err := qrQuery(f.exec, d.SQL)
	if err != nil {
		t.Fatalf("served SQL failed: %v\nSQL: %s", err, d.SQL)
	}
	if strings.Join(got.rows, "\n") != strings.Join(want.rows, "\n") {
		t.Fatalf("after recovery: got %v, want %v\nSQL: %s", got.rows, want.rows, d.SQL)
	}
}
