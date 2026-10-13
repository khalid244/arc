package rollup

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// QR-6 on the rollup side: when the existence listing for a merge source
// branch (fresh tail or head patch) fails, the router must not serve the
// query. Before, it read the whole-table glob for the branch (a full-table
// LIST plus footer reads against an object store that is already failing)
// or, had the pruner dropped the failed day, silently undercounted. It now
// declines, so the API runs the query from source, where a listing that is
// still failing answers 503 and a recovered one gives the complete result.

// The production pruner reports listing failures to the rollup adapter.
var _ SourceListingReporter = (*pruning.PartitionPruner)(nil)

// failingLister is an object store whose every listing fails until healed.
type failingLister struct {
	storage.Backend
	healed atomic.Bool
}

func (f *failingLister) ListDirectories(context.Context, string) ([]string, error) {
	if f.healed.Load() {
		return []string{}, nil
	}
	return nil, errors.New("simulated ListObjectsV2 503 SlowDown")
}

func (f *failingLister) List(context.Context, string) ([]string, error) {
	if f.healed.Load() {
		return nil, nil
	}
	return nil, errors.New("simulated ListObjectsV2 timeout")
}

func (f *failingLister) Type() string { return "s3" }

type verdictPruner struct {
	fakeSourcePruner
	failed bool
}

func (v verdictPruner) ExistingPartitionPathsVerdict(string, time.Time, time.Time) ([]string, bool, bool) {
	return v.paths, v.optimizable, v.failed
}

func TestPrunedSourceGlobListingFailed_QR6(t *testing.T) {
	const wt = "['s3://b/default/events/**/*.parquet']"
	const lo, hi = "2026-06-03 00:00:00+00", "2026-06-03 12:00:00+00"

	if got := prunedSourceGlob(wt, lo, hi, verdictPruner{failed: true}); got != sourceListingFailed {
		t.Fatalf("a failed listing must be reported, got %q", got)
	}
	// Not prunable without a failure keeps the whole-table glob.
	if got := prunedSourceGlob(wt, lo, hi, verdictPruner{}); got != wt {
		t.Fatalf("not prunable must stay the whole-table glob, got %q", got)
	}
	// The real pruner over a failing store.
	be := &failingLister{}
	p := pruning.NewPartitionPruner(zerolog.Nop())
	p.SetStorageBackend(be)
	if got := prunedSourceGlob(wt, lo, hi, p); got != sourceListingFailed {
		t.Fatalf("real pruner: a failed listing must be reported, got %q", got)
	}
	be.healed.Store(true)
	if got := prunedSourceGlob(wt, lo, hi, p); got != "" {
		t.Fatalf("real pruner, healed and verified empty: want \"\", got %q", got)
	}
}

func TestRouterDeclinesWhenSourceListingFails_QR6(t *testing.T) {
	spec := CubeSpec{Source: "default.events", Grain: "hour", Dims: []string{"region"}, Aggs: []Aggregate{{Kind: AggCount}}}
	f := newQRFixture(t, spec, "2026-06-03 12:00:00+00")
	be := &failingLister{}
	p := pruning.NewPartitionPruner(zerolog.Nop())
	p.SetStorageBackend(be)
	const wt = "'s3://b/default/events/**/*.parquet'"
	f.r.SourceWindow = func(_, lo, hi string) string { return prunedSourceGlob(wt, lo, hi, p) }

	// The tail [2026-06-03 00:00, 12:00) needs a source listing.
	q := sprintfWhere("SELECT region, count(*) AS n FROM events WHERE %s GROUP BY 1",
		"2026-06-01 00:00:00+00", "2026-06-03 12:00:00+00")
	d := f.r.Route(q)
	if d.Served {
		t.Fatalf("a query whose fresh-tail listing failed must not be served, got:\n%s", d.SQL)
	}
	if d.Reason != "source_listing_failed" {
		t.Fatalf("reason = %q, want source_listing_failed", d.Reason)
	}
	if !strings.Contains(humanizeReason(d.Reason), "listing") {
		t.Fatalf("explain text must name the listing failure: %q", humanizeReason(d.Reason))
	}
	if _, served, _ := f.r.RouteHTTP(q, ""); served {
		t.Fatal("RouteHTTP must decline too")
	}

	// A sealed window needs no source listing: still served from the cube.
	sealed := sprintfWhere("SELECT region, count(*) AS n FROM events WHERE %s GROUP BY 1",
		"2026-06-01 00:00:00+00", "2026-06-03 00:00:00+00")
	if d := f.r.Route(sealed); !d.Served {
		t.Fatalf("a sealed window must still be served: %s", d.Reason)
	}

	// Storage recovers: served again, the tail verified empty and omitted.
	be.healed.Store(true)
	if d := f.r.Route(q); !d.Served {
		t.Fatalf("after recovery the query must be served again: %s", d.Reason)
	}
}
