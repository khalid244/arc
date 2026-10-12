package pruning

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// QR-6: when partition pruning cannot verify which partitions exist because a
// storage listing failed or timed out, the query must not run on a guessed
// path set. The pruner records the failure on the query's context (the API
// answers 503) and never caches a path set built on it.

// flakyRemote is an S3-shaped backend whose listings can be made to fail or to
// hang until the caller's deadline, per call kind.
type flakyRemote struct {
	mockS3Backend
	dayFiles   map[string][]string // day prefix -> direct parquet files
	failDirs   atomic.Bool         // ListDirectories errors
	failList   atomic.Bool         // List (day-level file check) errors
	hangDirs   atomic.Bool         // ListDirectories blocks until ctx is done
	dirCalls   atomic.Int64
	listCalls  atomic.Int64
	failPrefix string // when set, only this ListDirectories prefix fails
}

func (f *flakyRemote) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	f.dirCalls.Add(1)
	if f.hangDirs.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.failDirs.Load() && (f.failPrefix == "" || f.failPrefix == prefix) {
		return nil, errors.New("simulated ListObjectsV2 503 SlowDown")
	}
	return f.mockS3Backend.ListDirectories(ctx, prefix)
}

func (f *flakyRemote) List(ctx context.Context, prefix string) ([]string, error) {
	f.listCalls.Add(1)
	if f.failList.Load() {
		return nil, errors.New("simulated ListObjectsV2 timeout")
	}
	var out []string
	for _, name := range f.dayFiles[prefix] {
		out = append(out, prefix+name)
	}
	return out, nil
}

const (
	lfGlob = "s3://bucket/db/cpu/**/*.parquet"
	// Two days: hours 10 and 11 of the first day exist; the second day was
	// daily-compacted (no hour dirs, one direct file).
	lfSQL = "SELECT v FROM db.cpu WHERE time >= '2026-02-01T00:00:00Z' AND time < '2026-02-03T00:00:00Z'"
)

func newFlakyRemote() *flakyRemote {
	return &flakyRemote{
		mockS3Backend: mockS3Backend{existingDirs: map[string][]string{
			"db/cpu/":         {"2026"},
			"db/cpu/2026/":    {"02"},
			"db/cpu/2026/02/": {"01", "02"},
			"db/cpu/2026/02/01/": {
				"10", "11",
			},
			"db/cpu/2026/02/02/": {},
		}},
		dayFiles: map[string][]string{"db/cpu/2026/02/02/": {"daily.parquet"}},
	}
}

func pathsOf(t *testing.T, result interface{}) []string {
	t.Helper()
	switch v := result.(type) {
	case string:
		return []string{v}
	case []string:
		return v
	}
	t.Fatalf("unexpected pruner result %T %v", result, result)
	return nil
}

func wantHealthy(t *testing.T, result interface{}) {
	t.Helper()
	got := strings.Join(pathsOf(t, result), " ")
	for _, want := range []string{
		"s3://bucket/db/cpu/2026/02/01/10/*.parquet",
		"s3://bucket/db/cpu/2026/02/01/11/*.parquet",
		"s3://bucket/db/cpu/2026/02/02/*.parquet",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("healthy listing must include %s, got %s", want, got)
		}
	}
}

func TestListingFailureRecordedAndNeverCached_QR6(t *testing.T) {
	cases := []struct {
		name    string
		breakIt func(f *flakyRemote)
		ctx     func() (context.Context, context.CancelFunc)
	}{
		{"parent ListDirectories error", func(f *flakyRemote) { f.failDirs.Store(true); f.failPrefix = "db/cpu/2026/02/01/" }, nil},
		{"day-level List error", func(f *flakyRemote) { f.failList.Store(true) }, nil},
		{"listing timeout", func(f *flakyRemote) { f.hangDirs.Store(true) }, func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 150*time.Millisecond)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			remote := newFlakyRemote()
			p := NewPartitionPruner(zerolog.Nop())
			p.SetStorageBackend(remote)
			tc.breakIt(remote)

			base := context.Background()
			if tc.ctx != nil {
				var cancel context.CancelFunc
				base, cancel = tc.ctx()
				defer cancel()
			}
			ctx, lf := WithListingFailure(base)
			ctx, vol := WithVolatileResult(ctx)
			p.OptimizeTablePath(ctx, lfGlob, lfSQL)
			failed, reason := lf.Failed()
			if !failed || reason == "" {
				t.Fatalf("a failed listing must be recorded (failed=%v reason=%q)", failed, reason)
			}
			if !vol.Volatile {
				t.Fatal("a path set built on a failed listing must be volatile (never in the SQL transform cache)")
			}
			if _, _, ok := p.partitionCache.get(p.partitionCache.cacheKey(lfGlob, lfSQL)); ok {
				t.Fatal("a path set built on a failed listing must not be cached")
			}

			// Storage recovers: the very next query lists again and gets the
			// complete set, with nothing recorded.
			remote.failDirs.Store(false)
			remote.failList.Store(false)
			remote.hangDirs.Store(false)
			ctx2, lf2 := WithListingFailure(context.Background())
			result, optimized := p.OptimizeTablePath(ctx2, lfGlob, lfSQL)
			if failed, reason := lf2.Failed(); failed {
				t.Fatalf("healthy listing recorded a failure: %s", reason)
			}
			if !optimized {
				t.Fatal("healthy listing must prune")
			}
			wantHealthy(t, result)
		})
	}
}

func TestHealthyListingRecordsNothing_QR6(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	p.SetStorageBackend(newFlakyRemote())
	ctx, lf := WithListingFailure(context.Background())
	result, optimized := p.OptimizeTablePath(ctx, lfGlob, lfSQL)
	if failed, reason := lf.Failed(); failed || !optimized {
		t.Fatalf("failed=%v (%s) optimized=%v", failed, reason, optimized)
	}
	wantHealthy(t, result)
	// Cached (verified) and served from the cache without listing again.
	if _, _, ok := p.partitionCache.get(p.partitionCache.cacheKey(lfGlob, lfSQL)); !ok {
		t.Fatal("a verified result must stay cacheable")
	}
}

// A verified-empty range is a genuine empty result: the narrow path the pruner
// returns for it is recognizable as proven empty, and nothing is recorded.
func TestVerifiedEmptyRangeIsMarkedProvenEmpty_QR6(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	p.SetStorageBackend(newFlakyRemote())
	ctx, lf := WithListingFailure(context.Background())
	sql := "SELECT v FROM db.cpu WHERE time >= '2026-03-01T00:00:00Z' AND time < '2026-03-02T00:00:00Z'"
	result, optimized := p.OptimizeTablePath(ctx, lfGlob, sql)
	path, isString := result.(string)
	if !optimized || !isString {
		t.Fatalf("verified empty: result=%v optimized=%v", result, optimized)
	}
	if failed, _ := lf.Failed(); failed {
		t.Fatal("a verified empty range is not a listing failure")
	}
	if !IsVerifiedEmptyPath(path) || !strings.HasPrefix(path, "s3://bucket/db/cpu/2026/03/01/") {
		t.Fatalf("verified-empty path must carry the proven-empty marker under the table: %s", path)
	}
	if IsPartitionPath(path) {
		t.Fatalf("the proven-empty marker must not classify as an unverified partition read: %s", path)
	}
}

// No listing capability is not a storage error: nothing was dropped (every
// path is kept), so nothing is recorded; a "No files found" on such a path is
// handled at execution (IsPartitionPath).
func TestNoListerIsNotAListingFailure_QR6(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop()) // no storage backend
	ctx, lf := WithListingFailure(context.Background())
	p.OptimizeTablePath(ctx, lfGlob, lfSQL)
	if failed, reason := lf.Failed(); failed {
		t.Fatalf("no backend is not a listing failure: %s", reason)
	}
}

func TestExistingPartitionPathsVerdict_QR6(t *testing.T) {
	from := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 2, 3, 0, 0, 0, 0, time.UTC)

	t.Run("listing error -> unknown and failed", func(t *testing.T) {
		remote := newFlakyRemote()
		remote.failList.Store(true)
		p := NewPartitionPruner(zerolog.Nop())
		p.SetStorageBackend(remote)
		paths, ok, failed := p.ExistingPartitionPathsVerdict(lfGlob, from, to)
		if ok || paths != nil || !failed {
			t.Fatalf("got (%v, %v, %v), want (nil, false, true)", paths, ok, failed)
		}
		// The plain entry point keeps its contract.
		if paths, ok := p.ExistingPartitionPaths(lfGlob, from, to); ok || paths != nil {
			t.Fatalf("ExistingPartitionPaths: got (%v, %v), want (nil, false)", paths, ok)
		}
	})
	t.Run("listing timeout -> unknown and failed", func(t *testing.T) {
		remote := newFlakyRemote()
		remote.hangDirs.Store(true)
		p := NewPartitionPruner(zerolog.Nop())
		p.SetStorageBackend(remote)
		p.listTimeout = 100 * time.Millisecond
		start := time.Now()
		_, ok, failed := p.ExistingPartitionPathsVerdict(lfGlob, from, to)
		if ok || !failed {
			t.Fatalf("got ok=%v failed=%v, want unknown + failed", ok, failed)
		}
		if time.Since(start) > 3*time.Second {
			t.Fatalf("listing timeout not honoured: %v", time.Since(start))
		}
	})
	t.Run("not prunable is not a failure", func(t *testing.T) {
		p := NewPartitionPruner(zerolog.Nop())
		p.SetStorageBackend(newFlakyRemote())
		huge := time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC)
		if _, ok, failed := p.ExistingPartitionPathsVerdict(lfGlob, from, huge); ok || failed {
			t.Fatalf("over-cap window: ok=%v failed=%v, want unknown, not failed", ok, failed)
		}
	})
	t.Run("healthy -> paths", func(t *testing.T) {
		p := NewPartitionPruner(zerolog.Nop())
		p.SetStorageBackend(newFlakyRemote())
		paths, ok, failed := p.ExistingPartitionPathsVerdict(lfGlob, from, to)
		if !ok || failed || len(paths) != 3 {
			t.Fatalf("got (%v, %v, %v)", paths, ok, failed)
		}
	})
}

func TestTierListingFailureRecorded_QR6(t *testing.T) {
	remote := newFlakyRemote()
	remote.failDirs.Store(true)
	p := NewPartitionPruner(zerolog.Nop())
	ctx, lf := WithListingFailure(context.Background())
	tr := p.ExtractTimeRange(lfSQL)
	p.PruneTierPaths(ctx, lfGlob, "db", "cpu", tr, remote, false)
	if failed, _ := lf.Failed(); !failed {
		t.Fatal("a failed tier listing must be recorded")
	}
}

func TestPartitionPathClassification_QR6(t *testing.T) {
	partition := []string{
		"s3://bucket/db/cpu/2026/02/01/10/*.parquet",
		"s3://bucket/prefix/db/cpu/2026/02/01/*.parquet",
		"/data/db/cpu/2026/02/01/10/*.parquet",
		"azure://c/db/cpu/2026/02/01/10/cpu_20260201_100000_1.parquet",
	}
	notPartition := []string{
		"s3://bucket/db/cpu/**/*.parquet",
		"/data/db/cpu/**/*.parquet",
		"s3://bucket/_arc/rollup/events/2026-06-01.parquet",
		"/tmp/user/file.parquet",
		"s3://bucket/db/cpu/2026/02/01/10/" + verifiedEmptyGlob,
	}
	for _, p := range partition {
		if !IsPartitionPath(p) {
			t.Errorf("%s must classify as a partition read", p)
		}
	}
	for _, p := range notPartition {
		if IsPartitionPath(p) {
			t.Errorf("%s must not classify as a partition read", p)
		}
	}
}
