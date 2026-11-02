package pruning

// Manual, READ-ONLY timing of the remote existence filter against a real
// bucket: the legacy request pattern (sequential per-parent ListDirectories +
// per-day List) vs the new one, over one month, with identical results
// required. Only ListObjectsV2 (and the HeadBucket NewS3Backend makes) is
// sent; nothing is read, written or deleted.
//
// Skipped unless ARC_PRUNER_LIST_BENCH=1. Connection: S3_BUCKET, S3_REGION,
// S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY. Range: ARC_PRUNER_LIST_BENCH_DB,
// _MEAS, _MONTH (default posthog / events / 2026-10).

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// benchCap stops the bench before it can exceed its request budget.
const benchCap = 150

type cappedBackend struct {
	storage.Backend
	lister    storage.DirectoryLister
	calls     atomic.Int64
	keys      atomic.Int64
	listCalls atomic.Int64
}

func (c *cappedBackend) take() error {
	if c.calls.Add(1) > benchCap {
		return fmt.Errorf("bench request cap %d reached", benchCap)
	}
	return nil
}

func (c *cappedBackend) List(ctx context.Context, prefix string) ([]string, error) {
	if err := c.take(); err != nil {
		return nil, err
	}
	c.listCalls.Add(1)
	keys, err := c.Backend.List(ctx, prefix)
	c.keys.Add(int64(len(keys)))
	return keys, err
}

func (c *cappedBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	if err := c.take(); err != nil {
		return nil, err
	}
	return c.lister.ListDirectories(ctx, prefix)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestManualS3_RemoteListingBench(t *testing.T) {
	if os.Getenv("ARC_PRUNER_LIST_BENCH") != "1" || os.Getenv("S3_BUCKET") == "" {
		t.Skip("ARC_PRUNER_LIST_BENCH=1 and S3_* not set; skipping real-S3 listing bench")
	}
	be, err := storage.NewS3Backend(&storage.S3Config{
		Bucket:    os.Getenv("S3_BUCKET"),
		Region:    os.Getenv("S3_REGION"),
		Endpoint:  os.Getenv("S3_ENDPOINT"),
		AccessKey: os.Getenv("S3_ACCESS_KEY"),
		SecretKey: os.Getenv("S3_SECRET_KEY"),
		UseSSL:    true,
		PathStyle: os.Getenv("S3_ENDPOINT") != "",
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewS3Backend: %v", err)
	}
	db, meas := envOr("ARC_PRUNER_LIST_BENCH_DB", "posthog"), envOr("ARC_PRUNER_LIST_BENCH_MEAS", "events")
	from, err := time.Parse("2006-01", envOr("ARC_PRUNER_LIST_BENCH_MONTH", "2026-10"))
	if err != nil {
		t.Fatal(err)
	}
	base := "s3://" + os.Getenv("S3_BUCKET")
	paths := genPaths(t, base, db, meas, from, from.AddDate(0, 1, 0))

	run := func(legacy bool) (filterRun, *cappedBackend) {
		cb := &cappedBackend{Backend: be, lister: be}
		p := NewPartitionPruner(zerolog.Nop())
		p.SetStorageBackend(cb)
		p.SetListingTimeout(60 * time.Second)
		return runFilter(context.Background(), p, legacy, paths, base, 0), cb
	}
	old, oldBE := run(true)
	got, newBE := run(false)
	t.Logf("%s/%s %s: %d generated paths", db, meas, from.Format("2006-01"), len(paths))
	t.Logf("OLD: %d LIST calls (%d recursive, %d keys) in %.2fs -> %d paths verified=%v failed=%v %s",
		oldBE.calls.Load(), oldBE.listCalls.Load(), oldBE.keys.Load(), old.elapsed.Seconds(), len(old.paths), old.verified, old.failed, old.reason)
	t.Logf("NEW: %d LIST calls (%d recursive, %d keys) in %.2fs -> %d paths verified=%v failed=%v %s",
		newBE.calls.Load(), newBE.listCalls.Load(), newBE.keys.Load(), got.elapsed.Seconds(), len(got.paths), got.verified, got.failed, got.reason)
	if !reflect.DeepEqual(old.paths, got.paths) || old.verified != got.verified || old.failed != got.failed {
		t.Fatalf("results differ: old %d paths, new %d paths", len(old.paths), len(got.paths))
	}
}
