//go:build duckdb_arrow

package api

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
)

// P3Q-1: while a compaction is between uploading its output and deleting its
// inputs, a partition holds every row twice, and the query hides the inputs
// named by the compaction manifests. When the manifests could not be listed or
// read (an S3 slow period: exactly when that window grows), the exclusion was
// dropped silently: the query double counted behind a 200, and the transform
// without the exclusion was cached for the transform-cache TTL. Unreadable
// manifests are now an incomplete listing: 503 + Retry-After, never cached.

// switchableManifests serves a fixed exclusion set, or fails like S3 does.
type switchableManifests struct {
	uploaded map[string]struct{}
	fail     atomic.Bool
}

var errManifestsSlowDown = errors.New("failed to list manifests: operation error S3: ListObjectsV2, https response error StatusCode: 503, SlowDown")

func (s *switchableManifests) GetFilesInManifests(context.Context) (map[string]struct{}, error) {
	if s.fail.Load() {
		return nil, errManifestsSlowDown
	}
	return s.uploaded, nil
}

func (s *switchableManifests) GetInputFilesForUploadedOutputs(context.Context) (map[string]struct{}, error) {
	if s.fail.Load() {
		return nil, errManifestsSlowDown
	}
	return s.uploaded, nil
}

const p3q1SQL = "SELECT count(*) AS n FROM logs WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-01T01:00:00Z'"

// p3q1Compaction writes one hour in its upload -> delete window: inputs in1 and
// in2 (60 rows each) beside the compacted output (the same 120 rows). It
// returns the inputs' storage keys and the output's.
func p3q1Compaction(e *forkQueryEnv) (inputs []string, output string) {
	rows := func(minute string) string {
		return "SELECT TIMESTAMPTZ '2026-06-01 00:" + minute + ":00+00' + INTERVAL (i) SECOND AS time, i AS v FROM range(60) t(i)"
	}
	in1 := e.writeParquet("default/logs/2026/06/01/00/in1.parquet", rows("10"))
	in2 := e.writeParquet("default/logs/2026/06/01/00/in2.parquet", rows("20"))
	out := e.writeParquet("default/logs/2026/06/01/00/out.parquet", "SELECT * FROM ("+rows("10")+" UNION ALL "+rows("20")+")")
	for _, p := range []string{in1, in2, out} {
		rel, err := filepath.Rel(e.dataDir, p)
		if err != nil {
			e.t.Fatal(err)
		}
		if p == out {
			output = filepath.ToSlash(rel)
		} else {
			inputs = append(inputs, filepath.ToSlash(rel))
		}
	}
	return inputs, output
}

// jsonCount posts p3q1SQL-like SQL to the JSON endpoint and returns n.
func jsonCount(t *testing.T, e *forkQueryEnv, sql string, headers map[string]string) float64 {
	t.Helper()
	status, out, _ := e.post("/api/v1/query", sql, headers)
	if status != fiber.StatusOK || len(out.Data) != 1 {
		t.Fatalf("status %d: %s", status, out.raw)
	}
	return out.Data[0][0].(float64)
}

func TestCompactionManifestsUnreadableAnswers503_P3Q1(t *testing.T) {
	requests := []struct {
		name, path, sql string
		headers         map[string]string
	}{
		{"JSON", "/api/v1/query", p3q1SQL, nil},
		{"Arrow IPC", "/api/v1/query/arrow", p3q1SQL, nil},
		{"msgpack", "/api/v1/query/msgpack", p3q1SQL, nil},
		{"estimate", "/api/v1/query/estimate", p3q1SQL, nil},
		// No quotes + x-arc-database: the parallel-partition transform, whose
		// exclusion check routes a compacting table to the standard path.
		{"JSON parallel transform", "/api/v1/query", "SELECT count(*) AS n FROM logs",
			map[string]string{"x-arc-database": "default"}},
	}
	for _, r := range requests {
		t.Run(r.name, func(t *testing.T) {
			e := newForkQueryEnv(t)
			inputs, _ := p3q1Compaction(e)
			mm := &switchableManifests{uploaded: map[string]struct{}{inputs[0]: {}, inputs[1]: {}}}
			mm.fail.Store(true)
			e.h.SetManifestManager(mm)

			before := listingIncompleteTotal(t)
			status, out, _ := e.post(r.path, r.sql, r.headers)
			if status != fiber.StatusServiceUnavailable {
				t.Fatalf("unreadable manifests: status %d, want 503 (a 200 here counts every row twice): %s", status, out.raw)
			}
			if !strings.Contains(out.raw, listingIncompleteMessage) || !strings.Contains(out.raw, "compaction manifests") {
				t.Fatalf("body must say %q and name the manifests: %s", listingIncompleteMessage, out.raw)
			}
			if got := listingIncompleteTotal(t); got != before+1 {
				t.Fatalf("arc_query_listing_incomplete_total = %d, want %d", got, before+1)
			}

			// Storage recovers. No cache was invalidated: a transform cached
			// without the exclusion would still count 240.
			mm.fail.Store(false)
			if n := jsonCount(t, e, r.sql, r.headers); n != 120 {
				t.Fatalf("after recovery n = %v, want 120 (each row once)", n)
			}
		})
	}
}

func TestCompactionManifestsUnreadableRetryAfter_P3Q1(t *testing.T) {
	e := newForkQueryEnv(t)
	inputs, _ := p3q1Compaction(e)
	mm := &switchableManifests{uploaded: map[string]struct{}{inputs[0]: {}, inputs[1]: {}}}
	mm.fail.Store(true)
	e.h.SetManifestManager(mm)
	q := &qr6RemoteEnv{t: t, h: e.h, app: e.app}
	status, body, retryAfter := q.post("/api/v1/query", p3q1SQL, nil)
	if status != fiber.StatusServiceUnavailable || retryAfter == "" {
		t.Fatalf("want 503 with Retry-After, got %d (Retry-After %q): %s", status, retryAfter, body)
	}
}

// The same end to end with the real ManifestManager: a manifest whose read
// fails transiently (the listing succeeded) must not shrink the exclusion set.
type manifestReadFailures struct {
	*storage.LocalBackend
	failRead atomic.Bool
}

func (m *manifestReadFailures) Read(ctx context.Context, p string) ([]byte, error) {
	if m.failRead.Load() && strings.HasSuffix(p, ".json") {
		return nil, errors.New("operation error S3: GetObject, https response error StatusCode: 503, SlowDown")
	}
	return m.LocalBackend.Read(ctx, p)
}

func TestRealManifestReadFailureAnswers503_P3Q1(t *testing.T) {
	e := newForkQueryEnv(t)
	inputs, output := p3q1Compaction(e)
	lb, err := storage.NewLocalBackend(e.dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	be := &manifestReadFailures{LocalBackend: lb}
	writer := compaction.NewManifestManager(be, zerolog.Nop())
	ctx := context.Background()
	p, err := writer.WriteManifest(ctx, &compaction.Manifest{
		OutputPath: output, InputFiles: inputs, Database: "default", Measurement: "logs",
		PartitionPath: "default/logs/2026/06/01/00", Tier: "hourly", JobID: "job1", CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.MarkOutputUploaded(ctx, p); err != nil {
		t.Fatal(err)
	}

	// The query pod's own manager: nothing cached.
	e.h.SetManifestManager(compaction.NewManifestManager(be, zerolog.Nop()))
	be.failRead.Store(true)
	for _, path := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
		status, out, _ := e.post(path, p3q1SQL, nil)
		if status != fiber.StatusServiceUnavailable {
			t.Fatalf("%s: manifest read failing: status %d, want 503: %s", path, status, out.raw)
		}
	}
	be.failRead.Store(false)
	if n := jsonCount(t, e, p3q1SQL, nil); n != 120 {
		t.Fatalf("after recovery n = %v, want 120", n)
	}
}
