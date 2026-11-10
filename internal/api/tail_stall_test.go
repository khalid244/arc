//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// stallS3 is a path-style S3 endpoint for httpfs: ListObjectsV2 plus HEAD and
// ranged GET of in-memory objects. stall holds every request without an
// answer (a store that accepts the connection and then sends nothing);
// delay answers each request that late (a slow but live store).
type stallS3 struct {
	stall    bool
	delay    time.Duration
	objects  map[string][]byte // "bucket/key"
	requests atomic.Int64
	held     atomic.Int64 // stalled requests not yet given up by the client
	release  chan struct{}
	once     sync.Once
}

func newStallS3(t *testing.T, f *stallS3) string {
	t.Helper()
	f.release = make(chan struct{})
	srv := httptest.NewServer(f)
	t.Cleanup(func() {
		f.unstall()
		srv.Close()
	})
	return strings.TrimPrefix(srv.URL, "http://")
}

// unstall makes every held and later request fail at once.
func (f *stallS3) unstall() { f.once.Do(func() { close(f.release) }) }

func (f *stallS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.requests.Add(1)
	if f.stall {
		f.held.Add(1)
		defer f.held.Add(-1)
		select {
		case <-r.Context().Done():
			return
		case <-f.release:
			http.Error(w, "released", http.StatusServiceUnavailable)
			return
		}
	}
	select {
	case <-time.After(f.delay):
	case <-r.Context().Done():
		return
	}
	p := strings.TrimPrefix(r.URL.Path, "/")
	if r.URL.Query().Get("list-type") == "2" {
		bucket := strings.TrimSuffix(p, "/")
		prefix := r.URL.Query().Get("prefix")
		var keys []string
		for k := range f.objects {
			if b, key, _ := strings.Cut(k, "/"); b == bucket && strings.HasPrefix(key, prefix) {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		var b strings.Builder
		fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>%s</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>`, bucket, prefix, len(keys))
		for _, k := range keys {
			fmt.Fprintf(&b, `<Contents><Key>%s</Key><LastModified>2026-11-10T00:00:00.000Z</LastModified><ETag>"e"</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>`, k, len(f.objects[bucket+"/"+k]))
		}
		b.WriteString(`</ListBucketResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(b.String()))
		return
	}
	body, ok := f.objects[p]
	if !ok {
		http.Error(w, "<Error><Code>NoSuchKey</Code></Error>", http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", `"e"`)
	http.ServeContent(w, r, "", time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC), bytes.NewReader(body))
}

// newS3TailEnv is a query handler whose DuckDB reads s3://arc from endpoint
// with the repo's own S3 setup (database.New -> configureS3Access, the
// cache_httpfs layer on, path-style as in production). httpTimeout is
// http_timeout in seconds; http_retries stays 3, so one stalled request holds
// its run for about 4 x httpTimeout plus the retry waits. pool is the DuckDB
// connection pool (database.max_connections).
func newS3TailEnv(t *testing.T, endpoint string, httpTimeout, pool int, f *stallS3) *forkQueryEnv {
	t.Helper()
	metrics.Init(zerolog.Nop())
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	backend, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	duck, err := database.New(&database.Config{
		MemoryLimit:       "256MB",
		ThreadCount:       2,
		MaxConnections:    pool,
		LocalStorageRoot:  dataDir,
		S3Region:          "us-east-1",
		S3AccessKey:       "test",
		S3SecretKey:       "test",
		S3Endpoint:        endpoint,
		S3PathStyle:       true,
		S3Bucket:          "arc",
		ReadTimeout:       httpTimeout,
		EnableS3Cache:     true,
		S3CacheSize:       64 << 20,
		S3CacheTTLSeconds: 3600,
	}, zerolog.Nop())
	if err != nil {
		t.Skipf("duckdb with httpfs unavailable: %v", err)
	}
	t.Cleanup(func() {
		// An abandoned run still holds its connection: let it end before
		// the database closes under it.
		f.unstall()
		waitPoolIdle(duck, 30*time.Second)
		duck.Close()
	})
	h := NewQueryHandler(duck, backend, zerolog.Nop(), 0, 0)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)
	return &forkQueryEnv{t: t, h: h, app: app, dataDir: dataDir}
}

// waitPoolIdle waits until no DuckDB connection is checked out; false when
// one still is after max.
func waitPoolIdle(duck *database.DuckDB, max time.Duration) bool {
	for end := time.Now().Add(max); ; time.Sleep(50 * time.Millisecond) {
		if duck.Stats().InUse == 0 {
			return true
		}
		if time.Now().After(end) {
			return false
		}
	}
}

const stallMerge = "SELECT count(*)::BIGINT AS n FROM read_parquet('s3://arc/default/events/2026/11/10/13/*.parquet')"

// httpTimeoutForStall is http_timeout for the stalled-store tests. A stalled
// run then lasts about 4 x 1 s plus the retry waits (~6 s); production's 60 s
// makes it about 4 minutes. ARC_TEST_STALL_HTTP_TIMEOUT overrides it to
// measure that.
func httpTimeoutForStall() int {
	if n, err := strconv.Atoi(os.Getenv("ARC_TEST_STALL_HTTP_TIMEOUT")); err == nil && n > 0 {
		return n
	}
	return 1
}

// stallRunLimit bounds how long a stalled run may hold its connection: four
// attempts of http_timeout, the retry waits, and a margin.
func stallRunLimit() time.Duration {
	return time.Duration(httpTimeoutForStall())*5*time.Second + 30*time.Second
}

// Review 2026-11-10 Q2: cancelling the merge's context only sets DuckDB's
// interrupt flag, which httpfs checks between requests. A store that accepts
// a request and never answers kept the merge, and the response, waiting for
// http_timeout x attempts. The deadline now stops the wait: the cube-only
// answer goes out at about deadline + tailAbandonGrace, and the abandoned run
// gives its connection back when httpfs gives up.
func TestTailFallbackDeadlineStalledStore(t *testing.T) {
	for _, endpoint := range queryEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			srv := &stallS3{stall: true}
			e := newS3TailEnv(t, newStallS3(t, srv), httpTimeoutForStall(), 4, srv)
			e.h.SetRollupRouter(&fakeTailRouter{merged: stallMerge, cubeOnly: "SELECT 42::BIGINT AS n", served: true,
				lo: time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC), hi: time.Date(2026, 11, 10, 13, 47, 0, 0, time.UTC)})

			start := time.Now()
			res := postEndpoint(t, e, endpoint, tailQ, map[string]string{tailFallbackHeader: "cube-only", tailDeadlineHeader: "300"})
			took := time.Since(start)
			t.Logf("deadline 300ms: answered after %v (status %d, requests %d, held %d, conns in use %d)",
				took, res.status, srv.requests.Load(), srv.held.Load(), e.h.db.Stats().InUse)
			if res.status != fiber.StatusOK || len(res.cols["n"]) != 1 || res.cols["n"][0].(float64) != 42 {
				t.Fatalf("status %d: %s, want the cube-only answer 42", res.status, res.raw)
			}
			if !strings.HasPrefix(res.header.Get(partialHeader), "tail-missing; from=") {
				t.Fatalf("X-Arc-Partial = %q", res.header.Get(partialHeader))
			}
			if limit := 300*time.Millisecond + tailAbandonGrace + 1500*time.Millisecond; took > limit {
				t.Fatalf("answered after %v, want under %v: the deadline waited for the stalled request", took, limit)
			}
			if srv.held.Load() == 0 || e.h.db.Stats().InUse == 0 || e.h.tailAbandon.running() != 1 {
				t.Fatalf("the merge is not abandoned in a stalled request (held %d, conns in use %d, abandoned %d)",
					srv.held.Load(), e.h.db.Stats().InUse, e.h.tailAbandon.running())
			}

			// The abandoned run ends when httpfs gives up and releases its
			// connection by itself.
			released := time.Now()
			if limit := stallRunLimit(); !waitPoolIdle(e.h.db, limit) {
				t.Fatalf("the abandoned run still holds a connection after %v", limit)
			}
			t.Logf("abandoned run released its connection %v after the answer", time.Since(released))
			if n := e.h.tailAbandon.running(); n != 0 {
				t.Fatalf("abandoned = %d after the run ended, want 0", n)
			}
		})
	}
}

// stallTailRouter is the fake router of the stalled-store tests: its merge
// reads the stalled store, its cube-only answer is 42.
func stallTailRouter() *fakeTailRouter {
	return &fakeTailRouter{merged: stallMerge, cubeOnly: "SELECT 42::BIGINT AS n", served: true,
		lo: time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC), hi: time.Date(2026, 11, 10, 13, 47, 0, 0, time.UTC)}
}

// cubeOnlyAnswer reports what is wrong with res as the labelled cube-only
// answer 42, "" when nothing is.
func cubeOnlyAnswer(res queryResult) string {
	if res.status != fiber.StatusOK || len(res.cols["n"]) != 1 || res.cols["n"][0].(float64) != 42 {
		return fmt.Sprintf("status %d: %s, want the cube-only answer 42", res.status, res.raw)
	}
	if !strings.HasPrefix(res.header.Get(partialHeader), "tail-missing; from=") {
		return fmt.Sprintf("X-Arc-Partial = %q", res.header.Get(partialHeader))
	}
	return ""
}

// Review 2026-11-10 N1: once query.tail_abandoned_max merges are abandoned
// and still running, the store is stalled. A new request does not start its
// merge: it is answered cube-only at once, holds no connection for a merge
// and does not wait for its deadline. Before, it ran its merge, waited out its
// deadline and then the whole stalled request.
func TestTailFallbackAbandonedCap(t *testing.T) {
	srv := &stallS3{stall: true}
	e := newS3TailEnv(t, newStallS3(t, srv), httpTimeoutForStall(), 4, srv)
	e.h.SetRollupRouter(stallTailRouter())
	e.h.SetTailAbandonedMax(1)

	stop := make(chan struct{})
	var peak, peakConns atomic.Int64
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				if n := int64(e.h.tailAbandon.running()); n > peak.Load() {
					peak.Store(n)
				}
				if n := int64(e.h.db.Stats().InUse); n > peakConns.Load() {
					peakConns.Store(n)
				}
			}
		}
	}()
	defer close(stop)

	fast := 300*time.Millisecond + tailAbandonGrace + 1500*time.Millisecond
	start := time.Now()
	first := postEndpoint(t, e, "/api/v1/query/arrow", tailQ, map[string]string{tailFallbackHeader: "cube-only", tailDeadlineHeader: "300"})
	if took := time.Since(start); cubeOnlyAnswer(first) != "" || took > fast || e.h.tailAbandon.running() != 1 {
		t.Fatalf("first: %s after %v, abandoned %d: want a fast cube-only answer and one abandoned merge",
			cubeOnlyAnswer(first), took, e.h.tailAbandon.running())
	}
	peakConns.Store(0)
	for _, endpoint := range queryEndpoints {
		start = time.Now()
		res := postEndpoint(t, e, endpoint, tailQ, map[string]string{tailFallbackHeader: "cube-only", tailDeadlineHeader: "5000"})
		took := time.Since(start)
		t.Logf("cap 1 full: %s answered after %v (status %d, peak abandoned %d, peak conns %d)",
			endpoint, took, res.status, peak.Load(), peakConns.Load())
		if msg := cubeOnlyAnswer(res); msg != "" {
			t.Fatalf("%s: %s", endpoint, msg)
		}
		if took > time.Second {
			t.Fatalf("%s answered after %v with the cap full: it started its merge instead of answering cube-only", endpoint, took)
		}
	}
	// The abandoned merge's connection plus one for each cube-only answer.
	if p := peakConns.Load(); p > 2 {
		t.Fatalf("peak connections in use = %d with the cap full, want at most 2: a merge was started", p)
	}
	srv.unstall()
	if !waitPoolIdle(e.h.db, stallRunLimit()) {
		t.Fatal("a merge still holds a connection after the stall ended")
	}
	if p := peak.Load(); p > 1 {
		t.Fatalf("peak abandoned = %d, want at most the cap 1", p)
	}
	if n := e.h.tailAbandon.running(); n != 0 {
		t.Fatalf("abandoned = %d after the stall ended, want 0", n)
	}
}

// burstResult is one response of postBurst and when it came.
type burstResult struct {
	queryResult
	took time.Duration
	err  error
}

// postBurst sends n tail-fallback requests at once, alternating the two query
// endpoints.
func postBurst(e *forkQueryEnv, n int, headers map[string]string) []burstResult {
	out := make([]burstResult, n)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range out {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := postEndpointErr(e, queryEndpoints[i%len(queryEndpoints)], tailQ, headers)
			out[i] = burstResult{queryResult: res, took: time.Since(start), err: err}
		}(i)
	}
	wg.Wait()
	return out
}

// Review 2026-11-10 N1, the reviewer's case: a dashboard sends more
// tail-fallback panels at once than query.tail_abandoned_max while the store
// is stalled. Every one is answered cube-only at about deadline +
// tailAbandonGrace. Before, the merges past the cap waited out the stalled
// request (12.8 s at http_timeout 3, about 4 minutes in production), and with
// a pool smaller than the burst the stalled merges held every connection, so
// even the abandoned requests waited for one for their cube-only answer.
func TestTailFallbackStallBurst(t *testing.T) {
	for _, c := range []struct {
		name             string
		pool, max, burst int
	}{
		{"pool 16, cap 4, 10 requests", 16, 4, 10},
		{"pool 4 (cap lowered to 2), 6 requests", 4, 4, 6},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv := &stallS3{stall: true}
			e := newS3TailEnv(t, newStallS3(t, srv), 3, c.pool, srv)
			e.h.SetRollupRouter(stallTailRouter())
			e.h.SetTailAbandonedMax(c.max)
			m := metrics.Get()
			abandoned0, skipped0 := m.QueryTailMergesAbandoned.Load(), m.QueryTailMergesSkipped.Load()

			results := postBurst(e, c.burst, map[string]string{tailFallbackHeader: "cube-only", tailDeadlineHeader: "300"})
			limit := 300*time.Millisecond + tailAbandonGrace + 1500*time.Millisecond
			var took []string
			for _, r := range results {
				took = append(took, r.took.Round(10*time.Millisecond).String())
			}
			t.Logf("%s: answered after %v (abandoned %d, conns in use %d)", c.name, took, e.h.tailAbandon.running(), e.h.db.Stats().InUse)
			for i, r := range results {
				if r.err != nil {
					t.Fatalf("request %d: %v", i, r.err)
				}
				if msg := cubeOnlyAnswer(r.queryResult); msg != "" {
					t.Fatalf("request %d: %s", i, msg)
				}
				if r.took > limit {
					t.Fatalf("request %d answered after %v, want every one under %v", i, r.took, limit)
				}
			}
			n := e.h.tailAbandon.running()
			if n > c.pool/2 {
				t.Fatalf("abandoned = %d, want at most half the pool (%d)", n, c.pool/2)
			}
			if d := m.QueryTailMergesAbandoned.Load() - abandoned0; d != int64(n) {
				t.Fatalf("arc_query_tail_merges_abandoned_total grew by %d, want %d", d, n)
			}

			start := time.Now()
			res := postEndpoint(t, e, "/api/v1/query", tailQ, map[string]string{tailFallbackHeader: "cube-only", tailDeadlineHeader: "5000"})
			if msg := cubeOnlyAnswer(res); msg != "" {
				t.Fatalf("after the burst: %s", msg)
			}
			if d := time.Since(start); d > time.Second {
				t.Fatalf("after the burst: answered after %v, want at once (the cap is full)", d)
			}
			if d := m.QueryTailMergesSkipped.Load() - skipped0; d != 1 {
				t.Fatalf("arc_query_tail_merges_skipped_total grew by %d, want 1", d)
			}

			srv.unstall()
			if !waitPoolIdle(e.h.db, stallRunLimit()) {
				t.Fatal("a merge still holds a connection after the stall ended")
			}
			if n := e.h.tailAbandon.running(); n != 0 {
				t.Fatalf("abandoned = %d after the stall ended, want 0", n)
			}
		})
	}
}

// A slow store that answers every request is interrupted between requests
// within the grace: the merge is not abandoned, its connection is back in the
// pool before the answer, and the answer comes at about the deadline.
func TestTailFallbackDeadlineSlowStoreNotAbandoned(t *testing.T) {
	body := parquetBytes(t, "SELECT TIMESTAMPTZ '2026-11-10 13:00:00+00' + INTERVAL (i) SECOND AS time FROM range(100) t(i)")
	objects := map[string][]byte{}
	for i := 0; i < 40; i++ {
		objects[fmt.Sprintf("arc/default/events/2026/11/10/13/f%02d.parquet", i)] = body
	}
	for _, endpoint := range queryEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			srv := &stallS3{delay: 500 * time.Millisecond, objects: objects}
			e := newS3TailEnv(t, newStallS3(t, srv), 60, 4, srv)
			e.h.SetRollupRouter(&fakeTailRouter{merged: stallMerge, cubeOnly: "SELECT 42::BIGINT AS n", served: true,
				lo: time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC), hi: time.Date(2026, 11, 10, 13, 47, 0, 0, time.UTC)})
			start := time.Now()
			res := postEndpoint(t, e, endpoint, tailQ, map[string]string{tailFallbackHeader: "cube-only", tailDeadlineHeader: "2000"})
			took := time.Since(start)
			t.Logf("slow store, deadline 2s: answered after %v (requests %d)", took, srv.requests.Load())
			if res.status != fiber.StatusOK || len(res.cols["n"]) != 1 || res.cols["n"][0].(float64) != 42 {
				t.Fatalf("status %d: %s, want the cube-only answer 42", res.status, res.raw)
			}
			if took < 2*time.Second || took > 2*time.Second+tailAbandonGrace+time.Second {
				t.Fatalf("answered after %v, want about the 2 s deadline", took)
			}
			if n, inUse := e.h.tailAbandon.running(), e.h.db.Stats().InUse; n != 0 || inUse != 0 {
				t.Fatalf("abandoned %d, conns in use %d: an interruptible merge must end before the answer", n, inUse)
			}
		})
	}
}

// parquetBytes is selectSQL written as one Parquet file.
func parquetBytes(t *testing.T, selectSQL string) []byte {
	t.Helper()
	p := filepath.Join(t.TempDir(), "f.parquet")
	db, err := gosql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("COPY (" + selectSQL + ") TO '" + p + "' (FORMAT parquet)"); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The cap stays at or below half the connection pool, so abandoned merges
// leave at least half of it to other queries.
func TestTailAbandonCap(t *testing.T) {
	for _, c := range []struct{ want, pool, cap int }{
		{4, 0, 4}, {4, 64, 4}, {4, 8, 4}, {4, 6, 3}, {4, 4, 2}, {4, 1, 0}, {0, 64, 0}, {-1, 8, 0},
	} {
		if got := tailAbandonCap(c.want, c.pool); got != c.cap {
			t.Errorf("tailAbandonCap(%d, pool %d) = %d, want %d", c.want, c.pool, got, c.cap)
		}
	}
	e := newForkQueryEnv(t) // MaxConnections 4
	if got := e.h.tailAbandon.max; got != 2 {
		t.Fatalf("default cap with a pool of 4 = %d, want 2", got)
	}
	e.h.SetTailAbandonedMax(10)
	if got := e.h.tailAbandon.max; got != 2 {
		t.Fatalf("SetTailAbandonedMax(10) with a pool of 4 = %d, want 2", got)
	}
	e.h.SetTailAbandonedMax(0)
	if got := e.h.tailAbandon.max; got != 0 {
		t.Fatalf("SetTailAbandonedMax(0) = %d, want 0", got)
	}
}

// The cap check and the slot are taken under one lock: a merge waiting for a
// slot is woken when a merge is abandoned, and once the abandoned merges are
// at the cap no merge starts, slot or not. Abandoned past the cap is allowed
// (those merges were already running).
func TestTailAbandonerSkipsOnceFull(t *testing.T) {
	a := newTailAbandoner(zerolog.Nop(), 2, 4) // cap 2, 2 slots
	tf := &tailFallback{}
	for i := 0; i < 2; i++ {
		if err := a.start(context.Background(), tf); err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
	}
	waiting := make(chan error, 1)
	go func() { waiting <- a.start(context.Background(), tf) }()
	time.Sleep(50 * time.Millisecond)
	if _, _, ok := a.abandon(); !ok {
		t.Fatal("abandon refused under the cap")
	}
	select {
	case err := <-waiting:
		t.Fatalf("waiter returned %v with a slot still taken and the cap not full", err)
	case <-time.After(50 * time.Millisecond):
	}
	a.abandon()
	select {
	case err := <-waiting:
		if !errors.Is(err, errTailSkipped) {
			t.Fatalf("waiter: %v, want errTailSkipped once the cap is full", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter was not woken when the cap filled")
	}
	if n, _, ok := a.abandon(); !ok || n != 3 {
		t.Fatalf("abandon past the cap: n=%d ok=%v, want 3 true", n, ok)
	}
	a.end()
	if err := a.start(context.Background(), tf); !errors.Is(err, errTailSkipped) {
		t.Fatalf("start with a free slot and the cap full: %v, want errTailSkipped", err)
	}
	for i := 0; i < 2; i++ {
		a.release()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := a.start(ctx, tf); err != nil {
		t.Fatalf("start under the cap with a free slot: %v", err)
	}
	if err := a.start(ctx, tf); !errors.Is(err, errTailDeadline) {
		t.Fatalf("start with no slot until ctx ends: %v, want errTailDeadline", err)
	}

	off := newTailAbandoner(zerolog.Nop(), 0, 4)
	for i := 0; i < 5; i++ {
		if err := off.start(context.Background(), tf); err != nil {
			t.Fatalf("cap 0 limits merges: %v", err)
		}
	}
	if _, _, ok := off.abandon(); ok {
		t.Fatal("cap 0 abandoned a merge")
	}
}
