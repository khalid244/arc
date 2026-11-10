//go:build duckdb_arrow

package api

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/rollup"
)

// main.go wires a *rollup.Manager as the router; both it and *rollup.Router
// must keep matching the optional interface, or the header silently does
// nothing.
var (
	_ rollupTailRouter = (*rollup.Manager)(nil)
	_ rollupTailRouter = (*rollup.Router)(nil)
)

// X-Arc-Tail-Fallback: cube-only (review 2026-11-10, suggestion 9). When the
// fresh tail of an AUTO merge fails or passes its deadline, a request that
// opted in gets the cube part with X-Arc-Partial naming the missing window.

const tailQ = "SELECT count(*) AS n FROM events WHERE time >= '2026-06-01T00:00:00Z' AND time < '2026-06-04T00:00:00Z'"

// newTailEnv's cube covers 2026-06-01; the tail is [2026-06-02, 2026-06-04).
const (
	tailCubeOnly = 1440
	tailComplete = 3480
	tailPartial  = "tail-missing; from=2026-06-02T00:00:00Z; to=2026-06-04T00:00:00Z"
)

var cubeOnlyHeaders = map[string]string{tailFallbackHeader: "cube-only"}

// breakTail writes a file that is not Parquet into a tail partition, so every
// read of the tail fails (as a storage read error would).
func breakTail(t *testing.T, e *forkQueryEnv) {
	t.Helper()
	p := filepath.Join(e.dataDir, "default/events/2026/06/03/05/broken.parquet")
	if err := os.WriteFile(p, []byte("not a parquet file"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestTailFallbackAnswersCubeOnlyWhenTailFails(t *testing.T) {
	for _, endpoint := range queryEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			env := newTailEnv(t)

			// Healthy tail: the complete answer, no label, header or not.
			for _, hdr := range []map[string]string{nil, cubeOnlyHeaders} {
				n, h := countOf(t, env.forkQueryEnv, endpoint, tailQ, hdr)
				if n != tailComplete || h.Get(partialHeader) != "" || h.Get("X-Arc-Rollup-Cube") == "" {
					t.Fatalf("healthy tail (headers %v): n=%v partial=%q cube=%q, want %d unlabelled from the cube",
						hdr, n, h.Get(partialHeader), h.Get("X-Arc-Rollup-Cube"), tailComplete)
				}
			}

			breakTail(t, env.forkQueryEnv)
			// Without the header nothing changes: the query fails.
			if res := postEndpoint(t, env.forkQueryEnv, endpoint, tailQ, nil); res.status == fiber.StatusOK || res.header.Get(partialHeader) != "" {
				t.Fatalf("header off: status %d partial=%q, want the unchanged failure: %s", res.status, res.header.Get(partialHeader), res.raw)
			}
			// With it: the cube part, labelled.
			n, h := countOf(t, env.forkQueryEnv, endpoint, tailQ, cubeOnlyHeaders)
			if n != tailCubeOnly || h.Get(partialHeader) != tailPartial {
				t.Fatalf("tail failed: n=%v partial=%q, want %d with %q", n, h.Get(partialHeader), tailCubeOnly, tailPartial)
			}
			if h.Get("X-Arc-Rollup-Fallback") != "" {
				t.Fatalf("the cube-only answer must not take the source fallback: %v", h)
			}
		})
	}
}

// A cube failure is still an error, whatever happens to the tail.
func TestTailFallbackCubeFailureIsError(t *testing.T) {
	for _, endpoint := range queryEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			env := newTailEnv(t)
			breakTail(t, env.forkQueryEnv)
			if err := os.Remove(env.cubeFile); err != nil {
				t.Fatal(err)
			}
			res := postEndpoint(t, env.forkQueryEnv, endpoint, tailQ, cubeOnlyHeaders)
			if res.status == fiber.StatusOK || res.header.Get(partialHeader) != "" {
				t.Fatalf("cube and tail both broken: status %d partial=%q, want an error: %s", res.status, res.header.Get(partialHeader), res.raw)
			}
		})
	}
}

// fakeTailRouter serves a fixed merge and cube-only answer.
type fakeTailRouter struct {
	merged, cubeOnly string
	served           bool
	lo, hi           time.Time
	routeCalls       atomic.Int64
	tailCalls        atomic.Int64
	onlyCalls        atomic.Int64
}

func (f *fakeTailRouter) RouteHTTP(string, string) (string, bool, string) {
	f.routeCalls.Add(1)
	return f.merged, f.served, "fake.cube"
}

func (f *fakeTailRouter) RouteOnlyHTTP(string, string) (string, bool, string) {
	f.onlyCalls.Add(1)
	return "SELECT 5::BIGINT AS n", true, "fake.cube"
}

func (f *fakeTailRouter) ExplainHTTP(string, string) (bool, string, string) { return false, "", "fake" }

func (f *fakeTailRouter) RouteTailHTTP(string, string) (string, bool, string, string, time.Time, time.Time) {
	f.tailCalls.Add(1)
	return f.merged, f.served, "fake.cube", f.cubeOnly, f.lo, f.hi
}

// slowMerge runs far longer than any test deadline unless it is interrupted.
const slowMerge = "SELECT count(*)::BIGINT AS n FROM range(1000000000000) t(i) WHERE i % 7 = 3"

func fakeTailEnv(t *testing.T, f *fakeTailRouter) *forkQueryEnv {
	t.Helper()
	e := newForkQueryEnv(t)
	f.lo = time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	f.hi = time.Date(2026, 11, 10, 13, 47, 12, 345e6, time.UTC)
	e.h.SetRollupRouter(f)
	return e
}

func TestTailFallbackDeadline(t *testing.T) {
	for _, endpoint := range queryEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			f := &fakeTailRouter{merged: slowMerge, cubeOnly: "SELECT 42::BIGINT AS n", served: true}
			e := fakeTailEnv(t, f)
			start := time.Now()
			n, h := countOf(t, e, endpoint, tailQ, map[string]string{tailFallbackHeader: "cube-only", tailDeadlineHeader: "300"})
			if n != 42 {
				t.Fatalf("n = %v, want the cube-only answer 42", n)
			}
			if want := "tail-missing; from=2026-11-10T00:00:00Z; to=2026-11-10T13:47:12.345Z"; h.Get(partialHeader) != want {
				t.Fatalf("X-Arc-Partial = %q, want %q", h.Get(partialHeader), want)
			}
			if took := time.Since(start); took > 20*time.Second {
				t.Fatalf("took %v: the deadline did not stop the merge", took)
			}
		})
	}
}

// A merge declined because its tail could not be planned (its listing
// failed) is answered cube-only at once when the request opted in; without
// the header it goes to source as before.
func TestTailFallbackUnplannedTailServesCubeOnly(t *testing.T) {
	for _, endpoint := range queryEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			f := &fakeTailRouter{cubeOnly: "SELECT 7::BIGINT AS n"}
			e := fakeTailEnv(t, f)
			n, h := countOf(t, e, endpoint, tailQ, cubeOnlyHeaders)
			if n != 7 || !strings.HasPrefix(h.Get(partialHeader), "tail-missing; from=") || h.Get("X-Arc-Rollup-Cube") != "fake.cube" {
				t.Fatalf("n=%v partial=%q cube=%q, want 7 labelled from fake.cube", n, h.Get(partialHeader), h.Get("X-Arc-Rollup-Cube"))
			}
			if res := postEndpoint(t, e, endpoint, tailQ, nil); res.header.Get(partialHeader) != "" || res.header.Get("X-Arc-Rollup-Cube") != "" {
				t.Fatalf("header off: the query must go to source unlabelled: %v", res.header)
			}
		})
	}
}

// Rollup-only and raw-only requests, and the msgpack endpoint, ignore the
// header.
func TestTailFallbackOnlyInAutoMode(t *testing.T) {
	cases := []struct {
		name, endpoint string
		headers        map[string]string
		wantTail       int
	}{
		{"auto JSON", "/api/v1/query", nil, 1},
		{"auto Arrow", "/api/v1/query/arrow", nil, 1},
		{"rollup-only", "/api/v1/query", map[string]string{"X-Arc-Rollup-Only": "true"}, 0},
		{"raw-only", "/api/v1/query", map[string]string{"X-Arc-No-Rollup": "true"}, 0},
		{"msgpack", "/api/v1/query/msgpack", nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeTailRouter{merged: "SELECT 1::BIGINT AS n", cubeOnly: "SELECT 2::BIGINT AS n", served: true}
			e := fakeTailEnv(t, f)
			headers := map[string]string{tailFallbackHeader: "cube-only"}
			for k, v := range c.headers {
				headers[k] = v
			}
			res := postEndpoint(t, e, c.endpoint, tailQ, headers)
			if f.tailCalls.Load() != int64(c.wantTail) {
				t.Fatalf("RouteTailHTTP calls = %d, want %d (route %d, only %d)", f.tailCalls.Load(), c.wantTail, f.routeCalls.Load(), f.onlyCalls.Load())
			}
			if res.header.Get(partialHeader) != "" {
				t.Fatalf("a healthy or non-auto query is never labelled partial: %v", res.header)
			}
		})
	}
}

func TestTailDeadlineHeader(t *testing.T) {
	app := fiber.New()
	var got []time.Duration
	app.Get("/", func(c *fiber.Ctx) error {
		got = append(got, tailDeadline(c))
		return nil
	})
	for _, v := range []string{"", "250", "0", "-5", "abc", "999999999999999"} {
		req := httptest.NewRequest("GET", "/", nil)
		if v != "" {
			req.Header.Set(tailDeadlineHeader, v)
		}
		if _, err := app.Test(req); err != nil {
			t.Fatal(err)
		}
	}
	want := []time.Duration{defaultTailDeadline, 250 * time.Millisecond, defaultTailDeadline, defaultTailDeadline, defaultTailDeadline, maxTailDeadline}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("deadline %d = %v, want %v (all %v)", i, got[i], want[i], got)
		}
	}
}

// Review 2026-11-10 Q5: when the tail fails and the cube-only answer then
// finds its cube object gone (a stale manifest pointer), the query takes the
// source fallback, as it does without the header, instead of failing.
func TestTailFallbackStaleCubeTakesSourceFallback(t *testing.T) {
	const staleCube = "SELECT error('HTTP GET error on ''s3://arc/_arc/rollup/default/events/c1/2026/06/01/cube.parquet'' (HTTP 404)')::BIGINT AS n"
	cases := []struct {
		name, merged string
		headers      map[string]string
	}{
		{"tail read error", "SELECT error('IO Error: tail read failed')::BIGINT AS n", cubeOnlyHeaders},
		{"tail deadline", slowMerge, map[string]string{tailFallbackHeader: "cube-only", tailDeadlineHeader: "300"}},
	}
	for _, tc := range cases {
		for _, endpoint := range queryEndpoints {
			t.Run(tc.name+endpoint, func(t *testing.T) {
				f := &fakeTailRouter{merged: tc.merged, cubeOnly: staleCube, served: true}
				e := fakeTailEnv(t, f)
				writeHours(e, "events", "2026-06-02", []string{"00", "01"}, 60)
				res := postEndpoint(t, e, endpoint, tailQ, tc.headers)
				if res.status != fiber.StatusOK || len(res.cols["n"]) != 1 || res.cols["n"][0].(float64) != 120 {
					t.Fatalf("status %d n=%v, want 200 n=120 from the source fallback: %s", res.status, res.cols["n"], res.raw)
				}
				if res.header.Get("X-Arc-Rollup-Fallback") != "source" || res.header.Get(partialHeader) != "" {
					t.Fatalf("fallback=%q partial=%q, want the unlabelled source answer",
						res.header.Get("X-Arc-Rollup-Fallback"), res.header.Get(partialHeader))
				}
			})
		}
	}
}

// Review 2026-11-10 N3: the merge runs on its own goroutine, outside Fiber's
// recover middleware, so a panic in it ended the process. The panic is now the
// merge's error and the request an error response (a panic is a bug, not a
// storage failure: no cube-only answer). The connection the panicking driver
// call held goes back to the pool (database/sql discards a connection whose
// Raw callback panics), and an abandoned merge that panics still ends its
// abandonment. The test binary surviving is part of the check.
func TestTailMergePanicIsAnError(t *testing.T) {
	panicOnConn := func(e *forkQueryEnv, ctx context.Context, wait <-chan struct{}) (arrowRun, error) {
		cn, err := e.h.db.DB().Conn(ctx)
		if err != nil {
			return arrowRun{}, err
		}
		_ = cn.Raw(func(any) error {
			if wait != nil {
				<-wait
			}
			panic("boom in exec")
		})
		return arrowRun{}, errors.New("unreachable")
	}

	t.Run("before the deadline", func(t *testing.T) {
		e := newForkQueryEnv(t)
		var cubeOnlyRuns atomic.Int64
		app := fiber.New()
		app.Post("/", func(c *fiber.Ctx) error {
			r := &rollupExec{tail: &tailFallback{mergedSQL: "merged", cubeOnlySQL: "cube-only", deadline: 5 * time.Second, abandon: e.h.tailAbandon}}
			_, err := runTail(r, c.Context(), "merged", func(ctx context.Context) (arrowRun, error) {
				return panicOnConn(e, ctx, nil)
			}, arrowRun.release)
			return e.h.degradeTail(c, c.Context(), r, err, func(string) error { cubeOnlyRuns.Add(1); return nil }, "test")
		})
		resp, err := app.Test(httptest.NewRequest("POST", "/", nil), 10000)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Logf("status %d: %s", resp.StatusCode, body)
		if resp.StatusCode != fiber.StatusInternalServerError || resp.Header.Get(partialHeader) != "" || cubeOnlyRuns.Load() != 0 {
			t.Fatalf("status %d, X-Arc-Partial %q, cube-only runs %d: want a 500 and no cube-only answer",
				resp.StatusCode, resp.Header.Get(partialHeader), cubeOnlyRuns.Load())
		}
		if !waitPoolIdle(e.h.db, 5*time.Second) {
			t.Fatal("the panicking merge's connection was not returned to the pool")
		}
	})

	t.Run("after the merge was abandoned", func(t *testing.T) {
		e := newForkQueryEnv(t)
		unblock := make(chan struct{})
		tf := &tailFallback{mergedSQL: "merged", cubeOnlySQL: "cube-only", deadline: 100 * time.Millisecond, abandon: e.h.tailAbandon}
		start := time.Now()
		_, err := runTail(&rollupExec{tail: tf}, context.Background(), "merged", func(ctx context.Context) (arrowRun, error) {
			return panicOnConn(e, ctx, unblock)
		}, arrowRun.release)
		if !errors.Is(err, errTailDeadline) || !tf.pending() || e.h.tailAbandon.running() != 1 {
			t.Fatalf("err %v after %v, pending %v, abandoned %d: want the merge abandoned at its deadline",
				err, time.Since(start), tf.pending(), e.h.tailAbandon.running())
		}
		close(unblock)
		for end := time.Now().Add(5 * time.Second); e.h.tailAbandon.running() != 0; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(end) {
				t.Fatalf("abandoned = %d after the merge panicked, want 0", e.h.tailAbandon.running())
			}
		}
		if !waitPoolIdle(e.h.db, 5*time.Second) {
			t.Fatal("the abandoned merge's connection was not returned to the pool")
		}
	})
}
