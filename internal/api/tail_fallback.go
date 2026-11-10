package api

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/metrics"
)

// Opt-in cube-only answer for an AUTO-mode rollup merge (review 2026-11-10,
// suggestion 9). A merge reads the sealed days from the cube in well under a
// second and today's newest hours (the tail) from raw files; when object
// storage is slow the tail times out or fails and the panel shows nothing. A
// client that can label a partial answer asks for the cube part instead:
//
//	X-Arc-Tail-Fallback: cube-only   enables it (AUTO mode; Arrow IPC and the
//	                                 JSON endpoint)
//	X-Arc-Tail-Deadline-Ms: <int>    the merge's deadline in milliseconds,
//	                                 default 10000; the query timeout still
//	                                 bounds the whole query
//
// When the merge fails (a storage or listing error, a raw file still missing
// after the retry, the deadline), the query is answered from the cube (and the
// head patch, a partial first bucket read from source) without the tail: 200
// with
//
//	X-Arc-Partial: tail-missing; from=<RFC3339 UTC>; to=<RFC3339 UTC>
//
// naming the window [from, to) that answer is missing. A merge stuck in a
// storage request the store never answers is left running one second past
// the deadline; once query.tail_abandoned_max of them are still running, new
// requests skip the merge and are answered cube-only at once (runMerge,
// tailAbandoner). A cube failure is still an error. Without the request
// header nothing changes.
const (
	tailFallbackHeader  = "X-Arc-Tail-Fallback"
	tailDeadlineHeader  = "X-Arc-Tail-Deadline-Ms"
	partialHeader       = "X-Arc-Partial"
	defaultTailDeadline = 10 * time.Second
	maxTailDeadline     = 24 * time.Hour

	// tailAbandonGrace is how long a merge past its deadline gets to notice
	// the interrupt before it is abandoned. DuckDB checks it between httpfs
	// requests, so a slow but answering store ends the run within about one
	// request; a request the store never answers runs to http_timeout x
	// (http_retries + 1), minutes in production.
	tailAbandonGrace = time.Second
	// DefaultTailAbandonedMax is query.tail_abandoned_max's default.
	DefaultTailAbandonedMax = 4
)

var (
	errTailDeadline = errors.New("rollup fresh tail deadline exceeded")
	errTailSkipped  = errors.New("rollup fresh tail not run: query.tail_abandoned_max merges are still running past their deadline")
	errTailPanic    = errors.New("rollup fresh-tail merge panicked")
)

// rollupTailRouter is the optional RollupRouter extension behind
// X-Arc-Tail-Fallback (rollup's RouteTailHTTP, matched structurally): the
// AUTO decision plus the served merge's cube-only answer and the window only
// its tail reads. cubeOnly is "" when there is no tail. served=false with
// cubeOnly set: the merge was declined only because its tail could not be
// planned (its listing or the compaction manifests failed).
type rollupTailRouter interface {
	RouteTailHTTP(sql, headerDB string) (rewritten string, served bool, cube, cubeOnly string, tailLo, tailHi time.Time)
}

// tailFallback is one tail-fallback query's plan and state. The executor
// runs every query of a request on its own goroutine, one after another.
type tailFallback struct {
	mergedSQL   string // the served merge; "" when the query is answered cube-only from the start
	cubeOnlySQL string
	from, to    time.Time
	deadline    time.Duration
	expires     time.Time // the merge's deadline, fixed at its first run
	// failed: the merge's latest run failed in a way the cube-only answer may
	// recover (tailMayRecover). tried: the cube-only answer was run for it.
	failed, tried bool
	// cubeOnly: the latest successful run was the cube-only answer.
	cubeOnly bool
	abandon  *tailAbandoner
}

// tailAbandoner bounds one QueryHandler's tail-fallback merges (runMerge). A
// merge still running tailAbandonGrace past its deadline is abandoned: its
// query is answered without it, and it holds its pooled DuckDB connection
// until its stalled httpfs request gives up, minutes in production. Once max
// abandoned merges are still running, storage is stalled: no merge starts and
// each tail-fallback query is answered cube-only at once (review 2026-11-10
// N1). Merges already running when the cap fills are abandoned past it, since
// waiting would not free their connections any sooner. Every merge holds one
// of slots, half the pool, from its start until it ends, abandoned or not, so
// merges stalled together never take the connections the cube-only answers
// and other queries need. max 0: nothing is abandoned, skipped or limited. A
// nil tailAbandoner abandons nothing.
type tailAbandoner struct {
	logger  zerolog.Logger
	mu      sync.Mutex
	max     int // query.tail_abandoned_max, at most slots
	slots   int // 0: unbounded pool
	n       int // abandoned merges still running
	merging int
	changed chan struct{} // closed when a merge ends or is abandoned
}

func newTailAbandoner(logger zerolog.Logger, want, pool int) *tailAbandoner {
	return &tailAbandoner{logger: logger, max: tailAbandonCap(want, pool), slots: pool / 2}
}

// tailAbandonCap is want, lowered to half the connection pool (pool 0:
// unbounded pool).
func tailAbandonCap(want, pool int) int {
	if want < 0 {
		want = 0
	}
	if pool > 0 && want > pool/2 {
		return pool / 2
	}
	return want
}

func (a *tailAbandoner) log() zerolog.Logger {
	if a == nil {
		return zerolog.Nop()
	}
	return a.logger
}

func (a *tailAbandoner) fullLocked() bool { return a.max > 0 && a.n >= a.max }

func (a *tailAbandoner) broadcastLocked() {
	if a.changed != nil {
		close(a.changed)
		a.changed = nil
	}
}

// skip reports whether t's merge must not start because the abandoned merges
// are at the cap, and counts and logs the skip.
func (a *tailAbandoner) skip(t *tailFallback) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	full, n, limit := a.fullLocked(), a.n, a.max
	a.mu.Unlock()
	if full {
		a.skipped(t, n, limit)
	}
	return full
}

func (a *tailAbandoner) skipped(t *tailFallback, n, limit int) {
	metrics.Get().IncQueryTailMergeSkipped()
	a.logger.Warn().Int("abandoned", n).Int("tail_abandoned_max", limit).Time("from", t.from).Time("to", t.to).
		Msg("Rollup: query.tail_abandoned_max fresh-tail merges are still running past their deadline (storage stalled); not starting this one, answering from the cube only (X-Arc-Partial)")
}

// start takes a merge slot for t, waiting for one while ctx lasts. It fails
// with errTailSkipped when the abandoned merges are at the cap: the check and
// the slot are taken under one lock, so no merge starts once the cap is full.
func (a *tailAbandoner) start(ctx context.Context, t *tailFallback) error {
	if a == nil {
		return nil
	}
	for {
		a.mu.Lock()
		if a.fullLocked() {
			n, limit := a.n, a.max
			a.mu.Unlock()
			a.skipped(t, n, limit)
			return errTailSkipped
		}
		if a.max == 0 || a.slots == 0 || a.merging < a.slots {
			a.merging++
			a.mu.Unlock()
			return nil
		}
		if a.changed == nil {
			a.changed = make(chan struct{})
		}
		changed := a.changed
		a.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("%w: waiting for a merge slot: %w", errTailDeadline, ctx.Err())
		}
	}
}

// end gives back the slot of a merge that ended, abandoned or not.
func (a *tailAbandoner) end() {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.merging--
	a.broadcastLocked()
	a.mu.Unlock()
}

// abandon counts a merge left running past its deadline; ok is false when
// abandoning is off (max 0).
func (a *tailAbandoner) abandon() (n, limit int, ok bool) {
	if a == nil {
		return 0, 0, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.max == 0 {
		return a.n, a.max, false
	}
	a.n++
	a.broadcastLocked()
	return a.n, a.max, true
}

// release counts an abandoned merge that ended.
func (a *tailAbandoner) release() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n--
	return a.n
}

func (a *tailAbandoner) running() int {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.n
}

// SetTailAbandonedMax sets query.tail_abandoned_max: how many tail-fallback
// merges left running past their deadline stop new merges from starting (0:
// none is abandoned; the response then waits for the merge to end). Lowered
// to half the DuckDB connection pool.
func (h *QueryHandler) SetTailAbandonedMax(n int) {
	pool := 0
	if h.db != nil {
		pool = h.db.Stats().MaxOpenConnections
	}
	effective := tailAbandonCap(n, pool)
	if effective != n {
		h.logger.Warn().Int("tail_abandoned_max", n).Int("max_connections", pool).Int("effective", effective).
			Msg("query.tail_abandoned_max lowered to half the DuckDB connection pool")
	}
	if h.tailAbandon == nil {
		h.tailAbandon = newTailAbandoner(h.logger, n, pool)
		return
	}
	h.tailAbandon.mu.Lock()
	h.tailAbandon.max = effective
	h.tailAbandon.slots = pool / 2
	h.tailAbandon.broadcastLocked()
	h.tailAbandon.mu.Unlock()
}

func tailFallbackRequested(c *fiber.Ctx) bool {
	return strings.EqualFold(strings.TrimSpace(c.Get(tailFallbackHeader)), "cube-only")
}

// tailDeadline is X-Arc-Tail-Deadline-Ms; a missing, malformed or
// non-positive value is the default.
func tailDeadline(c *fiber.Ctx) time.Duration {
	ms, err := strconv.ParseInt(strings.TrimSpace(c.Get(tailDeadlineHeader)), 10, 64)
	if err != nil || ms <= 0 {
		return defaultTailDeadline
	}
	if ms > maxTailDeadline.Milliseconds() {
		return maxTailDeadline
	}
	return time.Duration(ms) * time.Millisecond
}

func (t *tailFallback) partialValue() string {
	return "tail-missing; from=" + t.from.UTC().Format(time.RFC3339Nano) + "; to=" + t.to.UTC().Format(time.RFC3339Nano)
}

// pending reports whether the merge failed and only the cube-only answer is
// left to try. The source fallback stays out of its way meanwhile: it would
// re-read the failing tail and the rest of the window from source.
func (t *tailFallback) pending() bool { return t != nil && t.failed && !t.tried }

// routeAuto is the AUTO-mode rollup decision. With X-Arc-Tail-Fallback:
// cube-only on an executor that honours it (degradable), it also plans the
// cube-only answer; a merge declined only because its tail could not be
// planned, or whose merge must not start because query.tail_abandoned_max
// merges are stalled, is then served cube-only at once.
func (h *QueryHandler) routeAuto(c *fiber.Ctx, sql, headerDB string, degradable bool) (rewritten string, served bool, cube string, tail *tailFallback) {
	tr, ok := h.rollupRouter.(rollupTailRouter)
	if !ok || !degradable || !tailFallbackRequested(c) {
		rewritten, served, cube = h.rollupRouter.RouteHTTP(sql, headerDB)
		return rewritten, served, cube, nil
	}
	rewritten, served, cube, cubeOnly, lo, hi := tr.RouteTailHTTP(sql, headerDB)
	if cubeOnly == "" {
		return rewritten, served, cube, nil
	}
	tail = &tailFallback{mergedSQL: rewritten, cubeOnlySQL: cubeOnly, from: lo, to: hi, deadline: tailDeadline(c), abandon: h.tailAbandon}
	if !served {
		h.logger.Warn().Str("cube", cube).Time("from", lo).Time("to", hi).
			Msg("Rollup: fresh tail could not be planned (storage listing or compaction manifests failed); serving the cube-only answer (X-Arc-Partial)")
		tail.mergedSQL = ""
		return cubeOnly, true, cube, tail
	}
	if tail.abandon.skip(tail) {
		tail.mergedSQL = ""
		return cubeOnly, true, cube, tail
	}
	return rewritten, true, cube, tail
}

// tailMayRecover reports whether a failed merge may be answered from the cube
// alone: not schema drift (the drift fill answers it in full), not a missing
// cube object (the cube-only answer reads it too; the source fallback
// recovers it) and not a panic (a bug, answered with an error as Fiber's
// recover middleware would).
func tailMayRecover(err error) bool {
	if errors.Is(err, errTailPanic) {
		return false
	}
	if _, drift := missingColumnFromError(err); drift {
		return false
	}
	return !((isStaleCubeFileError(err) || isNoFilesFoundError(err)) && strings.Contains(err.Error(), "_arc/"))
}

// runTail runs q through exec. A run of a tail-fallback query's merge gets
// the tail deadline (runMerge). exec returns what it built (a reader and the
// connection it holds) instead of setting the caller's variables: a merge
// abandoned past its deadline still runs on another goroutine, and release
// frees what it builds there.
func runTail[T any](r *rollupExec, ctx context.Context, q string, exec func(context.Context) (T, error), release func(T)) (T, error) {
	var t *tailFallback
	if r != nil {
		t = r.tail
	}
	if t == nil || t.mergedSQL == "" || q != t.mergedSQL {
		v, err := exec(ctx)
		if t != nil {
			t.cubeOnly = err == nil && q == t.cubeOnlySQL
		}
		return v, err
	}
	if t.expires.IsZero() {
		t.expires = time.Now().Add(t.deadline)
	}
	v, err := runMerge(ctx, t, exec, release)
	t.failed = err != nil && ctx.Err() == nil && tailMayRecover(err)
	t.cubeOnly = false
	return v, err
}

type tailRun[T any] struct {
	v   T
	err error
}

// runMerge runs the merge, cancelling exec's context at the tail deadline.
// That context is also the reader's, so the timer stops as soon as exec
// returns; a deadline that fires as exec returns still fails the run, and
// release frees what exec built.
//
// The cancel only sets DuckDB's interrupt flag, which httpfs checks between
// requests: a request the store accepted and never answers holds exec until
// http_timeout x (http_retries + 1) (review 2026-11-10 Q2). So a merge still
// running tailAbandonGrace after the deadline is abandoned: the caller gets
// errTailDeadline at once, and the merge's goroutine releases what it built
// when it ends. The merge waits for a slot and is not started at all once the
// abandoned merges are at the cap (tailAbandoner).
func runMerge[T any](ctx context.Context, t *tailFallback, exec func(context.Context) (T, error), release func(T)) (T, error) {
	var zero T
	wait := time.Until(t.expires)
	if wait <= 0 {
		return zero, errTailDeadline
	}
	runCtx, cancel := context.WithCancel(ctx)
	expired := make(chan struct{})
	timer := time.AfterFunc(wait, func() {
		cancel()
		close(expired)
	})
	a := t.abandon
	if err := a.start(runCtx, t); err != nil {
		timer.Stop()
		cancel()
		return zero, err
	}
	logger := a.log()
	done := make(chan tailRun[T], 1)
	go func() {
		defer a.end()
		// Fiber's recover middleware covers only the request goroutine.
		defer func() {
			if p := recover(); p != nil {
				done <- tailRun[T]{err: tailPanic(logger, p)}
			}
		}()
		v, err := exec(runCtx)
		done <- tailRun[T]{v, err}
	}()
	select {
	case res := <-done:
		if timer.Stop() {
			return res.v, res.err
		}
		return zero, pastTailDeadline(res, release)
	case <-expired:
	}
	grace := time.NewTimer(tailAbandonGrace)
	defer grace.Stop()
	select {
	case res := <-done:
		return zero, pastTailDeadline(res, release)
	case <-grace.C:
	}
	n, limit, ok := a.abandon()
	if !ok {
		return zero, pastTailDeadline(<-done, release)
	}
	metrics.Get().IncQueryTailMergeAbandoned()
	logger.Warn().Int("abandoned", n).Int("tail_abandoned_max", limit).Dur("deadline", t.deadline).Time("from", t.from).Time("to", t.to).
		Msg("Rollup: fresh tail still running past its deadline (stalled storage request); answering without it and releasing its connection when it ends")
	abandoned := time.Now()
	go func() {
		res := <-done
		defer func() {
			if p := recover(); p != nil {
				res.err = tailPanic(logger, p)
			}
			left := a.release()
			logger.Info().Err(res.err).Dur("ran_after_abandon", time.Since(abandoned)).Int("abandoned", left).
				Msg("Rollup: abandoned fresh-tail merge ended; connection released")
		}()
		if res.err == nil {
			release(res.v)
		}
	}()
	return zero, errTailDeadline
}

// tailPanic is a panic on a merge goroutine as an error, logged with its
// stack (review 2026-11-10 N3).
func tailPanic(logger zerolog.Logger, p any) error {
	err := fmt.Errorf("%w: %v", errTailPanic, p)
	logger.Error().Err(err).Str("stack", string(debug.Stack())).Msg("Rollup: fresh-tail merge panicked")
	return err
}

func pastTailDeadline[T any](res tailRun[T], release func(T)) error {
	if res.err != nil {
		return fmt.Errorf("%w: %w", errTailDeadline, res.err)
	}
	release(res.v)
	return errTailDeadline
}

// degradeTail answers a tail-fallback query from the cube when its merge
// failed (see tailFallback), and labels a cube-only answer with X-Arc-Partial.
// A failing cube-only answer is returned as the error: a cube failure is
// still an error. When it failed in a way the source fallback recovers (its
// cube object is gone: a stale manifest pointer), the query takes the source
// fallback, as it does without the header (review 2026-11-10 Q5).
func (h *QueryHandler) degradeTail(c *fiber.Ctx, ctx context.Context, rollup *rollupExec, err error, run func(string) error, logPrefix string) error {
	if rollup == nil || rollup.tail == nil {
		return err
	}
	t := rollup.tail
	if err != nil && t.pending() && ctx.Err() == nil {
		t.tried = true
		if cubeErr := run(t.cubeOnlySQL); cubeErr != nil {
			if shouldSourceFallback(cubeErr, t.cubeOnlySQL, rollup.sourceFallback(ctx, cubeErr), ctx.Err()) {
				h.logger.Warn().Err(cubeErr).AnErr("tail_error", err).
					Msg(logPrefix + ": rollup fresh tail failed and the cube-only answer failed recoverably (stale cube pointer or empty source partition); falling back to source scan")
				c.Set("X-Arc-Rollup-Fallback", "source")
				return run(rollup.sourceFallbackSQL)
			}
			h.logger.Warn().Err(cubeErr).AnErr("tail_error", err).
				Msg(logPrefix + ": rollup fresh tail failed and the cube-only answer failed too")
			return cubeErr
		}
		h.logger.Warn().Err(err).Time("from", t.from).Time("to", t.to).Bool("deadline", errors.Is(err, errTailDeadline)).
			Msg(logPrefix + ": rollup fresh tail failed; answering from the cube only (X-Arc-Partial)")
		err = nil
	}
	if err == nil && t.cubeOnly {
		c.Set(partialHeader, t.partialValue())
	}
	return err
}
