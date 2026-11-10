package api

// Graceful shutdown with writes in flight, through the real HTTP server, the
// real msgpack handler and the shutdown coordinator in main.go's order
// (ready-flag-off, http-server drain, arrow-buffer close).
//
// Production 2026-10-13 (11feca96, rolling update, SIGTERM after a 10 s
// preStop): in-flight writes were answered 500 {"error":"Failed to write
// records: context canceled"} and the producer, which does not retry, dropped
// them. fasthttp's RequestCtx is the request context the handlers passed to
// the buffer, and its Done() is the SERVER's done channel, closed as the first
// step of Shutdown: every in-flight write saw context.Canceled as soon as the
// drain began. The schema-evolution loop checked ctx.Err() between flushes, so
// any write still looping when the drain began turned into a 500.

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// delayedStore is a local backend whose writes take delay (object store /
// fsynced stage latency): it keeps writes in flight long enough for the
// shutdown to overlap them.
type delayedStore struct {
	storage.Backend
	delay time.Duration
}

func (d *delayedStore) Write(ctx context.Context, path string, data []byte) error {
	select {
	case <-time.After(d.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return d.Backend.Write(ctx, path, data)
}

func (d *delayedStore) WriteReader(ctx context.Context, path string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return d.Write(ctx, path, data)
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// sparseMsgpackBatch is one posthog-shaped producer batch (see
// hammel-posthog-api write_buffer.rs): fixed columns plus a random subset of
// a 300-column property pool, all strings.
func sparseMsgpackBatch(rnd *rand.Rand, rows int, prefix string) ([]byte, []string) {
	now := time.Now().UnixMilli()
	uuids := make([]string, rows)
	cols := map[string]interface{}{}
	tcol, ucol, ecol, dcol, scol := make([]int64, rows), make([]string, rows), make([]string, rows), make([]string, rows), make([]int64, rows)
	for i := 0; i < rows; i++ {
		tcol[i] = now - int64(rnd.Intn(30_000))
		uuids[i] = fmt.Sprintf("%s-%05d", prefix, i)
		ucol[i] = uuids[i]
		ecol[i] = fmt.Sprintf("event_%d", rnd.Intn(20))
		dcol[i] = fmt.Sprintf("user-%d", rnd.Intn(50_000))
		scol[i] = int64(i)
	}
	cols["time"], cols["uuid"], cols["event"], cols["distinct_id"], cols["seq"] = tcol, ucol, ecol, dcol, scol
	for _, j := range rnd.Perm(300)[:10+rnd.Intn(70)] {
		v := make([]string, rows)
		for i := range v {
			v[i] = fmt.Sprintf("v%d", rnd.Intn(100))
		}
		cols[fmt.Sprintf("prop_%03d", j)] = v
	}
	body, err := msgpack.Marshal(map[string]interface{}{"m": "lifecycle", "columns": cols})
	if err != nil {
		panic(err)
	}
	return body, uuids
}

func storedUUIDs(t *testing.T, root string) map[string]int {
	t.Helper()
	counts := map[string]int{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		rdr, err := file.NewParquetReader(f)
		if err != nil {
			return err
		}
		defer rdr.Close()
		fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		if err != nil {
			return err
		}
		tbl, err := fr.ReadTable(context.Background())
		if err != nil {
			return err
		}
		defer tbl.Release()
		idx := tbl.Schema().FieldIndices("uuid")
		if len(idx) != 1 {
			return fmt.Errorf("%s: no uuid column", p)
		}
		for _, chunk := range tbl.Column(idx[0]).Data().Chunks() {
			a := chunk.(*array.String)
			for i := 0; i < a.Len(); i++ {
				counts[a.Value(i)]++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return counts
}

type reqOutcome struct {
	id        int64
	uuids     []string
	sentAt    time.Time
	status    int // 0 = no HTTP response
	transport error
	body      string
	lat       time.Duration
}

// TestGracefulShutdown_InFlightWritesNever500 drives 16 concurrent writers of
// sparse posthog-shaped batches into one measurement and runs the shutdown
// sequence while they are in flight.
//
// Required: no 500 caused by our own shutdown; every request that reached the
// handler gets a response, 204 or a retryable 503; every 204-acked row is
// durable (stored exactly once) after the buffer's Close.
func TestGracefulShutdown_InFlightWritesNever500(t *testing.T) {
	if testing.Short() {
		t.Skip("shutdown integration regression; skipped in -short mode")
	}
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	store := &delayedStore{Backend: backend, delay: 150 * time.Millisecond}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize:       50_000, // prod 600000, scaled like the e2e profile
		MaxBufferAgeMS:      3_000,
		Compression:         "snappy",
		ShardCount:          32,
		FlushWorkers:        16,
		FlushQueueSize:      1500,
		FlushTimeoutSeconds: 30,
		DataPageVersion:     "2.0",
	}, store, zerolog.Nop())
	buf.SetCloseBudget(30 * time.Second) // main.go: shutdown_timeout / 2

	port := freeTCPPort(t)
	srv := NewServer(&ServerConfig{Host: "127.0.0.1", Port: port, ReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second, MaxPayloadSize: 64 << 20}, zerolog.Nop())

	// Which requests reached the handler, and what they were answered.
	var reached sync.Map // id -> int status (0 while running)
	srv.GetApp().Use("/api/v1/write/msgpack", func(c *fiber.Ctx) error {
		id := c.Get("X-Test-Req")
		reached.Store(id, 0)
		err := c.Next()
		reached.Store(id, c.Response().StatusCode())
		return err
	})
	NewMsgPackHandler(zerolog.Nop(), buf, 64<<20).RegisterRoutes(srv.GetApp())
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	srv.MarkReady()
	url := fmt.Sprintf("http://127.0.0.1:%d/api/v1/write/msgpack", port)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if r, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port)); err == nil {
			r.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// main.go's order: ready-flag-off (standalone: no grace), http-server drain,
	// then the arrow-buffer component.
	coord := shutdown.New(60*time.Second, zerolog.Nop())
	coord.RegisterHook("ready-flag-off", func(context.Context) error { srv.MarkNotReady(); return nil }, 5)
	coord.RegisterHook("http-server", func(context.Context) error { return srv.Shutdown(15 * time.Second) }, shutdown.PriorityHTTPServer)
	coord.Register("arrow-buffer", buf, shutdown.PriorityBuffer)

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second}}
	var (
		stop     atomic.Bool
		nextID   atomic.Int64
		mu       sync.Mutex
		outcomes []reqOutcome
		wg       sync.WaitGroup
	)
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(w)))
			for !stop.Load() {
				id := nextID.Add(1)
				body, uuids := sparseMsgpackBatch(rnd, 200, fmt.Sprintf("r%06d", id))
				req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/msgpack")
				req.Header.Set("x-arc-database", "posthog")
				req.Header.Set("X-Test-Req", fmt.Sprint(id))
				o := reqOutcome{id: id, uuids: uuids, sentAt: time.Now()}
				resp, err := client.Do(req)
				o.lat = time.Since(o.sentAt)
				if err != nil {
					o.transport = err
				} else {
					b, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					o.status, o.body = resp.StatusCode, string(b)
				}
				mu.Lock()
				outcomes = append(outcomes, o)
				mu.Unlock()
				// A producer flushes every couple of seconds; 16 of them at
				// 100 ms between batches keep ~30k rows/s offered.
				if err != nil {
					time.Sleep(100 * time.Millisecond)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(w)
	}

	time.Sleep(1500 * time.Millisecond)
	tShutdown := time.Now()
	if err := coord.Shutdown(); err != nil {
		t.Logf("coordinator: %v", err)
	}
	shutdownTook := time.Since(tShutdown)
	stop.Store(true)
	wg.Wait()

	stored := storedUUIDs(t, dir)
	var (
		ok, s503, s500, other, transport, reachedNoAnswer, ackedMissing, dups int
		refused, cut                                                          int
		firstCut                                                              string
		lats                                                                  []time.Duration
		first500, firstNoAnswer                                               string
	)
	for _, o := range outcomes {
		lats = append(lats, o.lat)
		v, wasReached := reached.Load(fmt.Sprint(o.id))
		switch {
		case o.status == http.StatusNoContent:
			ok++
			for _, u := range o.uuids {
				switch stored[u] {
				case 0:
					ackedMissing++
				case 1:
				default:
					dups++
				}
			}
		case o.status == http.StatusServiceUnavailable:
			s503++
		case o.status == http.StatusInternalServerError:
			s500++
			if first500 == "" {
				first500 = o.body
			}
		case o.status != 0:
			other++
		default:
			transport++
			if strings.Contains(o.transport.Error(), "connection refused") {
				refused++
			} else if cut++; firstCut == "" {
				firstCut = fmt.Sprintf("sent %v after shutdown start: %v", o.sentAt.Sub(tShutdown), o.transport)
			}
			if wasReached {
				reachedNoAnswer++
				if firstNoAnswer == "" {
					firstNoAnswer = fmt.Sprintf("req %d (handler status %v, sent %v after shutdown start): %v",
						o.id, v, o.sentAt.Sub(tShutdown), o.transport)
				}
			}
		}
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	t.Logf("requests=%d 204=%d 503=%d 500=%d other=%d transport_errors=%d (reached handler: %d); shutdown took %v; "+
		"latency p50=%v p99=%v max=%v", len(outcomes), ok, s503, s500, other, transport, reachedNoAnswer, shutdownTook,
		lats[len(lats)/2], lats[len(lats)*99/100], lats[len(lats)-1])

	t.Logf("transport errors: %d connection refused (listener closed), %d on an open connection (first: %s)", refused, cut, firstCut)
	if s500 > 0 {
		t.Errorf("%d writes answered 500 during graceful shutdown; first: %s", s500, first500)
	}
	if other > 0 {
		t.Errorf("%d writes answered neither 204 nor 503", other)
	}
	if reachedNoAnswer > 0 {
		t.Errorf("%d requests reached the handler but the client got no response; first: %s", reachedNoAnswer, firstNoAnswer)
	}
	if ok == 0 {
		t.Fatal("no write was acknowledged: the test did not exercise anything")
	}
	if ackedMissing > 0 || dups > 0 {
		t.Errorf("of the rows acknowledged with 204: %d missing from storage, %d stored more than once", ackedMissing, dups)
	}
}

// TestIngestWriteContext_SurvivesServerShutdown pins the mechanism behind the
// "context canceled" 500s: fasthttp cancels c.Context() of every in-flight
// request as soon as the server's Shutdown begins. The context a write handler
// gives the buffer must not be cancelled by it.
func TestIngestWriteContext_SurvivesServerShutdown(t *testing.T) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	entered := make(chan struct{})
	type result struct{ request, write error }
	got := make(chan result, 1)
	app.Post("/w", func(c *fiber.Ctx) error {
		close(entered)
		// Wait until the server's shutdown has begun (fasthttp closes its done
		// channel first), then look at both contexts.
		deadline := time.Now().Add(5 * time.Second)
		for c.Context().Err() == nil && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		got <- result{request: c.Context().Err(), write: ingestWriteContext(c).Err()}
		return c.SendStatus(fiber.StatusNoContent)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Listener(ln) }()
	status := make(chan int, 1)
	go func() {
		resp, err := http.Post("http://"+ln.Addr().String()+"/w", "application/msgpack", strings.NewReader("x"))
		if err != nil {
			status <- 0
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	}()
	<-entered
	if err := app.ShutdownWithTimeout(10 * time.Second); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	r := <-got
	if r.request == nil {
		t.Fatal("fasthttp no longer cancels the request context at shutdown; revisit ingestWriteContext")
	}
	if r.write != nil {
		t.Fatalf("the ingest write context was cancelled by our own shutdown: %v", r.write)
	}
	if s := <-status; s != http.StatusNoContent {
		t.Fatalf("in-flight request answered %d during graceful shutdown, want 204", s)
	}
}

// TestShutdown_KeepAliveRequestAfterDrainBeginsIsAnswered: a producer keeps
// pooled keep-alive connections, and kube-proxy keeps an established
// connection pinned to its pod after the pod leaves the Service endpoints, so
// requests keep arriving on those connections after SIGTERM. fasthttp's
// Shutdown closed every idle connection at once, without a Connection: close
// first, so a request the producer sent onto one of them was cut ("connection
// closed mid-request", 110 events lost on 2026-10-13).
//
// Required: a request sent on an existing keep-alive connection after the
// drain began is answered, and the answer tells the client to close the
// connection, so it reconnects (to another pod) instead of reusing it.
func TestShutdown_KeepAliveRequestAfterDrainBeginsIsAnswered(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 50_000, MaxBufferAgeMS: 3_000, Compression: "snappy",
		ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 16, FlushTimeoutSeconds: 10, DataPageVersion: "2.0"}, backend, zerolog.Nop())
	defer buf.Close()
	port := freeTCPPort(t)
	srv := NewServer(&ServerConfig{Host: "127.0.0.1", Port: port, ReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second, MaxPayloadSize: 64 << 20}, zerolog.Nop())
	NewMsgPackHandler(zerolog.Nop(), buf, 64<<20).RegisterRoutes(srv.GetApp())
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	srv.MarkReady()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	var conn net.Conn
	for deadline := time.Now().Add(5 * time.Second); ; {
		if conn, err = net.Dial("tcp", addr); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	rnd := rand.New(rand.NewSource(1))
	send := func(prefix string) (*http.Response, error) {
		body, _ := sparseMsgpackBatch(rnd, 50, prefix)
		req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/api/v1/write/msgpack", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/msgpack")
		req.Header.Set("x-arc-database", "posthog")
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := req.Write(conn); err != nil {
			return nil, err
		}
		resp, err := http.ReadResponse(br, req)
		if err != nil {
			return nil, err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp, nil
	}
	resp, err := send("before")
	if err != nil || resp.StatusCode != http.StatusNoContent || resp.Close {
		t.Fatalf("first request on the keep-alive connection: resp=%v err=%v", resp, err)
	}

	// The http-server hook: ready flag off, then the drain.
	srv.MarkNotReady()
	done := make(chan error, 1)
	go func() { done <- srv.Shutdown(10 * time.Second) }()
	time.Sleep(300 * time.Millisecond) // the drain is under way; the connection sat idle through its start

	resp, err = send("after")
	if err != nil {
		t.Fatalf("request on an existing keep-alive connection after the drain began was not answered: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("request after the drain began answered %d, want 204", resp.StatusCode)
	}
	if !resp.Close {
		t.Error("response during the drain did not carry Connection: close; the client would reuse a connection the server is about to close")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("shutdown: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("shutdown did not finish")
	}
}
