// faultproxy is an S3 fault-injecting reverse proxy for the Arc E2E harness.
//
// It sits between Arc and a local MinIO and can simulate the failure shapes
// observed on Hetzner Object Storage (Ceph RGW): hard outages, 503 SlowDown
// storms (including RequestIds that contain "404"), connections cut
// mid-transfer, black-holed requests, and PUTs that commit server-side but
// whose response never reaches the client ("commit then timeout").
//
// One process serves several named data listeners (one per Arc role, e.g.
// ingest / compactor / query / rollup) so faults can be scoped to a role.
// A control listener exposes:
//
//	GET  /health
//	GET  /mode                 current fault config per listener
//	POST /mode                 {"scope": "all"|"<name>"|["a","b"], "mode": ..., ...}
//	GET  /stats                per-listener request counters
//	POST /stats/reset
//
// Safety: upstream must be a loopback address and must not match production
// endpoints (*your-objectstorage.com*, *.gyoom.sa). The proxy refuses to
// start otherwise.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// FaultConfig is the per-listener fault specification. Zero values mean
// "use the default" so a POST only needs the fields it cares about.
type FaultConfig struct {
	Mode      string   `json:"mode"`                 // normal|down|http503|reset_midbody|hang|commit_then_timeout|latency|flaky
	Methods   []string `json:"methods,omitempty"`    // only fault these methods (empty = all)
	PathRegex string   `json:"path_regex,omitempty"` // only fault paths matching (empty = all)
	Pct       float64  `json:"pct,omitempty"`        // % of matching requests faulted (default 100)

	// http503
	Status       int    `json:"status,omitempty"`        // default 503
	ErrorCode    string `json:"error_code,omitempty"`    // default SlowDown
	RequestID404 bool   `json:"requestid_404,omitempty"` // RequestId / x-amz-request-id contains "404"

	// latency (also used as base latency for forwarded requests in other modes)
	LatencyMs int `json:"latency_ms,omitempty"`
	JitterMs  int `json:"jitter_ms,omitempty"`

	// hang: 0 = until client gives up; >0 = then answer 504
	HangMs int `json:"hang_ms,omitempty"`

	// reset_midbody: fraction of the body delivered before the TCP reset
	CutFraction float64 `json:"cut_fraction,omitempty"` // default 0.5

	// commit_then_timeout
	EveryN      int    `json:"every_n,omitempty"`      // fault every Nth matching PUT (default 3)
	CttResponse string `json:"ctt_response,omitempty"` // 504|hang|reset (default 504)
	CttDelayMs  int    `json:"ctt_delay_ms,omitempty"` // wait before answering (default 0)
	StickyMs    int    `json:"sticky_ms,omitempty"`    // after a key is hit, every retry of that key is also commit+fail for this long

	// flaky: weights of fault modes applied to Pct% of matching requests
	Mix map[string]float64 `json:"mix,omitempty"`

	// Rules are evaluated in order; the first whose methods and path_regex
	// match the request replaces the whole config for that request. No match
	// falls back to this (parent) config. Rules cannot nest or use "down".
	Rules []FaultConfig `json:"rules,omitempty"`
}

var validModes = map[string]bool{
	"normal": true, "down": true, "http503": true, "reset_midbody": true, "hang": true,
	"commit_then_timeout": true, "latency": true, "flaky": true,
}

type event struct {
	TS       string `json:"ts"`
	Listener string `json:"listener"`
	Mode     string `json:"mode"`
	Method   string `json:"method"`
	Path     string `json:"path"`
	Detail   string `json:"detail,omitempty"`
}

type listener struct {
	name string
	addr string

	mu        sync.Mutex
	cfg       FaultConfig
	re        *regexp.Regexp
	ruleRes   []*regexp.Regexp
	ln        net.Listener
	srv       *http.Server
	conns     map[net.Conn]struct{}
	isDown    bool
	matchSeq  uint64
	sticky    map[string]time.Time
	counters  map[string]uint64
	cttKeys   []string
	startedAt time.Time
	// putOK counts 2xx PUT responses per object key (whole-object PUTs only).
	// A key stored twice is a silent overwrite unless it was a deliberate
	// retry of a failed attempt (commit_then_timeout re-commits the same key).
	putOK map[string]int
	// release is closed whenever the config changes, so requests parked in
	// "hang" are reset (like a load balancer coming back) instead of hanging on.
	release chan struct{}
}

type proxy struct {
	upstream  *url.URL
	transport *http.Transport
	listeners map[string]*listener
	order     []string
	events    *eventLog
	rng       *rand.Rand
	rngMu     sync.Mutex
}

type eventLog struct {
	mu    sync.Mutex
	f     *os.File
	lines int64
	max   int64
}

func (e *eventLog) write(ev event) {
	if e == nil || e.f == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.lines >= e.max {
		return
	}
	b, _ := json.Marshal(ev)
	e.f.Write(append(b, '\n'))
	e.lines++
}

func (p *proxy) rand() float64 {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return p.rng.Float64()
}

func (p *proxy) randInt(n int) int {
	if n <= 0 {
		return 0
	}
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return p.rng.Intn(n)
}

// ---------------------------------------------------------------- safety

var forbidden = []*regexp.Regexp{
	regexp.MustCompile(`(?i)your-objectstorage\.com`),
	regexp.MustCompile(`(?i)(^|\.)gyoom\.sa`),
}

func checkSafeUpstream(raw string) (*url.URL, error) {
	for _, re := range forbidden {
		if re.MatchString(raw) {
			return nil, fmt.Errorf("refusing production-looking upstream %q", raw)
		}
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	host := u.Hostname()
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, fmt.Errorf("resolve upstream %q: %w", host, err)
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return nil, fmt.Errorf("refusing non-loopback upstream %q (%s)", raw, ip)
		}
	}
	return u, nil
}

// ---------------------------------------------------------------- listener lifecycle

func (l *listener) count(key string) {
	l.mu.Lock()
	l.counters[key]++
	l.mu.Unlock()
}

func (l *listener) snapshot() (FaultConfig, *regexp.Regexp, []*regexp.Regexp) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cfg, l.re, l.ruleRes
}

func (l *listener) notePut(r *http.Request, status int) {
	if r.Method != http.MethodPut || status < 200 || status >= 300 {
		return
	}
	if q := r.URL.Query(); q.Has("partNumber") || q.Has("uploadId") {
		return
	}
	l.mu.Lock()
	l.putOK[r.URL.Path]++
	l.mu.Unlock()
}

func (l *listener) releaseCh() chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.release
}

func (p *proxy) startServing(l *listener) error {
	ln, err := net.Listen("tcp", l.addr)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.ln = ln
	l.isDown = false
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { p.handle(l, w, r) }),
		ConnState: func(c net.Conn, s http.ConnState) {
			l.mu.Lock()
			defer l.mu.Unlock()
			switch s {
			case http.StateNew:
				l.conns[c] = struct{}{}
			case http.StateClosed, http.StateHijacked:
				delete(l.conns, c)
			}
		},
		ReadHeaderTimeout: 60 * time.Second,
		ErrorLog:          log.New(io.Discard, "", 0),
	}
	l.srv = srv
	l.mu.Unlock()
	go func() {
		err := srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			log.Printf("[%s] serve: %v", l.name, err)
		}
	}()
	return nil
}

// goDown closes the listening socket (new connections are refused) and
// resets every open connection, mimicking an endpoint that disappeared.
func (p *proxy) goDown(l *listener) {
	l.mu.Lock()
	if l.isDown {
		l.mu.Unlock()
		return
	}
	l.isDown = true
	ln := l.ln
	conns := make([]net.Conn, 0, len(l.conns))
	for c := range l.conns {
		conns = append(conns, c)
	}
	l.mu.Unlock()
	if ln != nil {
		ln.Close()
	}
	for _, c := range conns {
		rstClose(c)
	}
}

func rstClose(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetLinger(0)
	}
	c.Close()
}

func (p *proxy) applyConfig(l *listener, cfg FaultConfig) error {
	if cfg.Mode == "" {
		cfg.Mode = "normal"
	}
	if !validModes[cfg.Mode] {
		return fmt.Errorf("unknown mode %q", cfg.Mode)
	}
	var re *regexp.Regexp
	if cfg.PathRegex != "" {
		var err error
		re, err = regexp.Compile(cfg.PathRegex)
		if err != nil {
			return fmt.Errorf("bad path_regex: %w", err)
		}
	}
	for i, m := range cfg.Methods {
		cfg.Methods[i] = strings.ToUpper(m)
	}
	var ruleRes []*regexp.Regexp
	for i := range cfg.Rules {
		sub := &cfg.Rules[i]
		if sub.Mode == "" {
			sub.Mode = "normal"
		}
		if !validModes[sub.Mode] || sub.Mode == "down" || len(sub.Rules) > 0 {
			return fmt.Errorf("rules[%d]: mode %q not allowed", i, sub.Mode)
		}
		for j, m := range sub.Methods {
			sub.Methods[j] = strings.ToUpper(m)
		}
		var rre *regexp.Regexp
		if sub.PathRegex != "" {
			var err error
			if rre, err = regexp.Compile(sub.PathRegex); err != nil {
				return fmt.Errorf("rules[%d]: bad path_regex: %w", i, err)
			}
		}
		ruleRes = append(ruleRes, rre)
	}
	l.mu.Lock()
	wasDown := l.isDown
	l.cfg = cfg
	l.re = re
	l.ruleRes = ruleRes
	l.matchSeq = 0
	l.sticky = map[string]time.Time{}
	if l.release != nil {
		close(l.release)
	}
	l.release = make(chan struct{})
	l.mu.Unlock()
	if cfg.Mode == "down" {
		p.goDown(l)
	} else if wasDown {
		// Re-listen; retry briefly in case the port is in TIME_WAIT.
		var err error
		for i := 0; i < 50; i++ {
			if err = p.startServing(l); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			return fmt.Errorf("re-listen %s: %w", l.addr, err)
		}
	}
	p.events.write(event{TS: now(), Listener: l.name, Mode: "config", Detail: mustJSON(cfg)})
	return nil
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func mustJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// ---------------------------------------------------------------- request handling

var hopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade", "Expect",
}

func (l *listener) matches(cfg FaultConfig, re *regexp.Regexp, r *http.Request) bool {
	if len(cfg.Methods) > 0 {
		ok := false
		for _, m := range cfg.Methods {
			if m == r.Method {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if re != nil && !re.MatchString(r.URL.Path) {
		return false
	}
	return true
}

func (p *proxy) handle(l *listener, w http.ResponseWriter, r *http.Request) {
	cfg, re, ruleRes := l.snapshot()
	l.count("req")
	l.count("req:" + r.Method)
	for i, rule := range cfg.Rules {
		if l.matches(rule, ruleRes[i], r) {
			l.count(fmt.Sprintf("rule:%d", i))
			cfg, re = rule, ruleRes[i]
			break
		}
	}

	mode := cfg.Mode
	if mode == "normal" || !l.matches(cfg, re, r) {
		l.count("pass")
		p.forward(l, w, r, 0, 0)
		return
	}
	pct := cfg.Pct
	if pct <= 0 {
		pct = 100
	}

	switch mode {
	case "down":
		// Race: request accepted just before the listener closed.
		l.count("fault:down")
		p.resetConn(w)
		return
	case "commit_then_timeout":
		p.commitThenTimeout(l, cfg, w, r)
		return
	case "flaky":
		if p.rand()*100 >= pct {
			l.count("pass:flaky")
			p.forward(l, w, r, cfg.LatencyMs, cfg.JitterMs)
			return
		}
		mode = p.pickMix(cfg.Mix)
	default:
		if pct < 100 && p.rand()*100 >= pct {
			l.count("pass:" + mode)
			p.forward(l, w, r, cfg.LatencyMs, cfg.JitterMs)
			return
		}
	}

	l.count("fault:" + mode)
	l.count("fault:" + mode + ":" + r.Method)
	switch mode {
	case "latency":
		p.forward(l, w, r, max(cfg.LatencyMs, 1), cfg.JitterMs)
	case "http503":
		p.sleepLatency(r.Context(), cfg.LatencyMs, cfg.JitterMs)
		drainBody(r)
		p.writeS3Error(w, r, cfg)
	case "hang":
		p.events.write(event{TS: now(), Listener: l.name, Mode: "hang", Method: r.Method, Path: r.URL.Path})
		drainBody(r)
		rel := l.releaseCh()
		var timeout <-chan time.Time
		if cfg.HangMs > 0 {
			timeout = time.After(time.Duration(cfg.HangMs) * time.Millisecond)
		}
		select {
		case <-r.Context().Done():
			return
		case <-rel:
			l.count("hang_released")
			p.resetConn(w)
			return
		case <-timeout:
			c := cfg
			c.Status, c.ErrorCode = 504, "GatewayTimeout"
			p.writeS3Error(w, r, c)
		}
	case "reset_midbody":
		p.resetMidBody(l, cfg, w, r)
	case "down":
		p.resetConn(w)
	default:
		p.forward(l, w, r, 0, 0)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (p *proxy) pickMix(mix map[string]float64) string {
	if len(mix) == 0 {
		return "http503"
	}
	keys := make([]string, 0, len(mix))
	total := 0.0
	for k, v := range mix {
		if v > 0 && validModes[k] && k != "flaky" && k != "commit_then_timeout" {
			keys = append(keys, k)
			total += v
		}
	}
	if total == 0 {
		return "http503"
	}
	sort.Strings(keys)
	x := p.rand() * total
	for _, k := range keys {
		x -= mix[k]
		if x < 0 {
			return k
		}
	}
	return keys[len(keys)-1]
}

func drainBody(r *http.Request) {
	if r.Body != nil {
		io.Copy(io.Discard, io.LimitReader(r.Body, 512<<20))
	}
}

func (p *proxy) sleepLatency(ctx context.Context, latMs, jitterMs int) {
	d := latMs
	if jitterMs > 0 {
		d += p.randInt(jitterMs)
	}
	if d <= 0 {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Duration(d) * time.Millisecond):
	}
}

func (p *proxy) requestID(want404 bool) string {
	const hex = "0123456789abcdef"
	b := make([]byte, 16)
	for i := range b {
		b[i] = hex[p.randInt(16)]
	}
	id := "tx" + string(b)
	if want404 {
		pos := 2 + p.randInt(len(b)-3)
		id = id[:pos] + "404" + id[pos+3:]
	} else {
		// Make sure a "normal" id never contains 404 by accident.
		id = strings.ReplaceAll(id, "404", "4a4")
	}
	return id + "-0066c0ffee-e2e-fsn1"
}

func (p *proxy) writeS3Error(w http.ResponseWriter, r *http.Request, cfg FaultConfig) {
	status := cfg.Status
	if status == 0 {
		status = 503
	}
	code := cfg.ErrorCode
	if code == "" {
		switch status {
		case 503:
			code = "SlowDown"
		case 504:
			code = "GatewayTimeout"
		case 500:
			code = "InternalError"
		default:
			code = "ServiceUnavailable"
		}
	}
	rid := p.requestID(cfg.RequestID404)
	w.Header().Set("x-amz-request-id", rid)
	w.Header().Set("x-amz-id-2", "e2e-host-"+rid[2:10])
	w.Header().Set("Content-Type", "application/xml")
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Error><Code>%s</Code><Message>Injected fault (%d) by arc-e2e faultproxy</Message>`+
		`<Resource>%s</Resource><RequestId>%s</RequestId><HostId>e2e</HostId></Error>`,
		code, status, r.URL.Path, rid)
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(status)
	io.WriteString(w, body)
}

func (p *proxy) resetConn(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic(http.ErrAbortHandler)
	}
	c, _, err := hj.Hijack()
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	rstClose(c)
}

func (p *proxy) buildUpstreamRequest(r *http.Request, body io.Reader) (*http.Request, error) {
	target := *p.upstream
	target.Path = r.URL.Path
	target.RawPath = r.URL.RawPath
	target.RawQuery = r.URL.RawQuery
	out, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), body)
	if err != nil {
		return nil, err
	}
	out.Header = r.Header.Clone()
	for _, h := range hopHeaders {
		out.Header.Del(h)
	}
	// SigV4 signs the Host header: forward the client's Host verbatim.
	out.Host = r.Host
	out.ContentLength = r.ContentLength
	if r.ContentLength == 0 {
		out.Body = http.NoBody
	}
	return out, nil
}

func (p *proxy) forward(l *listener, w http.ResponseWriter, r *http.Request, latMs, jitterMs int) {
	p.sleepLatency(r.Context(), latMs, jitterMs)
	out, err := p.buildUpstreamRequest(r, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	resp, err := p.transport.RoundTrip(out)
	if err != nil {
		l.count("upstream_error")
		if r.Context().Err() != nil {
			return
		}
		c := FaultConfig{Status: 502, ErrorCode: "BadGateway"}
		p.writeS3Error(w, r, c)
		return
	}
	defer resp.Body.Close()
	l.notePut(r, resp.StatusCode)
	copyResponse(w, resp, -1)
}

func copyResponse(w http.ResponseWriter, resp *http.Response, limit int64) int64 {
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	for _, h := range hopHeaders {
		w.Header().Del(h)
	}
	w.WriteHeader(resp.StatusCode)
	var src io.Reader = resp.Body
	if limit >= 0 {
		src = io.LimitReader(resp.Body, limit)
	}
	n, _ := io.Copy(flushWriter{w}, src)
	return n
}

type flushWriter struct{ w http.ResponseWriter }

func (f flushWriter) Write(b []byte) (int, error) {
	n, err := f.w.Write(b)
	if fl, ok := f.w.(http.Flusher); ok {
		fl.Flush()
	}
	return n, err
}

// resetMidBody cuts the transfer part-way and resets the TCP connection.
// For requests with a body (PUT/POST) the upload is truncated and never
// reaches MinIO. For GETs the response is truncated after cut_fraction.
func (p *proxy) resetMidBody(l *listener, cfg FaultConfig, w http.ResponseWriter, r *http.Request) {
	frac := cfg.CutFraction
	if frac <= 0 || frac >= 1 {
		frac = 0.5
	}
	p.events.write(event{TS: now(), Listener: l.name, Mode: "reset_midbody", Method: r.Method, Path: r.URL.Path})
	if r.Method == http.MethodPut || r.Method == http.MethodPost {
		if r.ContentLength > 0 {
			io.CopyN(io.Discard, r.Body, int64(float64(r.ContentLength)*frac))
		}
		p.resetConn(w)
		return
	}
	out, err := p.buildUpstreamRequest(r, r.Body)
	if err != nil {
		p.resetConn(w)
		return
	}
	resp, err := p.transport.RoundTrip(out)
	if err != nil {
		p.resetConn(w)
		return
	}
	defer resp.Body.Close()
	if r.Method == http.MethodHead || resp.ContentLength <= 0 {
		// Nothing to truncate: drop the connection before any response.
		p.resetConn(w)
		return
	}
	cut := int64(float64(resp.ContentLength) * frac)
	copyResponse(w, resp, cut)
	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
	p.resetConn(w)
}

// commitThenTimeout forwards the PUT so MinIO stores the object, then
// withholds the success: 504 after a delay, hang until the client gives up,
// or a TCP reset. With sticky_ms, every retry of the same key in the window
// gets the same treatment (each retry also commits).
func (p *proxy) commitThenTimeout(l *listener, cfg FaultConfig, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		l.count("pass:ctt_nonput")
		p.forward(l, w, r, cfg.LatencyMs, cfg.JitterMs)
		return
	}
	everyN := cfg.EveryN
	if everyN <= 0 {
		everyN = 3
	}
	key := r.URL.Path
	// Multipart part uploads carry partNumber; treat the whole object as the key.
	l.mu.Lock()
	hit := false
	if exp, ok := l.sticky[key]; ok && time.Now().Before(exp) {
		hit = true
	} else {
		l.matchSeq++
		if l.matchSeq%uint64(everyN) == 0 {
			hit = true
			if cfg.StickyMs > 0 {
				l.sticky[key] = time.Now().Add(time.Duration(cfg.StickyMs) * time.Millisecond)
			}
		}
	}
	l.mu.Unlock()
	if !hit {
		l.count("pass:ctt")
		p.forward(l, w, r, cfg.LatencyMs, cfg.JitterMs)
		return
	}

	out, err := p.buildUpstreamRequest(r, r.Body)
	if err != nil {
		p.resetConn(w)
		return
	}
	// Detach from the client context: the commit must complete even if
	// the client gives up while we are uploading.
	out = out.WithContext(context.Background())
	resp, err := p.transport.RoundTrip(out)
	upstreamStatus := 0
	if err == nil {
		upstreamStatus = resp.StatusCode
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		l.notePut(r, upstreamStatus)
	}
	l.count("fault:commit_then_timeout")
	if upstreamStatus >= 200 && upstreamStatus < 300 {
		l.count("ctt:committed")
		l.mu.Lock()
		if len(l.cttKeys) < 5000 {
			l.cttKeys = append(l.cttKeys, key)
		}
		l.mu.Unlock()
	}
	resp2 := cfg.CttResponse
	if resp2 == "" {
		resp2 = "504"
	}
	p.events.write(event{TS: now(), Listener: l.name, Mode: "commit_then_timeout", Method: r.Method, Path: key,
		Detail: fmt.Sprintf("upstream_status=%d response=%s delay_ms=%d", upstreamStatus, resp2, cfg.CttDelayMs)})

	if cfg.CttDelayMs > 0 {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Duration(cfg.CttDelayMs) * time.Millisecond):
		}
	}
	switch resp2 {
	case "hang":
		select {
		case <-r.Context().Done():
		case <-l.releaseCh():
			p.resetConn(w)
		}
	case "reset":
		p.resetConn(w)
	default:
		c := cfg
		c.Status, c.ErrorCode = 504, "GatewayTimeout"
		p.writeS3Error(w, r, c)
	}
}

// ---------------------------------------------------------------- control API

type modeRequest struct {
	Scope json.RawMessage `json:"scope"`
	FaultConfig
}

func (p *proxy) resolveScope(raw json.RawMessage) ([]*listener, error) {
	if len(raw) == 0 {
		return p.all(), nil
	}
	var one string
	if err := json.Unmarshal(raw, &one); err == nil {
		if one == "all" || one == "" {
			return p.all(), nil
		}
		l, ok := p.listeners[one]
		if !ok {
			return nil, fmt.Errorf("unknown listener %q", one)
		}
		return []*listener{l}, nil
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err != nil {
		return nil, fmt.Errorf("scope must be a string or list of strings")
	}
	var out []*listener
	for _, n := range many {
		l, ok := p.listeners[n]
		if !ok {
			return nil, fmt.Errorf("unknown listener %q", n)
		}
		out = append(out, l)
	}
	return out, nil
}

func (p *proxy) all() []*listener {
	out := make([]*listener, 0, len(p.order))
	for _, n := range p.order {
		out = append(out, p.listeners[n])
	}
	return out
}

func (p *proxy) controlHandler() http.Handler {
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, code int, v interface{}) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(v)
	}
	modes := func() map[string]interface{} {
		out := map[string]interface{}{}
		for _, l := range p.all() {
			l.mu.Lock()
			out[l.name] = map[string]interface{}{"addr": l.addr, "config": l.cfg, "down": l.isDown}
			l.mu.Unlock()
		}
		return out
	}
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/mode", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			writeJSON(w, 200, modes())
			return
		}
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "GET or POST"})
			return
		}
		var req modeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		ls, err := p.resolveScope(req.Scope)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		for _, l := range ls {
			cfg := req.FaultConfig
			cfg.Methods = append([]string(nil), req.Methods...)
			if err := p.applyConfig(l, cfg); err != nil {
				writeJSON(w, 400, map[string]string{"error": err.Error()})
				return
			}
		}
		writeJSON(w, 200, modes())
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		out := map[string]interface{}{}
		for _, l := range p.all() {
			l.mu.Lock()
			c := map[string]uint64{}
			for k, v := range l.counters {
				c[k] = v
			}
			keys := append(make([]string, 0, len(l.cttKeys)), l.cttKeys...)
			over := map[string]int{}
			for k, n := range l.putOK {
				if n > 1 && len(over) < 2000 {
					over[k] = n
				}
			}
			out[l.name] = map[string]interface{}{
				"mode": l.cfg.Mode, "down": l.isDown, "counters": c, "ctt_committed_keys": keys,
				"distinct_keys_put": len(l.putOK), "keys_stored_more_than_once": over,
			}
			l.mu.Unlock()
		}
		writeJSON(w, 200, out)
	})
	mux.HandleFunc("/stats/reset", func(w http.ResponseWriter, r *http.Request) {
		for _, l := range p.all() {
			l.mu.Lock()
			l.counters = map[string]uint64{}
			l.cttKeys = nil
			l.putOK = map[string]int{}
			l.mu.Unlock()
		}
		writeJSON(w, 200, map[string]string{"status": "reset"})
	})
	return mux
}

// ---------------------------------------------------------------- main

type listenFlags []string

func (f *listenFlags) String() string     { return strings.Join(*f, ",") }
func (f *listenFlags) Set(v string) error { *f = append(*f, v); return nil }

func main() {
	var listens listenFlags
	upstream := flag.String("upstream", "http://127.0.0.1:29000", "MinIO endpoint (must be loopback)")
	control := flag.String("control", "127.0.0.1:29019", "control API listen address")
	eventsPath := flag.String("events", "", "JSONL fault event log path (optional)")
	seed := flag.Int64("seed", 0, "RNG seed (0 = time-based)")
	flag.Var(&listens, "listen", "name=addr data listener (repeatable), e.g. ingest=127.0.0.1:29010")
	flag.Parse()

	up, err := checkSafeUpstream(*upstream)
	if err != nil {
		log.Fatalf("faultproxy: %v", err)
	}
	if len(listens) == 0 {
		listens = append(listens, "default=127.0.0.1:29010")
	}
	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	p := &proxy{
		upstream: up,
		transport: &http.Transport{
			Proxy:               nil,
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
		listeners: map[string]*listener{},
		rng:       rand.New(rand.NewSource(*seed)),
	}
	if *eventsPath != "" {
		f, err := os.OpenFile(*eventsPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatalf("events: %v", err)
		}
		p.events = &eventLog{f: f, max: 500000}
	}
	for _, spec := range listens {
		name, addr, ok := strings.Cut(spec, "=")
		if !ok {
			log.Fatalf("bad -listen %q (want name=addr)", spec)
		}
		host, _, err := net.SplitHostPort(addr)
		if err != nil || (host != "127.0.0.1" && host != "localhost" && host != "::1") {
			log.Fatalf("listener %q must bind loopback, got %q", name, addr)
		}
		l := &listener{name: name, addr: addr, conns: map[net.Conn]struct{}{}, sticky: map[string]time.Time{},
			counters: map[string]uint64{}, cfg: FaultConfig{Mode: "normal"}, startedAt: time.Now(),
			release: make(chan struct{}), putOK: map[string]int{}}
		p.listeners[name] = l
		p.order = append(p.order, name)
		if err := p.startServing(l); err != nil {
			log.Fatalf("listen %s: %v", addr, err)
		}
		log.Printf("faultproxy: %s listening on %s -> %s", name, addr, up)
	}
	ctrl := &http.Server{Addr: *control, Handler: p.controlHandler()}
	go func() {
		if err := ctrl.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("control: %v", err)
		}
	}()
	log.Printf("faultproxy: control API on %s", *control)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("faultproxy: shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctrl.Shutdown(ctx)
	for _, l := range p.all() {
		l.mu.Lock()
		if l.srv != nil {
			l.srv.Close()
		}
		l.mu.Unlock()
	}
}
