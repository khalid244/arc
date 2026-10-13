package ingest

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/basekick-labs/arc/internal/circuitbreaker"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// S3Health tracks whether the object store is reachable for the ingest
// stagers.
//
// An upload that fails with an availability error (timeout, 5xx/429,
// connection refused/reset, circuit open) marks the store unhealthy. While it
// is unhealthy the stagers do not merge or upload, so an outage costs no
// CPU/IO churn, and a prober checks the store with a cheap PUT of
// _arc/health/ingest-<pod> through the UNWRAPPED backend (past the circuit
// breaker, whose open state would otherwise answer for the store), backing
// off exponentially with jitter between MinBackoff and MaxBackoff. After
// SuccessesToRecover consecutive successful probes the store is healthy again
// and Recovered() fires, which starts an immediate drain in every stager.
type S3Health struct {
	cfg      S3HealthConfig
	backend  storage.Backend
	probeKey string
	logger   zerolog.Logger

	mu        sync.Mutex
	healthy   bool
	recovered chan struct{} // closed at the next unhealthy -> healthy transition
	probing   bool
	lastErr   error

	// failures counts availability failures; a drain pass stops when it moves.
	failures atomic.Int64

	stopCh    chan struct{}
	stopOnce  sync.Once
	proberWG  sync.WaitGroup
	closedFlg atomic.Bool
}

// S3HealthConfig configures an S3Health. Zero values take the defaults.
type S3HealthConfig struct {
	// Backend is the stagers' storage backend; probes use storage.Unwrap(Backend).
	Backend storage.Backend
	// PodID names the probe object (_arc/health/ingest-<pod>).
	PodID string
	// MinBackoff / MaxBackoff bound the probe interval (default 1s / 30s).
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// SuccessesToRecover consecutive successful probes mark the store healthy
	// (default 3).
	SuccessesToRecover int
	// ProbeTimeout bounds one probe (default 10s).
	ProbeTimeout time.Duration
	Logger       zerolog.Logger
	// ReportMetric publishes arc_s3_healthy. Only the process-wide instance
	// sets it.
	ReportMetric bool
}

const (
	defaultS3HealthMinBackoff   = time.Second
	defaultS3HealthMaxBackoff   = 30 * time.Second
	defaultS3HealthSuccesses    = 3
	defaultS3HealthProbeTimeout = 10 * time.Second
)

// NewS3Health returns a tracker that starts healthy.
func NewS3Health(cfg S3HealthConfig) *S3Health {
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = defaultS3HealthMinBackoff
	}
	if cfg.MaxBackoff < cfg.MinBackoff {
		cfg.MaxBackoff = max(defaultS3HealthMaxBackoff, cfg.MinBackoff)
	}
	if cfg.SuccessesToRecover <= 0 {
		cfg.SuccessesToRecover = defaultS3HealthSuccesses
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = defaultS3HealthProbeTimeout
	}
	pod := sanitizeStagerPodID(cfg.PodID)
	if strings.TrimSpace(cfg.PodID) == "" {
		pod = sanitizeStagerPodID(defaultStagerPodID())
	}
	h := &S3Health{
		cfg:       cfg,
		probeKey:  "_arc/health/ingest-" + pod,
		logger:    cfg.Logger.With().Str("component", "s3-health").Logger(),
		healthy:   true,
		recovered: make(chan struct{}),
		stopCh:    make(chan struct{}),
	}
	if cfg.Backend != nil {
		h.backend = storage.Unwrap(cfg.Backend)
	}
	if cfg.ReportMetric {
		metrics.Get().SetS3Healthy(true)
	}
	return h
}

// Healthy reports whether stagers may merge and upload.
func (h *S3Health) Healthy() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.healthy
}

// Recovered returns a channel closed at the next transition from unhealthy to
// healthy. While healthy it is a fresh, open channel for the next cycle.
func (h *S3Health) Recovered() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.recovered
}

// failureCount is a monotonic count of reported availability failures.
func (h *S3Health) failureCount() int64 { return h.failures.Load() }

// ReportFailure records an upload error. It returns true, marks the store
// unhealthy and starts the prober when err is an availability error; any
// other error (a bad request, a local file problem) says nothing about the
// store and is ignored.
func (h *S3Health) ReportFailure(err error) bool {
	if !isS3AvailabilityError(err) {
		return false
	}
	h.failures.Add(1)
	h.mu.Lock()
	h.lastErr = err
	wasHealthy := h.healthy
	h.healthy = false
	startProber := !h.probing && !h.closedFlg.Load() && h.backend != nil
	if startProber {
		h.probing = true
	}
	h.mu.Unlock()
	if wasHealthy {
		h.logger.Error().Err(err).Str("probe_key", h.probeKey).
			Msg("object store unavailable: ingest stagers pause merging and uploading; probing for recovery")
		if h.cfg.ReportMetric {
			metrics.Get().SetS3Healthy(false)
		}
	}
	if startProber {
		h.proberWG.Add(1)
		go h.probeLoop()
	}
	return true
}

// Probe performs one probe without changing the state.
func (h *S3Health) Probe(ctx context.Context) error {
	if h.backend == nil {
		return errors.New("s3 health: no backend")
	}
	ctx, cancel := context.WithTimeout(ctx, h.cfg.ProbeTimeout)
	defer cancel()
	return h.backend.Write(ctx, h.probeKey, []byte(time.Now().UTC().Format(time.RFC3339Nano)))
}

func (h *S3Health) probeLoop() {
	defer h.proberWG.Done()
	backoff := h.cfg.MinBackoff
	successes := 0
	for {
		timer := time.NewTimer(h.jitter(backoff))
		select {
		case <-timer.C:
		case <-h.stopCh:
			timer.Stop()
			h.mu.Lock()
			h.probing = false
			h.mu.Unlock()
			return
		}
		err := h.Probe(context.Background())
		if err != nil {
			successes = 0
			backoff = min(backoff*2, h.cfg.MaxBackoff)
			h.logger.Debug().Err(err).Dur("next_backoff", backoff).Msg("object store probe failed")
			continue
		}
		successes++
		backoff = h.cfg.MinBackoff
		if successes < h.cfg.SuccessesToRecover {
			continue
		}
		h.mu.Lock()
		h.healthy = true
		h.probing = false
		close(h.recovered)
		h.recovered = make(chan struct{})
		h.mu.Unlock()
		h.logger.Info().Int("consecutive_successes", successes).
			Msg("object store reachable again: ingest stagers drain now")
		if h.cfg.ReportMetric {
			metrics.Get().SetS3Healthy(true)
		}
		return
	}
}

// jitter spreads d over [0.8d, 1.2d], clamped to [MinBackoff, MaxBackoff*1.2],
// so pods probing the same store do not move in lockstep.
func (h *S3Health) jitter(d time.Duration) time.Duration {
	j := time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
	if j < h.cfg.MinBackoff {
		j = h.cfg.MinBackoff
	}
	return j
}

// Close stops the prober.
func (h *S3Health) Close() {
	h.closedFlg.Store(true)
	h.stopOnce.Do(func() { close(h.stopCh) })
	h.proberWG.Wait()
}

// isS3AvailabilityError reports whether an upload error means the object store
// is unavailable (as opposed to a problem with this request or local file).
func isS3AvailabilityError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, circuitbreaker.ErrCircuitOpen) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	var status interface{ HTTPStatusCode() int }
	if errors.As(err, &status) {
		code := status.HTTPStatusCode()
		return code >= 500 || code == 429 || code == 408
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"connection refused", "connection reset", "broken pipe", "i/o timeout",
		"deadline exceeded", "timeout", "unexpected eof", "no such host",
		"server misbehaving", "tls handshake", "slowdown", "service unavailable",
		"internal error", "bad gateway", "gateway timeout", "circuit breaker is open",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
