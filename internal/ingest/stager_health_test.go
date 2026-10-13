package ingest

// Live drain on recovery: when stager uploads fail because the object store is
// unavailable, the stagers stop merging (no churn while unhealthy) and an S3
// health prober checks the store with exponential backoff and jitter through
// the unwrapped backend. After 3 consecutive successful probes it signals
// healthy and the stagers drain at once, oldest first, without waiting for
// the next flush tick and without a restart.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/circuitbreaker"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// outageBackend wraps a real backend and, while down, fails every call the
// way an unreachable object store does (connection refused).
type outageBackend struct {
	storage.Backend
	down   atomic.Bool
	mu     sync.Mutex
	probes []time.Time // times of writes under _arc/health/
	puts   atomic.Int64
	// firstPutAfterUp is the time of the first successful data PUT after the
	// store came back.
	upAt            atomic.Int64
	firstPutAfterUp atomic.Int64
}

func newOutageBackend(inner storage.Backend) *outageBackend { return &outageBackend{Backend: inner} }

var errConnRefused = &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}

func (o *outageBackend) setDown(down bool) {
	if !down {
		o.upAt.Store(time.Now().UnixNano())
	}
	o.down.Store(down)
}

func (o *outageBackend) record(path string) error {
	if strings.HasPrefix(path, "_arc/health/") {
		o.mu.Lock()
		o.probes = append(o.probes, time.Now())
		o.mu.Unlock()
	}
	if o.down.Load() {
		return fmt.Errorf("put %s: %w", path, errConnRefused)
	}
	return nil
}

func (o *outageBackend) Write(ctx context.Context, path string, data []byte) error {
	if err := o.record(path); err != nil {
		return err
	}
	return o.Backend.Write(ctx, path, data)
}

func (o *outageBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if err := o.record(path); err != nil {
		return err
	}
	if err := o.Backend.WriteReader(ctx, path, r, size); err != nil {
		return err
	}
	if !strings.HasPrefix(path, "_arc/") {
		o.puts.Add(1)
		o.firstPutAfterUp.CompareAndSwap(0, time.Now().UnixNano())
	}
	return nil
}

func (o *outageBackend) Exists(ctx context.Context, path string) (bool, error) {
	if o.down.Load() {
		return false, errConnRefused
	}
	return o.Backend.Exists(ctx, path)
}

func (o *outageBackend) probeTimes() []time.Time {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]time.Time(nil), o.probes...)
}

type statusErr struct{ code int }

func (s statusErr) Error() string       { return fmt.Sprintf("api error: StatusCode: %d", s.code) }
func (s statusErr) HTTPStatusCode() int { return s.code }

func TestIsS3AvailabilityError(t *testing.T) {
	timeoutErr := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	yes := map[string]error{
		"deadline exceeded":  fmt.Errorf("upload: %w", context.DeadlineExceeded),
		"net timeout":        fmt.Errorf("upload: %w", timeoutErr),
		"connection refused": fmt.Errorf("upload: %w", errConnRefused),
		"connection reset":   fmt.Errorf("upload: %w", &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}),
		"503 slow down":      fmt.Errorf("storage write failed after 3 retries: %w", statusErr{503}),
		"500":                statusErr{500},
		"429":                statusErr{429},
		"circuit open":       fmt.Errorf("storage write: %w", circuitbreaker.ErrCircuitOpen),
		"reset (text)":       errors.New("simulated S3 outage: connection reset"),
		"eof (text)":         errors.New("Put \"https://x\": unexpected EOF"),
	}
	no := map[string]error{
		"nil":            nil,
		"canceled":       context.Canceled,
		"403":            statusErr{403},
		"404":            fmt.Errorf("x: %w", storage.ErrNotFound),
		"local file":     fmt.Errorf("open /x: %w", os.ErrNotExist),
		"generic upload": errors.New("simulated upload failure (call=2)"),
	}
	for name, err := range yes {
		if !isS3AvailabilityError(err) {
			t.Errorf("%s: %v must count as an availability error", name, err)
		}
	}
	for name, err := range no {
		if isS3AvailabilityError(err) {
			t.Errorf("%s: %v must not count as an availability error", name, err)
		}
	}
}

// TestS3Health_BackoffJitterAndRecovery: an availability failure marks the
// store unhealthy; probes back off exponentially (with jitter, capped); three
// consecutive successes mark it healthy and close the recovery channel.
func TestS3Health_BackoffJitterAndRecovery(t *testing.T) {
	inner, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ob := newOutageBackend(inner)
	ob.setDown(true)
	h := NewS3Health(S3HealthConfig{
		Backend: ob, PodID: "ingest-a", MinBackoff: 10 * time.Millisecond, MaxBackoff: 80 * time.Millisecond,
		SuccessesToRecover: 3, ProbeTimeout: time.Second, Logger: zerolog.Nop(),
	})
	defer h.Close()

	if h.ReportFailure(errors.New("simulated upload failure")) || !h.Healthy() {
		t.Fatal("a non-availability error must not mark the store unhealthy")
	}
	recovered := h.Recovered()
	if !h.ReportFailure(fmt.Errorf("put: %w", errConnRefused)) || h.Healthy() {
		t.Fatal("connection refused must mark the store unhealthy")
	}
	time.Sleep(600 * time.Millisecond)
	down := ob.probeTimes()
	if len(down) < 4 {
		t.Fatalf("only %d probes in 600ms of outage", len(down))
	}
	var gaps []time.Duration
	for i := 1; i < len(down); i++ {
		gaps = append(gaps, down[i].Sub(down[i-1]))
	}
	if gaps[len(gaps)-1] <= gaps[0] {
		t.Errorf("probe gaps do not grow: %v", gaps)
	}
	for _, g := range gaps {
		if g > 80*time.Millisecond*13/10+20*time.Millisecond {
			t.Errorf("probe gap %v exceeds the 80ms cap (+jitter)", g)
		}
	}
	select {
	case <-recovered:
		t.Fatal("recovered while the store is still down")
	default:
	}

	ob.setDown(false)
	select {
	case <-recovered:
	case <-time.After(2 * time.Second):
		t.Fatal("no recovery signal within 2s of the store coming back")
	}
	if !h.Healthy() {
		t.Fatal("recovery signalled but Healthy() is false")
	}
	if after := len(ob.probeTimes()) - len(down); after < 3 {
		t.Errorf("recovered after %d successful probes, want 3 consecutive", after)
	}
	// The next cycle gets a fresh channel.
	select {
	case <-h.Recovered():
		t.Fatal("Recovered() returned an already-closed channel while healthy")
	default:
	}
}

// TestS3Health_MetricGauge: the shared prober publishes arc_s3_healthy.
func TestS3Health_MetricGauge(t *testing.T) {
	inner, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ob := newOutageBackend(inner)
	ob.setDown(true)
	h := NewS3Health(S3HealthConfig{Backend: ob, PodID: "p", MinBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond,
		Logger: zerolog.Nop(), ReportMetric: true})
	defer h.Close()
	if !strings.Contains(metrics.Get().PrometheusFormat(), "\narc_s3_healthy 1\n") {
		t.Fatal("arc_s3_healthy must start at 1")
	}
	h.ReportFailure(errConnRefused)
	if !strings.Contains(metrics.Get().PrometheusFormat(), "\narc_s3_healthy 0\n") {
		t.Fatal("arc_s3_healthy must be 0 while unhealthy")
	}
	ob.setDown(false)
	select {
	case <-h.Recovered():
	case <-time.After(2 * time.Second):
		t.Fatal("no recovery")
	}
	if !strings.Contains(metrics.Get().PrometheusFormat(), "\narc_s3_healthy 1\n") {
		t.Fatal("arc_s3_healthy must return to 1 after recovery")
	}
}

// TestStagerLiveDrain_DrainsWithinSecondsOfRecovery: an outage parks the
// stager (no re-merge on later flush ticks), and when the store comes back
// the backlog drains on its own within seconds, with no restart and no
// manual Flush, exactly once.
func TestStagerLiveDrain_DrainsWithinSecondsOfRecovery(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	ob := newOutageBackend(inner)
	health := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-a", MinBackoff: 20 * time.Millisecond,
		MaxBackoff: 200 * time.Millisecond, Logger: zerolog.Nop()})
	defer health.Close()

	freshDir, lateDir := filepath.Join(root, "fresh"), filepath.Join(root, "late")
	nf := writeStagedBacklog(t, freshDir, "fresh_stage_", "db", "m", 40, 3)
	nl := writeStagedBacklog(t, lateDir, "late_stage_", "db", "m", 25, 2)
	opts := StagerOptions{PodID: "ingest-a", JobMaxFiles: 10, Health: health}
	// FlushAge of an hour: no tick fires during the test, so whatever drains
	// after the outage was started by the recovery signal.
	fs, err := NewFreshStager(&FreshStagerConfig{Storage: ob, StageDir: freshDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	ls, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: lateDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Close()
	var merges atomic.Int64
	count := func(context.Context, *sql.Conn) { merges.Add(1) }
	fs.hooks.onMergeConn = count
	ls.hooks.onMergeConn = count

	ob.setDown(true)
	_ = fs.Flush(context.Background()) // the first job's upload fails: unhealthy
	if health.Healthy() {
		t.Fatal("precondition: the failed upload must mark the store unhealthy")
	}
	afterFirst := merges.Load()
	for i := 0; i < 5; i++ { // flush ticks during the outage
		_ = fs.Flush(context.Background())
		_ = ls.Flush(context.Background())
	}
	if got := merges.Load(); got != afterFirst {
		t.Fatalf("CHURN: %d merges ran while the store was unhealthy (after the first failure: %d)", got, afterFirst)
	}

	ob.setDown(false)
	deadline := time.Now().Add(10 * time.Second)
	for {
		pf, _ := fs.Pending()
		pl, _ := ls.Pending()
		if pf == 0 && pl == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no drain within 10s of recovery: fresh=%d late=%d staged files left", pf, pl)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lag := time.Duration(ob.firstPutAfterUp.Load() - ob.upAt.Load()); lag > 3*time.Second {
		t.Errorf("first upload %v after recovery, want within seconds", lag)
	}
	// Both backlogs reuse the same row_id scheme, so count each tree alone.
	rows, distinct, _ := countRowsUnder(t, filepath.Join(objRoot, "db", "m"))
	if want := int64(nf * 3); rows != want || distinct != want {
		t.Fatalf("fresh rows=%d distinct=%d, want exactly %d", rows, distinct, want)
	}
	rows, distinct, _ = countRowsUnder(t, filepath.Join(objRoot, "db", "m_late"))
	if want := int64(nl * 2); rows != want || distinct != want {
		t.Fatalf("late rows=%d distinct=%d, want exactly %d", rows, distinct, want)
	}
}

// TestStagerLiveDrain_CloseProbesBeforeFinalDrain: a Close while the store is
// marked unhealthy probes once; if the store answers, the final drain runs.
func TestStagerLiveDrain_CloseProbesBeforeFinalDrain(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	ob := newOutageBackend(inner)
	health := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-a", MinBackoff: time.Hour, MaxBackoff: time.Hour, Logger: zerolog.Nop()})
	defer health.Close()
	dir := filepath.Join(root, "late")
	n := writeStagedBacklog(t, dir, "late_stage_", "db", "m", 5, 2)
	ls, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a", Health: health}})
	if err != nil {
		t.Fatal(err)
	}
	ob.setDown(true)
	_ = ls.Flush(context.Background())
	if health.Healthy() {
		t.Fatal("precondition: unhealthy")
	}
	ob.setDown(false) // back, but the prober (1h backoff) has not noticed
	if err := ls.Close(); err != nil {
		t.Fatalf("Close() = %v; the final drain should have probed and uploaded", err)
	}
	rows, distinct, _ := countRowsUnder(t, objRoot)
	if want := int64(n * 2); rows != want || distinct != want {
		t.Fatalf("rows=%d distinct=%d, want %d", rows, distinct, want)
	}
}

// stallBackend fails the first data PUT (connection refused) and holds the
// second until released; probes always succeed. The store is "back" while the
// pass that saw the failure is still blocked on the held PUT.
type stallBackend struct {
	storage.Backend
	puts    atomic.Int32
	release chan struct{}
}

func (s *stallBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if !strings.HasPrefix(path, "_arc/") {
		switch s.puts.Add(1) {
		case 1:
			return fmt.Errorf("put %s: %w", path, errConnRefused)
		case 2:
			<-s.release
		}
	}
	return s.Backend.WriteReader(ctx, path, r, size)
}

// TestStagerLiveDrain_RecoveryDuringTheFailedPass: the store recovers while
// the pass that saw it fail is still waiting on another PUT. The recovery
// signal fires before the loop is back in its select; the loop must still
// drain right away instead of waiting for the next flush tick.
func TestStagerLiveDrain_RecoveryDuringTheFailedPass(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	sb := &stallBackend{Backend: inner, release: make(chan struct{})}
	health := NewS3Health(S3HealthConfig{Backend: sb, PodID: "ingest-a", MinBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, Logger: zerolog.Nop()})
	defer health.Close()
	dir := filepath.Join(root, "fresh")
	n := writeStagedBacklog(t, dir, "fresh_stage_", "db", "m", 6, 2)
	fs, err := NewFreshStager(&FreshStagerConfig{Storage: sb, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a", Health: health, UploadConcurrency: 2}})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	fs.kick() // the loop's own pass: PUT 1 fails, PUT 2 hangs
	deadline := time.Now().Add(5 * time.Second)
	for health.Healthy() || sb.puts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("precondition: the pass should fail one PUT and hold another")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for !health.Healthy() { // the prober sees the store back
		if time.Now().After(deadline) {
			t.Fatal("prober did not recover")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(sb.release) // the stuck pass ends now, after the recovery signal
	deadline = time.Now().Add(3 * time.Second)
	for {
		if p, _ := fs.Pending(); p == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the recovery that happened during the failed pass was lost: nothing drained before the next tick")
		}
		time.Sleep(10 * time.Millisecond)
	}
	rows, distinct, _ := countRowsUnder(t, objRoot)
	if want := int64(n * 2); rows != want || distinct != want {
		t.Fatalf("rows=%d distinct=%d, want %d", rows, distinct, want)
	}
}

// otherStagerFailsBackend: during the first data PUT, the OTHER stager that
// shares the health tracker reports the store down.
type otherStagerFailsBackend struct {
	storage.Backend
	health *S3Health
	puts   atomic.Int32
}

func (o *otherStagerFailsBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if !strings.HasPrefix(path, "_arc/") && o.puts.Add(1) == 1 {
		o.health.ReportFailure(errConnRefused)
	}
	return o.Backend.WriteReader(ctx, path, r, size)
}

// TestStagerLiveDrain_PassStoppedByOtherStagerKeepsSources: a pass that stops
// launching PUTs because the shared tracker went unhealthy (the other stager
// failed) must leave its job pending, never finish it with outputs missing.
func TestStagerLiveDrain_PassStoppedByOtherStagerKeepsSources(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	health := NewS3Health(S3HealthConfig{Backend: inner, PodID: "ingest-a", MinBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, Logger: zerolog.Nop()})
	defer health.Close()
	ob := &otherStagerFailsBackend{Backend: inner, health: health}
	dir := filepath.Join(root, "fresh")
	n := writeStagedBacklog(t, dir, "fresh_stage_", "db", "m", 6, 2) // three partitions
	fs, err := NewFreshStager(&FreshStagerConfig{Storage: ob, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a", Health: health, UploadConcurrency: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	_ = fs.Flush(context.Background())
	rows, _, _ := countRowsUnder(t, objRoot)
	if p, _ := fs.Pending(); p == 0 && rows != int64(n*2) {
		t.Fatalf("LOSS: the job finished and deleted its sources with only %d of %d rows uploaded", rows, n*2)
	}
	// Once the tracker recovers, the job completes with its recorded keys.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if p, _ := fs.Pending(); p == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job did not complete after recovery")
		}
		time.Sleep(10 * time.Millisecond)
	}
	rows, distinct, _ := countRowsUnder(t, objRoot)
	if want := int64(n * 2); rows != want || distinct != want {
		t.Fatalf("rows=%d distinct=%d, want %d", rows, distinct, want)
	}
}
