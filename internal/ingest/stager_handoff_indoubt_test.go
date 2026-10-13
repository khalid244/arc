package ingest

// Handoff units whose outcome is unknown ("in doubt").
//
// A unit the peer stored but whose 2xx never reached the sender (the shutdown
// budget ended while the answer was pending, the connection was reset after
// the request was sent, the process was killed mid-request) is kept locally,
// which is right: nothing proves the peer holds it. But when the CONTAINER
// restarts on the same emptyDir, the sender used to upload its local copy
// while the peer uploaded the stored one: every row of the unit twice.
//
// The sender records each unit durably before it is sent (a marker in the
// stage dir naming its files or job, the peer URL and the source pod the peer
// keys it under) and keeps that marker whenever the outcome is unknown. On the
// next start, before anything is drained, the unit is offered to the peer
// again; the peer is idempotent (content-hash identity, .imported tombstones)
// and acknowledges a unit it holds or already uploaded, so the local copy is
// deleted. Only a definitive refusal, or a peer unreachable for the re-offer
// window, sends the unit to the local upload (counted in
// arc_stager_handoff_in_doubt_fallbacks_total). A refused or never-sent unit
// is not in doubt and behaves as before.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// inDoubtMarkerGlob matches the sender's in-doubt markers in a stage dir.
const inDoubtMarkerGlob = ".handoff_*"

func inDoubtMarkers(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, inDoubtMarkerGlob))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// keyCountingBackend counts the successful writes of every data key, so a
// test sees an object uploaded twice even when the second write carried the
// same rows to the same key.
type keyCountingBackend struct {
	storage.Backend
	mu   sync.Mutex
	puts map[string]int
}

func (k *keyCountingBackend) count(path string, err error) error {
	if err == nil && !strings.HasPrefix(path, "_arc/") {
		k.mu.Lock()
		if k.puts == nil {
			k.puts = make(map[string]int)
		}
		k.puts[path]++
		k.mu.Unlock()
	}
	return err
}

func (k *keyCountingBackend) Write(ctx context.Context, path string, data []byte) error {
	return k.count(path, k.Backend.Write(ctx, path, data))
}

func (k *keyCountingBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	return k.count(path, k.Backend.WriteReader(ctx, path, r, size))
}

func (k *keyCountingBackend) rewritten() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	var keys []string
	for key, n := range k.puts {
		if n > 1 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

// peerMode is what the test peer does with one unit.
type peerMode int

const (
	peerStore         peerMode = iota // store the unit, answer 2xx
	peerStoreNoAnswer                 // store the unit, hold the answer until the sender gives up
	peerDropNoAnswer                  // read the unit, store nothing, hold the answer
	peerRefuse                        // answer the given status, store nothing
)

type peerReply struct {
	kind            string
	n               int
	stored, skipped int
	status          int // 0: no answer reached the sender
}

// inDoubtPeer is the import endpoint of peer pod B (same semantics as the
// real route: unit -> ImportUnit -> status), with per-unit misbehaviour.
type inDoubtPeer struct {
	mu      sync.Mutex
	engines map[string]*stagerEngine
	mode    func(kind string, n int) (peerMode, int)
	gate    chan struct{} // non-nil: every unit waits for it before import
	onUnit  func(kind string, n int)
	seen    map[string]int
	replies []peerReply
	srv     *httptest.Server
}

func newInDoubtPeer(t *testing.T, engines map[string]*stagerEngine) *inDoubtPeer {
	t.Helper()
	p := &inDoubtPeer{engines: engines, seen: map[string]int{},
		mode: func(string, int) (peerMode, int) { return peerStore, 0 }}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind := r.Header.Get(StagerKindHeader)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return // the sender gave up mid-body: nothing to import
		}
		p.mu.Lock()
		p.seen[kind]++
		n := p.seen[kind]
		mode, status := p.mode(kind, n)
		e, gate, onUnit := p.engines[kind], p.gate, p.onUnit
		p.mu.Unlock()
		if gate != nil {
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
		}
		if onUnit != nil {
			onUnit(kind, n)
		}
		reply := peerReply{kind: kind, n: n}
		record := func() {
			p.mu.Lock()
			p.replies = append(p.replies, reply)
			p.mu.Unlock()
		}
		if mode == peerRefuse {
			reply.status = status
			record()
			http.Error(w, http.StatusText(status), status)
			return
		}
		var res StagerImportResult
		if mode != peerDropNoAnswer {
			if res, err = e.ImportUnit(bytes.NewReader(body)); err != nil {
				reply.status = StagerImportStatus(err)
				record()
				http.Error(w, err.Error(), reply.status)
				return
			}
		}
		reply.stored, reply.skipped = res.Stored, res.Skipped
		if mode == peerStoreNoAnswer || mode == peerDropNoAnswer {
			record()
			select { // the answer never reaches the sender
			case <-r.Context().Done():
			case <-time.After(30 * time.Second):
			}
			return
		}
		reply.status = http.StatusOK
		record()
		_ = json.NewEncoder(w).Encode(res)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *inDoubtPeer) set(f func(p *inDoubtPeer)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f(p)
}

func (p *inDoubtPeer) repliesSince(i int) []peerReply {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]peerReply(nil), p.replies[min(i, len(p.replies)):]...)
}

func (p *inDoubtPeer) replyCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.replies)
}

// inDoubtRig: pod A (sender) with a fresh merge job (1 of 3 outputs uploaded
// before the outage) and 210 unjobbed late files (a 200-file unit plus 10),
// peer pod B, one object store.
type inDoubtRig struct {
	objRoot             string
	ob                  *outageBackend
	store               *keyCountingBackend
	bFresh              *FreshStager
	bLate               *LateStager
	bFreshDir           string
	peer                *inDoubtPeer
	aFreshDir, aLateDir string
	nFresh, nLate       int
}

const inDoubtPodA = "ingest-a"

func newInDoubtRig(t *testing.T) *inDoubtRig {
	t.Helper()
	root := t.TempDir()
	r := &inDoubtRig{objRoot: filepath.Join(root, "s3")}
	inner, err := storage.NewLocalBackend(r.objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inner.Close() })
	r.ob = newOutageBackend(inner)
	r.store = &keyCountingBackend{Backend: r.ob}

	hB := NewS3Health(S3HealthConfig{Backend: r.store, PodID: "ingest-b", MinBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, Logger: zerolog.Nop()})
	t.Cleanup(hB.Close)
	optsB := StagerOptions{PodID: "ingest-b", Health: hB}
	r.bFreshDir = filepath.Join(root, "b-fresh")
	if r.bFresh, err = NewFreshStager(&FreshStagerConfig{Storage: r.store, StageDir: r.bFreshDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: optsB}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.bFresh.Close() })
	if r.bLate, err = NewLateStager(&LateStagerConfig{Storage: r.store, StageDir: filepath.Join(root, "b-late"), FlushAge: time.Hour, Logger: zerolog.Nop(), Options: optsB}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.bLate.Close() })
	// A failing test closes B with the store still down: keep that short.
	r.bFresh.SetCloseBudget(time.Second)
	r.bLate.SetCloseBudget(time.Second)
	r.peer = newInDoubtPeer(t, map[string]*stagerEngine{"fresh": r.bFresh.stagerEngine, "late": r.bLate.stagerEngine})

	r.aFreshDir, r.aLateDir = filepath.Join(root, "a-fresh"), filepath.Join(root, "a-late")
	r.nFresh = writeStagedBacklog(t, r.aFreshDir, "fresh_stage_", "db", "m", 10, 3)
	r.nLate = writeStagedBacklog(t, r.aLateDir, "late_stage_", "db", "m", 210, 1)
	return r
}

// startA starts pod A's stagers on its dirs (a container start: same pod id,
// same emptyDir).
func (r *inDoubtRig) startA(t *testing.T, reoffer time.Duration) (*FreshStager, *LateStager) {
	t.Helper()
	hA := NewS3Health(S3HealthConfig{Backend: r.store, PodID: inDoubtPodA, MinBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, Logger: zerolog.Nop()})
	t.Cleanup(hA.Close)
	opts := StagerOptions{PodID: inDoubtPodA, Health: hA, JobMaxFiles: 10, UploadConcurrency: 1,
		HandoffURL: r.peer.srv.URL, HandoffReofferWindow: reoffer}
	f, err := NewFreshStager(&FreshStagerConfig{Storage: r.store, StageDir: r.aFreshDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewLateStager(&LateStagerConfig{Storage: r.store, StageDir: r.aLateDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	return f, l
}

// shutdownAInOutage: A builds its fresh job (one output uploaded), the store
// goes down, and A shuts down with a 2s budget, handing its units to B.
func (r *inDoubtRig) shutdownAInOutage(t *testing.T) {
	t.Helper()
	f, l := r.startA(t, 0)
	f.hooks.crash = killAt(stagerPointOutputDone, 1)
	_ = f.Flush(context.Background())
	f.hooks.crash = nil
	if jobs := readJobFiles(t, r.aFreshDir); len(jobs) != 1 || len(jobs[0].pending()) != 2 {
		t.Fatalf("precondition: one fresh job with 2 of 3 outputs pending, got %+v", jobs)
	}
	r.ob.setDown(true)
	f.SetCloseBudget(2 * time.Second)
	l.SetCloseBudget(2 * time.Second)
	if err := f.Close(); !errors.Is(err, ErrStagedNotUploaded) {
		t.Fatalf("A fresh Close() = %v, want ErrStagedNotUploaded (its unit's outcome is unknown)", err)
	}
	if err := l.Close(); !errors.Is(err, ErrStagedNotUploaded) {
		t.Fatalf("A late Close() = %v, want ErrStagedNotUploaded (its unit's outcome is unknown)", err)
	}
}

func staged(t *testing.T, dir, marker string) int {
	t.Helper()
	n, err := countStagedFiles(dir, marker)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func waitUntil(t *testing.T, timeout time.Duration, what string, poke func(), cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("not within %v: %s", timeout, what)
		}
		if poke != nil {
			poke()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func flushAll(stagers ...interface{ Flush(context.Context) error }) func() {
	return func() {
		for _, s := range stagers {
			_ = s.Flush(context.Background())
		}
	}
}

// checkExactlyOnce: every row of A's backlog is in the store exactly once and
// no object was written twice.
func (r *inDoubtRig) checkExactlyOnce(t *testing.T, label string) {
	t.Helper()
	rows, distinct, _ := countRowsUnder(t, filepath.Join(r.objRoot, "db", "m"))
	if want := int64(r.nFresh * 3); rows != want || distinct != want {
		t.Errorf("%s: fresh rows=%d distinct=%d, want exactly %d", label, rows, distinct, want)
	}
	rows, distinct, _ = countRowsUnder(t, filepath.Join(r.objRoot, "db", "m_late"))
	if want := int64(r.nLate); rows != want || distinct != want {
		t.Errorf("%s: late rows=%d distinct=%d, want exactly %d", label, rows, distinct, want)
	}
	if again := r.store.rewritten(); len(again) > 0 {
		t.Errorf("%s: objects written more than once: %v", label, again)
	}
}

func fallbacks(t *testing.T, kind string) float64 {
	return max(promValue(t, `arc_stager_handoff_in_doubt_fallbacks_total{kind="`+kind+`"}`), 0)
}

// 1. B stores A's units but the answers are lost when A's budget ends. A
// keeps its copies with a durable marker; restarted on the same dirs while
// the store is still down, A offers them again, B acknowledges what it holds,
// A deletes its copies; after the recovery every row is stored exactly once.
func TestStagerHandoffInDoubt_ResponseLostThenSameDirsRestart(t *testing.T) {
	r := newInDoubtRig(t)
	var writeAhead sync.Map // kind -> marker present when B received the unit
	r.peer.set(func(p *inDoubtPeer) {
		p.mode = func(kind string, n int) (peerMode, int) {
			if n == 1 {
				return peerStoreNoAnswer, 0
			}
			return peerStore, 0
		}
		p.onUnit = func(kind string, n int) {
			if n == 1 {
				dir := map[string]string{"fresh": r.aFreshDir, "late": r.aLateDir}[kind]
				writeAhead.Store(kind, len(inDoubtMarkers(t, dir)) > 0)
			}
		}
	})
	r.shutdownAInOutage(t)

	for kind, dir := range map[string]string{"fresh": r.aFreshDir, "late": r.aLateDir} {
		if v, _ := writeAhead.Load(kind); v != true {
			t.Errorf("%s: no marker on A's disk while B was receiving the unit; a kill at that moment would leave no record", kind)
		}
		if m := inDoubtMarkers(t, dir); len(m) != 1 {
			t.Errorf("%s: %d in-doubt markers after the shutdown, want 1: %v", kind, len(m), m)
		}
	}
	if got := staged(t, r.aFreshDir, "fresh_stage_"); got != r.nFresh {
		t.Fatalf("A kept %d of %d fresh files; nothing may be deleted without a 2xx", got, r.nFresh)
	}
	if got := staged(t, r.aLateDir, "late_stage_"); got != r.nLate {
		t.Fatalf("A kept %d of %d late files; nothing may be deleted without a 2xx", got, r.nLate)
	}
	if got := countImportedJobs(t, r.bFreshDir); got != 1 {
		t.Fatalf("precondition: B holds %d imported jobs, want 1", got)
	}
	if p, _ := r.bLate.Pending(); p != stagerHandoffMaxFiles {
		t.Fatalf("precondition: B holds %d late files, want %d", p, stagerHandoffMaxFiles)
	}

	// Container restart on the same emptyDir; the store is still down.
	fbFresh, fbLate := fallbacks(t, "fresh"), fallbacks(t, "late")
	mark := r.peer.replyCount()
	r.peer.set(func(p *inDoubtPeer) {
		p.mode, p.onUnit = func(string, int) (peerMode, int) { return peerStore, 0 }, nil
	})
	f, l := r.startA(t, 0)
	defer f.Close()
	defer l.Close()
	waitUntil(t, 10*time.Second, "A re-offers its in-doubt units and deletes them once B acknowledges", nil, func() bool {
		return staged(t, r.aFreshDir, "fresh_stage_") == 0 && staged(t, r.aLateDir, "late_stage_") == r.nLate-stagerHandoffMaxFiles
	})
	for _, dir := range []string{r.aFreshDir, r.aLateDir} {
		if m := inDoubtMarkers(t, dir); len(m) != 0 {
			t.Errorf("markers left after the re-offer was acknowledged: %v", m)
		}
	}
	if left := leftoverJobState(t, r.aFreshDir); len(left) != 0 {
		t.Errorf("A still holds the handed-off job's state: %v", left)
	}
	reoffers := r.peer.repliesSince(mark)
	if len(reoffers) != 2 {
		t.Fatalf("B got %d units after A's restart, want the 2 re-offers: %+v", len(reoffers), reoffers)
	}
	for _, rep := range reoffers {
		if rep.status != http.StatusOK || rep.stored != 0 || rep.skipped == 0 {
			t.Errorf("re-offer %+v: want 200 with everything acknowledged as already held", rep)
		}
	}
	if fallbacks(t, "fresh") != fbFresh || fallbacks(t, "late") != fbLate {
		t.Errorf("a re-offer B acknowledged counted as a fallback")
	}
	if got := countImportedJobs(t, r.bFreshDir); got != 1 {
		t.Errorf("B holds %d imported jobs after the re-offer, want still 1", got)
	}
	if p, _ := r.bLate.Pending(); p != stagerHandoffMaxFiles {
		t.Errorf("B holds %d late files after the re-offer, want still %d", p, stagerHandoffMaxFiles)
	}

	// The store recovers: B uploads what it holds, A its 10 never-sent files.
	r.ob.setDown(false)
	waitUntil(t, 20*time.Second, "everything drains after the recovery", flushAll(f, l, r.bFresh, r.bLate), func() bool {
		pf, _ := f.Pending()
		pl, _ := l.Pending()
		bf, _ := r.bFresh.Pending()
		bl, _ := r.bLate.Pending()
		return pf+pl+bf+bl == 0 && countImportedJobs(t, r.bFreshDir) == 0
	})
	r.checkExactlyOnce(t, "after the recovery")
}

// 2. B already uploaded the units and deleted them (tombstones) when A comes
// back. A's re-offer is acknowledged from the tombstones; while it is pending
// A neither resumes the job nor merges the files (the store is up: a local
// upload would duplicate B's).
func TestStagerHandoffInDoubt_PeerAlreadyUploadedTombstone(t *testing.T) {
	r := newInDoubtRig(t)
	r.peer.set(func(p *inDoubtPeer) {
		p.mode = func(kind string, n int) (peerMode, int) {
			if n == 1 {
				return peerStoreNoAnswer, 0
			}
			return peerStore, 0
		}
	})
	r.shutdownAInOutage(t)
	for _, dir := range []string{r.aFreshDir, r.aLateDir} {
		if m := inDoubtMarkers(t, dir); len(m) != 1 {
			t.Fatalf("%s: %d in-doubt markers after the shutdown, want 1", dir, len(m))
		}
	}

	// B uploads and deletes everything before A restarts.
	r.ob.setDown(false)
	waitUntil(t, 20*time.Second, "B drains the imported units", flushAll(r.bFresh, r.bLate), func() bool {
		bf, _ := r.bFresh.Pending()
		bl, _ := r.bLate.Pending()
		return bf+bl == 0 && countImportedJobs(t, r.bFreshDir) == 0
	})

	gate := make(chan struct{})
	mark := r.peer.replyCount()
	r.peer.set(func(p *inDoubtPeer) {
		p.mode = func(string, int) (peerMode, int) { return peerStore, 0 }
		p.gate = gate
	})
	f, l := r.startA(t, 30*time.Second)
	defer f.Close()
	defer l.Close()
	// The re-offers wait at B. A drains whatever is not in doubt meanwhile.
	flushAll(f, l)()
	if got := staged(t, r.aLateDir, "late_stage_"); got != stagerHandoffMaxFiles {
		t.Errorf("A holds %d late files while its re-offer is pending, want the %d in doubt (the 10 never sent are uploaded)", got, stagerHandoffMaxFiles)
	}
	if got := staged(t, r.aFreshDir, "fresh_stage_"); got != r.nFresh {
		t.Errorf("A holds %d fresh files while its re-offer is pending, want the job's %d", got, r.nFresh)
	}
	if again := r.store.rewritten(); len(again) > 0 {
		t.Errorf("A resumed its in-doubt job while the re-offer was pending: %v written twice", again)
	}
	close(gate)
	waitUntil(t, 10*time.Second, "A deletes its units once B acknowledges them", flushAll(f, l), func() bool {
		pf, _ := f.Pending()
		pl, _ := l.Pending()
		return pf+pl == 0
	})
	reoffers := r.peer.repliesSince(mark)
	if len(reoffers) != 2 {
		t.Fatalf("B got %d units after A's restart, want the 2 re-offers: %+v", len(reoffers), reoffers)
	}
	for _, rep := range reoffers {
		if rep.status != http.StatusOK || rep.stored != 0 || rep.skipped == 0 {
			t.Errorf("re-offer %+v: want 200 acknowledged from B's tombstones, nothing stored again", rep)
		}
	}
	flushAll(r.bFresh, r.bLate)()
	r.checkExactlyOnce(t, "after the tombstoned re-offer")
}

// 3. The peer never stored the units and is gone when A restarts. After the
// re-offer window A uploads them itself (counted as a fallback): no loss.
func TestStagerHandoffInDoubt_PeerUnreachableAtRestartFallsBack(t *testing.T) {
	r := newInDoubtRig(t)
	r.peer.set(func(p *inDoubtPeer) {
		p.mode = func(kind string, n int) (peerMode, int) {
			if n == 1 {
				return peerDropNoAnswer, 0
			}
			return peerStore, 0
		}
	})
	r.shutdownAInOutage(t)
	for _, dir := range []string{r.aFreshDir, r.aLateDir} {
		if m := inDoubtMarkers(t, dir); len(m) != 1 {
			t.Fatalf("%s: %d in-doubt markers after the shutdown, want 1", dir, len(m))
		}
	}
	r.peer.srv.Close()
	r.ob.setDown(false)
	fbFresh, fbLate := fallbacks(t, "fresh"), fallbacks(t, "late")
	const window = 1500 * time.Millisecond
	start := time.Now()
	f, l := r.startA(t, window)
	defer f.Close()
	defer l.Close()
	time.Sleep(300 * time.Millisecond)
	flushAll(f, l)()
	if got := staged(t, r.aLateDir, "late_stage_"); got < stagerHandoffMaxFiles {
		t.Errorf("A uploaded in-doubt late files %v after start, inside the re-offer window", time.Since(start))
	}
	if got := staged(t, r.aFreshDir, "fresh_stage_"); got != r.nFresh {
		t.Errorf("A resumed its in-doubt fresh job %v after start, inside the re-offer window", time.Since(start))
	}
	waitUntil(t, 15*time.Second, "A uploads its in-doubt units itself after the re-offer window", flushAll(f, l), func() bool {
		pf, _ := f.Pending()
		pl, _ := l.Pending()
		return pf+pl == 0
	})
	if took := time.Since(start); took < window {
		t.Errorf("fell back after %v, before the %v re-offer window ended", took, window)
	}
	if got := fallbacks(t, "fresh") - fbFresh; got != 1 {
		t.Errorf("arc_stager_handoff_in_doubt_fallbacks_total{kind=fresh} +%v, want +1", got)
	}
	if got := fallbacks(t, "late") - fbLate; got != 1 {
		t.Errorf("arc_stager_handoff_in_doubt_fallbacks_total{kind=late} +%v, want +1", got)
	}
	for _, dir := range []string{r.aFreshDir, r.aLateDir} {
		if m := inDoubtMarkers(t, dir); len(m) != 0 {
			t.Errorf("markers left after the fallback: %v", m)
		}
	}
	r.checkExactlyOnce(t, "after the fallback")
}

// 4. A definitive refusal is not in doubt: no marker, the units stay local
// and upload at the next start without waiting for a re-offer.
func TestStagerHandoffInDoubt_DefinitiveRefusalIsNotInDoubt(t *testing.T) {
	for _, status := range []int{http.StatusConflict, http.StatusRequestEntityTooLarge} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			r := newInDoubtRig(t)
			r.peer.set(func(p *inDoubtPeer) {
				p.mode = func(string, int) (peerMode, int) { return peerRefuse, status }
			})
			r.shutdownAInOutage(t)
			for _, dir := range []string{r.aFreshDir, r.aLateDir} {
				if m := inDoubtMarkers(t, dir); len(m) != 0 {
					t.Errorf("a refused unit was recorded in doubt: %v", m)
				}
			}
			if got := staged(t, r.aLateDir, "late_stage_") + staged(t, r.aFreshDir, "fresh_stage_"); got != r.nLate+r.nFresh {
				t.Fatalf("A kept %d of %d files after the refusals", got, r.nLate+r.nFresh)
			}
			if bf, _ := r.bFresh.Pending(); bf != 0 {
				t.Fatalf("B stored %d fresh files although it refused", bf)
			}
			mark := r.peer.replyCount()
			r.ob.setDown(false)
			f, l := r.startA(t, 30*time.Second)
			defer f.Close()
			defer l.Close()
			waitUntil(t, 5*time.Second, "A uploads refused units at once", flushAll(f, l), func() bool {
				pf, _ := f.Pending()
				pl, _ := l.Pending()
				return pf+pl == 0
			})
			if got := r.peer.repliesSince(mark); len(got) != 0 {
				t.Errorf("A offered refused units again after the restart: %+v", got)
			}
			r.checkExactlyOnce(t, "after the refusal")
		})
	}
}

// A unit whose request body was not completely sent when the budget ended
// cannot have been stored (the peer imports only a complete unit): it is not
// in doubt.
func TestStagerHandoffInDoubt_BodyNotFullySentIsNotInDoubt(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	ob := newOutageBackend(inner)
	ob.setDown(true)
	hold := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-hold })) // never reads the unit
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(hold) })
	dir := filepath.Join(root, "late")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		if err := os.WriteFile(filepath.Join(dir, "db__m__late_stage_"+string(rune('0'+i))+".parquet"), randomParquet(t, 120000), 0600); err != nil {
			t.Fatal(err)
		}
	}
	ls, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: inDoubtPodA, HandoffURL: srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	ls.SetCloseBudget(time.Second)
	if err := ls.Close(); !errors.Is(err, ErrStagedNotUploaded) {
		t.Fatalf("Close() = %v, want ErrStagedNotUploaded", err)
	}
	if m := inDoubtMarkers(t, dir); len(m) != 0 {
		t.Errorf("a unit whose body never fully left was recorded in doubt: %v", m)
	}
	if p, _ := ls.Pending(); p != 4 {
		t.Errorf("A keeps %d of 4 files", p)
	}
}

// The sender treats an answer other than 2xx as "not stored" and keeps the
// unit. A peer that fails after storing part of a unit (a rename or a
// directory fsync failing half way) must therefore take back what it stored,
// or both pods upload those rows.
func TestStagerImport_FailedImportStoresNothing(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	ob := newOutageBackend(inner)
	ob.setDown(true)
	b, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, "b"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-b"}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.abandon()

	names := []string{"db__m__late_stage_1.parquet", "db__m__late_stage_2.parquet", "db__m__late_stage_3.parquet"}
	var files []stagerUnitFile
	for i, n := range names {
		files = append(files, stagerUnitFile{name: n, data: makeParquet(t, "f"+string(rune('a'+i))+"-%d", 2, map[string]struct{}{}), mtime: time.Now()})
	}
	var sources bytes.Buffer
	if err := writeStagerUnit(&sources, stagerUnit{Version: stagerUnitVersion, Kind: "late", SourcePod: inDoubtPodA, Type: stagerUnitSources, Files: names}, files); err != nil {
		t.Fatal(err)
	}
	job := &stagerJob{Version: stagerJobVersion, ID: inDoubtPodA + "-00112233aabbccdd", Kind: "late", Pod: inDoubtPodA, Database: "db", Measurement: "m",
		CreatedUnixNano: time.Now().UnixNano(), Sources: names, Outputs: []stagerJobOutput{{Partition: "late", Key: "db/m_late/k.parquet"}}}
	var jobUnit bytes.Buffer
	if err := writeStagerUnit(&jobUnit, stagerUnit{Version: stagerUnitVersion, Kind: "late", SourcePod: inDoubtPodA, Type: stagerUnitJob, Job: job, Files: names}, files); err != nil {
		t.Fatal(err)
	}

	errDisk := errors.New("injected disk error")
	for _, tc := range []struct {
		name   string
		unit   []byte
		failAt int
	}{
		{"sources, after the 2nd file", sources.Bytes(), 2},
		{"sources, after the last file", sources.Bytes(), 3},
		{"job, after its directory", jobUnit.Bytes(), 1},
	} {
		b.hooks.importStored = func(n int) error {
			if n == tc.failAt {
				return errDisk
			}
			return nil
		}
		if _, err := b.ImportUnit(bytes.NewReader(tc.unit)); !errors.Is(err, errDisk) || StagerImportStatus(err)/100 == 2 {
			t.Fatalf("%s: import = %v, want the injected failure (non-2xx)", tc.name, err)
		}
		if p, _ := b.Pending(); p != 0 {
			t.Errorf("%s: the failed import left %d files on the peer; the sender keeps the unit, so they would be uploaded twice", tc.name, p)
		}
		if n := countImportedJobs(t, filepath.Join(root, "b")); n != 0 {
			t.Errorf("%s: the failed import left %d imported jobs", tc.name, n)
		}
	}
	b.hooks.importStored = nil
	if res, err := b.ImportUnit(bytes.NewReader(sources.Bytes())); err != nil || res.Stored != 3 {
		t.Fatalf("retry of the sources unit: %+v %v, want all 3 stored", res, err)
	}
}
