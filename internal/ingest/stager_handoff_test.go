package ingest

// Pod deletion without a PVC (config-gated): when a pod shuts down while the
// object store is still unavailable, its staged work would be lost with its
// emptyDir. With ingest.stager_handoff_url set, the stager hands every unit —
// an in-progress job (job file + its sources) or unjobbed source files — to a
// live peer's POST /api/v1/internal/stager/import after the bounded final
// drain, and deletes it locally only after a 2xx. The peer stores each unit
// atomically and idempotently (a retried unit is stored once, also after the
// peer already uploaded it) and resumes imported jobs with their recorded
// keys, so the rows land exactly once when the store recovers.

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

// importPeer serves the stager import endpoint for in-process stagers and
// keeps every request body so a test can replay ("retry") it.
type importPeer struct {
	mu        sync.Mutex
	importers map[string]*stagerEngine
	bodies    []capturedUnit
	srv       *httptest.Server
}

type capturedUnit struct {
	kind string
	body []byte
}

func newImportPeer(t *testing.T, importers map[string]*stagerEngine) *importPeer {
	t.Helper()
	p := &importPeer{importers: importers}
	mux := http.NewServeMux()
	mux.HandleFunc(StagerImportPath, func(w http.ResponseWriter, r *http.Request) {
		kind := r.Header.Get(StagerKindHeader)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.bodies = append(p.bodies, capturedUnit{kind: kind, body: body})
		e := p.importers[kind]
		p.mu.Unlock()
		if e == nil {
			http.Error(w, "no stager of kind "+kind, http.StatusServiceUnavailable)
			return
		}
		res, err := e.ImportUnit(bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), StagerImportStatus(err))
			return
		}
		_ = json.NewEncoder(w).Encode(res)
	})
	p.srv = httptest.NewServer(mux)
	t.Cleanup(p.srv.Close)
	return p
}

func (p *importPeer) captured() []capturedUnit {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]capturedUnit(nil), p.bodies...)
}

func countImportedJobs(t *testing.T, dir string) int {
	t.Helper()
	entries, _ := os.ReadDir(dir)
	n := 0
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), stagerImportJobPrefix) {
			n++
		}
	}
	return n
}

func TestStagerHandoff_OutageShutdownPeerUploadsExactlyOnce(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	ob := newOutageBackend(inner)

	// Peer B: live, imports enabled.
	hB := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-b", MinBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, Logger: zerolog.Nop()})
	defer hB.Close()
	optsB := StagerOptions{PodID: "ingest-b", Health: hB}
	bFresh, err := NewFreshStager(&FreshStagerConfig{Storage: ob, StageDir: filepath.Join(root, "b-fresh"), FlushAge: time.Hour, Logger: zerolog.Nop(), Options: optsB})
	if err != nil {
		t.Fatal(err)
	}
	defer bFresh.Close()
	bLate, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, "b-late"), FlushAge: time.Hour, Logger: zerolog.Nop(), Options: optsB})
	if err != nil {
		t.Fatal(err)
	}
	defer bLate.Close()
	peer := newImportPeer(t, map[string]*stagerEngine{"fresh": bFresh.stagerEngine, "late": bLate.stagerEngine})

	// Pod A: a fresh job that uploaded 1 of its 3 outputs, unjobbed fresh
	// sources, and unjobbed late sources.
	aFreshDir, aLateDir := filepath.Join(root, "a-fresh"), filepath.Join(root, "a-late")
	nf := writeStagedBacklog(t, aFreshDir, "fresh_stage_", "db", "m", 30, 3)
	nl := writeStagedBacklog(t, aLateDir, "late_stage_", "db", "m", 15, 2)
	hA := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-a", MinBackoff: time.Hour, MaxBackoff: time.Hour, Logger: zerolog.Nop()})
	defer hA.Close()
	optsA := StagerOptions{PodID: "ingest-a", Health: hA, JobMaxFiles: 10, UploadConcurrency: 1, HandoffURL: peer.srv.URL}
	aFresh, err := NewFreshStager(&FreshStagerConfig{Storage: ob, StageDir: aFreshDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: optsA})
	if err != nil {
		t.Fatal(err)
	}
	aLate, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: aLateDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: optsA})
	if err != nil {
		t.Fatal(err)
	}
	aFresh.SetCloseBudget(20 * time.Second)
	aLate.SetCloseBudget(20 * time.Second)
	aFresh.hooks.crash = killAt(stagerPointOutputDone, 1)
	_ = aFresh.Flush(context.Background())
	aFresh.hooks.crash = nil
	jobs := readJobFiles(t, aFreshDir)
	if len(jobs) != 1 || len(jobs[0].pending()) != 2 {
		t.Fatalf("precondition: one job with 2 of 3 outputs pending, got %+v", jobs)
	}
	recorded := make([]string, 0, 3)
	for _, o := range jobs[0].Outputs {
		recorded = append(recorded, o.Key)
	}
	sort.Strings(recorded)

	// Outage, then pod A is deleted.
	ob.setDown(true)
	if err := aFresh.Close(); err != nil {
		t.Fatalf("fresh Close() = %v; every unit should have been handed to the peer", err)
	}
	if err := aLate.Close(); err != nil {
		t.Fatalf("late Close() = %v; every unit should have been handed to the peer", err)
	}
	for _, d := range []string{aFreshDir, aLateDir} {
		if left := leftoverJobState(t, d); len(left) != 0 {
			t.Errorf("%s still holds job state after the handoff: %v", d, left)
		}
	}
	if p, _ := aFresh.Pending(); p != 0 {
		t.Errorf("pod A still has %d fresh staged files after the handoff", p)
	}

	// B holds every unit.
	if got := countImportedJobs(t, filepath.Join(root, "b-fresh")); got != 1 {
		t.Fatalf("peer holds %d imported jobs, want 1", got)
	}
	if p, _ := bFresh.Pending(); p != nf {
		t.Fatalf("peer holds %d fresh staged files, want %d", p, nf)
	}
	if p, _ := bLate.Pending(); p != nl {
		t.Fatalf("peer holds %d late staged files, want %d", p, nl)
	}

	// A retry of every unit (a 2xx lost on the way back) is stored once.
	replay := func(label string) {
		for _, u := range peer.captured() {
			e := map[string]*stagerEngine{"fresh": bFresh.stagerEngine, "late": bLate.stagerEngine}[u.kind]
			res, err := e.ImportUnit(bytes.NewReader(u.body))
			if err != nil {
				t.Fatalf("%s: re-import: %v", label, err)
			}
			if res.Stored != 0 {
				t.Fatalf("%s: a re-sent unit stored %d new files/jobs: %+v", label, res.Stored, res)
			}
		}
	}
	replay("before upload")
	if p, _ := bFresh.Pending(); p != nf {
		t.Fatalf("after the retry the peer holds %d fresh staged files, want %d", p, nf)
	}

	// The store recovers: B uploads on its own (the import kicked a drain
	// that hit the outage, so B's prober brings it back), no restart.
	ob.setDown(false)
	deadline := time.Now().Add(15 * time.Second)
	for {
		pf, _ := bFresh.Pending()
		pl, _ := bLate.Pending()
		if pf == 0 && pl == 0 && countImportedJobs(t, filepath.Join(root, "b-fresh")) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer did not drain within 15s of recovery: fresh=%d late=%d", pf, pl)
		}
		time.Sleep(20 * time.Millisecond)
	}
	check := func(label string) {
		t.Helper()
		rows, distinct, _ := countRowsUnder(t, filepath.Join(objRoot, "db", "m"))
		if want := int64(nf * 3); rows != want || distinct != want {
			t.Fatalf("%s: fresh rows=%d distinct=%d, want exactly %d", label, rows, distinct, want)
		}
		rows, distinct, _ = countRowsUnder(t, filepath.Join(objRoot, "db", "m_late"))
		if want := int64(nl * 2); rows != want || distinct != want {
			t.Fatalf("%s: late rows=%d distinct=%d, want exactly %d", label, rows, distinct, want)
		}
	}
	check("after the peer's upload")
	keys := objectKeysUnder(t, objRoot)
	for _, k := range recorded {
		found := false
		for _, have := range keys {
			found = found || have == k
		}
		if !found {
			t.Errorf("recorded key %s of the handed-off job is missing: the peer must resume it with its keys", k)
		}
	}
	if p, _ := bFresh.Pending(); p != 0 {
		t.Errorf("peer still has %d fresh staged files", p)
	}
	before := len(keys)

	// A late retry after the peer already uploaded everything changes nothing.
	replay("after upload")
	_ = bFresh.Flush(context.Background())
	_ = bLate.Flush(context.Background())
	if after := len(objectKeysUnder(t, objRoot)); after != before {
		t.Errorf("a re-sent unit was uploaded again: %d objects, was %d", after, before)
	}
	check("after the late retry")
}

func TestStagerHandoff_PeerUnreachableKeepsEverything(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ob := newOutageBackend(inner)
	ob.setDown(true)
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	dir := filepath.Join(root, "late")
	n := writeStagedBacklog(t, dir, "late_stage_", "db", "m", 5, 2)
	ls, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a", HandoffURL: url}})
	if err != nil {
		t.Fatal(err)
	}
	ls.SetCloseBudget(3 * time.Second)
	if err := ls.Close(); !errors.Is(err, ErrStagedNotUploaded) {
		t.Fatalf("Close() = %v, want ErrStagedNotUploaded", err)
	}
	if p, _ := ls.Pending(); p != n {
		t.Fatalf("%d of %d staged files left after a failed handoff; nothing may be deleted without a 2xx", p, n)
	}
}

func TestStagerHandoff_BoundedByTheCloseBudget(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ob := newOutageBackend(inner)
	ob.setDown(true)
	release := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer hang.Close()
	defer close(release)
	dir := filepath.Join(root, "late")
	writeStagedBacklog(t, dir, "late_stage_", "db", "m", 3, 2)
	ls, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a", HandoffURL: hang.URL}})
	if err != nil {
		t.Fatal(err)
	}
	ls.SetCloseBudget(time.Second)
	start := time.Now()
	err = ls.Close()
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("Close took %v with a 1s budget and a hanging peer", took)
	}
	if !errors.Is(err, ErrStagedNotUploaded) {
		t.Fatalf("Close() = %v, want ErrStagedNotUploaded", err)
	}
}

func TestStagerHandoff_ImportRefusals(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewLateStager(&LateStagerConfig{Storage: inner, StageDir: filepath.Join(root, "b"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-b"}})
	if err != nil {
		t.Fatal(err)
	}
	unit := func(t *testing.T, pod, name string) []byte {
		t.Helper()
		var buf bytes.Buffer
		data := makeParquet(t, "r-%d", 2, map[string]struct{}{})
		if err := writeStagerUnit(&buf, stagerUnit{Version: stagerUnitVersion, Kind: "late", SourcePod: pod, Type: stagerUnitSources, Files: []string{name}},
			[]stagerUnitFile{{name: name, data: data, mtime: time.Now()}}); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	if _, err := b.ImportUnit(bytes.NewReader(unit(t, "ingest-b", "db__m__late_stage_1.parquet"))); !errors.Is(err, ErrStagerImportFromSelf) || StagerImportStatus(err) != http.StatusConflict {
		t.Errorf("own pod's unit: %v, want ErrStagerImportFromSelf (409)", err)
	}
	for _, bad := range []string{"../db__m__late_stage_1.parquet", "db__m__fresh_stage_1.parquet", ".db__m__late_stage_1.parquet", "db__m__late_stage_1.txt"} {
		if _, err := b.ImportUnit(bytes.NewReader(unit(t, "ingest-a", bad))); !errors.Is(err, ErrStagerImportInvalid) || StagerImportStatus(err) != http.StatusBadRequest {
			t.Errorf("file %q: %v, want ErrStagerImportInvalid (400)", bad, err)
		}
	}
	if _, err := b.ImportUnit(bytes.NewReader([]byte("not a tar stream at all, truncated"))); !errors.Is(err, ErrStagerImportInvalid) {
		t.Errorf("garbage body: %v, want ErrStagerImportInvalid", err)
	}
	if p, _ := b.Pending(); p != 0 {
		t.Fatalf("refused units left %d files", p)
	}
	res, err := b.ImportUnit(bytes.NewReader(unit(t, "ingest-a", "db__m__late_stage_7.parquet")))
	if err != nil || res.Stored != 1 {
		t.Fatalf("valid unit: %+v %v", res, err)
	}
	// Stored under its identity: source pod, sequence, content hash prefix.
	if m, _ := filepath.Glob(filepath.Join(root, "b", "db__m__late_stage_ingest-a-7-????????????????.parquet")); len(m) != 1 {
		t.Errorf("imported file is not stored under the source pod's prefix: %v", m)
	}
	if err := b.Close(); !errors.Is(err, ErrStagedNotUploaded) {
		// The store is fine, so the final drain uploads it: Close is clean.
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if _, err := b.ImportUnit(bytes.NewReader(unit(t, "ingest-a", "db__m__late_stage_8.parquet"))); !errors.Is(err, ErrStagerClosing) || StagerImportStatus(err) != http.StatusServiceUnavailable {
		t.Errorf("import into a closed stager: %v, want ErrStagerClosing (503)", err)
	}
}
