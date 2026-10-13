package ingest

// Identity of handed-off staged files (P3-H1). The receiver de-duplicates a
// re-sent unit (a lost 2xx, a retry) by the name it stores the file under and
// by a 24h tombstone of that name. The identity must therefore be unique to
// the file's CONTENT, not just to <source pod>-<seq>: a container restart on
// the same emptyDir (same HOSTNAME), a StatefulSet pod with a new emptyDir, or
// a clock step can hand off a different file under a <pod>-<seq> the peer
// already holds or tombstoned. Acknowledging it without storing it is silent
// loss: the sender deletes its only copy after the 2xx.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// lateSourcesUnit builds a sources unit carrying one file.
func lateSourcesUnit(t *testing.T, pod, name string, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := writeStagerUnit(&buf, stagerUnit{Version: stagerUnitVersion, Kind: "late", SourcePod: pod, Type: stagerUnitSources, Files: []string{name}},
		[]stagerUnitFile{{name: name, data: data, mtime: time.Now()}}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// Two incarnations of pod "ingest-a" on the same emptyDir each stage files
// during an outage and hand them to peer B at shutdown. Every row both
// incarnations acknowledged must reach the store exactly once, also when the
// second incarnation's sequence numbers repeat the first's (clock step, or a
// seed that restarts from an emptied directory).
func TestStagerHandoff_RestartedPodWithRepeatedSeqLosesNothing(t *testing.T) {
	for _, tc := range []struct {
		name        string
		forceSeqTo0 bool
	}{
		{"natural restart", false},
		{"sequence numbers repeat", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			objRoot := filepath.Join(root, "s3")
			inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			defer inner.Close()
			ob := newOutageBackend(inner)
			ob.setDown(true)

			hB := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-b", MinBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, Logger: zerolog.Nop()})
			defer hB.Close()
			bLate, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, "b-late"), FlushAge: time.Hour, Logger: zerolog.Nop(),
				Options: StagerOptions{PodID: "ingest-b", Health: hB}})
			if err != nil {
				t.Fatal(err)
			}
			defer bLate.Close()
			peer := newImportPeer(t, map[string]*stagerEngine{"late": bLate.stagerEngine})

			aDir := filepath.Join(root, "a-late")
			expected := map[string]struct{}{}
			incarnation := func(label string) {
				hA := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-a", MinBackoff: time.Hour, MaxBackoff: time.Hour, Logger: zerolog.Nop()})
				defer hA.Close()
				a, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: aDir, FlushAge: time.Hour, Logger: zerolog.Nop(),
					Options: StagerOptions{PodID: "ingest-a", Health: hA, HandoffURL: peer.srv.URL}})
				if err != nil {
					t.Fatal(err)
				}
				if tc.forceSeqTo0 {
					a.seq.Store(0)
				}
				for i := 0; i < 3; i++ {
					if err := a.Stage("db", "m", makeParquet(t, label+"-"+string(rune('a'+i))+"-%d", 4, expected)); err != nil {
						t.Fatal(err)
					}
				}
				hA.ReportFailure(errConnRefused) // the outage was already noticed
				a.SetCloseBudget(2 * time.Second)
				if err := a.Close(); err != nil {
					t.Fatalf("%s: Close() = %v", label, err)
				}
				if p, _ := a.Pending(); p != 0 {
					t.Fatalf("%s: A still holds %d staged files after a clean Close", label, p)
				}
			}
			incarnation("first")
			incarnation("second")
			if p, _ := bLate.Pending(); p != 6 {
				t.Fatalf("peer holds %d staged files, want the 6 both incarnations handed off", p)
			}

			ob.setDown(false)
			deadline := time.Now().Add(10 * time.Second)
			for {
				if p, _ := bLate.Pending(); p == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("B did not drain")
				}
				_ = bLate.drain(context.Background(), true)
				time.Sleep(50 * time.Millisecond)
			}
			rows, distinct, _ := countRowsUnder(t, filepath.Join(objRoot, "db", "m_late"))
			if rows != int64(len(expected)) || distinct != int64(len(expected)) {
				t.Fatalf("object store holds %d rows (%d distinct); A acknowledged and handed off %d", rows, distinct, len(expected))
			}
		})
	}
}

// The peer's de-duplication of imported files is by content: a re-sent file
// (same name, same bytes) is acknowledged without a second copy, whether the
// peer still holds it or already uploaded it; a different file under a name
// the peer already holds or tombstoned is stored.
func TestStagerImport_SameNameNewContentIsStored(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	b, err := NewLateStager(&LateStagerConfig{Storage: inner, StageDir: filepath.Join(root, "b"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-b"}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	const name = "db__m__late_stage_1.parquet"
	expected := map[string]struct{}{}
	x := makeParquet(t, "x-%d", 3, expected)
	y := makeParquet(t, "y-%d", 3, expected)
	z := makeParquet(t, "z-%d", 3, expected)

	imp := func(label string, data []byte, wantStored, wantSkipped int) {
		t.Helper()
		res, err := b.ImportUnit(bytes.NewReader(lateSourcesUnit(t, "ingest-a", name, data)))
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if res.Stored != wantStored || res.Skipped != wantSkipped {
			t.Fatalf("%s: %+v, want stored=%d skipped=%d", label, res, wantStored, wantSkipped)
		}
	}
	imp("x", x, 1, 0)
	imp("y under the name x holds", y, 1, 0)
	imp("x re-sent", x, 0, 1)
	imp("y re-sent", y, 0, 1)
	if p, _ := b.Pending(); p != 2 {
		t.Fatalf("peer holds %d files, want 2", p)
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p, _ := b.Pending(); p != 0 {
		t.Fatalf("peer still holds %d files after a healthy drain", p)
	}
	imp("z under the tombstoned name", z, 1, 0)
	imp("x re-sent after its upload", x, 0, 1)
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows, distinct, _ := countRowsUnder(t, filepath.Join(objRoot, "db", "m_late"))
	if rows != int64(len(expected)) || distinct != int64(len(expected)) {
		t.Fatalf("object store holds %d rows (%d distinct), want each of the %d imported rows once", rows, distinct, len(expected))
	}
}

// A restarted stager on an emptied stage dir never hands out a sequence
// number its previous incarnation used.
func TestStagerSeq_NeverRepeatsAcrossRestarts(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	dir := filepath.Join(root, "late")
	stageOne := func() string {
		e, err := newStagerEngine(lateStagerSpec(), inner, dir, time.Hour, zerolog.Nop(), StagerOptions{PodID: "ingest-a"})
		if err != nil {
			t.Fatal(err)
		}
		p, err := e.stageFile("db", "m", []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(p); err != nil { // uploaded or handed off
			t.Fatal(err)
		}
		return strings.TrimSuffix(strings.TrimPrefix(filepath.Base(p), "db__m__late_stage_"), ".parquet")
	}
	first := stageOne()
	second := stageOne()
	if first == second {
		t.Fatalf("the restarted stager reused sequence %s", first)
	}
}

// Rolling update during an outage (P3-H3): pod 1 hands its staged files to
// pod 2, which is terminated next and hands them to pod 3, and so on to a new
// live pod. Deployment pod names are ~34 characters; the identity of a file
// must not grow with every hop, or a later peer refuses it (400) and it stays
// on a terminated pod.
func TestStagerHandoff_ThreeHopsWithDeploymentPodNames(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	ob := newOutageBackend(inner)
	ob.setDown(true)
	pods := []string{
		"hammel-arc-ingest-6d5f8b9c4d-abcde",
		"hammel-arc-ingest-6d5f8b9c4d-fghij",
		"hammel-arc-ingest-6d5f8b9c4d-klmno",
		"hammel-arc-ingest-7e6a9c0d5e-pqrst",
	}
	stagers := make([]*LateStager, len(pods))
	nextURL := ""
	for i := len(pods) - 1; i >= 0; i-- {
		h := NewS3Health(S3HealthConfig{Backend: ob, PodID: pods[i], MinBackoff: time.Hour, MaxBackoff: time.Hour, Logger: zerolog.Nop()})
		defer h.Close()
		ls, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, pods[i]), FlushAge: time.Hour, Logger: zerolog.Nop(),
			Options: StagerOptions{PodID: pods[i], Health: h, HandoffURL: nextURL}})
		if err != nil {
			t.Fatal(err)
		}
		stagers[i] = ls
		h.ReportFailure(errConnRefused)
		nextURL = newImportPeer(t, map[string]*stagerEngine{"late": ls.stagerEngine}).srv.URL
	}
	live := stagers[len(stagers)-1]
	defer live.abandon()
	expected := map[string]struct{}{}
	for i := 0; i < 3; i++ {
		if err := stagers[0].Stage("db", "m", makeParquet(t, string(rune('a'+i))+"-%d", 2, expected)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < len(pods)-1; i++ {
		stagers[i].SetCloseBudget(2 * time.Second)
		if err := stagers[i].Close(); err != nil {
			t.Fatalf("pod %d (%s) Close: %v", i+1, pods[i], err)
		}
	}
	if p, _ := live.Pending(); p != 3 {
		t.Fatalf("the live pod holds %d of the 3 files", p)
	}
	ob.setDown(false)
	if err := live.drain(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	rows, distinct, _ := countRowsUnder(t, filepath.Join(objRoot, "db", "m_late"))
	if rows != int64(len(expected)) || distinct != int64(len(expected)) {
		t.Fatalf("object store holds %d rows (%d distinct), want %d once", rows, distinct, len(expected))
	}
}

// A merge job's id is <pod>-<16 hex>. With a pod name over 46 characters it is
// longer than a pod id may be; the peer must still take the job unit.
func TestStagerHandoff_JobUnitFromALongPodName(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	inner, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	ob := newOutageBackend(inner)
	hB := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-b", MinBackoff: time.Hour, MaxBackoff: time.Hour, Logger: zerolog.Nop()})
	defer hB.Close()
	b, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, "b"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-b", Health: hB}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.abandon()
	peer := newImportPeer(t, map[string]*stagerEngine{"late": b.stagerEngine})

	longPod := "hammel-arc-ingest-statefulset-replica-name-00012" // 48 chars
	hA := NewS3Health(S3HealthConfig{Backend: ob, PodID: longPod, MinBackoff: time.Hour, MaxBackoff: time.Hour, Logger: zerolog.Nop()})
	defer hA.Close()
	a, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, "a"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: longPod, Health: hA, HandoffURL: peer.srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]struct{}{}
	if err := a.Stage("db", "m", makeParquet(t, "j-%d", 3, expected)); err != nil {
		t.Fatal(err)
	}
	ob.setDown(true)
	_ = a.Flush(context.Background()) // the job is persisted, its PUT fails
	a.mu.Lock()
	jobs, _ := a.loadJobsLocked()
	a.mu.Unlock()
	if len(jobs) != 1 || len(jobs[0].ID) <= 63 {
		t.Fatalf("setup: want one persisted job with an id over 63 chars, got %d", len(jobs))
	}
	keys := map[string]bool{}
	for _, o := range jobs[0].Outputs {
		keys[o.Key] = true
	}
	a.SetCloseBudget(2 * time.Second)
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v (the peer refused the job unit)", err)
	}
	if n := countImportedJobs(t, filepath.Join(root, "b")); n != 1 {
		t.Fatalf("peer holds %d imported jobs, want 1", n)
	}
	ob.setDown(false)
	if err := b.drain(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	rows, distinct, files := countRowsUnder(t, filepath.Join(objRoot, "db", "m_late"))
	if rows != int64(len(expected)) || distinct != int64(len(expected)) || files != len(keys) {
		t.Fatalf("object store holds %d rows (%d distinct) in %d objects, want %d rows in the job's %d recorded keys", rows, distinct, files, len(expected), len(keys))
	}
	for k := range keys {
		if _, err := os.Stat(filepath.Join(objRoot, k)); err != nil {
			t.Errorf("recorded key %s not uploaded: %v", k, err)
		}
	}
}

func TestValidStagerJobID(t *testing.T) {
	for _, id := range []string{"a", "ingest-a-0123456789abcdef", strings.Repeat("p", 63) + "-0123456789abcdef"} {
		if !validStagerJobID(id) {
			t.Errorf("%q refused", id)
		}
	}
	for _, id := range []string{"", "-a", "a-", "a/b", "../x", "a.b", "a_b", strings.Repeat("p", maxStagerJobIDLen+1)} {
		if validStagerJobID(id) {
			t.Errorf("%q accepted", id)
		}
	}
}
