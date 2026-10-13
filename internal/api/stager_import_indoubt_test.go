package api

// The in-doubt handoff through the real import route: the peer stores a unit
// but its answer never reaches the terminating sender (the sender's shutdown
// budget ends first). The sender keeps the unit, recorded in doubt; when its
// container restarts on the same dirs it offers the unit again, the route
// answers 2xx for what the peer already holds, and the sender deletes its
// copy. Every row lands in the store exactly once.

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

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

var errStoreRefused = &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}

// downableStore fails every call like an unreachable object store while down.
type downableStore struct {
	storage.Backend
	down atomic.Bool
}

func (d *downableStore) Write(ctx context.Context, path string, data []byte) error {
	if d.down.Load() {
		return fmt.Errorf("put %s: %w", path, errStoreRefused)
	}
	return d.Backend.Write(ctx, path, data)
}

func (d *downableStore) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if d.down.Load() {
		return fmt.Errorf("put %s: %w", path, errStoreRefused)
	}
	return d.Backend.WriteReader(ctx, path, r, size)
}

func (d *downableStore) Exists(ctx context.Context, path string) (bool, error) {
	if d.down.Load() {
		return false, errStoreRefused
	}
	return d.Backend.Exists(ctx, path)
}

// answerLostImporter imports every unit, but holds the answer to the next one
// after holdNext is set until release closes: the sender gives up first.
type answerLostImporter struct {
	inner    StagerImporter
	holdNext atomic.Bool
	release  chan struct{}
	mu       sync.Mutex
	results  []ingest.StagerImportResult
}

func (a *answerLostImporter) ImportUnit(r io.Reader) (ingest.StagerImportResult, error) {
	res, err := a.inner.ImportUnit(r)
	if err == nil {
		a.mu.Lock()
		a.results = append(a.results, res)
		a.mu.Unlock()
	}
	if a.holdNext.CompareAndSwap(true, false) {
		<-a.release
	}
	return res, err
}

func (a *answerLostImporter) since(i int) []ingest.StagerImportResult {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]ingest.StagerImportResult(nil), a.results[i:]...)
}

// uniqueParquet returns a parquet of n rows with ids <prefix>-<i>, spread
// over the last three hours.
func uniqueParquet(t *testing.T, prefix string, n int) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "p.parquet")
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	q := fmt.Sprintf(`COPY (SELECT now()::TIMESTAMP - to_hours(i %% 3) AS time, '%s-' || i AS row_id FROM range(%d) t(i)) TO '%s' (FORMAT PARQUET)`, prefix, n, path)
	if _, err := d.Exec(q); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func countStoredRows(t *testing.T, root string) (rows, distinct int64) {
	t.Helper()
	var paths []string
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(p, ".parquet") {
			paths = append(paths, "'"+p+"'")
		}
		return nil
	})
	if len(paths) == 0 {
		return 0, 0
	}
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	q := "SELECT count(*), count(DISTINCT row_id) FROM read_parquet([" + strings.Join(paths, ",") + "], union_by_name=true)"
	if err := d.QueryRow(q).Scan(&rows, &distinct); err != nil {
		t.Fatal(err)
	}
	return rows, distinct
}

func TestStagerImportRoute_InDoubtUnitReofferedAfterSameDirsRestart(t *testing.T) {
	root := t.TempDir()
	objRoot := filepath.Join(root, "s3")
	local, err := storage.NewLocalBackend(objRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	store := &downableStore{Backend: local}
	health := func(pod string) *ingest.S3Health {
		h := ingest.NewS3Health(ingest.S3HealthConfig{Backend: store, PodID: pod, MinBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, Logger: zerolog.Nop()})
		t.Cleanup(h.Close)
		return h
	}

	// Peer B behind the real route.
	optsB := ingest.StagerOptions{PodID: "ingest-b", Health: health("ingest-b")}
	bFresh, err := ingest.NewFreshStager(&ingest.FreshStagerConfig{Storage: store, StageDir: filepath.Join(root, "b-fresh"), FlushAge: time.Hour, Logger: zerolog.Nop(), Options: optsB})
	if err != nil {
		t.Fatal(err)
	}
	bLate, err := ingest.NewLateStager(&ingest.LateStagerConfig{Storage: store, StageDir: filepath.Join(root, "b-late"), FlushAge: time.Hour, Logger: zerolog.Nop(), Options: optsB})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []interface{ SetCloseBudget(time.Duration) }{bFresh, bLate} {
		s.SetCloseBudget(time.Second)
	}
	t.Cleanup(func() { _ = bFresh.Close(); _ = bLate.Close() })
	release := make(chan struct{})
	imp := map[string]*answerLostImporter{
		"fresh": {inner: bFresh, release: release},
		"late":  {inner: bLate, release: release},
	}
	app := fiber.New(fiber.Config{BodyLimit: 64 << 20, DisableStartupMessage: true})
	RegisterStagerImportRoute(app, map[string]StagerImporter{"fresh": imp["fresh"], "late": imp["late"]}, nil, zerolog.Nop())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Listener(ln) }()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(2 * time.Second) })
	peerURL := "http://" + ln.Addr().String()

	// Pod A: a fresh merge job whose upload failed in the outage, and
	// unjobbed late files.
	aFreshDir, aLateDir := filepath.Join(root, "a-fresh"), filepath.Join(root, "a-late")
	startA := func() (*ingest.FreshStager, *ingest.LateStager) {
		opts := ingest.StagerOptions{PodID: "ingest-a", Health: health("ingest-a"), HandoffURL: peerURL}
		f, err := ingest.NewFreshStager(&ingest.FreshStagerConfig{Storage: store, StageDir: aFreshDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
		if err != nil {
			t.Fatal(err)
		}
		l, err := ingest.NewLateStager(&ingest.LateStagerConfig{Storage: store, StageDir: aLateDir, FlushAge: time.Hour, Logger: zerolog.Nop(), Options: opts})
		if err != nil {
			t.Fatal(err)
		}
		return f, l
	}
	f, l := startA()
	const nFiles, nRows = 4, 5
	for i := 0; i < nFiles; i++ {
		if err := f.Stage("db", "m", uniqueParquet(t, fmt.Sprintf("fresh%d", i), nRows)); err != nil {
			t.Fatal(err)
		}
		if err := l.Stage("db", "m", uniqueParquet(t, fmt.Sprintf("late%d", i), nRows)); err != nil {
			t.Fatal(err)
		}
	}
	store.down.Store(true)
	_ = f.Flush(context.Background()) // the job is persisted, its PUTs fail
	if jobs, _ := filepath.Glob(filepath.Join(aFreshDir, "job_*.json")); len(jobs) != 1 {
		t.Fatalf("setup: want one persisted fresh job, got %v", jobs)
	}

	// SIGTERM in the outage: B stores each unit, its answer is lost.
	imp["fresh"].holdNext.Store(true)
	imp["late"].holdNext.Store(true)
	for _, s := range []interface {
		SetCloseBudget(time.Duration)
		Close() error
	}{f, l} {
		s.SetCloseBudget(2 * time.Second)
		if err := s.Close(); !errors.Is(err, ingest.ErrStagedNotUploaded) {
			t.Fatalf("A Close() = %v, want ErrStagedNotUploaded", err)
		}
	}
	close(release)
	if p, _ := bFresh.Pending(); p != nFiles {
		t.Fatalf("B holds %d fresh files, want the job's %d", p, nFiles)
	}
	if p, _ := bLate.Pending(); p != nFiles {
		t.Fatalf("B holds %d late files, want %d", p, nFiles)
	}

	// Container restart on the same dirs, store still down.
	mark := map[string]int{"fresh": len(imp["fresh"].since(0)), "late": len(imp["late"].since(0))}
	f, l = startA()
	defer f.Close()
	defer l.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		pf, _ := f.Pending()
		pl, _ := l.Pending()
		if pf+pl == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A still holds %d fresh + %d late files 10s after the restart; its in-doubt units were not re-offered", pf, pl)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for kind, i := range mark {
		got := imp[kind].since(i)
		if len(got) != 1 || got[0].Stored != 0 || got[0].Skipped == 0 {
			t.Errorf("%s re-offer results %+v: want one unit acknowledged as already held", kind, got)
		}
	}
	if p, _ := bLate.Pending(); p != nFiles {
		t.Errorf("B holds %d late files after the re-offer, want still %d", p, nFiles)
	}

	// The store recovers: B uploads; A has nothing left.
	store.down.Store(false)
	deadline = time.Now().Add(20 * time.Second)
	for {
		for _, s := range []interface{ Flush(context.Context) error }{f, l, bFresh, bLate} {
			_ = s.Flush(context.Background())
		}
		bf, _ := bFresh.Pending()
		bl, _ := bLate.Pending()
		if bf+bl == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("B did not drain after the recovery: fresh %d late %d", bf, bl)
		}
		time.Sleep(20 * time.Millisecond)
	}
	want := int64(nFiles * nRows)
	if rows, distinct := countStoredRows(t, filepath.Join(objRoot, "db", "m")); rows != want || distinct != want {
		t.Errorf("fresh rows=%d distinct=%d, want exactly %d", rows, distinct, want)
	}
	if rows, distinct := countStoredRows(t, filepath.Join(objRoot, "db", "m_late")); rows != want || distinct != want {
		t.Errorf("late rows=%d distinct=%d, want exactly %d", rows, distinct, want)
	}
}
