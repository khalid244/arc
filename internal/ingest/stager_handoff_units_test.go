package ingest

// Handoff unit failures (P3-H4). One unit the peer refuses (413 over its body
// limit, 500 on a disk error, 409/503 from a pod that is going away too) is a
// failure of THAT unit: the handoff goes on with the next one, and stops only
// when the peer cannot be reached or the budget ends. Job units are kept below
// the peer's server.max_payload_size, so a full-size job is never a 413.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// deniedBackend answers 403 to every data PUT (not an availability error, so
// the tracker stays healthy and every group gets its own job).
type deniedBackend struct{ storage.Backend }

func (d *deniedBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	return fmt.Errorf("put %s: %w", path, statusErr{403})
}

// refusingPeer imports into b, except units refuse() answers with a status.
func refusingPeer(t *testing.T, b *stagerEngine, refuse func(body []byte) int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var refused atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if status := refuse(body); status != 0 {
			refused.Add(1)
			http.Error(w, http.StatusText(status), status)
			return
		}
		res, err := b.ImportUnit(bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), StagerImportStatus(err))
			return
		}
		_ = json.NewEncoder(w).Encode(res)
	}))
	t.Cleanup(srv.Close)
	return srv, &refused
}

func TestStagerHandoff_RefusedJobUnitDoesNotStopTheRest(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	ob := newOutageBackend(inner)
	hB := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-b", MinBackoff: time.Hour, MaxBackoff: time.Hour, Logger: zerolog.Nop()})
	defer hB.Close()
	b, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, "b"), FlushAge: time.Hour, Logger: zerolog.Nop(), Options: StagerOptions{PodID: "ingest-b", Health: hB}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.abandon()
	// The peer refuses every job unit as too large (413).
	srv, refused := refusingPeer(t, b.stagerEngine, func(body []byte) int {
		if bytes.Contains(body, []byte(`"type":"job"`)) {
			return http.StatusRequestEntityTooLarge
		}
		return 0
	})

	hA := NewS3Health(S3HealthConfig{Backend: ob, PodID: "ingest-a", MinBackoff: time.Hour, MaxBackoff: time.Hour, Logger: zerolog.Nop()})
	defer hA.Close()
	a, err := NewLateStager(&LateStagerConfig{Storage: ob, StageDir: filepath.Join(root, "a"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a", Health: hA, HandoffURL: srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Stage("db", "m", makeParquet(t, "j-%d", 3, map[string]struct{}{})); err != nil {
		t.Fatal(err)
	}
	ob.setDown(true)
	_ = a.Flush(context.Background()) // job persisted, PUT failed, store unhealthy
	for i := 0; i < 5; i++ {
		if err := a.Stage("db", "m", makeParquet(t, string(rune('a'+i))+"-%d", 3, map[string]struct{}{})); err != nil {
			t.Fatal(err)
		}
	}
	a.SetCloseBudget(4 * time.Second)
	start := time.Now()
	_ = a.Close()
	pa, _ := a.Pending()
	pb, _ := b.Pending()
	if pb != 5 || pa != 1 {
		t.Fatalf("A keeps %d staged files and the peer holds %d; want the 5 unjobbed files handed off and only the refused job's source kept", pa, pb)
	}
	if n := refused.Load(); n != 1 {
		t.Fatalf("the refused job unit was sent %d times; a 413 is not retried", n)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("Close took %v", took)
	}
}

func TestStagerHandoff_OneRefusedJobOthersHandedOff(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	// The peer cannot upload either (403), so what it received stays visible.
	b, err := NewLateStager(&LateStagerConfig{Storage: &deniedBackend{Backend: inner}, StageDir: filepath.Join(root, "b"), FlushAge: time.Hour, Logger: zerolog.Nop(), Options: StagerOptions{PodID: "ingest-b"}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.abandon()
	// A disk error on the peer for m1's job only.
	srv, _ := refusingPeer(t, b.stagerEngine, func(body []byte) int {
		if bytes.Contains(body, []byte(`"type":"job"`)) && bytes.Contains(body, []byte(`"measurement":"m1"`)) {
			return http.StatusInternalServerError
		}
		return 0
	})
	a, err := NewLateStager(&LateStagerConfig{Storage: &deniedBackend{Backend: inner}, StageDir: filepath.Join(root, "a"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a", HandoffURL: srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"m1", "m2"} {
		if err := a.Stage("db", m, makeParquet(t, m+"-%d", 3, map[string]struct{}{})); err != nil {
			t.Fatal(err)
		}
	}
	_ = a.Flush(context.Background()) // one job per group, both PUTs refused (403)
	a.SetCloseBudget(2 * time.Second)
	_ = a.Close()
	if n := countImportedJobs(t, filepath.Join(root, "b")); n != 1 {
		t.Fatalf("peer holds %d imported jobs, want m2's", n)
	}
	if pa, _ := a.Pending(); pa != 1 {
		t.Fatalf("A keeps %d staged files, want only m1's job source", pa)
	}
}

// randomParquet writes n rows of incompressible ids (about 70 bytes a row).
func randomParquet(t *testing.T, n int) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "r.parquet")
	d, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	q := fmt.Sprintf(`COPY (SELECT now()::TIMESTAMP AS time, md5(random()::VARCHAR) || md5(random()::VARCHAR) AS row_id FROM range(%d)) TO '%s' (FORMAT PARQUET)`, n, path)
	if _, err := d.Exec(q); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// The peer refuses bodies over its body limit (server.max_payload_size). The
// sender's jobs must fit it with their tar and manifest overhead.
func TestStagerHandoff_JobUnitsFitThePeerBodyLimit(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	// The peer cannot upload either (403), so what it received stays visible.
	b, err := NewLateStager(&LateStagerConfig{Storage: &deniedBackend{Backend: inner}, StageDir: filepath.Join(root, "b"), FlushAge: time.Hour, Logger: zerolog.Nop(), Options: StagerOptions{PodID: "ingest-b"}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.abandon()
	const jobMaxFiles = 20
	limit := handoffUnitOverhead(jobMaxFiles) + 100<<10
	var largest atomic.Int64
	srv, refused := refusingPeer(t, b.stagerEngine, func(body []byte) int {
		if n := int64(len(body)); n > largest.Load() {
			largest.Store(n)
		}
		if int64(len(body)) > limit {
			return http.StatusRequestEntityTooLarge
		}
		return 0
	})
	a, err := NewLateStager(&LateStagerConfig{Storage: &deniedBackend{Backend: inner}, StageDir: filepath.Join(root, "a"), FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{PodID: "ingest-a", HandoffURL: srv.URL, HandoffMaxUnitBytes: limit, JobMaxFiles: jobMaxFiles}})
	if err != nil {
		t.Fatal(err)
	}
	var staged int64
	files := 0
	for staged < 4*limit/3 {
		data := randomParquet(t, 700)
		if err := a.Stage("db", "m", data); err != nil {
			t.Fatal(err)
		}
		staged += int64(len(data))
		files++
	}
	for i := 0; i < files; i++ {
		_ = a.Flush(context.Background()) // jobs persist (403)
	}
	a.SetCloseBudget(4 * time.Second)
	_ = a.Close()
	pa, _ := a.Pending()
	pb, _ := b.Pending()
	if refused.Load() != 0 || pa != 0 || pb != files {
		t.Fatalf("%d unit(s) refused as too large (largest %d bytes, limit %d); A keeps %d of %d staged files, peer holds %d",
			refused.Load(), largest.Load(), limit, pa, files, pb)
	}
}

func TestStagerOptions_HandoffCapsTheJobSize(t *testing.T) {
	o := StagerOptions{HandoffURL: "http://peer:8000", HandoffMaxUnitBytes: 1 << 30, JobMaxFiles: 500}.withDefaults()
	if max := int64(1<<30) - handoffUnitOverhead(500); o.JobMaxBytes > max {
		t.Fatalf("JobMaxBytes %d: a full job unit would exceed the peer's 1GiB body limit (cap %d)", o.JobMaxBytes, max)
	}
	o = StagerOptions{HandoffURL: "http://peer:8000", HandoffMaxUnitBytes: 1 << 30, JobMaxBytes: 256 << 20}.withDefaults()
	if o.JobMaxBytes != 256<<20 {
		t.Fatalf("a job cap already below the body limit changed to %d", o.JobMaxBytes)
	}
	o = StagerOptions{HandoffMaxUnitBytes: 1 << 20}.withDefaults()
	if o.JobMaxBytes != defaultStagerJobMaxBytes {
		t.Fatalf("without a handoff peer the job cap changed to %d", o.JobMaxBytes)
	}
}
