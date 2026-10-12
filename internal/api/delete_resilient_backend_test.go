package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// main.go wraps every s3/minio backend in a storage.ResilientBackend. The
// DELETE handler must still recognise the wrapped backend as remote: a type
// switch on the wrapper sent S3 data down the local rewrite path, which writes
// the survivors to a relative "s3:/bucket/..." directory on local disk and then
// deletes the real S3 source (#975 immutable rewrite).
func TestIsRemoteBackendUnwrapsResilientBackend(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()

	for _, tc := range []struct {
		name    string
		backend storage.Backend
		want    bool
	}{
		{"s3", &storage.S3Backend{}, true},
		{"azure", &storage.AzureBlobBackend{}, true},
		{"local", local, false},
		{"resilient s3", storage.NewResilientBackend(&storage.S3Backend{}, nil, zerolog.Nop()), true},
		{"resilient azure", storage.NewResilientBackend(&storage.AzureBlobBackend{}, nil, zerolog.Nop()), true},
		{"resilient local", storage.NewResilientBackend(local, nil, zerolog.Nop()), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &DeleteHandler{storage: tc.backend, logger: zerolog.Nop()}
			if got := h.isRemoteBackend(); got != tc.want {
				t.Fatalf("isRemoteBackend() = %v, want %v", got, tc.want)
			}
		})
	}
}

// fakeS3 is the minimal S3 surface a partial-row rewrite touches: HEAD on the
// source, PUT of the rewritten object, DELETE of the superseded source.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    []string
	deletes []string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/")
	switch r.Method {
	case http.MethodHead:
		body, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		f.objects[key] = body
		f.puts = append(f.puts, key)
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		delete(f.objects, key)
		f.deletes = append(f.deletes, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// A standalone partial-row DELETE on a ResilientBackend-wrapped S3 backend must
// upload the survivors to S3 before the source object is deleted, and must not
// write anything next to the source on local disk.
func TestPartialRewriteResilientS3UploadsBeforeDeletingSource(t *testing.T) {
	const bucket = "test-bucket"
	const key = "db/cpu/2026/10/07/20/cpu_20261007_204851_558252000.parquet"

	root := t.TempDir()
	uploadDir := filepath.ToSlash(filepath.Join(root, "uploads"))
	if err := os.MkdirAll(uploadDir, 0700); err != nil {
		t.Fatal(err)
	}
	logger := zerolog.Nop()
	db, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 1, LocalStorageRoot: root, UploadDir: uploadDir}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// DuckDB in a unit test cannot read from the fake S3 endpoint, so the
	// source rows are read from a local copy standing in for the s3:// URI.
	// The object key, the HEAD, the PUT and the DELETE all go to fake S3.
	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0700); err != nil {
		t.Fatal(err)
	}
	srcFile := filepath.Join(srcDir, filepath.Base(key))
	if _, err := db.DB().Exec("CREATE TABLE source(host VARCHAR); INSERT INTO source VALUES ('h1'), ('h2'), ('h2'), ('h3');"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(fmt.Sprintf("COPY source TO %q (FORMAT PARQUET)", srcFile)); err != nil {
		t.Fatal(err)
	}
	srcBytes, err := os.ReadFile(srcFile)
	if err != nil {
		t.Fatal(err)
	}

	fake := &fakeS3{objects: map[string][]byte{bucket + "/" + key: srcBytes}}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	s3Backend, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: srv.URL,
		PathStyle: true, AccessKey: "test", SecretKey: "test",
	}, logger)
	if err != nil {
		t.Fatalf("S3 backend: %v", err)
	}
	defer s3Backend.Close()
	wrapped := storage.NewResilientBackend(s3Backend, &storage.ResilientConfig{
		MaxFailures: 5, Timeout: time.Second, HalfOpenMaxRequests: 1,
		MaxRetries: 0, RetryDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
	}, logger)

	h := &DeleteHandler{db: db, storage: wrapped, tempDir: uploadDir, logger: logger}
	deleted, err := h.rewriteFileWithoutDeletedRows(context.Background(), srcFile, key, "host = 'h1'")
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}

	fake.mu.Lock()
	puts := append([]string(nil), fake.puts...)
	deletes := append([]string(nil), fake.deletes...)
	objects := make(map[string][]byte, len(fake.objects))
	for k, v := range fake.objects {
		objects[k] = v
	}
	fake.mu.Unlock()

	if len(puts) != 1 {
		t.Fatalf("S3 PUTs = %v, want exactly one upload of the rewritten file (local rewrite path taken?)", puts)
	}
	newKey := puts[0]
	if newKey == bucket+"/"+key || !strings.HasPrefix(newKey, bucket+"/db/cpu/2026/10/07/20/") || !strings.HasSuffix(newKey, ".parquet") {
		t.Fatalf("rewritten object key = %q, want a fresh immutable key next to the source", newKey)
	}
	if len(deletes) != 1 || deletes[0] != bucket+"/"+key {
		t.Fatalf("S3 DELETEs = %v, want only the superseded source %q", deletes, bucket+"/"+key)
	}
	if _, ok := objects[bucket+"/"+key]; ok {
		t.Fatalf("superseded source still present in S3")
	}

	// The survivors are in S3, not only on local disk. Read them back from
	// inside the DuckDB sandbox root.
	uploaded := filepath.Join(root, "uploaded.parquet")
	if err := os.WriteFile(uploaded, objects[newKey], 0600); err != nil {
		t.Fatal(err)
	}
	var rows, h1 int
	if err := db.DB().QueryRow(fmt.Sprintf("SELECT COUNT(*), COUNT(*) FILTER (WHERE host = 'h1') FROM read_parquet(%q)", uploaded)).Scan(&rows, &h1); err != nil {
		t.Fatalf("read uploaded rewrite: %v", err)
	}
	if rows != 3 || h1 != 0 {
		t.Fatalf("uploaded rows=%d h1=%d, want 3/0", rows, h1)
	}

	// Nothing was published next to the source on local disk.
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(key) {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("local source dir = %v, want only the source file (local rewrite path taken)", names)
	}
}
