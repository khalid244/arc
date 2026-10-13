package compaction

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// P3Q-1: GetInputFilesForUploadedOutputs is the query-time exclusion set: the
// inputs of a compaction whose output is already uploaded, hidden so the rows
// are not counted twice during the upload -> delete window. A manifest that
// could not be read (an S3 slow period) used to be skipped: the call returned a
// nil error with the set incomplete, and cached it for the TTL, so queries
// double counted behind a 200 even after storage recovered.

// manifestReadFaults is a local backend whose manifest reads can fail
// transiently, or report one manifest as already deleted.
type manifestReadFaults struct {
	*storage.LocalBackend
	failRead atomic.Bool
	mu       sync.Mutex
	vanished map[string]bool
}

func (f *manifestReadFaults) Read(ctx context.Context, p string) ([]byte, error) {
	if strings.HasSuffix(p, ".json") {
		if f.failRead.Load() {
			return nil, errors.New("operation error S3: GetObject, https response error StatusCode: 503, SlowDown")
		}
		f.mu.Lock()
		gone := f.vanished[p]
		f.mu.Unlock()
		if gone {
			return nil, fmt.Errorf("read %s: %w", p, storage.ErrObjectNotFound)
		}
	}
	return f.LocalBackend.Read(ctx, p)
}

func newManifestReadFaults(t *testing.T) *manifestReadFaults {
	t.Helper()
	lb, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return &manifestReadFaults{LocalBackend: lb, vanished: map[string]bool{}}
}

func writeUploadedManifest(t *testing.T, mm *ManifestManager, job, input, output string) string {
	t.Helper()
	ctx := context.Background()
	p, err := mm.WriteManifest(ctx, &Manifest{
		OutputPath: output, InputFiles: []string{input}, Database: "default", Measurement: "logs",
		PartitionPath: "default/logs/2026/06/01/00", Tier: "hourly", JobID: job, CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mm.MarkOutputUploaded(ctx, p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestUploadedInputsReadFailureIsAnErrorAndNotCached_P3Q1(t *testing.T) {
	b := newManifestReadFaults(t)
	writer := NewManifestManager(b, zerolog.Nop())
	writeUploadedManifest(t, writer, "job1", "default/logs/2026/06/01/00/in1.parquet", "default/logs/2026/06/01/00/out1.parquet")
	writeUploadedManifest(t, writer, "job2", "default/logs/2026/06/01/00/in2.parquet", "default/logs/2026/06/01/00/out2.parquet")

	// A query pod: its own manager, nothing cached yet.
	reader := NewManifestManager(b, zerolog.Nop())
	b.failRead.Store(true)
	got, err := reader.GetInputFilesForUploadedOutputs(context.Background())
	if err == nil {
		t.Fatalf("a transient manifest read failure must be an error, got nil with %v", got)
	}
	if got != nil {
		t.Fatalf("a failed read must return no set, got %v", got)
	}

	// Storage recovers within the cache TTL: the full set, not a cached partial.
	b.failRead.Store(false)
	got, err = reader.GetInputFilesForUploadedOutputs(context.Background())
	if err != nil {
		t.Fatalf("after recovery: %v", err)
	}
	for _, in := range []string{"default/logs/2026/06/01/00/in1.parquet", "default/logs/2026/06/01/00/in2.parquet"} {
		if _, ok := got[in]; !ok {
			t.Fatalf("after recovery the exclusion set must hold %s, got %v", in, got)
		}
	}

	// A complete set IS cached: a read failure inside the TTL does not matter.
	b.failRead.Store(true)
	if got, err := reader.GetInputFilesForUploadedOutputs(context.Background()); err != nil || len(got) != 2 {
		t.Fatalf("complete set within the TTL: got %v, %v", got, err)
	}
}

// A manifest deleted between the listing and the read (its job finished and
// deleted the inputs) and one whose body cannot be parsed (it names nothing a
// query could hide; recovery parks it) are not read failures: the rest of the
// set is served.
func TestUploadedInputsSkipsVanishedAndUnparseableManifests_P3Q1(t *testing.T) {
	b := newManifestReadFaults(t)
	writer := NewManifestManager(b, zerolog.Nop())
	writeUploadedManifest(t, writer, "job1", "default/logs/2026/06/01/00/in1.parquet", "default/logs/2026/06/01/00/out1.parquet")
	gone := writeUploadedManifest(t, writer, "job2", "default/logs/2026/06/01/00/in2.parquet", "default/logs/2026/06/01/00/out2.parquet")
	b.mu.Lock()
	b.vanished[gone] = true
	b.mu.Unlock()
	if err := b.Write(context.Background(), ManifestBasePath+"/hourly/default/garbled.json", []byte("{not json")); err != nil {
		t.Fatal(err)
	}

	got, err := NewManifestManager(b, zerolog.Nop()).GetInputFilesForUploadedOutputs(context.Background())
	if err != nil {
		t.Fatalf("vanished/unparseable manifests must not fail the set: %v", err)
	}
	if _, ok := got["default/logs/2026/06/01/00/in1.parquet"]; !ok || len(got) != 1 {
		t.Fatalf("want exactly in1 excluded, got %v", got)
	}
}
