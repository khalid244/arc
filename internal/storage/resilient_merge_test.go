package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/rs/zerolog"
)

// fastResilientConfig retries quickly so the tests stay fast.
func fastResilientConfig() *ResilientConfig {
	return &ResilientConfig{
		MaxFailures:         100,
		Timeout:             30 * time.Second,
		HalfOpenMaxRequests: 3,
		MaxRetries:          2,
		RetryDelay:          time.Millisecond,
		RetryMaxDelay:       2 * time.Millisecond,
	}
}

// DRY-1: the read-path helpers type-switch on the concrete backend. Wrapped in
// ResilientBackend (as prod does for every S3 backend) they must still produce
// s3:// URIs, never the "./data" default, or every S3 query reads a local glob
// and returns an empty 200.
func TestResilientBackend_PathHelpersUnwrap(t *testing.T) {
	for _, tc := range []struct {
		name   string
		inner  *S3Backend
		prefix string
	}{
		{"no-prefix", &S3Backend{bucket: "bkt"}, "s3://bkt/"},
		{"prefixed", &S3Backend{bucket: "bkt", prefix: "tenant/"}, "s3://bkt/tenant/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := NewResilientBackend(tc.inner, nil, zerolog.Nop())

			glob, err := GetStoragePath(wrapped, "db", "m")
			if err != nil {
				t.Fatalf("GetStoragePath: %v", err)
			}
			if want := tc.prefix + "db/m/**/*.parquet"; glob != want {
				t.Fatalf("GetStoragePath(wrapped) = %q, want %q", glob, want)
			}

			uri, err := ObjectURI(wrapped, "db/m/2026/01/01/00/f.parquet")
			if err != nil {
				t.Fatalf("ObjectURI: %v", err)
			}
			if want := tc.prefix + "db/m/2026/01/01/00/f.parquet"; uri != want {
				t.Fatalf("ObjectURI(wrapped) = %q, want %q", uri, want)
			}

			// GetFullPath must name the same location DuckDB reports for files
			// read through GetStoragePath's glob (query-time compaction
			// exclusion compares the two).
			if got, want := GetFullPath(wrapped, "db/m/f.parquet"), tc.prefix+"db/m/f.parquet"; got != want {
				t.Fatalf("GetFullPath(wrapped) = %q, want %q", got, want)
			}

			if got := GetLocalBasePath(wrapped, nil, "x", "fallback"); got != "" {
				t.Fatalf("GetLocalBasePath(wrapped S3) = %q, want empty", got)
			}

			if _, ok := Unwrap(wrapped).(*S3Backend); !ok {
				t.Fatalf("Unwrap(wrapped) = %T, want *S3Backend", Unwrap(wrapped))
			}
		})
	}
}

// flakyBackend fails the first readFailures ReadTo/ReadToAt calls after
// writing part of the object, and the first writeFailures WriteReader calls
// after consuming part of the body.
type flakyBackend struct {
	*LocalBackend
	object        []byte
	readFailures  int
	writeFailures int
	reads         int
	writes        int
	lastUpload    []byte
}

func (f *flakyBackend) ReadTo(ctx context.Context, path string, w io.Writer) error {
	return f.ReadToAt(ctx, path, w, 0)
}

func (f *flakyBackend) ReadToAt(_ context.Context, _ string, w io.Writer, offset int64) error {
	f.reads++
	body := f.object[offset:]
	if f.reads <= f.readFailures {
		_, _ = w.Write(body[:len(body)/2])
		return errors.New("connection reset by peer")
	}
	_, err := w.Write(body)
	return err
}

func (f *flakyBackend) WriteReader(_ context.Context, _ string, r io.Reader, _ int64) error {
	f.writes++
	if f.writes <= f.writeFailures {
		buf := make([]byte, 4)
		_, _ = io.ReadFull(r, buf)
		return errors.New("connection reset by peer")
	}
	data, err := io.ReadAll(r)
	f.lastUpload = data
	return err
}

func newFlaky(t *testing.T) *flakyBackend {
	t.Helper()
	local, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return &flakyBackend{LocalBackend: local, object: []byte("PAR1-0123456789-ABCDEFGHIJ-footer-PAR1")}
}

// CS-4: a retry after a failure mid-body must not append the object after the
// partial prefix. Into a truncatable sink the result is the exact object.
func TestResilientBackend_ReadToRetryTruncatesFile(t *testing.T) {
	f := newFlaky(t)
	f.readFailures = 1
	r := NewResilientBackend(f, fastResilientConfig(), zerolog.Nop())

	out, err := os.CreateTemp(t.TempDir(), "dl-*")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	if err := r.ReadTo(context.Background(), "a/b.parquet", out); err != nil {
		t.Fatalf("ReadTo: %v", err)
	}
	got, err := os.ReadFile(out.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, f.object) {
		t.Fatalf("CORRUPT got %d bytes %q want %d", len(got), got, len(f.object))
	}

	var buf bytes.Buffer
	buf.WriteString("keep:")
	f.reads = 0
	if err := r.ReadToAt(context.Background(), "a/b.parquet", &buf, 5); err != nil {
		t.Fatalf("ReadToAt: %v", err)
	}
	if want := "keep:" + string(f.object[5:]); buf.String() != want {
		t.Fatalf("ReadToAt into buffer = %q, want %q", buf.String(), want)
	}
}

// CS-4: into a sink that cannot be truncated, the wrapper must not retry once
// bytes were delivered.
func TestResilientBackend_ReadToNoRetryIntoUnrewindableSink(t *testing.T) {
	f := newFlaky(t)
	f.readFailures = 1
	r := NewResilientBackend(f, fastResilientConfig(), zerolog.Nop())

	var sink strings.Builder
	err := r.ReadTo(context.Background(), "a/b.parquet", &sink)
	if err == nil {
		t.Fatalf("ReadTo succeeded into an unrewindable sink after a partial write: %q", sink.String())
	}
	if f.reads != 1 {
		t.Fatalf("reads = %d, want 1 (no retry after a partial write)", f.reads)
	}
}

// A seekable body is rewound before a retry; an unseekable one is not retried
// (a retry would upload the unread remainder as the whole object).
func TestResilientBackend_WriteReaderRetryRewindsOrRefuses(t *testing.T) {
	payload := []byte("PAR1-payload-PAR1")

	f := newFlaky(t)
	f.writeFailures = 1
	r := NewResilientBackend(f, fastResilientConfig(), zerolog.Nop())
	if err := r.WriteReader(context.Background(), "a/b.parquet", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatalf("WriteReader(seekable): %v", err)
	}
	if !bytes.Equal(f.lastUpload, payload) {
		t.Fatalf("uploaded %q, want %q", f.lastUpload, payload)
	}

	f2 := newFlaky(t)
	f2.writeFailures = 1
	r2 := NewResilientBackend(f2, fastResilientConfig(), zerolog.Nop())
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write(payload)
		_ = pw.Close()
	}()
	err := r2.WriteReader(context.Background(), "a/b.parquet", pr, int64(len(payload)))
	if err == nil {
		t.Fatalf("WriteReader(unseekable) retried a half-consumed body and uploaded %q", f2.lastUpload)
	}
	if f2.writes != 1 {
		t.Fatalf("writes = %d, want 1", f2.writes)
	}
}

// Caller errors (the key contract) are returned at once and never open the
// breaker for everyone else.
func TestResilientBackend_InvalidPathDoesNotTripBreaker(t *testing.T) {
	local, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	cfg := fastResilientConfig()
	cfg.MaxFailures = 2
	r := NewResilientBackend(local, cfg, zerolog.Nop())

	for i := 0; i < 10; i++ {
		if err := r.Write(context.Background(), "../escape", []byte("x")); !errors.Is(err, ErrInvalidPath) {
			t.Fatalf("Write(../escape) = %v, want ErrInvalidPath", err)
		}
	}
	if r.IsCircuitOpen() {
		t.Fatal("breaker opened on caller errors")
	}
}

// The wrapper's not-found answer satisfies both the fork's and upstream's
// predicates.
func TestResilientBackend_NotFoundSatisfiesBothSentinels(t *testing.T) {
	local, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	r := NewResilientBackend(local, fastResilientConfig(), zerolog.Nop())
	_, readErr := r.Read(context.Background(), "no/such/object.json")
	if !errors.Is(readErr, ErrNotFound) || !errors.Is(readErr, ErrObjectNotFound) || !IsNotFound(readErr) {
		t.Fatalf("Read(missing) = %v; want ErrNotFound, ErrObjectNotFound and IsNotFound", readErr)
	}
}

func s3ResponseError(status int, requestID string) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      errors.New("api error InternalError: We encountered an internal error"),
		},
		RequestID: requestID,
	}
}

// CS-8: a 5xx whose hex RequestID contains "404" is NOT a missing object.
func TestS3NotFoundClassificationIgnoresHexRequestIDs(t *testing.T) {
	err503 := fmt.Errorf("operation error S3: HeadObject, %w", s3ResponseError(503, "tx00000a3f2e0404b1c2d3e-0066a1b2c3-1234-default"))
	if isNotFoundError(err503) {
		t.Fatalf("MISCLASSIFIED: %v", err503)
	}
	if isNotFound(err503) {
		t.Fatalf("resilient isNotFound misclassified: %v", err503)
	}

	err404 := fmt.Errorf("operation error S3: HeadObject, %w", s3ResponseError(404, "tx0000000000000000000a-default"))
	if !isNotFoundError(err404) {
		t.Fatalf("404 response not classified as not-found: %v", err404)
	}
	if !isNotFoundError(errors.New("operation error S3: GetObject, api error NoSuchKey: The specified key does not exist.")) {
		t.Fatal("NoSuchKey text not classified as not-found")
	}
}
