package storage

// ResilientBackend is a FORK-ONLY wrapper. Upstream deleted it in #561 as dead
// code; this fork wraps every S3 backend in it (cmd/arc/main.go) so a Hetzner
// object-store outage trips a circuit breaker instead of hanging queries and
// flushes on per-request timeouts.
//
// Two rules keep it transparent to upstream code:
//
//  1. Anything that type-switches on a CONCRETE backend (*S3Backend,
//     *AzureBlobBackend, *LocalBackend) or asserts a concrete accessor
//     (GetPrefix, GetBucket, GetBasePath, GetAccessKey, ...) must call
//     storage.Unwrap first. A switch on the wrapper falls to its default arm,
//     and for the read-path helpers that default is a local "./data" glob, so
//     S3 queries would silently return empty results (DRY-1). backendRoot,
//     GetLocalBasePath and GetFullPath in util.go already unwrap.
//
//  2. The wrapper implements the optional capability interfaces the wrapped
//     object stores implement (DirectoryLister, BatchDeleter, ObjectLister,
//     PrefixProber, UnusableLister, DirectoryRemover) and deliberately NOT the
//     local-filesystem-only ones (AppendingBackend, StagingInspector,
//     FileAdopter). Callers of those type-assert and fall back, which is
//     exactly what S3 and Azure answer anyway. Wrapping a LocalBackend is
//     therefore correct but loses those optimisations; prod never does it.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"time"

	"github.com/basekick-labs/arc/internal/circuitbreaker"
	"github.com/rs/zerolog"
)

// isNotFound returns true when err indicates the requested object does not
// exist (HTTP 404 / S3 NoSuchKey / a local not-exist). These errors are not
// backend failures and must not be counted by the circuit breaker or retried.
//
// It deliberately does not use IsNotFound: that predicate also runs the Azure
// classifier, whose string fallback matches a bare "404", and an S3 5xx whose
// hex RequestID contains "404" must stay a failure (CS-8).
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrObjectNotFound) || errors.Is(err, fs.ErrNotExist) {
		return true
	}
	return isNotFoundError(err)
}

// isPermanent reports errors that a retry cannot fix and that say nothing
// about the backend's health: the caller's own mistakes (a key the contract
// refuses, a body whose length does not match its declaration), capability
// answers, and a store that does not exist. They are returned at once and are
// not counted as circuit-breaker failures, so a burst of bad keys cannot open
// the breaker for every other caller.
func isPermanent(err error) bool {
	return errors.Is(err, ErrInvalidPath) ||
		errors.Is(err, ErrBodyLength) ||
		errors.Is(err, ErrResumeNotSupported) ||
		errors.Is(err, ErrAdoptUnsupported) ||
		errors.Is(err, ErrStoreNotFound)
}

// errNotRewindable is the reason a failed attempt is not retried when the
// previous attempt consumed a body or wrote into a sink that cannot be undone.
var errNotRewindable = errors.New("previous attempt consumed or produced bytes that cannot be rewound")

// ResilientBackend wraps a storage backend with circuit breaker and retry logic
type ResilientBackend struct {
	backend Backend
	cb      *circuitbreaker.CircuitBreaker
	logger  zerolog.Logger

	// Retry configuration
	maxRetries    int
	retryDelay    time.Duration
	retryMaxDelay time.Duration
}

// ResilientConfig holds configuration for the resilient backend
type ResilientConfig struct {
	// Circuit breaker settings
	MaxFailures         int
	Timeout             time.Duration
	HalfOpenMaxRequests int

	// Retry settings
	MaxRetries    int
	RetryDelay    time.Duration
	RetryMaxDelay time.Duration
}

// DefaultResilientConfig returns default resilient backend configuration
func DefaultResilientConfig() *ResilientConfig {
	return &ResilientConfig{
		MaxFailures:         5,
		Timeout:             30 * time.Second,
		HalfOpenMaxRequests: 3,
		MaxRetries:          3,
		RetryDelay:          100 * time.Millisecond,
		RetryMaxDelay:       5 * time.Second,
	}
}

// NewResilientBackend creates a new resilient storage backend
func NewResilientBackend(backend Backend, cfg *ResilientConfig, logger zerolog.Logger) *ResilientBackend {
	if cfg == nil {
		cfg = DefaultResilientConfig()
	}

	cbConfig := &circuitbreaker.Config{
		Name:                "storage",
		MaxFailures:         cfg.MaxFailures,
		Timeout:             cfg.Timeout,
		HalfOpenMaxRequests: cfg.HalfOpenMaxRequests,
	}

	return &ResilientBackend{
		backend:       backend,
		cb:            circuitbreaker.New(cbConfig, logger),
		logger:        logger.With().Str("component", "resilient-storage").Logger(),
		maxRetries:    cfg.MaxRetries,
		retryDelay:    cfg.RetryDelay,
		retryMaxDelay: cfg.RetryMaxDelay,
	}
}

// Compile-time checks: the wrapper must stay a full Backend and keep every
// optional capability the wrapped object stores offer (rule 2 above).
var (
	_ Backend          = (*ResilientBackend)(nil)
	_ DirectoryLister  = (*ResilientBackend)(nil)
	_ BatchDeleter     = (*ResilientBackend)(nil)
	_ ObjectLister     = (*ResilientBackend)(nil)
	_ PrefixProber     = (*ResilientBackend)(nil)
	_ UnusableLister   = (*ResilientBackend)(nil)
	_ DirectoryRemover = (*ResilientBackend)(nil)
)

// do runs op under the circuit breaker, retrying with exponential backoff.
//
// A not-found or permanent error (see isPermanent) is handed back at once and
// is reported to the breaker as a success: it is an answer, not an outage. A
// cancellation of the caller's own context is likewise not counted.
//
// rewind, when non-nil, runs before every retry and must restore whatever the
// failed attempt consumed or produced (a request body read part way, a
// destination file written part way). If it fails, the last error is returned
// without retrying: replaying onto a half-consumed body or appending to a
// half-written sink is how a retry turns into a truncated upload or a
// corrupt-but-valid-looking download (CS-4).
func (r *ResilientBackend) do(ctx context.Context, what, path string, op func() error, rewind func() error) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		if attempt > 0 && rewind != nil {
			if rwErr := rewind(); rwErr != nil {
				r.logger.Warn().
					Err(lastErr).
					Str("path", path).
					Str("op", what).
					Str("reason", rwErr.Error()).
					Msg("Storage operation failed and cannot be retried safely")
				return fmt.Errorf("storage %s failed (not retried: %v): %w", what, rwErr, lastErr)
			}
		}

		var answer error
		err := r.cb.Execute(func() error {
			opErr := op()
			if opErr == nil {
				return nil
			}
			if isNotFound(opErr) || isPermanent(opErr) ||
				(errors.Is(opErr, context.Canceled) && errors.Is(ctx.Err(), context.Canceled)) {
				answer = opErr
				return nil
			}
			return opErr
		})

		if answer != nil {
			return answer
		}
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry if circuit is open
		if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
			r.logger.Warn().
				Str("path", path).
				Str("op", what).
				Msg("Storage operation rejected - circuit breaker open")
			return err
		}

		// Don't retry on context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if attempt == r.maxRetries {
			break
		}

		// Calculate retry delay with exponential backoff
		delay := r.retryDelay * time.Duration(1<<uint(attempt))
		if delay > r.retryMaxDelay {
			delay = r.retryMaxDelay
		}

		r.logger.Warn().
			Err(err).
			Str("path", path).
			Str("op", what).
			Int("attempt", attempt+1).
			Int("max_retries", r.maxRetries).
			Dur("retry_delay", delay).
			Msg("Storage operation failed, retrying")

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("storage %s failed after %d retries: %w", what, r.maxRetries, lastErr)
}

// notFound wraps a backend's not-found answer so it satisfies both the fork's
// ErrNotFound and upstream's IsNotFound/ErrObjectNotFound while keeping the
// backend's own error inspectable.
func notFound(path string, err error) error {
	if errors.Is(err, ErrNotFound) {
		return err
	}
	return fmt.Errorf("%w: %s: %w", ErrNotFound, path, err)
}

// Write writes data to the storage backend with resilience. Each attempt
// re-sends the whole slice, so it is always safe to retry.
func (r *ResilientBackend) Write(ctx context.Context, path string, data []byte) error {
	return r.do(ctx, "write", path, func() error {
		return r.backend.Write(ctx, path, data)
	}, nil)
}

// WriteReader writes data from a reader to the storage backend.
//
// A retry re-reads the body, so it is only attempted when the reader can be
// seeked back to where the first attempt started (an *os.File, a
// *bytes.Reader). Otherwise the first failure is returned: retrying a
// half-consumed stream would upload a truncated object — and the S3 backend
// streams unknown-length and large bodies through the multipart Uploader,
// which does not check the length, so that upload would SUCCEED.
func (r *ResilientBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	rewind := func() error { return errNotRewindable }
	if seeker, ok := reader.(io.Seeker); ok {
		if start, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			rewind = func() error {
				if _, err := seeker.Seek(start, io.SeekStart); err != nil {
					return fmt.Errorf("rewinding request body: %w", err)
				}
				return nil
			}
		}
	}
	return r.do(ctx, "write (reader)", path, func() error {
		return r.backend.WriteReader(ctx, path, reader, size)
	}, rewind)
}

// Read reads data from the storage backend with resilience.
// A 404 (NoSuchKey) is returned immediately as ErrNotFound without retrying
// and without counting as a circuit-breaker failure.
func (r *ResilientBackend) Read(ctx context.Context, path string) ([]byte, error) {
	var data []byte
	err := r.do(ctx, "read", path, func() error {
		var readErr error
		data, readErr = r.backend.Read(ctx, path)
		return readErr
	}, nil)
	if err != nil {
		if isNotFound(err) {
			return nil, notFound(path, err)
		}
		return nil, err
	}
	return data, nil
}

// countingWriter counts the bytes an attempt delivered to the caller's writer.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// sinkRewinder returns the rewind step for a download into writer: a no-op
// when the failed attempt delivered nothing, a truncate back to the starting
// offset for a sink that supports it (*os.File, *bytes.Buffer), and
// errNotRewindable otherwise.
func sinkRewinder(writer io.Writer, cw *countingWriter) func() error {
	undo := func() error { return errNotRewindable }

	switch w := writer.(type) {
	case *bytes.Buffer:
		start := w.Len()
		undo = func() error {
			w.Truncate(start)
			return nil
		}
	case interface {
		io.Seeker
		Truncate(int64) error
	}:
		if start, err := w.Seek(0, io.SeekCurrent); err == nil {
			undo = func() error {
				if err := w.Truncate(start); err != nil {
					return fmt.Errorf("truncating partial download: %w", err)
				}
				if _, err := w.Seek(start, io.SeekStart); err != nil {
					return fmt.Errorf("rewinding partial download: %w", err)
				}
				return nil
			}
		}
	}

	return func() error {
		if cw.n == 0 {
			return nil
		}
		if err := undo(); err != nil {
			return err
		}
		cw.n = 0
		return nil
	}
}

// ReadTo reads data to a writer from the storage backend.
//
// A retry after a failure mid-body would append the whole object after the
// partial prefix already written, producing a file that still starts and ends
// with valid Parquet magic but has shifted column-chunk offsets (CS-4). So a
// retry first truncates the sink back to where it started, and is skipped
// when the sink cannot be truncated.
func (r *ResilientBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	cw := &countingWriter{w: writer}
	err := r.do(ctx, "read", path, func() error {
		return r.backend.ReadTo(ctx, path, cw)
	}, sinkRewinder(writer, cw))
	if err != nil && isNotFound(err) {
		return notFound(path, err)
	}
	return err
}

// ReadToAt reads data from path starting at offset into writer, with the same
// partial-write rule as ReadTo.
func (r *ResilientBackend) ReadToAt(ctx context.Context, path string, writer io.Writer, offset int64) error {
	cw := &countingWriter{w: writer}
	err := r.do(ctx, "read", path, func() error {
		return r.backend.ReadToAt(ctx, path, cw, offset)
	}, sinkRewinder(writer, cw))
	if err != nil && isNotFound(err) {
		return notFound(path, err)
	}
	return err
}

// StatFile returns the size of the file at path, delegating to the wrapped backend.
func (r *ResilientBackend) StatFile(ctx context.Context, path string) (int64, error) {
	var size int64
	err := r.do(ctx, "stat", path, func() error {
		var inner error
		size, inner = r.backend.StatFile(ctx, path)
		return inner
	}, nil)
	if err != nil {
		return -1, err
	}
	return size, nil
}

// List lists files in the storage backend
func (r *ResilientBackend) List(ctx context.Context, prefix string) ([]string, error) {
	var files []string
	err := r.do(ctx, "list", prefix, func() error {
		var listErr error
		files, listErr = r.backend.List(ctx, prefix)
		return listErr
	}, nil)
	if err != nil {
		return nil, err
	}
	return files, nil
}

// Delete deletes a file from the storage backend
func (r *ResilientBackend) Delete(ctx context.Context, path string) error {
	return r.do(ctx, "delete", path, func() error {
		return r.backend.Delete(ctx, path)
	}, nil)
}

// Exists checks if a file exists in the storage backend
func (r *ResilientBackend) Exists(ctx context.Context, path string) (bool, error) {
	var exists bool
	err := r.do(ctx, "exists check", path, func() error {
		var existsErr error
		exists, existsErr = r.backend.Exists(ctx, path)
		return existsErr
	}, nil)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// Close closes the underlying storage backend
func (r *ResilientBackend) Close() error {
	return r.backend.Close()
}

// Type delegates to the underlying backend
func (r *ResilientBackend) Type() string {
	return r.backend.Type()
}

// ConfigJSON delegates to the underlying backend
func (r *ResilientBackend) ConfigJSON() string {
	return r.backend.ConfigJSON()
}

// ListDirectories delegates to the underlying backend with resilience
func (r *ResilientBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	lister, ok := r.backend.(DirectoryLister)
	if !ok {
		return nil, fmt.Errorf("underlying backend does not support ListDirectories")
	}

	var dirs []string
	err := r.do(ctx, "ListDirectories", prefix, func() error {
		var listErr error
		dirs, listErr = lister.ListDirectories(ctx, prefix)
		return listErr
	}, nil)
	if err != nil {
		return nil, err
	}
	return dirs, nil
}

// DeleteBatch delegates to the underlying backend with resilience. Deleting
// an already-deleted key succeeds, so re-sending the whole batch is safe.
func (r *ResilientBackend) DeleteBatch(ctx context.Context, paths []string) error {
	deleter, ok := r.backend.(BatchDeleter)
	if !ok {
		return fmt.Errorf("underlying backend does not support DeleteBatch")
	}

	return r.do(ctx, "DeleteBatch", fmt.Sprintf("%d paths", len(paths)), func() error {
		return deleter.DeleteBatch(ctx, paths)
	}, nil)
}

// ListObjects delegates to the underlying backend with resilience
func (r *ResilientBackend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	lister, ok := r.backend.(ObjectLister)
	if !ok {
		return nil, fmt.Errorf("underlying backend does not support ListObjects")
	}

	var objects []ObjectInfo
	err := r.do(ctx, "ListObjects", prefix, func() error {
		var listErr error
		objects, listErr = lister.ListObjects(ctx, prefix)
		return listErr
	}, nil)
	if err != nil {
		return nil, err
	}
	return objects, nil
}

// HasObjectsUnderPrefix delegates to the underlying backend's PrefixProber
// with resilience. A backend without one is answered from ListObjects (or
// List), which is the fallback PrefixProber's callers use themselves.
func (r *ResilientBackend) HasObjectsUnderPrefix(ctx context.Context, prefix string) (bool, error) {
	if prober, ok := r.backend.(PrefixProber); ok {
		var found bool
		err := r.do(ctx, "HasObjectsUnderPrefix", prefix, func() error {
			var probeErr error
			found, probeErr = prober.HasObjectsUnderPrefix(ctx, prefix)
			return probeErr
		}, nil)
		if err != nil {
			return false, err
		}
		return found, nil
	}
	if _, ok := r.backend.(ObjectLister); ok {
		objects, err := r.ListObjects(ctx, prefix)
		if err != nil {
			return false, err
		}
		return len(objects) > 0, nil
	}
	files, err := r.List(ctx, prefix)
	if err != nil {
		return false, err
	}
	return len(files) > 0, nil
}

// ListUnusable delegates to the underlying backend's UnusableLister with
// resilience. A backend without one is an error, never an empty answer: an
// empty result means "verified clean" to backup and retention, and the wrapper
// cannot verify anything its backend cannot enumerate.
func (r *ResilientBackend) ListUnusable(ctx context.Context, prefix string) ([]UnusableObject, error) {
	lister, ok := r.backend.(UnusableLister)
	if !ok {
		return nil, fmt.Errorf("underlying backend does not support ListUnusable")
	}

	var objects []UnusableObject
	err := r.do(ctx, "ListUnusable", prefix, func() error {
		var listErr error
		objects, listErr = lister.ListUnusable(ctx, prefix)
		return listErr
	}, nil)
	if err != nil {
		return nil, err
	}
	return objects, nil
}

// Unwrap returns the underlying storage backend.
// Used by GetStoragePath and other code that needs the concrete backend type;
// see storage.Unwrap for the exported, wrapper-agnostic form.
func (r *ResilientBackend) Unwrap() Backend {
	return r.backend
}

// RemoveDirectory delegates to the underlying backend
func (r *ResilientBackend) RemoveDirectory(ctx context.Context, path string) error {
	remover, ok := r.backend.(DirectoryRemover)
	if !ok {
		return nil
	}
	return remover.RemoveDirectory(ctx, path)
}

// CircuitBreakerStats returns circuit breaker statistics
func (r *ResilientBackend) CircuitBreakerStats() map[string]interface{} {
	return r.cb.Stats()
}

// IsCircuitOpen returns true if the circuit breaker is open
func (r *ResilientBackend) IsCircuitOpen() bool {
	return r.cb.IsOpen()
}

// ResetCircuitBreaker resets the circuit breaker
func (r *ResilientBackend) ResetCircuitBreaker() {
	r.cb.Reset()
}
