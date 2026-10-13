package ingest

import (
	"context"
	"sync/atomic"
)

// A client request may be written in several buffer calls: one per
// measurement (line protocol, LP import) or one per record group (Write). A
// refusal (503 + Retry-After) tells the client to resend the WHOLE request, so
// it is only correct while nothing of the request has been accepted: once part
// of it is buffered (and in the WAL), refusing the rest makes the retry write
// the accepted part a second time. The admission gates (cluster fence,
// local-disk backpressure) and a failed WAL append with
// wal.reject_on_append_failure therefore refuse a request only before any of
// it was accepted; after that the rest of the request is admitted too, and a
// failed WAL append behaves as in the default mode (logged, counted,
// acknowledged).

type ingestRequestKey struct{}

// ingestRequest is the admission state of one client request.
type ingestRequest struct {
	accepted atomic.Bool
}

// WithIngestRequest marks ctx as carrying ONE client request whose rows are
// written in several ArrowBuffer calls. Pass the returned context to every one
// of them: once any call was accepted, later calls are no longer refused (see
// above). A call made without it is a request of its own.
func WithIngestRequest(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if ingestRequestFrom(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, ingestRequestKey{}, &ingestRequest{})
}

func ingestRequestFrom(ctx context.Context) *ingestRequest {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(ingestRequestKey{}).(*ingestRequest)
	return r
}

// requestAccepted reports whether part of the request in ctx was accepted.
func requestAccepted(ctx context.Context) bool {
	r := ingestRequestFrom(ctx)
	return r != nil && r.accepted.Load()
}

// markRequestAccepted records that part of the request in ctx was accepted
// (buffered). A no-op for a call without a request context.
func markRequestAccepted(ctx context.Context) {
	if r := ingestRequestFrom(ctx); r != nil {
		r.accepted.Store(true)
	}
}
