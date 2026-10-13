package ingest

// A refusal is all-or-nothing per client request (P3-H7). A 503 tells the
// client to resend the whole request, so it is only correct while nothing of
// the request has been accepted: once part of it is buffered (and in the WAL),
// refusing the rest makes the retry write the accepted part twice. The
// admission gates (fence, local-disk backpressure) and a failed WAL append
// with wal.reject_on_append_failure therefore refuse a request only before
// any of it was accepted; after that the rest is accepted as well (a failed
// WAL append then behaves like the default mode: logged, counted,
// acknowledged).

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
)

// nthAppendFails fails the n-th WAL append the way a full async WAL channel
// does (ErrWALDropped).
type nthAppendFails struct {
	checkpointRecordingWAL
	n     int64
	calls atomic.Int64
}

func (w *nthAppendFails) AppendTracked(r []map[string]interface{}) ([]string, error) {
	if w.calls.Add(1) == w.n {
		return nil, wal.ErrWALDropped
	}
	return w.checkpointRecordingWAL.AppendTracked(r)
}

func (w *nthAppendFails) AppendRawWithMetaTracked(db string, p []byte) ([]string, error) {
	if w.calls.Add(1) == w.n {
		return nil, wal.ErrWALDropped
	}
	return w.checkpointRecordingWAL.AppendRawWithMetaTracked(db, p)
}

func twoMeasurementRows() []interface{} {
	now := time.Now()
	return []interface{}{
		&models.Record{Measurement: "m1", Time: now, Fields: map[string]interface{}{"v": 1.0}},
		&models.Record{Measurement: "m2", Time: now, Fields: map[string]interface{}{"v": 2.0}},
	}
}

func buffered(b *ArrowBuffer) int64 {
	n, _ := b.GetStats()["total_records_buffered"].(int64)
	return n
}

func TestIngestRequest_WALRefusalOnlyBeforeAnythingWasAccepted(t *testing.T) {
	t.Run("first append fails: refused, nothing accepted", func(t *testing.T) {
		buffer, _ := newCheckpointBuffer(1000)
		defer buffer.Close()
		buffer.SetWAL(&nthAppendFails{n: 1})
		buffer.SetRejectOnWALAppendFailure(true)
		err := buffer.Write(context.Background(), "db", twoMeasurementRows())
		if !errors.Is(err, ErrIngestBackpressure) || buffered(buffer) != 0 {
			t.Fatalf("err=%v buffered=%d; want a refusal with nothing accepted", err, buffered(buffer))
		}
	})
	t.Run("second append fails: the whole request is accepted", func(t *testing.T) {
		buffer, _ := newCheckpointBuffer(1000)
		defer buffer.Close()
		buffer.SetWAL(&nthAppendFails{n: 2})
		buffer.SetRejectOnWALAppendFailure(true)
		err := buffer.Write(context.Background(), "db", twoMeasurementRows())
		if err != nil || buffered(buffer) != 2 {
			t.Fatalf("err=%v buffered=%d; a 503 after one record was accepted makes the client's retry duplicate it", err, buffered(buffer))
		}
	})
	t.Run("second call of a multi-call request", func(t *testing.T) {
		buffer, _ := newCheckpointBuffer(1000)
		defer buffer.Close()
		buffer.SetWAL(&nthAppendFails{n: 2})
		buffer.SetRejectOnWALAppendFailure(true)
		ctx := WithIngestRequest(context.Background())
		for _, m := range []string{"m1", "m2"} {
			rec := checkpointTestRecord()
			rec.Measurement = m
			if err := buffer.WriteColumnarRecord(ctx, "db", rec); err != nil {
				t.Fatalf("%s: %v (part of the request was already accepted)", m, err)
			}
		}
	})
}

func TestIngestRequest_GateRefusalOnlyBeforeAnythingWasAccepted(t *testing.T) {
	var refuse atomic.Bool
	gate := func() error {
		if refuse.Load() {
			return ErrIngestBackpressure
		}
		return nil
	}
	write := func(b *ArrowBuffer, ctx context.Context, m string) error {
		rec := checkpointTestRecord()
		rec.Measurement = m
		return b.WriteColumnarRecord(ctx, "db", rec)
	}

	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetBackpressureGate(gate)
	ctx := WithIngestRequest(context.Background())
	if err := write(buffer, ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	refuse.Store(true) // the disk crosses the hard watermark mid-request
	if err := write(buffer, ctx, "m2"); err != nil {
		t.Fatalf("second measurement of a partly accepted request refused: %v", err)
	}
	// A new request is refused.
	if err := write(buffer, WithIngestRequest(context.Background()), "m3"); !errors.Is(err, ErrIngestBackpressure) {
		t.Fatalf("new request: %v, want ErrIngestBackpressure", err)
	}
	// A call without a request context is a request of its own.
	if err := write(buffer, context.Background(), "m4"); !errors.Is(err, ErrIngestBackpressure) {
		t.Fatalf("plain call: %v, want ErrIngestBackpressure", err)
	}
	if n := buffered(buffer); n != 2 {
		t.Fatalf("buffered %d records, want the 2 of the accepted request", n)
	}
}
