package ingest

// Disk safety and WAL-append rejection (IW-02, IW-11).
//
// ingest.local_disk_max_bytes budgets the WAL dir plus both stager dirs. Above
// 70% of it the node warns (and exports the state); above 85% every live write
// is refused with ErrIngestBackpressure BEFORE anything reaches the WAL, so
// the handler can answer 503 + Retry-After and the client retries. Nothing is
// ever accepted and then dropped. wal.reject_on_append_failure (default off,
// upstream behaviour) turns a failed WAL append into the same refusal instead
// of an acknowledged write that only lives in memory.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
)

func fillDir(t *testing.T, dir, name string, n int) {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), make([]byte, n), 0600); err != nil {
		t.Fatal(err)
	}
}

func backpressureRejections(reason string) string {
	for _, line := range strings.Split(metrics.Get().PrometheusFormat(), "\n") {
		if strings.HasPrefix(line, `arc_ingest_backpressure_rejections_total{reason="`+reason+`"} `) {
			return strings.TrimSpace(line[strings.LastIndex(line, " "):])
		}
	}
	return ""
}

func TestDiskGuard_Watermarks(t *testing.T) {
	root := t.TempDir()
	walDir, lateDir, freshDir := filepath.Join(root, "wal"), filepath.Join(root, "late"), filepath.Join(root, "fresh")
	g := NewDiskGuard(DiskGuardConfig{Dirs: []string{walDir, lateDir, freshDir}, MaxBytes: 10_000, Interval: time.Hour})
	defer g.Stop()

	fillDir(t, walDir, "a.wal", 3000)
	fillDir(t, freshDir, "x__y__fresh_stage_1.parquet", 3000)
	g.Refresh()
	if g.State() != DiskOK || g.Gate() != nil {
		t.Fatalf("60%%: state %v gate %v, want ok/admit", g.State(), g.Gate())
	}

	fillDir(t, lateDir, "x__y__late_stage_1.parquet", 1500) // 75%
	g.Refresh()
	if g.State() != DiskSoft {
		t.Fatalf("75%%: state %v, want soft", g.State())
	}
	if err := g.Gate(); err != nil {
		t.Fatalf("75%%: gate refused (%v); the soft watermark only warns", err)
	}

	fillDir(t, filepath.Join(freshDir, ".out_fresh_j"), "p.parquet", 1500) // 90%, nested dirs count
	g.Refresh()
	if g.State() != DiskHard {
		t.Fatalf("90%%: state %v (used %d), want hard", g.State(), g.Used())
	}
	before := backpressureRejections("local_disk")
	err := g.Gate()
	if !errors.Is(err, ErrIngestBackpressure) {
		t.Fatalf("90%%: gate = %v, want ErrIngestBackpressure", err)
	}
	if after := backpressureRejections("local_disk"); after == before || after == "" {
		t.Errorf("arc_ingest_backpressure_rejections_total{reason=\"local_disk\"} did not move (%q -> %q)", before, after)
	}
	if !strings.Contains(metrics.Get().PrometheusFormat(), "arc_ingest_local_disk_bytes 9000") {
		t.Errorf("arc_ingest_local_disk_bytes not exported")
	}

	if err := os.RemoveAll(filepath.Join(freshDir, ".out_fresh_j")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(lateDir, "x__y__late_stage_1.parquet")); err != nil {
		t.Fatal(err)
	}
	g.Refresh()
	if g.State() != DiskOK || g.Gate() != nil {
		t.Fatalf("after draining: state %v, want ok", g.State())
	}

	off := NewDiskGuard(DiskGuardConfig{Dirs: []string{walDir}, MaxBytes: 0})
	defer off.Stop()
	off.Refresh()
	if off.Gate() != nil {
		t.Fatal("local_disk_max_bytes = 0 must disable the guard")
	}
}

// The background refresher notices growth without an explicit Refresh.
func TestDiskGuard_RefreshesInBackground(t *testing.T) {
	dir := t.TempDir()
	g := NewDiskGuard(DiskGuardConfig{Dirs: []string{dir}, MaxBytes: 1000, Interval: 10 * time.Millisecond})
	g.Start()
	defer g.Stop()
	fillDir(t, dir, "big", 900)
	deadline := time.Now().Add(2 * time.Second)
	for g.Gate() == nil {
		if time.Now().After(deadline) {
			t.Fatal("the guard did not refuse within 2s of the disk passing the hard watermark")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Above the hard watermark every live entry point refuses before the WAL.
func TestBackpressureGate_RefusesBeforeTheWAL(t *testing.T) {
	ctx := context.Background()
	refuse := func() error { return ErrIngestBackpressure }
	entries := map[string]func(*ArrowBuffer) error{
		"WriteColumnarRecord": func(b *ArrowBuffer) error { return b.WriteColumnarRecord(ctx, "db", checkpointTestRecord()) },
		"WriteColumnarDirect": func(b *ArrowBuffer) error {
			return b.WriteColumnarDirect(ctx, "db", "m", map[string][]interface{}{"time": {time.Now().UnixMicro()}, "v": {1.0}})
		},
		"Write (rows)": func(b *ArrowBuffer) error {
			return b.Write(ctx, "db", []interface{}{&models.Record{Measurement: "m", Time: time.Now(), Fields: map[string]interface{}{"v": 1.0}}})
		},
		"WriteTypedColumnarDirect": func(b *ArrowBuffer) error {
			batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{time.Now().UnixMicro()}, "v": []float64{1}}}
			return b.WriteTypedColumnarDirect(ctx, "db", "m", batch, 1)
		},
		"CheckIngestGate": func(b *ArrowBuffer) error { return b.CheckIngestGate() },
	}
	for name, write := range entries {
		t.Run(name, func(t *testing.T) {
			buffer, w := newCheckpointBuffer(1000)
			defer buffer.Close()
			buffer.SetIngestGate(func() error { return nil }) // cluster fence open
			buffer.SetBackpressureGate(refuse)
			if err := write(buffer); !errors.Is(err, ErrIngestBackpressure) {
				t.Fatalf("err = %v, want ErrIngestBackpressure", err)
			}
			if handedOut, _ := w.counts(); handedOut != 0 {
				t.Fatalf("a refused write reached the WAL (%d entries)", handedOut)
			}
		})
	}
	// Replay is never refused: it carries rows already acknowledged.
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetBackpressureGate(refuse)
	if err := buffer.WriteColumnarDirectReplay(ctx, "db", "m", map[string][]interface{}{"time": {time.Now().UnixMicro()}, "v": {1.0}}, ""); err != nil {
		t.Fatalf("replay refused by the backpressure gate: %v", err)
	}
}

// failingWAL refuses every append the way a full async WAL channel does.
type failingWAL struct{ checkpointRecordingWAL }

func (w *failingWAL) Append([]map[string]interface{}) error { return wal.ErrWALDropped }
func (w *failingWAL) AppendRawWithMeta(string, []byte) error { return wal.ErrWALDropped }
func (w *failingWAL) AppendTracked([]map[string]interface{}) ([]string, error) {
	return nil, wal.ErrWALDropped
}
func (w *failingWAL) AppendRawWithMetaTracked(string, []byte) ([]string, error) {
	return nil, wal.ErrWALDropped
}

func TestWALAppendFailure_RejectOrAck(t *testing.T) {
	ctx := context.Background()
	entries := map[string]func(*ArrowBuffer) error{
		"raw payload": func(b *ArrowBuffer) error { return b.WriteColumnarRecord(ctx, "db", checkpointTestRecord()) },
		"row fallback": func(b *ArrowBuffer) error {
			return b.WriteColumnarDirect(ctx, "db", "m", map[string][]interface{}{"time": {time.Now().UnixMicro()}, "v": {1.0}})
		},
		"typed": func(b *ArrowBuffer) error {
			batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{time.Now().UnixMicro()}, "v": []float64{1}}}
			return b.WriteTypedColumnarDirect(ctx, "db", "m", batch, 1)
		},
	}
	for name, write := range entries {
		t.Run(name+"/default acks", func(t *testing.T) {
			buffer, _ := newCheckpointBuffer(1000)
			defer buffer.Close()
			buffer.SetWAL(&failingWAL{})
			if err := write(buffer); err != nil {
				t.Fatalf("with reject_on_append_failure off the write is acknowledged (upstream behaviour), got %v", err)
			}
		})
		t.Run(name+"/reject", func(t *testing.T) {
			buffer, _ := newCheckpointBuffer(1000)
			defer buffer.Close()
			buffer.SetWAL(&failingWAL{})
			buffer.SetRejectOnWALAppendFailure(true)
			before := backpressureRejections("wal_append")
			err := write(buffer)
			if !errors.Is(err, ErrIngestBackpressure) || !errors.Is(err, wal.ErrWALDropped) {
				t.Fatalf("err = %v, want ErrIngestBackpressure wrapping the WAL error", err)
			}
			if n := buffer.GetStats()["total_records_buffered"]; n != int64(0) {
				t.Fatalf("a rejected write left %v records in the buffer: they would be written although the client was told to retry", n)
			}
			if after := backpressureRejections("wal_append"); after == before || after == "" {
				t.Errorf("arc_ingest_backpressure_rejections_total{reason=\"wal_append\"} did not move (%q -> %q)", before, after)
			}
		})
	}
}
