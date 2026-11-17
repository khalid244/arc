package compaction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// validParquetBytes is a small, readable parquet file.
func validParquetBytes(t *testing.T) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "host", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := 0; i < 100; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("h%d", i%3))
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(schema, &buf, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func (f *finishFixture) read(t *testing.T, key string) []byte {
	t.Helper()
	b, err := f.backend.Read(f.ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func metricCount(key string) int64 { return metrics.Get().Snapshot()[key].(int64) }

// keptOutputShape writes the partial-DeleteBatch shape (one input live, the
// rest gone) with the given output bytes, whose size disagrees with the
// manifest, and returns the manifest path.
func (f *finishFixture) keptOutputShape(t *testing.T, output []byte) (string, *Manifest) {
	t.Helper()
	_, m := f.wideManifest(t, "j", 4, 1)
	m.OutputSize = int64(len(output)) + 7
	path := f.rewriteManifest(t, m)
	f.put(t, m.OutputPath, string(output))
	return path, m
}

// A kept output that is readable parquet stays where it is: queries can read
// it, and it may hold the only copy of the gone inputs' rows.
func TestParkKeptOutput_ReadableOutputStaysInPlace(t *testing.T) {
	f := newFinishFixture(t)
	body := validParquetBytes(t)
	path, m := f.keptOutputShape(t, body)
	parked, quarantined := metricCount("compaction_outputs_parked"), metricCount("compaction_outputs_quarantined")

	if _, err := f.mm.RecoverOrphanedManifests(f.ctx, nil, nil); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if !bytes.Equal(f.read(t, m.OutputPath), body) {
		t.Fatal("readable output not kept in place unchanged")
	}
	if f.exists(t, OutputQuarantinePrefix+m.OutputPath) {
		t.Fatal("readable output copied to quarantine")
	}
	f.assertParked(t, path)
	if got := metricCount("compaction_outputs_parked"); got != parked+1 {
		t.Fatalf("parked counter = %d, want %d", got, parked+1)
	}
	if got := metricCount("compaction_outputs_quarantined"); got != quarantined {
		t.Fatalf("quarantined counter moved for a readable output")
	}
}

// A kept output that is not readable parquet would fail every query that
// reads its partition, so it is moved out of the partition to
// _arc/quarantine/<key>, bytes intact, instead of being left or deleted.
func TestParkKeptOutput_UnreadableOutputMovedToQuarantine(t *testing.T) {
	valid := validParquetBytes(t)
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"not parquet", []byte("output rewritten to another size")},
		{"truncated parquet", valid[:len(valid)-20]},
		{"footer length past the start", append(append([]byte{}, valid[:len(valid)-8]...), 0xff, 0xff, 0xff, 0x7f, 'P', 'A', 'R', '1')},
		{"garbage footer", append([]byte("PAR1xxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 16, 0, 0, 0, 'P', 'A', 'R', '1')},
		{"tiny", []byte("PAR1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFinishFixture(t)
			path, m := f.keptOutputShape(t, tc.body)
			quarantined := metricCount("compaction_outputs_quarantined")

			if _, err := f.mm.RecoverOrphanedManifests(f.ctx, nil, nil); err != nil {
				t.Fatalf("recovery: %v", err)
			}
			if f.exists(t, m.OutputPath) {
				t.Fatal("unreadable output left in the partition: every query over it fails")
			}
			dst := OutputQuarantinePrefix + m.OutputPath
			if !bytes.Equal(f.read(t, dst), tc.body) {
				t.Fatal("quarantined copy missing or altered: the bytes are kept for manual recovery")
			}
			if !f.exists(t, m.InputFiles[1]) {
				t.Fatal("live input deleted")
			}
			f.assertParked(t, path)
			if got := metricCount("compaction_outputs_quarantined"); got != quarantined+1 {
				t.Fatalf("quarantined counter = %d, want %d", got, quarantined+1)
			}
		})
	}
}

// readFaultBackend fails ranged reads of failKey (the output check cannot
// decide) and deletes of failDeleteKey (the move cannot finish).
type readFaultBackend struct {
	*headBackend
	failKey       string
	failDeleteKey string
}

func (b *readFaultBackend) ReadToAt(ctx context.Context, key string, w io.Writer, off int64) error {
	if key == b.failKey {
		return errors.New("GetObject: exceeded maximum number of attempts, 3, StatusCode: 503")
	}
	return b.headBackend.ReadToAt(ctx, key, w, off)
}

func (b *readFaultBackend) Delete(ctx context.Context, key string) error {
	if key == b.failDeleteKey {
		return errors.New("DeleteObject: StatusCode: 503")
	}
	return b.headBackend.Delete(ctx, key)
}

func (b *readFaultBackend) DeleteBatch(ctx context.Context, keys []string) error {
	for _, k := range keys {
		if err := b.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

// An output that cannot be read is neither kept as readable nor condemned as
// corrupt: the manifest stays for the next pass and nothing moves.
func TestParkKeptOutput_CheckReadErrorKeepsEverything(t *testing.T) {
	f := newFinishFixture(t)
	path, m := f.keptOutputShape(t, []byte("output rewritten to another size"))
	mm := NewManifestManager(&readFaultBackend{headBackend: newHeadBackend(f.backend, 0), failKey: m.OutputPath}, zerolog.Nop())

	n, err := mm.RecoverOrphanedManifests(f.ctx, nil, nil)
	if err == nil || n != 0 {
		t.Fatalf("recovered = %d, err = %v; want 0 and the read error", n, err)
	}
	if !f.exists(t, m.OutputPath) || !f.exists(t, path) || f.exists(t, OutputQuarantinePrefix+m.OutputPath) {
		t.Fatal("a failed output read must leave output and manifest in place")
	}
}

// A failed move keeps the manifest in the work set (the parked copy may
// already exist), so the next pass retries the move.
func TestParkKeptOutput_MoveFailureRetriesNextPass(t *testing.T) {
	f := newFinishFixture(t)
	body := []byte("output rewritten to another size")
	path, m := f.keptOutputShape(t, body)
	rb := &readFaultBackend{headBackend: newHeadBackend(f.backend, 0), failDeleteKey: m.OutputPath}
	mm := NewManifestManager(rb, zerolog.Nop())

	if _, err := mm.RecoverOrphanedManifests(f.ctx, nil, nil); err == nil {
		t.Fatal("recovery succeeded although the output could not be removed from the partition")
	}
	if !f.exists(t, path) || !f.exists(t, m.OutputPath) {
		t.Fatal("manifest or output gone after a failed move")
	}
	rb.failDeleteKey = ""
	if _, err := mm.RecoverOrphanedManifests(f.ctx, nil, nil); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if f.exists(t, m.OutputPath) || !bytes.Equal(f.read(t, OutputQuarantinePrefix+m.OutputPath), body) {
		t.Fatal("second pass did not finish the move")
	}
	f.assertParked(t, path)
}

// (3) Finishing a manifest with an input key no backend can address reports
// that key, as the kept-output branch does.
func TestFinishCompleted_ReportsUnaddressableInput(t *testing.T) {
	f := newFinishFixture(t)
	_, m := f.wideManifest(t, "j", 3)
	bad := `db\cpu/2026/11/10/04/in-bad.parquet`
	m.InputFiles[1] = bad
	f.rewriteManifest(t, m)
	var logs bytes.Buffer
	mm := NewManifestManager(f.backend, zerolog.New(&logs))

	if n, err := mm.finishCompletedManifests(f.ctx, recoveryScope{}, nil, nil, recoveryHooks{}); err != nil || n != 1 {
		t.Fatalf("finished = %d, err = %v; want 1, nil", n, err)
	}
	var reported bool
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "Compaction input cannot be deleted") && strings.Contains(line, `in-bad.parquet`) {
			reported = true
		}
	}
	if !reported {
		t.Fatalf("unaddressable input %q not reported; log:\n%s", bad, logs.String())
	}
}

// (3) The subprocess leaves its manifest for the parent exactly when the
// parent observes consumed inputs.
func TestManagerSubprocessJobConfig_ParentFinalizesManifest(t *testing.T) {
	f := newFinishFixture(t)
	m := NewManager(&ManagerConfig{StorageBackend: f.backend, TempDirectory: t.TempDir(), Logger: zerolog.Nop()})
	c := Candidate{Database: "db", Measurement: "cpu", PartitionPath: "db/cpu/2026/11/10/04", Tier: "hourly", BatchNumber: 1}
	if m.subprocessJobConfig(c, "j").ParentFinalizesManifest {
		t.Fatal("ParentFinalizesManifest = true without a consumed-inputs observer")
	}
	m.SetOnConsumedInputs(func([]string) error { return nil })
	if !m.subprocessJobConfig(c, "j").ParentFinalizesManifest {
		t.Fatal("ParentFinalizesManifest = false with a consumed-inputs observer: a parent crash would lose the receipt marks")
	}
}

// shortCopyBackend silently stores only part of anything written under the
// quarantine prefix: a copy that "succeeded" but is not complete.
type shortCopyBackend struct {
	*headBackend
}

func (b *shortCopyBackend) WriteReader(ctx context.Context, key string, r io.Reader, size int64) error {
	if !strings.HasPrefix(key, OutputQuarantinePrefix) {
		return b.headBackend.WriteReader(ctx, key, r, size)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return b.headBackend.Write(ctx, key, data[:len(data)/2])
}

// The original leaves its partition only once the quarantine copy has every
// byte: an incomplete copy keeps the original and the manifest.
func TestParkKeptOutput_IncompleteCopyKeepsOriginal(t *testing.T) {
	f := newFinishFixture(t)
	body := []byte("output rewritten to another size")
	path, m := f.keptOutputShape(t, body)
	mm := NewManifestManager(&shortCopyBackend{headBackend: newHeadBackend(f.backend, 0)}, zerolog.Nop())

	if _, err := mm.RecoverOrphanedManifests(f.ctx, nil, nil); err == nil {
		t.Fatal("recovery succeeded with an incomplete quarantine copy")
	}
	if !bytes.Equal(f.read(t, m.OutputPath), body) || !f.exists(t, path) {
		t.Fatal("original output or manifest removed after an incomplete copy")
	}
}
