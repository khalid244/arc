package ingest

// What a flush writes: columns never stored (empty name, "_" prefix) are
// skipped safely, and the flush path's copies (merge, sort, Arrow, Parquet)
// write the rows a buffer holds, in sort order, with their types and NULLs.

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// flushRig is a buffer over a local directory.
type flushRig struct {
	buf *ArrowBuffer
	dir string
}

func newFlushRig(t *testing.T, cfg *config.IngestConfig) *flushRig {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	require.NoError(t, err)
	if cfg == nil {
		cfg = &config.IngestConfig{}
	}
	cfg.MaxBufferSize, cfg.MaxBufferAgeMS, cfg.Compression = 1_000_000, 600000, "snappy"
	cfg.FlushWorkers, cfg.FlushQueueSize, cfg.ShardCount, cfg.DataPageVersion = 1, 8, 1, "2.0"
	b := NewArrowBuffer(cfg, backend, zerolog.Nop())
	t.Cleanup(func() { b.Close() })
	return &flushRig{buf: b, dir: dir}
}

// flushedFile is one Parquet file: its schema's column types and its rows
// (column -> values in file order, nil for NULL).
type flushedFile struct {
	path  string
	types map[string]string
	cols  map[string][]interface{}
}

// files flushes and reads every Parquet file under the rig, sorted by path.
func (r *flushRig) files(t *testing.T) []flushedFile {
	t.Helper()
	require.NoError(t, r.buf.FlushAllAndWait(context.Background()))
	var out []flushedFile
	require.NoError(t, filepath.WalkDir(r.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		f, err := os.Open(p)
		require.NoError(t, err)
		defer f.Close()
		pf, err := file.NewParquetReader(f)
		require.NoError(t, err)
		defer pf.Close()
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		require.NoError(t, err)
		tbl, err := fr.ReadTable(context.Background())
		require.NoError(t, err)
		defer tbl.Release()
		ff := flushedFile{path: p, types: map[string]string{}, cols: map[string][]interface{}{}}
		for i, fld := range tbl.Schema().Fields() {
			ff.types[fld.Name] = fld.Type.String()
			for _, chunk := range tbl.Column(i).Data().Chunks() {
				for r := 0; r < chunk.Len(); r++ {
					if chunk.IsNull(r) {
						ff.cols[fld.Name] = append(ff.cols[fld.Name], nil)
						continue
					}
					var v interface{}
					switch c := chunk.(type) {
					case *array.Int64:
						v = c.Value(r)
					case *array.Float64:
						v = c.Value(r)
					case *array.String:
						v = c.Value(r)
					case *array.Boolean:
						v = c.Value(r)
					case *array.Timestamp:
						v = int64(c.Value(r))
					case *array.Decimal128:
						v = c.ValueStr(r)
					default:
						v = chunk.ValueStr(r)
					}
					ff.cols[fld.Name] = append(ff.cols[fld.Name], v)
				}
			}
		}
		out = append(out, ff)
		return nil
	}))
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}

// A column never stored (an empty name, or "_"-prefixed) whose type differs
// between two writes of one buffer used to crash the flush (the merge typed
// it by the first batch); an empty name crashed every flush (the schema read
// its first byte). Both are skipped; the stored columns are written.
func TestFlushSkipsUnstoredColumns(t *testing.T) {
	for _, name := range []string{"_x", ""} {
		for _, second := range []interface{}{int64(2), 2.5} {
			r := newFlushRig(t, nil)
			ctx := context.Background()
			require.NoError(t, r.buf.WriteColumnarRecord(ctx, "d", &models.ColumnarRecord{Measurement: "m", Columnar: true,
				Columns: map[string][]interface{}{"time": {int64(1700000000000000)}, "v": {1.0}, name: {int64(1)}}}))
			require.NoError(t, r.buf.WriteColumnarRecord(ctx, "d", &models.ColumnarRecord{Measurement: "m", Columnar: true,
				Columns: map[string][]interface{}{"time": {int64(1700000000000001)}, "v": {2.0}, name: {second}}}))
			files := r.files(t)
			require.Len(t, files, 1, "name %q", name)
			require.Equal(t, []interface{}{1.0, 2.0}, files[0].cols["v"], "name %q", name)
			_, has := files[0].types[name]
			require.False(t, has, "name %q was stored", name)
		}
	}
}

// One batch keeps its columns as written, unstored ones included: the
// schema skips them.
func TestFlushOneBatchSkipsUnstoredColumn(t *testing.T) {
	r := newFlushRig(t, nil)
	require.NoError(t, r.buf.WriteColumnarRecord(context.Background(), "d", &models.ColumnarRecord{Measurement: "m", Columnar: true,
		Columns: map[string][]interface{}{"time": {int64(1700000000000000)}, "v": {1.0}, "_x": {int64(1)}}}))
	files := r.files(t)
	require.Len(t, files, 1)
	_, has := files[0].types["_x"]
	require.False(t, has, "_x was stored")
	require.Equal(t, []interface{}{1.0}, files[0].cols["v"])
}
