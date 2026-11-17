//go:build duckdb_arrow

package compaction

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Production outputs are written by DuckDB: the kept-output check must read
// them as readable (several row groups, dictionary-encoded strings, a ZSTD
// file and a footer larger than one tail read included), or recovery would
// move good data out of its partition.
func TestCheckKeptOutput_DuckDBParquetIsReadable(t *testing.T) {
	ctx := context.Background()
	db := openDuckDBForCompactionTest(t)
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	mm := NewManifestManager(local, zerolog.Nop())
	for _, tc := range []struct{ name, copy string }{
		{"rows", `COPY (SELECT TIMESTAMPTZ '2026-11-15 00:00:00Z' + to_seconds(t.i) AS time, 'h' || (t.i %% 7) AS host, t.i * 1.5 AS value FROM range(300000) t(i)) TO '%s' (FORMAT PARQUET, ROW_GROUP_SIZE 50000)`},
		{"zstd", `COPY (SELECT range AS id, repeat('x', 40) AS pad FROM range(20000)) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`},
		{"wide footer", `COPY (SELECT ` + wideSelect(4000) + ` FROM range(10)) TO '%s' (FORMAT PARQUET)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "out.parquet")
			if _, err := db.ExecContext(ctx, sprintfPath(tc.copy, path)); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "wide footer" {
				if footerLen := int64(binary.LittleEndian.Uint32(data[len(data)-8:])); footerLen+8 <= footerTailRead {
					t.Fatalf("footer is %d bytes; this case needs one larger than a tail read (%d)", footerLen, footerTailRead)
				}
			}
			key := "db/cpu/2026/11/15/00/" + tc.name + "_compacted.parquet"
			if err := local.Write(ctx, key, data); err != nil {
				t.Fatal(err)
			}
			state, size, err := mm.checkKeptOutput(ctx, key)
			if err != nil || state != keptOutputReadable || size != int64(len(data)) {
				t.Fatalf("checkKeptOutput = %v, %d, %v; want readable, %d, nil", state, size, err, len(data))
			}
		})
	}
}

func wideSelect(cols int) string {
	s := ""
	for i := 0; i < cols; i++ {
		if i > 0 {
			s += ", "
		}
		s += "range AS column_with_a_rather_long_name_" + itoa(i)
	}
	return s
}

func itoa(i int) string { return strconv.Itoa(i) }

func sprintfPath(q, path string) string {
	return fmt.Sprintf(q, escapeSQLPath(filepath.ToSlash(path)))
}
