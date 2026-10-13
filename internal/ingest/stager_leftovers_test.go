package ingest

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// A process killed mid-merge leaves merge scratch on the stage volume: the
// merge out dir of older binaries, merged-but-not-uploaded outputs, and the
// DuckDB spill directory. No merge runs before start, so none of it is live;
// left alone it leaks emptyDir space on every crash (seen in the S4/S8 e2e
// kill -9 runs). Staged sources must survive untouched.
func TestStagerStartup_RemovesCrashedMergeScratch(t *testing.T) {
	root := t.TempDir()
	store, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for _, kind := range []string{"fresh", "late"} {
		t.Run(kind, func(t *testing.T) {
			dir := filepath.Join(root, kind)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			mustWrite := func(rel string) string {
				p := filepath.Join(dir, rel)
				if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
				return p
			}
			spill := mustWrite(filepath.Join(stagerDuckDBTempDir, "duckdb_temp_storage-0.tmp"))
			oldOut := mustWrite(filepath.Join(".merge_out_3106709194", "_y=2026", "part.parquet"))
			merged := mustWrite(".merged_default_lp%5Fmetrics_1791849637038666000.parquet")
			n := writeStagedBacklog(t, dir, kind+"_stage_", "db", "m", 3, 2)

			var closeFn func() error
			switch kind {
			case "fresh":
				s, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop()})
				if err != nil {
					t.Fatal(err)
				}
				closeFn = s.Close
			default:
				s, err := NewLateStager(&LateStagerConfig{Storage: store, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop()})
				if err != nil {
					t.Fatal(err)
				}
				closeFn = s.Close
			}
			for _, p := range []string{spill, oldOut, merged} {
				if _, err := os.Stat(p); !os.IsNotExist(err) {
					t.Errorf("crash leftover %s still present after startup (err=%v)", p, err)
				}
			}
			staged, err := filepath.Glob(filepath.Join(dir, "db__m__"+kind+"_stage_*"))
			if err != nil {
				t.Fatal(err)
			}
			if len(staged) != n {
				t.Errorf("startup cleanup touched staged sources: %d left, want %d", len(staged), n)
			}
			_ = closeFn()
		})
	}
}
