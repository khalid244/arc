package ingest

// Stager observability: the backlog an outage builds and how the stagers work
// it off are visible as metrics.
//
//	arc_stager_pending_files{kind}            staged sources not yet uploaded
//	arc_stager_pending_bytes{kind}            their bytes
//	arc_stager_oldest_pending_age_seconds{kind} age of the oldest one
//	arc_stager_upload_failures_total{kind}    PUTs that failed (and HEAD did not find)
//	arc_stager_head_dedup_total{kind}         PUT errors resolved by HEAD (duplicates avoided)
//	arc_stager_jobs_resumed_total             persisted merge jobs resumed

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// promValue returns the value of the exported series (name plus labels as
// printed), or -1 when it is absent.
func promValue(t *testing.T, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(metrics.Get().PrometheusFormat(), "\n") {
		if strings.HasPrefix(line, series+" ") {
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, series)), 64)
			if err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return v
		}
	}
	return -1
}

func TestStagerMetrics_PendingGauges(t *testing.T) {
	root := t.TempDir()
	store, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	dir := filepath.Join(root, "fresh")
	n := writeStagedBacklog(t, dir, "fresh_stage_", "db", "m", 4, 2)
	var bytes int64
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		info, _ := e.Info()
		bytes += info.Size()
	}
	oldest := time.Now().Add(-10 * time.Minute)
	if err := os.Chtimes(filepath.Join(dir, "db__m__fresh_stage_1.parquet"), oldest, oldest); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFreshStager(&FreshStagerConfig{Storage: store, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop(),
		Options: StagerOptions{ReportMetrics: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	if got := promValue(t, `arc_stager_pending_files{kind="fresh"}`); got != float64(n) {
		t.Errorf("pending_files = %v, want %d", got, n)
	}
	if got := promValue(t, `arc_stager_pending_bytes{kind="fresh"}`); got != float64(bytes) {
		t.Errorf("pending_bytes = %v, want %d", got, bytes)
	}
	if got := promValue(t, `arc_stager_oldest_pending_age_seconds{kind="fresh"}`); got < 600 || got > 700 {
		t.Errorf("oldest_pending_age_seconds = %v, want ~600", got)
	}
	if err := fs.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := promValue(t, `arc_stager_pending_files{kind="fresh"}`); got != 0 {
		t.Errorf("pending_files after the drain = %v, want 0", got)
	}
	if got := promValue(t, `arc_stager_oldest_pending_age_seconds{kind="fresh"}`); got != 0 {
		t.Errorf("oldest_pending_age_seconds after the drain = %v, want 0", got)
	}
}

func TestStagerMetrics_UploadFailuresAndHeadDedup(t *testing.T) {
	headBefore := promValue(t, `arc_stager_head_dedup_total{kind="late"}`)
	phantom := newFlakyStore(1, true) // the PUT commits but reports an error
	ls := newTestLateStager(t, phantom, filepath.Join(t.TempDir(), "late"))
	defer ls.Close()
	if err := ls.Stage("db", "m", makeParquet(t, "h-%d", 3, map[string]struct{}{})); err != nil {
		t.Fatal(err)
	}
	_ = ls.Flush(context.Background())
	if got := promValue(t, `arc_stager_head_dedup_total{kind="late"}`); got != headBefore+1 {
		t.Errorf("head_dedup_total{late} = %v, want %v", got, headBefore+1)
	}

	failBefore := promValue(t, `arc_stager_upload_failures_total{kind="fresh"}`)
	failing := newFlakyStore(0, false)
	now := time.Now().UTC()
	failing.failKeyContains = now.Format("2006/01/02/15")
	fs := newTestFreshStager(t, failing, filepath.Join(t.TempDir(), "fresh"))
	defer fs.Close()
	if err := fs.Stage("db", "m", makeParquet(t, "f-%d", 3, map[string]struct{}{})); err != nil {
		t.Fatal(err)
	}
	_ = fs.Flush(context.Background())
	if got := promValue(t, `arc_stager_upload_failures_total{kind="fresh"}`); got != failBefore+1 {
		t.Errorf("upload_failures_total{fresh} = %v, want %v", got, failBefore+1)
	}
}

func TestStagerMetrics_JobsResumed(t *testing.T) {
	root := t.TempDir()
	store, err := storage.NewLocalBackend(filepath.Join(root, "s3"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	dir := filepath.Join(root, "late")
	writeStagedBacklog(t, dir, "late_stage_", "db", "m", 3, 2)
	a, err := NewLateStager(&LateStagerConfig{Storage: store, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	a.hooks.crash = killAt(stagerPointJobPersisted, 1)
	_ = a.Flush(context.Background())
	a.abandon()

	before := promValue(t, "arc_stager_jobs_resumed_total")
	b, err := NewLateStager(&LateStagerConfig{Storage: store, StageDir: dir, FlushAge: time.Hour, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := promValue(t, "arc_stager_jobs_resumed_total"); got != before+1 {
		t.Errorf("jobs_resumed_total = %v, want %v", got, before+1)
	}
}
