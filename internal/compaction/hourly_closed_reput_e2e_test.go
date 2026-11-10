//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2"
)

// End to end with real Parquet: the grace merges a closed hour, the stager
// PUTs one of its consumed keys again, and the hourly tier re-merges that lone
// file with the output within the max wait. The exact-row dedup must leave
// every row once. The job is the one the compaction subprocess runs
// (RunSubprocessJob), on a local backend; the tier runs with the deploy
// values (hourly_min_files 3, grace 10, re-merge 200 / 30m).
func TestHourlyClosedGrace_RePutOfMergedKeyIsDedupedWithinMaxWait(t *testing.T) {
	for _, tc := range []struct {
		name string
		kv   string
	}{
		{"tagless", ""},
		{"tagged", `{'arc:tags': 'host'}`},
	} {
		t.Run(tc.name, func(t *testing.T) { runRePutE2E(t, tc.kv) })
	}
}

func runRePutE2E(t *testing.T, kv string) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	tempDir := filepath.Join(t.TempDir(), "compaction")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// The job names its output with the wall clock, so the tier's clock runs
	// from the real now: t0 is the first pass.
	t0 := time.Now().UTC().Truncate(time.Second)
	hour := t0.Add(-75 * time.Minute).Truncate(time.Hour)
	partition := fmt.Sprintf("testdb/cpu/%04d/%02d/%02d/%02d", hour.Year(), int(hour.Month()), hour.Day(), hour.Hour())
	rawKey := func(w time.Time, n int) string {
		return fmt.Sprintf("%s/cpu_pod-a_%s_%09d.parquet", partition, w.Format("20060102_150405"), n)
	}
	ts := func(d time.Duration) string {
		return fmt.Sprintf("TIMESTAMPTZ '%s'", hour.Add(d).Format("2006-01-02 15:04:05Z07:00"))
	}
	put := func(key, rows string) []byte {
		t.Helper()
		local := filepath.ToSlash(filepath.Join(t.TempDir(), "f.parquet"))
		opts := "FORMAT PARQUET"
		if kv != "" {
			opts += ", KV_METADATA " + kv
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`COPY (%s) TO '%s' (%s)`, rows, escapeSQLPath(local), opts)); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
		data, err := os.ReadFile(local)
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Write(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	three := func(host string, at time.Duration, extra string) string {
		var parts []string
		for i := 0; i < 3; i++ {
			parts = append(parts, fmt.Sprintf("SELECT %s AS time, '%s' AS host, %d.5::DOUBLE AS value%s",
				ts(at+time.Duration(i)*time.Second), host, i, extra))
		}
		return strings.Join(parts, " UNION ALL ")
	}
	// a carries a column the others lack, so K's rows sit in the output with
	// region NULL and K is read without it: the dedup must still match them.
	put(rawKey(hour.Add(10*time.Minute), 1), three("a", 5*time.Minute, ", 'eu' AS region"))
	put(rawKey(hour.Add(40*time.Minute), 2), three("b", 35*time.Minute, ""))
	k := rawKey(t0.Add(-12*time.Minute), 3) // a late file, 12m old at the first pass
	kBytes := put(k, three("k", 50*time.Minute, ""))

	tier := NewHourlyTier(&HourlyTierConfig{
		StorageBackend: backend, MinAgeHours: 1, MinFiles: 3, Enabled: true, Logger: zerolog.Nop(),
		ClosedGraceMinutes: 10, ClosedRemergeMinFiles: 200, ClosedRemergeMaxWaitMinutes: 30,
	})
	now := t0
	tier.clock = func() time.Time { return now }

	find := func() *Candidate {
		t.Helper()
		got, err := tier.FindCandidates(ctx, "testdb", "cpu")
		if err != nil {
			t.Fatal(err)
		}
		for i := range got {
			if got[i].PartitionPath == partition {
				return &got[i]
			}
		}
		return nil
	}
	compact := func(c *Candidate) {
		t.Helper()
		enrichCandidateFileSizes(ctx, backend, c)
		batches, lone := withoutLoneBatches(SplitCandidateByBudget(*c, 0, tierMaxOutputBytes(tier)))
		if len(batches) != 1 || len(lone) != 0 {
			t.Fatalf("batches = %d, lone = %d, want one batch", len(batches), len(lone))
		}
		b := batches[0]
		res, err := RunSubprocessJob(&SubprocessJobConfig{
			Database: "testdb", Measurement: "cpu", PartitionPath: b.PartitionPath, Files: b.Files,
			Tier: "hourly", TempDirectory: tempDir, SortKeys: []string{"time"},
			StorageType: backend.Type(), StorageConfig: backend.ConfigJSON(), PartitionTime: b.PartitionTime,
			JobID: fmt.Sprintf("reput-%d", now.Unix()), BatchNumber: b.BatchNumber,
		})
		if err != nil || !res.Success || !res.SourcesDeleted {
			t.Fatalf("job: err=%v result=%+v", err, res)
		}
	}
	hourFiles := func() []string {
		t.Helper()
		objs, err := backend.List(ctx, partition+"/")
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, o := range objs {
			if strings.HasSuffix(o, ".parquet") {
				out = append(out, o)
			}
		}
		return out
	}
	rows := func() (total, distinct, kRows int) {
		t.Helper()
		var paths []string
		for _, f := range hourFiles() {
			paths = append(paths, "'"+escapeSQLPath(filepath.ToSlash(filepath.Join(root, f)))+"'")
		}
		q := fmt.Sprintf(`SELECT count(*), (SELECT count(*) FROM (SELECT DISTINCT time, host, value FROM read_parquet([%[1]s], union_by_name=true))),
			count(*) FILTER (WHERE host = 'k') FROM read_parquet([%[1]s], union_by_name=true)`, strings.Join(paths, ","))
		if err := db.QueryRowContext(ctx, q).Scan(&total, &distinct, &kRows); err != nil {
			t.Fatalf("count rows: %v", err)
		}
		return total, distinct, kRows
	}

	// First pass at t0: the hour closed more than the grace ago.
	c := find()
	if c == nil || len(c.Files) != 3 {
		t.Fatalf("first pass: candidate %v, want the 3 raw files", c)
	}
	compact(c)
	if files := hourFiles(); len(files) != 1 || !tier.IsCompactedFile(files[0]) {
		t.Fatalf("after the first pass the hour holds %v, want one output", files)
	}
	if total, distinct, _ := rows(); total != 9 || distinct != 9 {
		t.Fatalf("after the first pass: %d rows, %d distinct, want 9 / 9", total, distinct)
	}

	// The stager PUTs K again: K's rows are now in the output and in K.
	if err := backend.Write(ctx, k, kBytes); err != nil {
		t.Fatal(err)
	}
	if total, _, kRows := rows(); total != 12 || kRows != 6 {
		t.Fatalf("after the re-PUT: %d rows (%d of K), want 12 (6): the duplicate this test is about", total, kRows)
	}

	for _, d := range []time.Duration{10 * time.Minute, 20 * time.Minute} {
		now = t0.Add(d)
		if c := find(); c != nil {
			t.Fatalf("t0+%v: the re-merge must wait for the max wait; got %v", d, c.Files)
		}
	}
	now = t0.Add(31 * time.Minute) // first tick past the 30m max wait (the output is named a few seconds after t0)
	c = find()
	if c == nil {
		t.Fatalf("t0+31m: the re-PUT K is still not merged with the output; it stays double counted until the daily merge (hour holds %v)", hourFiles())
	}
	if len(c.Files) != 2 {
		t.Fatalf("t0+31m: candidate %v, want the output and K", c.Files)
	}
	compact(c)

	files := hourFiles()
	if len(files) != 1 || !tier.IsCompactedFile(files[0]) {
		t.Fatalf("after the re-merge the hour holds %v, want one output", files)
	}
	if total, distinct, kRows := rows(); total != 9 || distinct != 9 || kRows != 3 {
		t.Fatalf("after the re-merge: %d rows, %d distinct, %d of K; want 9 / 9 / 3 (the dedup must drop K's copy)", total, distinct, kRows)
	}
	var region int
	q := fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s') WHERE region = 'eu'`, escapeSQLPath(filepath.ToSlash(filepath.Join(root, files[0]))))
	if err := db.QueryRowContext(ctx, q).Scan(&region); err != nil || region != 3 {
		t.Fatalf("region column after the re-merge: %d rows with 'eu' (err %v), want 3", region, err)
	}

	for _, d := range []time.Duration{41 * time.Minute, 2 * time.Hour} {
		now = t0.Add(d)
		if c := find(); c != nil {
			t.Fatalf("t0+%v: nothing raw is left, yet the hour was selected again: %v", d, c.Files)
		}
	}
}
