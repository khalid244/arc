//go:build duckdb_arrow

package compaction

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2"
)

// reputRig is a closed hour on a local backend, compacted by the real hourly
// tier inside a real Manager cycle; every batch runs the body the compaction
// subprocess runs (RunSubprocessJob).
type reputRig struct {
	t         *testing.T
	ctx       context.Context
	db        *sql.DB
	m         *Manager
	backend   storage.Backend
	root      string
	tempDir   string
	hour      time.Time
	partition string
	logs      *lockedLog

	mu  sync.Mutex
	ran [][]string
	n   int
}

func newReputRig(t *testing.T) *reputRig {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	m, backend, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	m.ManifestManager = nil
	m.MaxConcurrent = 1
	r := &reputRig{
		t: t, ctx: context.Background(), db: db, m: m, backend: backend,
		root:    backend.(*storage.LocalBackend).GetBasePath(),
		tempDir: t.TempDir(),
		logs:    &lockedLog{},
	}
	r.hour = time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Hour)
	r.partition = fmt.Sprintf("testdb/cpu/%04d/%02d/%02d/%02d", r.hour.Year(), int(r.hour.Month()), r.hour.Day(), r.hour.Hour())
	m.logger = zerolog.New(r.logs).Level(zerolog.DebugLevel)
	m.compactBatchForTest = func(_ context.Context, b Candidate) error {
		r.mu.Lock()
		r.ran = append(r.ran, append([]string(nil), b.Files...))
		r.mu.Unlock()
		return r.runJob("hourly", b.PartitionPath, b.Files)
	}
	return r
}

// rawKey is a fresh-stager key (freshJobKey) created at minute min of the hour.
func (r *reputRig) rawKey(name string, min int) string {
	w := r.hour.Add(time.Duration(min) * time.Minute)
	return fmt.Sprintf("%s/cpu_pod-a-%s_%s_%09d.parquet", r.partition, name, w.Format("20060102_150405"), min)
}

// rows is a SELECT of n rows for host, one per second from the hour's minute
// at; the random value keeps the files from compressing to nothing.
func (r *reputRig) rows(host string, at, n int) string {
	return fmt.Sprintf(`SELECT TIMESTAMPTZ '%s' + to_seconds(i::BIGINT) AS time, '%s' AS host, random() AS value FROM range(%d) t(i)`,
		r.hour.Add(time.Duration(at)*time.Minute).Format("2006-01-02 15:04:05Z07:00"), host, n)
}

// parquet writes the rows to a local Parquet file and returns its bytes.
func (r *reputRig) parquet(rows, kv string) []byte {
	r.t.Helper()
	local := filepath.ToSlash(filepath.Join(r.t.TempDir(), "f.parquet"))
	opts := "FORMAT PARQUET"
	if kv != "" {
		opts += ", KV_METADATA " + kv
	}
	if _, err := r.db.ExecContext(r.ctx, fmt.Sprintf(`COPY (%s) TO '%s' (%s)`, rows, escapeSQLPath(local), opts)); err != nil {
		r.r(err)
	}
	data, err := os.ReadFile(local)
	if err != nil {
		r.r(err)
	}
	return data
}

func (r *reputRig) r(err error) { r.t.Helper(); r.t.Fatal(err) }

func (r *reputRig) put(key string, data []byte) {
	r.t.Helper()
	if err := r.backend.Write(r.ctx, key, data); err != nil {
		r.r(err)
	}
}

func (r *reputRig) runJob(tier, partition string, files []string) error {
	r.mu.Lock()
	r.n++
	id := fmt.Sprintf("reput-%d", r.n)
	r.mu.Unlock()
	res, err := RunSubprocessJob(&SubprocessJobConfig{
		Database: "testdb", Measurement: "cpu", PartitionPath: partition, Files: files,
		Tier: tier, TempDirectory: r.tempDir, SortKeys: []string{"time"},
		StorageType: r.backend.Type(), StorageConfig: r.backend.ConfigJSON(), PartitionTime: r.hour,
		JobID: id, BatchNumber: 1,
	})
	if err != nil {
		return err
	}
	if !res.Success || !res.SourcesDeleted {
		return fmt.Errorf("job %s: %+v", id, res)
	}
	return nil
}

// compactNow runs one job on files and returns the output it wrote. The next
// output is named in a later second, so outputs list in the order made.
func (r *reputRig) compactNow(files ...string) string {
	r.t.Helper()
	before := r.outputs()
	if err := r.runJob("hourly", r.partition, files); err != nil {
		r.r(err)
	}
	var out string
	for _, f := range r.outputs() {
		if !slices.Contains(before, f) {
			out = f
		}
	}
	if out == "" {
		r.t.Fatalf("job on %v wrote no output", files)
	}
	for s := time.Now().Second(); time.Now().Second() == s; {
		time.Sleep(20 * time.Millisecond)
	}
	return out
}

func (r *reputRig) hourFiles() []string {
	r.t.Helper()
	objs, err := r.backend.List(r.ctx, r.partition+"/")
	if err != nil {
		r.r(err)
	}
	var out []string
	for _, o := range objs {
		if strings.HasSuffix(o, ".parquet") {
			out = append(out, o)
		}
	}
	slices.Sort(out)
	return out
}

func (r *reputRig) outputs() []string {
	var out []string
	for _, f := range r.hourFiles() {
		if strings.HasSuffix(f, "_compacted.parquet") {
			out = append(out, f)
		}
	}
	return out
}

func (r *reputRig) size(key string) int64 {
	r.t.Helper()
	n, err := r.backend.StatFile(r.ctx, key)
	if err != nil || n < 0 {
		r.t.Fatalf("stat %s: %d %v", key, n, err)
	}
	return n
}

func (r *reputRig) count() (total, distinct int) {
	r.t.Helper()
	var paths []string
	for _, f := range r.hourFiles() {
		paths = append(paths, "'"+escapeSQLPath(filepath.ToSlash(filepath.Join(r.root, f)))+"'")
	}
	q := fmt.Sprintf(`SELECT count(*), (SELECT count(*) FROM (SELECT DISTINCT time, host, value FROM read_parquet([%[1]s], union_by_name=true)))
		FROM read_parquet([%[1]s], union_by_name=true)`, strings.Join(paths, ","))
	if err := r.db.QueryRowContext(r.ctx, q).Scan(&total, &distinct); err != nil {
		r.t.Fatalf("count rows: %v", err)
	}
	return total, distinct
}

// tier is the hourly tier with the deploy values and a byte budget, its clock
// 31 minutes past the real now (the outputs carry the real now in their names).
func (r *reputRig) tier(budget int64) *HourlyTier {
	tier := NewHourlyTier(&HourlyTierConfig{
		StorageBackend: r.backend, MinAgeHours: 1, MinFiles: 3, Enabled: true, Logger: zerolog.Nop(),
		MaxOutputBytes:     budget,
		ClosedGraceMinutes: 10, ClosedRemergeMinFiles: 200, ClosedRemergeMaxWaitMinutes: 30,
		DropConsumedInputs: true,
	})
	now := time.Now().UTC().Add(31 * time.Minute)
	tier.clock = func() time.Time { return now }
	r.m.Tiers = []Tier{tier}
	return tier
}

func (r *reputRig) cycle() {
	r.t.Helper()
	if _, err := r.m.RunCompactionCycleForTiers(r.ctx, []string{"hourly"}); err != nil {
		r.t.Fatalf("hourly cycle: %v", err)
	}
}

// threeOutputs builds the prod shape: an hour with three outputs, K's rows in
// the first. writeOutput makes an output from raw files (the real job, or a
// legacy writer). It returns K's key and bytes, and the budget under which the
// splitter packs [O1] [O2] [O3 K]: no two outputs fit together, O3 and K do.
func (r *reputRig) threeOutputs(writeOutput func(raws ...string) string) (k string, kBytes []byte, budget int64) {
	r.t.Helper()
	a1, a2 := r.rawKey("a1", 5), r.rawKey("a2", 10)
	b1, b2 := r.rawKey("b1", 15), r.rawKey("b2", 20)
	c1, c2 := r.rawKey("c1", 25), r.rawKey("c2", 30)
	k = r.rawKey("k", 50)
	for key, rows := range map[string]string{
		a1: r.rows("a1", 5, 600), a2: r.rows("a2", 10, 600),
		b1: r.rows("b1", 15, 600), b2: r.rows("b2", 20, 600),
		c1: r.rows("c1", 25, 600), c2: r.rows("c2", 30, 600),
	} {
		r.put(key, r.parquet(rows, ""))
	}
	kBytes = r.parquet(r.rows("k", 50, 3), "")
	r.put(k, kBytes)

	o1 := writeOutput(a1, a2, k)
	o2 := writeOutput(b1, b2)
	o3 := writeOutput(c1, c2)
	if got := r.outputs(); !slices.Equal(got, []string{o1, o2, o3}) {
		r.t.Fatalf("outputs list as %v, want %v", got, []string{o1, o2, o3})
	}
	s1, s2, s3, sk := r.size(o1), r.size(o2), r.size(o3), int64(len(kBytes))
	budget = max(s1, s2, s3) + sk
	if min(s1+s2, s1+s3, s2+s3) <= budget {
		r.t.Fatalf("precondition: outputs %d/%d/%d and K %d do not force [O1] [O2] [O3 K] under %d", s1, s2, s3, sk, budget)
	}
	return k, kBytes, budget
}

// N2: the re-PUT K sits next to three outputs, its rows in the first. The
// splitter pairs K with O3, so a merge cannot remove the copy; K must go.
func TestHourlyConsumedInputs_RePutDroppedWithSeveralOutputs(t *testing.T) {
	r := newReputRig(t)
	k, kBytes, budget := r.threeOutputs(r.compactNow)
	total, distinct := r.count()
	if total != distinct || total != 3603 {
		t.Fatalf("before the re-PUT: %d rows, %d distinct, want 3603 / 3603", total, distinct)
	}
	r.put(k, kBytes)
	if total, _ := r.count(); total != 3606 {
		t.Fatalf("after the re-PUT: %d rows, want 3606 (the duplicate this test is about)", total)
	}

	r.tier(budget)
	dropped0 := droppedCount()
	r.cycle()

	total, distinct = r.count()
	if total != distinct {
		t.Fatalf("after the hourly pass: %d rows, %d distinct; K's copy survived (batches run: %v)", total, distinct, r.ran)
	}
	if d := droppedCount() - dropped0; d != 1 {
		t.Fatalf("dropped counter moved by %d, want 1", d)
	}
	if !r.logged("Deleted a raw file whose rows an hourly output already holds", k, `"level":"info"`) {
		t.Fatalf("no Info line naming K: %s", r.logs.String())
	}
	if total != 3603 {
		t.Fatalf("after the hourly pass: %d rows, want 3603", total)
	}
	if slices.Contains(r.hourFiles(), k) {
		t.Fatalf("K is still in the hour: %v", r.hourFiles())
	}
	for _, b := range r.ran {
		if slices.Contains(b, k) {
			t.Fatalf("K was merged (%v) instead of dropped", b)
		}
	}
}

func droppedCount() int64 {
	return metrics.Get().Snapshot()["compaction_consumed_inputs_dropped"].(int64)
}

func (r *reputRig) logged(parts ...string) bool {
	for _, line := range strings.Split(r.logs.String(), "\n") {
		all := true
		for _, p := range parts {
			all = all && strings.Contains(line, p)
		}
		if all {
			return true
		}
	}
	return false
}

// legacyOutput writes what an older build wrote: the merged rows of raws as a
// _compacted output with no consumed-input list, and deletes the raws.
func (r *reputRig) legacyOutput(raws ...string) string {
	r.t.Helper()
	var paths []string
	for _, f := range raws {
		paths = append(paths, "'"+escapeSQLPath(filepath.ToSlash(filepath.Join(r.root, f)))+"'")
	}
	data := r.parquet(fmt.Sprintf(`SELECT * FROM read_parquet([%s], union_by_name=true) ORDER BY time`, strings.Join(paths, ",")),
		fmt.Sprintf(`{'%s': 'true'}`, exactDedupMarker))
	out := fmt.Sprintf("%s/cpu_%s_%08x_compacted.parquet", r.partition, time.Now().UTC().Format("20060102_150405"), r.n)
	r.n++
	r.put(out, data)
	for _, f := range raws {
		if err := r.backend.Delete(r.ctx, f); err != nil {
			r.r(err)
		}
	}
	for s := time.Now().Second(); time.Now().Second() == s; {
		time.Sleep(20 * time.Millisecond)
	}
	return out
}

// Outputs from an older build list nothing: K is never deleted on their
// account, and the hour behaves as before (K merged with the output that has
// room). No row is lost.
func TestHourlyConsumedInputs_LegacyOutputsFallBackToMerge(t *testing.T) {
	r := newReputRig(t)
	k, kBytes, budget := r.threeOutputs(r.legacyOutput)
	r.put(k, kBytes)
	r.tier(budget)
	dropped0 := droppedCount()
	r.cycle()

	if d := droppedCount() - dropped0; d != 0 {
		t.Fatalf("dropped counter moved by %d with legacy outputs", d)
	}
	if r.logged("Deleted a raw file whose rows") {
		t.Fatal("a raw file was deleted on the account of an output without a list")
	}
	if len(r.ran) != 1 || !slices.Contains(r.ran[0], k) {
		t.Fatalf("batches run = %v, want today's [O3 K]", r.ran)
	}
	if _, distinct := r.count(); distinct != 3603 {
		t.Fatalf("%d distinct rows, want 3603: no row may be lost", distinct)
	}
}

// Only the same key with the same size and bytes is dropped. Another size, or
// the same size with other bytes, is left for the merge; so is a key no output
// lists, and everything when the switch is off.
func TestHourlyConsumedInputs_DropNeedsSameKeySizeAndBytes(t *testing.T) {
	r := newReputRig(t)
	k, kBytes, budget := r.threeOutputs(r.compactNow)
	other := r.rawKey("late", 55)
	r.put(other, r.parquet(r.rows("late", 55, 3), ""))

	flipped := slices.Clone(kBytes)
	flipped[len(flipped)/2] ^= 0xff
	for _, tc := range []struct {
		name string
		data []byte
		off  bool
	}{
		{"other size", r.parquet(r.rows("k", 50, 4), ""), false},
		{"same size, other bytes", flipped, false},
		{"switch off", kBytes, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r.put(k, tc.data)
			tier := r.tier(budget)
			tier.DropConsumedInputs = !tc.off
			c := Candidate{Database: "testdb", Measurement: "cpu", PartitionPath: r.partition, PartitionTime: r.hour, Tier: "hourly", Files: r.hourFiles()}
			enrichCandidateFileSizes(r.ctx, r.backend, &c)
			got, ok := r.m.dropConsumedInputs(r.ctx, tier, c)
			if !ok || !slices.Equal(got.Files, c.Files) {
				t.Fatalf("candidate changed: ok=%v files=%v, want %v", ok, got.Files, c.Files)
			}
			if !slices.Contains(r.hourFiles(), k) || !slices.Contains(r.hourFiles(), other) {
				t.Fatalf("a raw file was deleted: %v", r.hourFiles())
			}
		})
	}

	// Control: the original bytes are dropped, the unlisted late file stays.
	r.put(k, kBytes)
	tier := r.tier(budget)
	c := Candidate{Database: "testdb", Measurement: "cpu", PartitionPath: r.partition, PartitionTime: r.hour, Tier: "hourly", Files: r.hourFiles()}
	enrichCandidateFileSizes(r.ctx, r.backend, &c)
	got, ok := r.m.dropConsumedInputs(r.ctx, tier, c)
	if slices.Contains(r.hourFiles(), k) || slices.Contains(got.Files, k) {
		t.Fatalf("control: K not dropped (hour %v, candidate %v)", r.hourFiles(), got.Files)
	}
	if !ok || !slices.Contains(got.Files, other) || len(got.FileSizes) != len(got.Files) {
		t.Fatalf("control: candidate after the drop ok=%v files=%v sizes=%v, want the outputs and the late file", ok, got.Files, got.FileSizes)
	}
}

func (r *reputRig) listOf(output string) map[string]consumedInput {
	r.t.Helper()
	r.m.consumedInputs = consumedInputsCache{}
	list, err := r.m.consumedInputsOf(r.ctx, output, -1)
	if err != nil {
		r.t.Fatalf("read the list of %s: %v", output, err)
	}
	out := make(map[string]consumedInput, len(list))
	for _, c := range list {
		out[c.Key] = c
	}
	return out
}

func (r *reputRig) entry(key string, data []byte) consumedInput {
	h, _ := hashPrefix(bytes.NewReader(data))
	return consumedInput{Key: key, Size: int64(len(data)), Hash: h}
}

// The list survives re-merges: an output made from outputs lists all of their
// inputs, an older build's output adds nothing, the tagged dedup path writes
// it too, and the daily tier does not carry it.
func TestHourlyConsumedInputs_ListCarriedThroughRemerges(t *testing.T) {
	for _, kv := range []string{"", `{'arc:tags': 'host'}`} {
		t.Run(fmt.Sprintf("kv=%q", kv), func(t *testing.T) {
			r := newReputRig(t)
			want := map[string]consumedInput{}
			raw := func(name string, min int) string {
				key := r.rawKey(name, min)
				data := r.parquet(r.rows(name, min, 5), kv)
				r.put(key, data)
				want[key] = r.entry(key, data)
				return key
			}
			a, b := raw("a", 1), raw("b", 2)
			o1 := r.compactNow(a, b)
			if got := r.listOf(o1); !maps.Equal(got, want) {
				t.Fatalf("O1 lists %v, want %v", got, want)
			}
			c := raw("c", 3)
			o2 := r.compactNow(o1, c)
			if got := r.listOf(o2); !maps.Equal(got, want) {
				t.Fatalf("O2 (O1 + c) lists %v, want %v", got, want)
			}

			x := r.rawKey("x", 4)
			r.put(x, r.parquet(r.rows("x", 4, 5), kv))
			legacy := r.legacyOutput(x)
			d := raw("d", 5)
			o3 := r.compactNow(o2, legacy, d)
			if got := r.listOf(o3); !maps.Equal(got, want) {
				t.Fatalf("O3 (O2 + legacy + d) lists %v, want %v", got, want)
			}
			if total, distinct := r.count(); total != 25 || distinct != 25 {
				t.Fatalf("hour holds %d rows, %d distinct, want 25", total, distinct)
			}

			day := filepath.Dir(r.partition)
			if err := r.runJob("daily", day, []string{o3, raw("e", 6)}); err != nil {
				t.Fatal(err)
			}
			objs, err := r.backend.List(r.ctx, day+"/")
			if err != nil {
				t.Fatal(err)
			}
			var daily string
			for _, o := range objs {
				if strings.HasSuffix(o, "_daily.parquet") {
					daily = o
				}
			}
			if daily == "" {
				t.Fatalf("no daily output in %v", objs)
			}
			if _, found, err := localParquetFooterValue(filepath.Join(r.root, daily), consumedInputsKey); err != nil || found {
				t.Fatalf("daily output carries %s (found=%v, err=%v); only hourly outputs list inputs", consumedInputsKey, found, err)
			}
		})
	}
}

// stagerKeys returns n keys shaped like the fresh stager's (freshJobKey) for
// one prod hour, with sizes and hashes.
func stagerKeys(n int) []consumedInput {
	rng := rand.New(rand.NewPCG(1, 2))
	out := make([]consumedInput, n)
	for i := range out {
		pod := fmt.Sprintf("arc-ingest-5d8f7b9c6-%05x", rng.IntN(1<<20))
		key := fmt.Sprintf("hammel/app_events/2026/11/10/05/app_events_%s-%016x_20261110_%02d%02d%02d_%09d.parquet",
			pod, rng.Uint64(), 5+rng.IntN(3), rng.IntN(60), rng.IntN(60), rng.IntN(1_000_000_000))
		out[i] = consumedInput{Key: key, Size: 20_000 + rng.Int64N(400_000), Hash: fmt.Sprintf("%016x", rng.Uint64())}
	}
	return normalizeConsumedInputs(out)
}

// Footer cost of the list for a prod-sized hour (1,500 stager files), measured
// on a real Parquet file DuckDB writes with it, and the two-read path of a
// footer larger than the first range read.
func TestHourlyConsumedInputs_FooterSize(t *testing.T) {
	r := newReputRig(t)
	footerLen := func(data []byte) int64 {
		return int64(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	}
	plain := r.parquet(r.rows("a", 0, 1000), "")
	for _, n := range []int{1500, 5000, maxConsumedInputs} {
		list := stagerKeys(n)
		v, err := encodeConsumedInputs(list)
		if err != nil {
			t.Fatal(err)
		}
		data := r.parquet(r.rows("a", 0, 1000), fmt.Sprintf(`{'%s': '%s'}`, consumedInputsKey, v))
		growth := footerLen(data) - footerLen(plain)
		t.Logf("%d inputs: value %d bytes (%.1f per input), footer +%d bytes (%d -> %d)", n, len(v), float64(len(v))/float64(n), growth, footerLen(plain), footerLen(data))
		if n == 1500 && len(v) > 96<<10 {
			t.Fatalf("1,500 inputs encode to %d bytes, want under 96 KiB", len(v))
		}
		reads := 0
		got, found, err := parquetFooterValue(int64(len(data)), func(off int64) ([]byte, error) {
			reads++
			return data[off:], nil
		}, consumedInputsKey)
		if err != nil || !found || got != v {
			t.Fatalf("%d inputs: footer read back found=%v err=%v equal=%v", n, found, err, got == v)
		}
		wantReads := 1
		if footerLen(data)+8 > footerTailRead {
			wantReads = 2
		}
		if reads != wantReads {
			t.Fatalf("%d inputs: %d range reads, want %d", n, reads, wantReads)
		}
		back, err := decodeConsumedInputs(got)
		if err != nil || !slices.Equal(back, list) {
			t.Fatalf("%d inputs: decode err=%v equal=%v", n, err, slices.Equal(back, list))
		}
	}
}
