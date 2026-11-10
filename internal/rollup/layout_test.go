package rollup

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func layoutSurveySpec() CubeSpec {
	return CubeSpec{
		Source: "posthog.events", Grain: "hour",
		Dims: []string{"app_version", "event", "os_name", "survey_name", "survey_response"},
		Aggs: []Aggregate{{Kind: AggCount}, {Kind: AggCountDistinct, Col: "distinct_id"}},
	}
}

// writeSyntheticSurveyDay writes a raw source day shaped like the survey cube:
// skewed dims so ~33k cube rows, and skewed distinct_id counts so sketches range
// from a few bytes to the lgK=14 maximum.
func writeSyntheticSurveyDay(t *testing.T, db *sql.DB, path string) {
	t.Helper()
	q := fmt.Sprintf(`COPY (
		SELECT TIMESTAMPTZ '2026-06-01 00:00:00' + to_seconds((random()*86399)::INT) AS "time",
		  (ARRAY['survey shown','survey sent','survey dismissed','$pageview','$autocapture','app open','app close','purchase'])[1+(7*pow(random(),2))::INT] AS event,
		  'os_' || (1*random())::INT AS os_name,
		  'v' || (3*pow(random(),2))::INT AS app_version,
		  'survey_' || (7*pow(random(),2))::INT AS survey_name,
		  'resp_' || (2*random())::INT AS survey_response,
		  'u' || (300000*pow(random(),3))::INT AS distinct_id
		FROM range(3000000)) TO '%s' (FORMAT PARQUET)`, path)
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("synthetic source: %v", err)
	}
}

type rgStat struct {
	rows, sketchBytes int64
	evMin, evMax      string
}

func rowGroupStats(t *testing.T, db *sql.DB, file string) []rgStat {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(`
		SELECT s.row_group_id, any_value(s.row_group_num_rows), s.sk, e.mn, e.mx FROM
		 (SELECT row_group_id, row_group_num_rows, total_compressed_size AS sk FROM parquet_metadata('%[1]s') WHERE path_in_schema = '_theta_distinct_id') s
		 JOIN (SELECT row_group_id, coalesce(stats_min_value, stats_min, '?') AS mn, coalesce(stats_max_value, stats_max, '?') AS mx FROM parquet_metadata('%[1]s') WHERE path_in_schema = 'event') e USING (row_group_id)
		GROUP BY ALL ORDER BY 1`, file))
	if err != nil {
		t.Fatalf("parquet_metadata: %v", err)
	}
	defer rows.Close()
	var out []rgStat
	for rows.Next() {
		var id int64
		var r rgStat
		if err := rows.Scan(&id, &r.rows, &r.sketchBytes, &r.evMin, &r.evMax); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

// sketchBytesForEvent is what a query filtered on event = ev must download of the
// sketch column: every row group whose event min/max range can contain ev.
func sketchBytesForEvent(rgs []rgStat, ev string) (read, total int64, groups int) {
	for _, g := range rgs {
		total += g.sketchBytes
		if g.evMin <= ev && ev <= g.evMax {
			read += g.sketchBytes
			groups++
		}
	}
	return
}

func buildLayoutFile(t *testing.T, db *sql.DB, spec CubeSpec, src, dest string) DayEntry {
	t.Helper()
	// Same session settings as a production build connection: unordered parallel
	// writes must not defeat the sort.
	for _, s := range []string{"SET preserve_insertion_order=false", "SET threads=4"} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	e, err := BuildDay(db, spec, "'"+src+"'", "time", "2026-06-01", dest)
	if err != nil {
		t.Fatalf("BuildDay: %v", err)
	}
	return e
}

func TestCubeLayoutPrunesSketchColumn(t *testing.T) {
	db := openLocalDuckSketch(t)
	defer db.Close()
	db.SetMaxOpenConns(1)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.parquet")
	writeSyntheticSurveyDay(t, db, src)

	base := layoutSurveySpec()
	laid := base
	laid.Layout = FileLayout{RowGroupSize: 4096, SortFirst: []string{"event"}}

	plain := filepath.Join(dir, "plain.parquet")
	sorted := filepath.Join(dir, "sorted.parquet")
	ePlain := buildLayoutFile(t, db, base, src, plain)
	eSorted := buildLayoutFile(t, db, laid, src, sorted)
	if ePlain.Rows != eSorted.Rows || ePlain.BucketLo != eSorted.BucketLo || ePlain.BucketHi != eSorted.BucketHi {
		t.Fatalf("manifest entries differ: %+v vs %+v", ePlain, eSorted)
	}

	pRG, sRG := rowGroupStats(t, db, plain), rowGroupStats(t, db, sorted)
	if len(pRG) != 1 {
		t.Fatalf("default layout: %d row groups, want 1: %v %v", len(pRG), dumpRows(t, db, "SELECT count(*), count(DISTINCT event), count(DISTINCT bucket), count(DISTINCT os_name), count(DISTINCT app_version), count(DISTINCT survey_name), count(DISTINCT survey_response) FROM read_parquet('"+plain+"')"), dumpRows(t, db, "DESCRIBE SELECT * FROM read_parquet('"+plain+"')"))
	}
	if len(sRG) < 5 {
		t.Fatalf("laid-out file has %d row groups, want several for %d rows", len(sRG), eSorted.Rows)
	}
	for _, ev := range []string{"survey shown", "survey sent", "purchase"} {
		pr, pt, pg := sketchBytesForEvent(pRG, ev)
		sr, st, sg := sketchBytesForEvent(sRG, ev)
		t.Logf("event=%-14q rows=%d | default: %d RG, reads %.2f of %.2f MB sketch | sorted: %d RG, reads %.2f of %.2f MB (%.1f%%)",
			ev, eSorted.Rows, pg, mb(pr), mb(pt), sg, mb(sr), mb(st), 100*float64(sr)/float64(pr))
		if sr >= pr/2 {
			t.Errorf("event %q: sorted layout reads %d of %d sketch bytes, want < half", ev, sr, pr)
		}
	}
	t.Logf("file sizes: default %.2f MB, sorted %.2f MB", mb(fileSize(t, db, plain)), mb(fileSize(t, db, sorted)))

	for _, n := range []int{1024, 2048, 8192} {
		sp := base
		sp.Layout = FileLayout{RowGroupSize: n, SortFirst: []string{"event"}}
		f := filepath.Join(dir, fmt.Sprintf("sorted_%d.parquet", n))
		buildLayoutFile(t, db, sp, src, f)
		rg := rowGroupStats(t, db, f)
		r, tot, g := sketchBytesForEvent(rg, "survey shown")
		t.Logf("row_group_size=%d: %d RG, event='survey shown' reads %d RG, %.2f of %.2f MB sketch, file %.2f MB", n, len(rg), g, mb(r), mb(tot), mb(fileSize(t, db, f)))
	}

	q := func(f string) string {
		return fmt.Sprintf(`SELECT event, os_name, sum(_cnt)::BIGINT, round(datasketch_theta_estimate(datasketch_theta(14, _theta_distinct_id::sketch_theta)))::BIGINT
			FROM read_parquet('%s') WHERE event IN ('survey shown','purchase') GROUP BY ALL ORDER BY ALL`, f)
	}
	if a, b := dumpRows(t, db, q(plain)), dumpRows(t, db, q(sorted)); a == "" || a != b {
		t.Fatalf("readers disagree:\n%s\nvs\n%s", a, b)
	}
	all := func(f string) string {
		return fmt.Sprintf(`SELECT bucket, event, os_name, app_version, survey_name, survey_response, _cnt, md5(_theta_distinct_id) FROM read_parquet('%s') ORDER BY ALL`, f)
	}
	if a, b := dumpRows(t, db, all(plain)), dumpRows(t, db, all(sorted)); a != b {
		t.Fatal("full row sets differ between layouts")
	}
}

func mb(b int64) float64 { return float64(b) / (1 << 20) }

func fileSize(t *testing.T, db *sql.DB, f string) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(fmt.Sprintf("SELECT sum(total_compressed_size) FROM parquet_metadata('%s')", f)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func dumpRows(t *testing.T, db *sql.DB, q string) string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var sb strings.Builder
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(&sb, vals...)
	}
	return sb.String()
}

func TestCubeLayoutSQLOffIsUnchanged(t *testing.T) {
	spec := layoutSurveySpec()
	got := spec.BuildCopySQL("'x.parquet'", "time", "2026-06-01 00:00:00+00", "2026-06-02 00:00:00+00", "out.parquet")
	if strings.Contains(got, "ORDER BY") || strings.Contains(got, "ROW_GROUP_SIZE") || !strings.HasSuffix(got, "TO 'out.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)") {
		t.Fatalf("layout off must keep today's COPY, got %s", got)
	}
	if spec.mergeCopyOptions() != "(FORMAT parquet)" || spec.orderByClause() != "" {
		t.Fatal("layout off must keep today's merge options")
	}
	spec.Layout = FileLayout{RowGroupSize: 4096, SortFirst: []string{"EVENT", "nope"}}
	if got := spec.orderByClause(); got != ` ORDER BY "event", "app_version", "os_name", "survey_name", "survey_response", bucket` {
		t.Fatalf("order by = %s", got)
	}
}

func TestMergeCopySQLLayout(t *testing.T) {
	db := openLocalDuckSketch(t)
	defer db.Close()
	db.SetMaxOpenConns(1)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.parquet")
	writeSyntheticSurveyDay(t, db, src)
	base := layoutSurveySpec()
	day := filepath.Join(dir, "day.parquet")
	buildLayoutFile(t, db, base, src, day)

	branches := []string{
		fmt.Sprintf("SELECT *, 1 AS _src FROM read_parquet(['%s'])", day),
		fmt.Sprintf("SELECT *, 0 AS _src FROM read_parquet(['%s'])", day),
	}
	off, on := filepath.Join(dir, "off.parquet"), filepath.Join(dir, "on.parquet")
	laid := base
	laid.Layout = FileLayout{RowGroupSize: 4096, SortFirst: []string{"event"}}
	for _, c := range []struct {
		spec CubeSpec
		dest string
	}{{base, off}, {laid, on}} {
		if _, err := db.Exec(mergeCopySQL(c.spec, branches, c.dest)); err != nil {
			t.Fatalf("merge copy: %v", err)
		}
	}
	sel := func(f string) string {
		return fmt.Sprintf(`SELECT bucket, event, os_name, app_version, survey_name, survey_response, _cnt, md5(_theta_distinct_id) FROM read_parquet('%s') ORDER BY ALL`, f)
	}
	a, b := dumpRows(t, db, sel(off)), dumpRows(t, db, sel(on))
	if a == "" || a != b {
		t.Fatal("merged files differ between layouts")
	}
	if a != dumpRows(t, db, sel(day)) {
		t.Fatal("merge did not dedup to the daily rows")
	}
	if n := len(rowGroupStats(t, db, on)); n < 5 {
		t.Fatalf("merged laid-out file has %d row groups, want several", n)
	}
}
