//go:build duckdb_arrow

package compaction

import (
	"fmt"
	"testing"
)

// TestCompactionDedupPolicy_ExactRowsOnly pins the merge's dedup rule through
// the production compaction path (Job.compactFiles): every batch is
// deduplicated to EXACT rows — a row is dropped only when another row of the
// batch is identical to it in every column. Tags (when present) and time only
// narrow where whole rows are compared; they never decide on their own that two
// rows are duplicates.
//
// Upstream #1234 gated dedup on footer metadata and left tagless batches
// alone. Production files carry no arc:tags footer, so that gate would have
// turned dedup off for all production data and kept every crash-replay
// duplicate forever; the fork's earlier tagged path kept one row per
// (tags, time) and so deleted rows that differed in a field. Both are pinned
// against here.
func TestCompactionDedupPolicy_ExactRowsOnly(t *testing.T) {
	// (a) Two rows sharing (tags, time) but differing in a field are
	// different data: both survive. The exact copy of one of them collapses.
	t.Run("tagged_rows_sharing_tags_and_time_but_differing_in_a_field_both_survive", func(t *testing.T) {
		e := newMarkerEnv(t)
		a := e.write(markerFixture{rows: union(row("h1", t1, 1), row("h1", t1, 2)), kv: tagged})
		b := e.write(markerFixture{rows: row("h1", t1, 1), kv: tagged}) // exact copy of one row
		_, got, _, _ := e.compact(a, b)
		expectRows(t, got, "h1@0=1", "h1@0=2")
	})

	// (b) Exact duplicate rows in a tagless batch — the production shape, and
	// what a WAL replay or a retried upload produces — are reduced to one.
	t.Run("tagless_exact_duplicates_are_reduced_to_one", func(t *testing.T) {
		e := newMarkerEnv(t)
		a := e.write(markerFixture{rows: union(row("h1", t1, 1), row("h2", t1, 5), row("h1", t2, 7))})
		b := e.write(markerFixture{rows: union(row("h1", t1, 1), row("h2", t1, 5))})
		c := e.write(markerFixture{rows: row("h1", t1, 1)})
		_, got, _, _ := e.compact(a, b, c)
		expectRows(t, got, "h1@0=1", "h1@1=7", "h2@0=5")
	})

	// The tagless rule is exact too: sharing time (and every other column)
	// except one field keeps both rows.
	t.Run("tagless_rows_differing_in_a_field_both_survive", func(t *testing.T) {
		e := newMarkerEnv(t)
		a := e.write(markerFixture{rows: row("h1", t1, 1)})
		b := e.write(markerFixture{rows: union(row("h1", t1, 1), row("h1", t1, 1.5))})
		_, got, _, _ := e.compact(a, b)
		expectRows(t, got, "h1@0=1", "h1@0=1.5")
	})

	// NULLs compare equal for dedup (an identical row with a NULL field is
	// still a duplicate) and never make two rows equal that differ elsewhere.
	t.Run("null_fields", func(t *testing.T) {
		for name, kv := range map[string]string{"tagged": tagged, "tagless": ""} {
			t.Run(name, func(t *testing.T) {
				e := newMarkerEnv(t)
				nullRow := func(v string) string {
					return fmt.Sprintf(`SELECT 'h1' AS host, %s AS "time", %s::DOUBLE AS v, NULL::VARCHAR AS note`, t1, v)
				}
				a := e.write(markerFixture{rows: union(nullRow("1"), nullRow("NULL")), kv: kv})
				b := e.write(markerFixture{rows: union(nullRow("1"), nullRow("NULL")), kv: kv})
				out, _, _, _ := e.compactRaw(a, b)
				var n, nullV int
				if err := e.db.QueryRow(fmt.Sprintf(
					`SELECT count(*), count(*) FILTER (WHERE v IS NULL) FROM read_parquet('%s')`,
					escapeSQLPath(out))).Scan(&n, &nullV); err != nil {
					t.Fatal(err)
				}
				if n != 2 || nullV != 1 {
					t.Errorf("rows=%d null-v rows=%d, want 2 rows of which 1 has v NULL", n, nullV)
				}
			})
		}
	})
}
