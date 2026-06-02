package rollup

import "testing"

// TestCompactedManifest_BuiltDays proves a compacted month file reports every
// daily date it absorbed, so the build loop won't rebuild (and double-count) them.
func TestCompactedManifest_BuiltDays(t *testing.T) {
	m := &Manifest{Days: []DayEntry{
		{Date: "2026-02", URI: "s3://b/_arc/rollup/default/downloads/coarse/m_2026-02_1.parquet",
			BucketLo: "2026-02-01 00:00:00", BucketHi: "2026-03-01 00:00:00",
			Covers: []string{"2026-02-01", "2026-02-02", "2026-02-03"}},
		{Date: "2026-03-01", URI: "s3://b/.../2026-03-01.parquet",
			BucketLo: "2026-03-01 00:00:00", BucketHi: "2026-03-02 00:00:00"},
	}}
	built := m.BuiltDays()
	for _, d := range []string{"2026-02-01", "2026-02-02", "2026-02-03", "2026-03-01"} {
		if !built[d] {
			t.Errorf("BuiltDays missing %s (compacted day not marked built)", d)
		}
	}
	if built["2026-02"] {
		t.Error("the month label itself should not be a built day")
	}
	if len(built) != 4 {
		t.Errorf("BuiltDays size = %d, want 4", len(built))
	}
}

// TestCompactedManifest_RangePrune proves the read path still selects the right
// files when a month is a single compacted entry: pruning is by bucket span, so a
// sub-month query picks the whole monthly file (and a daily file unchanged).
func TestCompactedManifest_RangePrune(t *testing.T) {
	m := &Manifest{Days: []DayEntry{
		{Date: "2026-02", URI: "month.parquet", BucketLo: "2026-02-01 00:00:00", BucketHi: "2026-03-01 00:00:00",
			Covers: []string{"2026-02-10"}},
		{Date: "2026-03-15", URI: "day.parquet", BucketLo: "2026-03-15 00:00:00", BucketHi: "2026-03-16 00:00:00"},
	}}
	// A query inside February overlaps only the monthly file.
	got := m.DaysInRange("2026-02-10 00:00:00", "2026-02-20 00:00:00")
	if len(got) != 1 || got[0].URI != "month.parquet" {
		t.Fatalf("Feb sub-range -> %v, want [month.parquet]", uris(got))
	}
	// A query spanning Feb..Mar picks both files; ReadExpr lists both URIs.
	got = m.DaysInRange("2026-02-15 00:00:00", "2026-03-16 00:00:00")
	if len(got) != 2 {
		t.Fatalf("Feb..Mar -> %v, want both", uris(got))
	}
	if ReadExpr(got) != "['month.parquet', 'day.parquet']" {
		t.Errorf("ReadExpr = %q", ReadExpr(got))
	}
	// A March-only query must NOT read the February monthly file.
	got = m.DaysInRange("2026-03-15 00:00:00", "2026-03-16 00:00:00")
	if len(got) != 1 || got[0].URI != "day.parquet" {
		t.Errorf("Mar-only -> %v, want [day.parquet]", uris(got))
	}
}

func uris(ds []DayEntry) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.URI
	}
	return out
}
