package metrics

import (
	"strings"
	"testing"
)

// The too-wide counter and gauge are on /metrics and in the snapshot.
func TestCompactionTooWideMetrics(t *testing.T) {
	m := &Metrics{}
	m.IncCompactionPartitionsSkippedTooWide()
	m.SetCompactionPartitionsTooWide(4)
	out := m.PrometheusFormat()
	for _, line := range []string{
		"\narc_compaction_partitions_skipped_too_wide_total 1\n",
		"\narc_compaction_partitions_too_wide 4\n",
		"# TYPE arc_compaction_partitions_too_wide gauge\n",
	} {
		if !strings.Contains(out, line) {
			t.Errorf("%q missing from /metrics", line)
		}
	}
	s := m.Snapshot()
	if s["compaction_partitions_skipped_too_wide"] != int64(1) || s["compaction_partitions_too_wide"] != int64(4) {
		t.Fatalf("snapshot %v %v", s["compaction_partitions_skipped_too_wide"], s["compaction_partitions_too_wide"])
	}
}
