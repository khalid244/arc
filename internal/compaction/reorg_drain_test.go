package compaction

import (
	"testing"
	"time"
)

func h(hourUTC int) time.Time {
	return time.Date(2026, 5, 30, hourUTC, 0, 0, 0, time.UTC)
}

func TestSelectBuckets_OldestFirstAndCapped(t *testing.T) {
	buckets := map[time.Time][]string{
		h(5): {"f5"}, h(1): {"f1"}, h(3): {"f3"}, h(2): {"f2"}, h(4): {"f4"},
	}
	got := selectBuckets(buckets, 3)
	want := []time.Time{h(1), h(2), h(3)}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("idx %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestSelectBuckets_UnlimitedWhenZeroOrNegative(t *testing.T) {
	buckets := map[time.Time][]string{h(2): {"f2"}, h(1): {"f1"}}
	for _, max := range []int{0, -1} {
		got := selectBuckets(buckets, max)
		if len(got) != 2 || !got[0].Equal(h(1)) || !got[1].Equal(h(2)) {
			t.Fatalf("max=%d: got %v, want [h1 h2]", max, got)
		}
	}
}
