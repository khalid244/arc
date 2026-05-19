package tiered

import (
	"testing"
	"time"
)

// bucketFileID is deterministic — same inputs always yield the same ID.
// This is the property that makes re-publishes idempotent: the second
// PUT lands on the same S3 key and overwrites the first object atomically
// instead of leaving 100 duplicate files per bucket.
func TestBucketFileID_DeterministicPerBucket(t *testing.T) {
	bucket := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	id1 := bucketFileID("default.events", Tier1h, "by_site", bucket, "spec_hash_a")
	id2 := bucketFileID("default.events", Tier1h, "by_site", bucket, "spec_hash_a")
	if id1 != id2 {
		t.Fatalf("id1=%q id2=%q — bucketFileID must be deterministic", id1, id2)
	}
}

// Every component that distinguishes one bucket from another must
// participate in the hash, or two distinct buckets would collide.
func TestBucketFileID_DifferentInputsDifferentIDs(t *testing.T) {
	base := func() (string, Tier, string, time.Time, string) {
		return "default.events", Tier1h, "by_site",
			time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC), "spec_hash_a"
	}
	baseID := bucketFileID(base())

	cases := []struct {
		name string
		mk   func() string
	}{
		{"different table", func() string {
			return bucketFileID("other.events", Tier1h, "by_site",
				time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC), "spec_hash_a")
		}},
		{"different variant", func() string {
			return bucketFileID("default.events", Tier1h, "sketch",
				time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC), "spec_hash_a")
		}},
		{"different bucket day", func() string {
			return bucketFileID("default.events", Tier1h, "by_site",
				time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC), "spec_hash_a")
		}},
		{"different schema hash", func() string {
			return bucketFileID("default.events", Tier1h, "by_site",
				time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC), "spec_hash_b")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.mk(); got == baseID {
				t.Fatalf("%s: ID %q matched baseID; component must influence hash", tc.name, got)
			}
		})
	}
}

// The id is short enough to be a clean URL/key segment and exactly 16
// hex chars (8 bytes). Catches accidental length drift.
func TestBucketFileID_Length(t *testing.T) {
	id := bucketFileID("t", Tier1h, "v", time.Now(), "h")
	if len(id) != 16 {
		t.Fatalf("expected 16-hex-char ID, got %d (%q)", len(id), id)
	}
}

// Two different bucketLo timestamps within the same day SHOULD still
// produce different IDs — the bucket key is the bucketLo *instant*, not
// the calendar day. (The scheduler always passes day-aligned UTC
// midnight for 1h tier; this test pins that contract.)
func TestBucketFileID_BucketLoTimestampMatters(t *testing.T) {
	t1 := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 15, 1, 0, 0, 0, time.UTC)
	id1 := bucketFileID("t", Tier1h, "v", t1, "h")
	id2 := bucketFileID("t", Tier1h, "v", t2, "h")
	if id1 == id2 {
		t.Fatalf("different bucketLo timestamps produced same ID: %q", id1)
	}
}
