package ingest

import (
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
)

// TestLateSplitDefaultOn verifies the default-on semantics: with
// LateWindowSeconds > 0, EVERY measurement late-splits unless it's listed in
// LateSplitExclude. This replaces the old opt-in LateSplitMeasurements gate.
func TestLateSplitDefaultOn(t *testing.T) {
	const lateWindow = 7200 // 2h
	oldBucket := time.Now().UTC().Add(-24 * time.Hour)       // far older than window -> late
	freshBucket := time.Now().UTC().Add(-30 * time.Minute)   // within window -> fresh

	t.Run("non-excluded measurement + old bucket routes to _late", func(t *testing.T) {
		b := &ArrowBuffer{config: &config.IngestConfig{
			LateWindowSeconds: lateWindow,
			FutureSkewSeconds: 300,
			LateSplitExclude:  []string{}, // nothing excluded
		}}
		if !b.lateSplitEnabled("events") {
			t.Fatalf("lateSplitEnabled(events) = false, want true (default-on)")
		}
		key := b.lateAwareStoragePath("posthog", "events", oldBucket)
		wantPrefix := "posthog/events" + LateSuffix + "/"
		if !strings.HasPrefix(key, wantPrefix) {
			t.Errorf("lateAwareStoragePath = %q, want prefix %q", key, wantPrefix)
		}
		if !b.isLateBucket("events", oldBucket) {
			t.Errorf("isLateBucket(events, old) = false, want true")
		}
	})

	t.Run("excluded measurement + old bucket routes to normal path", func(t *testing.T) {
		b := &ArrowBuffer{config: &config.IngestConfig{
			LateWindowSeconds: lateWindow,
			FutureSkewSeconds: 300,
			LateSplitExclude:  []string{"events"},
		}}
		if b.lateSplitEnabled("events") {
			t.Fatalf("lateSplitEnabled(events) = true, want false (excluded)")
		}
		if b.isLateBucket("events", oldBucket) {
			t.Errorf("isLateBucket(events, old) = true, want false (excluded -> normal path)")
		}
		key := b.lateAwareStoragePath("posthog", "events", oldBucket)
		if strings.Contains(key, LateSuffix) {
			t.Errorf("lateAwareStoragePath = %q, want normal Y/M/D/H path (no %q)", key, LateSuffix)
		}
		// A NON-excluded measurement on the same buffer still late-splits.
		if !b.isLateBucket("lifecycle", oldBucket) {
			t.Errorf("isLateBucket(lifecycle, old) = false, want true (not excluded)")
		}
	})

	t.Run("fresh bucket for non-excluded measurement routes to normal path", func(t *testing.T) {
		b := &ArrowBuffer{config: &config.IngestConfig{
			LateWindowSeconds: lateWindow,
			FutureSkewSeconds: 300,
			LateSplitExclude:  []string{},
		}}
		if b.isLateBucket("events", freshBucket) {
			t.Errorf("isLateBucket(events, fresh) = true, want false")
		}
		key := b.lateAwareStoragePath("posthog", "events", freshBucket)
		if strings.Contains(key, LateSuffix) {
			t.Errorf("lateAwareStoragePath(fresh) = %q, want normal path (no %q)", key, LateSuffix)
		}
	})

	t.Run("LateWindowSeconds=0 always routes to normal path", func(t *testing.T) {
		b := &ArrowBuffer{config: &config.IngestConfig{
			LateWindowSeconds: 0,
			FutureSkewSeconds: 300,
			LateSplitExclude:  []string{},
		}}
		if b.lateSplitEnabled("events") {
			t.Fatalf("lateSplitEnabled(events) = true with window=0, want false")
		}
		if b.isLateBucket("events", oldBucket) {
			t.Errorf("isLateBucket(events, old) = true with window=0, want false")
		}
		key := b.lateAwareStoragePath("posthog", "events", oldBucket)
		if strings.Contains(key, LateSuffix) {
			t.Errorf("lateAwareStoragePath = %q with window=0, want normal path (no %q)", key, LateSuffix)
		}
	})
}
