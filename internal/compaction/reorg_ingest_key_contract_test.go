package compaction

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
)

// Every late-sidecar key the ingest LateStager actually writes must be a drain
// source for the reorganizer, for every pod-name shape Kubernetes produces;
// otherwise late rows sit in <m>_late/ forever. Keys come from the ingest
// package itself, not a copy of its format.
func TestReorgAcceptsEveryIngestLateStagerKey(t *testing.T) {
	created := time.Date(2026, 10, 13, 4, 5, 6, 7, time.UTC)
	pods := []string{
		"hammel-arc-ingest-6d8f9c7b5-x2k4p", // Deployment pod
		"arc-ingest-0",                      // StatefulSet pod
		"Ingest_Pod.With.Dots",              // sanitized by ingest
		"a",
		"pod-with-a-very-long-name-that-exceeds-typical-kubernetes-limits-1234567890",
	}
	for _, pod := range pods {
		for _, hex := range []string{"0123456789abcdef", "ffffffffffffffff", "0000000000000000"} {
			key := ingest.LateStagerObjectKey("posthog", "events", pod, hex, created)
			if got := classifySidecarKey(key, "posthog/events_late/", "events_late"); got != sidecarKeySource {
				t.Errorf("pod %q: reorg classifies %s as %v, want a drain source", pod, key, got)
			}
			ft, ok := parseFilenameTime(key)
			if !ok || !ft.Equal(created.Truncate(time.Second)) {
				t.Errorf("pod %q: parseFilenameTime(%s) = %v,%v, want %v", pod, key, ft, ok, created.Truncate(time.Second))
			}
		}
	}
}
