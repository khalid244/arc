package compaction

import (
	"testing"

	"github.com/rs/zerolog"
)

// Cycle 4a: pin that JobConfig accepts MaxOutputBytes and it lands on
// the constructed Job. Used downstream by subprocess.go to decide
// between single-file COPY and the multi-output FILE_SIZE_BYTES path.

func TestJob_MaxOutputBytes_PropagatedFromConfig(t *testing.T) {
	job := NewJob(&JobConfig{
		Measurement:    "events",
		PartitionPath:  "posthog/events/2026/05/19",
		Database:       "posthog",
		MaxOutputBytes: 1024 * 1024 * 1024, // 1 GB
		Logger:         zerolog.Nop(),
	})
	if job.MaxOutputBytes != 1024*1024*1024 {
		t.Errorf("Job.MaxOutputBytes: got %d, want %d", job.MaxOutputBytes, 1024*1024*1024)
	}
}

func TestJob_MaxOutputBytes_DefaultZero(t *testing.T) {
	job := NewJob(&JobConfig{
		Measurement:   "events",
		PartitionPath: "posthog/events/2026/05/19",
		Database:      "posthog",
		Logger:        zerolog.Nop(),
	})
	if job.MaxOutputBytes != 0 {
		t.Errorf("Job.MaxOutputBytes default: got %d, want 0 (legacy single-file mode)", job.MaxOutputBytes)
	}
}
