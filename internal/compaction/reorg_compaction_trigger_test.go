package compaction

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// TestShouldCompact_FoldsReorgFilesBelowMinFiles verifies the reorg-aware
// compaction trigger: when a late-arrival reorganizer output (a `_reorg_`
// file) lands in a partition that already holds a tier-compacted file, the
// tier must re-merge it IMMEDIATELY — regardless of MinFiles.
//
// Why this matters (the gap it closes): reorg's crash-recovery contract
// permits a bounded set of DUPLICATE target files (a crash between upload
// and source-delete re-runs the bucket, writing a second identical
// `events_reorg_<jobID>_<seq>.parquet`). Those duplicates are only folded
// away as a side effect of tier compaction's dedup. But the normal trigger
// is gated on MinFiles (10 hourly / 12 daily): a small duplicate set in an
// already-compacted OLD partition never reaches the threshold, so the
// duplicate rows stay live and double-count queries indefinitely. This case
// makes "a reorg file sitting next to a sealed file" a sufficient trigger on
// its own — mirroring Case 3's "heal a partition the threshold logic would
// otherwise leave wrong."
func TestShouldCompact_FoldsReorgFilesBelowMinFiles(t *testing.T) {
	// MinFiles deliberately high (10) so every "want: true" below proves the
	// trigger fires DESPITE being under the file-count threshold.
	tier := &BaseTier{
		MinFiles: 10,
		Logger:   zerolog.Nop(),
	}

	// Production hourly classifier: anything not ending _compacted.parquet is
	// uncompacted input (hourly.go).
	isHourlyInput := func(f string) bool {
		return !strings.Contains(f, "_compacted.parquet")
	}
	// Production daily classifier: 7-path-part (hour-level) files are input
	// (daily.go). A reorg file lives at db/m/Y/M/D/H/file = 7 parts.
	isDailyInput := func(f string) bool {
		return strings.Count(f, "/") == 6
	}

	tests := []struct {
		name     string
		suffix   string
		input    func(string) bool
		files    []string
		want     bool
		describe string
	}{
		{
			name:   "hourly_one_reorg_with_compacted_below_minfiles_should_compact",
			suffix: "_compacted.parquet",
			input:  isHourlyInput,
			files: []string{
				"posthog/events/2026/02/18/14/events_20260218_140000_111_compacted.parquet",
				"posthog/events/2026/02/18/14/events_reorg_1717252801123456789_0.parquet",
			},
			want:     true,
			describe: "late reorg file on a sealed hour partition — fold now, not at MinFiles",
		},
		{
			name:   "hourly_two_reorg_dups_with_compacted_should_compact",
			suffix: "_compacted.parquet",
			input:  isHourlyInput,
			files: []string{
				"posthog/events/2026/02/18/14/events_20260218_140000_111_compacted.parquet",
				"posthog/events/2026/02/18/14/events_reorg_1717252801123456789_0.parquet",
				"posthog/events/2026/02/18/14/events_reorg_1717252899987654321_0.parquet",
			},
			want:     true,
			describe: "crash-induced duplicate reorg pair on a sealed partition — must fold",
		},
		{
			name:   "daily_reorg_hour_file_with_daily_should_compact",
			suffix: "_daily.parquet",
			input:  isDailyInput,
			files: []string{
				"posthog/events/2026/02/18/events_20260218_daily.parquet",
				"posthog/events/2026/02/18/14/events_reorg_1717252801123456789_0.parquet",
			},
			want:     true,
			describe: "emptied hour dir: dup folds at the daily tier (reorg 7-part + _daily)",
		},
		{
			name:   "hourly_only_compacted_no_reorg_should_NOT_compact",
			suffix: "_compacted.parquet",
			input:  isHourlyInput,
			files: []string{
				"posthog/events/2026/02/18/14/events_20260218_140000_111_compacted.parquet",
			},
			want:     false,
			describe: "post-fold steady state — no reorg marker left, must not re-fire (no loop)",
		},
		{
			name:   "hourly_compacted_plus_one_normal_uncompacted_should_NOT_compact",
			suffix: "_compacted.parquet",
			input:  isHourlyInput,
			files: []string{
				"posthog/events/2026/02/18/14/events_20260218_140000_111_compacted.parquet",
				"posthog/events/2026/02/18/14/events_20260218_141500_222.parquet",
			},
			want:     false,
			describe: "ordinary sub-threshold late file (not reorg) must still wait for MinFiles",
		},
		{
			name:   "hourly_lone_reorg_no_compacted_should_NOT_compact",
			suffix: "_compacted.parquet",
			input:  isHourlyInput,
			files: []string{
				"posthog/events/2026/02/18/14/events_reorg_1717252801123456789_0.parquet",
			},
			want:     false,
			describe: "single reorg file with nothing sealed to merge into — nothing to fold yet",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tier.ShouldCompactByFileSuffix(tc.files, tc.suffix, tc.input)
			if got != tc.want {
				t.Errorf("%s: want %v got %v", tc.describe, tc.want, got)
			}
		})
	}
}
