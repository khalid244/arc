package compaction

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// TestShouldCompact_RemergesFragmentedTierOutputs verifies F3: when a
// partition has 2+ already-tier-compacted files but zero source files,
// the tier triggers a re-merge to consolidate the fragments.
//
// This heals partitions stuck in a fragmented state — for example, the
// default/devices days with 9-14 _daily.parquet files each, where the
// adaptive-split partial-success bug left multiple compacted outputs.
// Before F3 these partitions stayed fragmented forever because
// `compacted >= 2 && uncompacted == 0` returned false from ShouldCompact.
func TestShouldCompact_RemergesFragmentedTierOutputs(t *testing.T) {
	tier := &BaseTier{
		MinFiles: 2,
		Logger:   zerolog.Nop(),
	}

	isHourlyInput := func(f string) bool {
		// hourly input has 7 path parts (database/measurement/Y/M/D/H/file.parquet)
		return strings.Count(f, "/") == 6
	}

	tests := []struct {
		name     string
		files    []string
		want     bool
		describe string
	}{
		{
			name: "two_daily_zero_sources_should_remerge",
			files: []string{
				"posthog/events/2026/02/18/events_a_daily.parquet",
				"posthog/events/2026/02/18/events_b_daily.parquet",
			},
			want:     true,
			describe: "fragmented partition with no new input — should re-merge",
		},
		{
			name: "thirteen_daily_zero_sources_should_remerge",
			files: []string{
				"posthog/events/2026/02/18/events_a_daily.parquet",
				"posthog/events/2026/02/18/events_b_daily.parquet",
				"posthog/events/2026/02/18/events_c_daily.parquet",
				"posthog/events/2026/02/18/events_d_daily.parquet",
				"posthog/events/2026/02/18/events_e_daily.parquet",
				"posthog/events/2026/02/18/events_f_daily.parquet",
				"posthog/events/2026/02/18/events_g_daily.parquet",
				"posthog/events/2026/02/18/events_h_daily.parquet",
				"posthog/events/2026/02/18/events_i_daily.parquet",
				"posthog/events/2026/02/18/events_j_daily.parquet",
				"posthog/events/2026/02/18/events_k_daily.parquet",
				"posthog/events/2026/02/18/events_l_daily.parquet",
				"posthog/events/2026/02/18/events_m_daily.parquet",
			},
			want:     true,
			describe: "heavily fragmented partition — should still re-merge",
		},
		{
			name: "one_daily_zero_sources_should_NOT_compact",
			files: []string{
				"posthog/events/2026/02/18/events_a_daily.parquet",
			},
			want:     false,
			describe: "clean partition with single daily file — nothing to do",
		},
		{
			name: "one_daily_one_uncompacted_should_NOT_compact",
			files: []string{
				"posthog/events/2026/02/18/events_a_daily.parquet",
				"posthog/events/2026/02/18/14/events_x_compacted.parquet",
			},
			want:     false,
			describe: "single new uncompacted below MinFiles — wait for more",
		},
		{
			name: "one_daily_two_uncompacted_should_compact",
			files: []string{
				"posthog/events/2026/02/18/events_a_daily.parquet",
				"posthog/events/2026/02/18/14/events_x_compacted.parquet",
				"posthog/events/2026/02/18/15/events_y_compacted.parquet",
			},
			want:     true,
			describe: "existing case 2 (new input on top of existing daily) still works",
		},
		{
			name: "two_uncompacted_zero_daily_should_compact",
			files: []string{
				"posthog/events/2026/02/18/14/events_x_compacted.parquet",
				"posthog/events/2026/02/18/15/events_y_compacted.parquet",
			},
			want:     true,
			describe: "existing case 1 (first-time daily merge) still works",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tier.ShouldCompactByFileSuffix(tc.files, "_daily.parquet", isHourlyInput)
			if got != tc.want {
				t.Errorf("%s: want %v got %v", tc.describe, tc.want, got)
			}
		})
	}
}
