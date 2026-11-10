package config

import (
	"strings"
	"testing"
)

func TestHourlyClosedGraceMinutesConfig(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    int
		wantErr bool
	}{
		{"default disabled", "", 0, false},
		{"set", "10", 10, false},
		{"negative", "-1", 0, true},
		{"above a day", "1441", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			if tc.value != "" {
				t.Setenv("ARC_COMPACTION_HOURLY_CLOSED_GRACE_MINUTES", tc.value)
			}
			cfg, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "compaction.hourly_closed_grace_minutes") {
					t.Fatalf("got %v, want a compaction.hourly_closed_grace_minutes validation error", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Compaction.HourlyClosedGraceMinutes != tc.want {
				t.Fatalf("HourlyClosedGraceMinutes = %d, want %d", cfg.Compaction.HourlyClosedGraceMinutes, tc.want)
			}
		})
	}
}

func TestHourlyClosedRemergeThrottleConfig(t *testing.T) {
	cases := []struct {
		name               string
		minFiles, maxWait  string
		wantFiles, wantMin int
		wantErr            string
	}{
		{"defaults", "", "", 30, 30, ""},
		{"set", "50", "45", 50, 45, ""},
		{"throttle off", "0", "", 0, 30, ""},
		{"negative files", "-1", "", 0, 0, "compaction.hourly_closed_remerge_min_files"},
		{"negative wait", "", "-1", 0, 0, "compaction.hourly_closed_remerge_max_wait_minutes"},
		{"wait above a day", "", "1441", 0, 0, "compaction.hourly_closed_remerge_max_wait_minutes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			if tc.minFiles != "" {
				t.Setenv("ARC_COMPACTION_HOURLY_CLOSED_REMERGE_MIN_FILES", tc.minFiles)
			}
			if tc.maxWait != "" {
				t.Setenv("ARC_COMPACTION_HOURLY_CLOSED_REMERGE_MAX_WAIT_MINUTES", tc.maxWait)
			}
			cfg, err := Load()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("got %v, want a %s validation error", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Compaction.HourlyRemergeMinFiles != tc.wantFiles || cfg.Compaction.HourlyRemergeMaxWaitMinutes != tc.wantMin {
				t.Fatalf("throttle = %d files / %d min, want %d / %d",
					cfg.Compaction.HourlyRemergeMinFiles, cfg.Compaction.HourlyRemergeMaxWaitMinutes, tc.wantFiles, tc.wantMin)
			}
		})
	}
}

// The hourly scan lists an hour partition only once it started
// hourly_min_age_hours ago (compaction hourly.go groupFilesByHourPartition), so
// with a larger value the grace cannot act at close + grace.
func TestHourlyClosedGraceNeedsMinAgeOneHour(t *testing.T) {
	cases := []struct {
		name, grace, minAge string
		wantErr             bool
	}{
		{"grace with min age 1", "10", "1", false},
		{"grace with min age 0 (raised to 1)", "10", "0", false},
		{"grace with min age 2", "10", "2", true},
		{"no grace, min age 2", "0", "2", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_COMPACTION_HOURLY_CLOSED_GRACE_MINUTES", tc.grace)
			t.Setenv("ARC_COMPACTION_HOURLY_MIN_AGE_HOURS", tc.minAge)
			_, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "compaction.hourly_min_age_hours") {
					t.Fatalf("got %v, want an error naming compaction.hourly_min_age_hours", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHourlyDropConsumedInputsConfig(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		want        bool
	}{
		{"default on", "", true},
		{"off", "false", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			if tc.value != "" {
				t.Setenv("ARC_COMPACTION_HOURLY_DROP_CONSUMED_INPUTS", tc.value)
			}
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Compaction.HourlyDropConsumedInputs != tc.want {
				t.Fatalf("HourlyDropConsumedInputs = %v, want %v", cfg.Compaction.HourlyDropConsumedInputs, tc.want)
			}
		})
	}
}
