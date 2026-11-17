package config

import (
	"strings"
	"testing"
)

func TestDailyTailOnlyConfig(t *testing.T) {
	cases := []struct {
		name                     string
		ratio, sealedMin, budget string
		wantRatio                float64
		wantSealedMin            int
		wantErr                  string
	}{
		{name: "defaults off", wantRatio: 0, wantSealedMin: 0},
		{name: "ratio, sealed size from the budget", ratio: "0.15", wantRatio: 0.15},
		{name: "ratio and sealed size", ratio: "0.15", sealedMin: "128", wantRatio: 0.15, wantSealedMin: 128},
		{name: "sealed size equal to the budget", ratio: "0.2", sealedMin: "1024", budget: "1024", wantRatio: 0.2, wantSealedMin: 1024},
		{name: "unbounded budget with a sealed size", ratio: "0.15", sealedMin: "256", budget: "0", wantRatio: 0.15, wantSealedMin: 256},
		{name: "negative ratio", ratio: "-0.1", wantErr: "compaction.daily_tail_only_ratio"},
		{name: "ratio above 1", ratio: "1.5", wantErr: "compaction.daily_tail_only_ratio"},
		{name: "negative sealed size", sealedMin: "-1", wantErr: "compaction.daily_tail_sealed_min_mb"},
		{name: "sealed size above the budget", sealedMin: "2048", budget: "1024", wantErr: "compaction.daily_tail_sealed_min_mb"},
		{name: "unbounded budget without a sealed size", ratio: "0.15", budget: "0", wantErr: "compaction.daily_tail_only_ratio"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for env, val := range map[string]string{
				"ARC_COMPACTION_DAILY_TAIL_ONLY_RATIO":    tc.ratio,
				"ARC_COMPACTION_DAILY_TAIL_SEALED_MIN_MB": tc.sealedMin,
				"ARC_COMPACTION_DAILY_MAX_OUTPUT_SIZE_MB": tc.budget,
			} {
				if val != "" {
					t.Setenv(env, val)
				}
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
			if cfg.Compaction.DailyTailOnlyRatio != tc.wantRatio || cfg.Compaction.DailyTailSealedMinMB != tc.wantSealedMin {
				t.Fatalf("tail-only = ratio %v / sealed %d MB, want %v / %d",
					cfg.Compaction.DailyTailOnlyRatio, cfg.Compaction.DailyTailSealedMinMB, tc.wantRatio, tc.wantSealedMin)
			}
		})
	}
}
