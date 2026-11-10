package config

import (
	"os"
	"strings"
	"testing"
)

// query.tail_abandoned_max bounds the X-Arc-Tail-Fallback merges left
// running past their deadline: default 4, 0 = never abandon, max 64.
func TestQueryTailAbandonedMaxConfig(t *testing.T) {
	load := func(t *testing.T, toml string) (*Config, error) {
		t.Helper()
		dir := t.TempDir()
		t.Chdir(dir)
		if toml != "" {
			if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return Load()
	}
	for _, tc := range []struct {
		name, toml, env string
		want            int
	}{
		{"default", "", "", 4},
		{"toml", "[query]\ntail_abandoned_max = 0\n", "", 0},
		{"env", "", "8", 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.env != "" {
				t.Setenv("ARC_QUERY_TAIL_ABANDONED_MAX", tc.env)
			}
			cfg, err := load(t, tc.toml)
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.Query.TailAbandonedMax; got != tc.want {
				t.Fatalf("tail_abandoned_max = %d, want %d", got, tc.want)
			}
		})
	}
	for _, bad := range []string{"-1", "65"} {
		t.Run("invalid "+bad, func(t *testing.T) {
			if _, err := load(t, "[query]\ntail_abandoned_max = "+bad+"\n"); err == nil || !strings.Contains(err.Error(), "query.tail_abandoned_max") {
				t.Fatalf("Load() error = %v, want one naming query.tail_abandoned_max", err)
			}
		})
	}
}
