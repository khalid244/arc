package tiered

import "testing"

func TestConfig_IsExcluded(t *testing.T) {
	cfg := &Config{ExcludeTables: []string{"*_late", "*_test", "foo"}}

	cases := []struct {
		table string
		want  bool
	}{
		{"default.downloads_late", true},
		{"default.events_late", true},
		{"posthog.events_test", true},
		{"default.foo", true},          // exact-name match
		{"default.downloads", false},
		{"default.events", false},
		{"default.foobar", false},      // glob requires exact match without wildcard
		{"downloads_late", true},       // unqualified name works too
		{"downloads", false},
	}
	for _, tc := range cases {
		t.Run(tc.table, func(t *testing.T) {
			if got := cfg.IsExcluded(tc.table); got != tc.want {
				t.Fatalf("IsExcluded(%q) = %v, want %v", tc.table, got, tc.want)
			}
		})
	}
}

func TestConfig_IsExcluded_EmptyPatternsNeverMatch(t *testing.T) {
	cfg := &Config{ExcludeTables: nil}
	if cfg.IsExcluded("anything") {
		t.Fatalf("nil ExcludeTables should not match")
	}
}

func TestConfig_Defaults_AddsLatePattern(t *testing.T) {
	cfg := &Config{Enabled: true, TZ: "UTC"}
	cfg.Defaults()
	if len(cfg.ExcludeTables) != 1 || cfg.ExcludeTables[0] != "*_late" {
		t.Fatalf("Defaults should set ExcludeTables=[*_late], got %v", cfg.ExcludeTables)
	}
}

// A user-supplied empty slice (explicit `exclude_tables = []` in TOML)
// should NOT be overwritten by Defaults — they wanted to disable the filter.
func TestConfig_Defaults_PreservesExplicitEmpty(t *testing.T) {
	cfg := &Config{ExcludeTables: []string{}}
	cfg.Defaults()
	// nil-vs-empty distinction: only nil triggers the default.
	if cfg.ExcludeTables == nil {
		t.Fatalf("explicit empty slice should not be replaced with default")
	}
	if len(cfg.ExcludeTables) != 0 {
		t.Fatalf("explicit empty slice should stay empty, got %v", cfg.ExcludeTables)
	}
}
