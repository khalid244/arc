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

// `*_late` is non-negotiable: even when the user provides an explicit
// exclude list, Defaults guarantees `*_late` is in it. Rolling up late
// variants would double-count their data, so this baseline is enforced.
func TestConfig_Defaults_AlwaysIncludesLatePattern(t *testing.T) {
	cfg := &Config{ExcludeTables: []string{"*_test", "foo"}}
	cfg.Defaults()
	found := false
	for _, p := range cfg.ExcludeTables {
		if p == "*_late" {
			found = true
		}
	}
	if !found {
		t.Fatalf("Defaults must always include *_late; got %v", cfg.ExcludeTables)
	}
	// User patterns preserved too
	if !sliceHas(cfg.ExcludeTables, "*_test") || !sliceHas(cfg.ExcludeTables, "foo") {
		t.Fatalf("user patterns must be preserved; got %v", cfg.ExcludeTables)
	}
}

func TestConfig_Defaults_DoesNotDuplicateLate(t *testing.T) {
	cfg := &Config{ExcludeTables: []string{"*_late"}}
	cfg.Defaults()
	n := 0
	for _, p := range cfg.ExcludeTables {
		if p == "*_late" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("*_late should appear exactly once; got %v", cfg.ExcludeTables)
	}
}

func sliceHas(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
