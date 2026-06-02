package rollup

import "testing"

// TestSkipMeasurement pins the discovery filter: normal tables are rolled up,
// while internal (_-prefixed), late-arrival (*_late), and operator-excluded
// measurements are skipped. *_late must match as a suffix only.
func TestSkipMeasurement(t *testing.T) {
	m := &Manager{cfg: Config{ExcludeMeasurements: []string{"events_raw"}}}
	cases := []struct {
		db, meas string
		skip     bool
	}{
		{"default", "downloads", false},     // normal table — built
		{"default", "events", false},        // normal table — built
		{"default", "events_late", true},    // late-arrival variant — auto-skipped
		{"default", "downloads_late", true}, // any *_late — auto-skipped (no glob/list needed)
		{"default", "late_events", false},   // "_late" is a SUFFIX, not a substring
		{"default", "_internal", true},      // _-prefixed measurement — skipped
		{"_meta", "downloads", true},        // _-prefixed database — skipped
		{"default", "events_raw", true},     // operator exclude_measurements (exact match)
		{"", "downloads", true},             // empty db
		{"default", "", true},               // empty measurement
	}
	for _, c := range cases {
		if got := m.skipMeasurement(c.db, c.meas); got != c.skip {
			t.Errorf("skipMeasurement(%q, %q) = %v, want %v", c.db, c.meas, got, c.skip)
		}
	}
}
