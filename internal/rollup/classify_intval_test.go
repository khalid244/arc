package rollup

import "testing"

// TestContinuousDimEligible pins the rule that lets a categorical code mis-typed as a
// float (e.g. an HTTP status stored as DOUBLE) gain a per-dim cube: it must be a
// continuous type, integer-valued in the sample, and low-card (<= MaxDimCard). A
// fractional or high-card float, or a non-continuous type, is not dim-eligible here.
func TestContinuousDimEligible(t *testing.T) {
	cfg := ClassifyConfig{MaxDimCard: 1024, MaxPerDimCard: 50000}
	cases := []struct {
		typ       string
		card      int
		intValued bool
		want      bool
	}{
		{"DOUBLE", 2, true, true},      // HTTP-status-as-DOUBLE: low-card, integer-valued -> dim
		{"DECIMAL(18,2)", 5, true, true}, // integer-valued decimal code -> dim
		{"DOUBLE", 2, false, false},    // fractional measure (price/latency) -> not a dim
		{"DOUBLE", 2000, true, false},  // integer-valued but high-card (> MaxDimCard) -> not a dim
		{"DOUBLE", 0, true, false},     // all-NULL in sample -> not a dim
		{"BIGINT", 2, true, false},     // not continuous: classifyColumn already makes ints dims
		{"VARCHAR", 2, true, false},    // not continuous
	}
	for _, c := range cases {
		if got := continuousDimEligible(c.typ, c.card, c.intValued, cfg); got != c.want {
			t.Errorf("continuousDimEligible(%q, card=%d, intValued=%v) = %v, want %v",
				c.typ, c.card, c.intValued, got, c.want)
		}
	}
}

// TestProfileTable_IntegerValuedFloatBecomesDim reproduces the prod default.downloads
// case: `response` is an HTTP status stored as DOUBLE (integer-valued, 2 distinct), so
// it was force-classified metric-only and never got a by_response cube. After the fix it
// must be BOTH a metric (AVG still works) AND a dimension (group-by/filter rolls up),
// while a genuinely fractional measure stays metric-only (forced) — no regression.
func TestProfileTable_IntegerValuedFloatBecomesDim(t *testing.T) {
	db := openLocalDuck(t)
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE downloads (
		time TIMESTAMP,
		response DOUBLE,   -- HTTP status mis-typed as float: 200.0 / 404.0
		latency  DOUBLE,   -- genuine fractional measure: 1.5 / 2.5
		site     VARCHAR
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO downloads VALUES
		('2026-01-01 00:00:00', 200.0, 1.5, 'a.youtube.com'),
		('2026-01-01 00:10:00', 404.0, 2.5, 'b.tiktok.com'),
		('2026-01-01 00:20:00', 200.0, 1.5, 'a.youtube.com'),
		('2026-01-01 00:30:00', 200.0, 2.5, 'c.x.com')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	p, err := ProfileTable(db, "default.downloads", "time", "hour", "downloads", ClassifyConfig{})
	if err != nil {
		t.Fatalf("ProfileTable: %v", err)
	}

	contains := func(xs []string, v string) bool {
		for _, x := range xs {
			if x == v {
				return true
			}
		}
		return false
	}

	// response: integer-valued low-card float -> BOTH metric and dimension.
	if !contains(p.Metrics, "response") {
		t.Errorf("response should remain a metric (AVG/percentile must still work); Metrics=%v", p.Metrics)
	}
	if _, ok := p.DimCard["response"]; !ok {
		t.Errorf("response should ALSO be a dimension (by_response cube); DimCard=%v", p.DimCard)
	}
	if _, ok := p.ForcedMetrics["response"]; ok {
		t.Errorf("response now has dim coverage and must not be flagged a forced metric")
	}

	// latency: fractional measure -> metric only, forced (no dim coverage).
	if !contains(p.Metrics, "latency") {
		t.Errorf("latency should be a metric; Metrics=%v", p.Metrics)
	}
	if _, ok := p.DimCard["latency"]; ok {
		t.Errorf("latency is fractional and must NOT become a dimension; DimCard=%v", p.DimCard)
	}
	if _, ok := p.ForcedMetrics["latency"]; !ok {
		t.Errorf("latency is a continuous measure with no dim coverage and should be a forced metric; ForcedMetrics=%v", p.ForcedMetrics)
	}

	// site: ordinary low-card string dimension.
	if _, ok := p.DimCard["site"]; !ok {
		t.Errorf("site should be a dimension; DimCard=%v", p.DimCard)
	}
}
