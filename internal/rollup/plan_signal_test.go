package rollup

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// F8 — classification tightening (0507bb9: continuous types always metrics,
// dim-rich restricted to low-card dims) is an intentional tradeoff, but it must
// emit an operator SIGNAL when it silently shrinks coverage. These tests pin the
// warnings at the classification/planning layer (once per source per process).

// signalManager builds a drift-style Manager whose log is captured in buf.
func signalManager(t *testing.T, root string, buf *bytes.Buffer) *Manager {
	t.Helper()
	db := openLocalDuck(t)
	t.Cleanup(func() { db.Close() })
	m := driftManager(t, db, root, newFakeStorage())
	m.log = zerolog.New(buf)
	return m
}

// TestForcedMetricWarnsOnce (F8a) — a continuous (DOUBLE/FLOAT/DECIMAL) column
// whose sampled cardinality is dim-eligible is still forced to metric (the
// intended 0507bb9 behavior), so per-dim/dim-rich coverage for it is
// unavailable. That coverage loss must be logged, once per source.
func TestForcedMetricWarnsOnce(t *testing.T) {
	root := t.TempDir()
	var buf bytes.Buffer
	m := signalManager(t, root, &buf)

	// "score" is a low-cardinality DOUBLE: pre-0507bb9 it would have been a dim.
	writeSourceDay(t, m.db, root, "2026/06/01",
		`SELECT TIMESTAMPTZ '2026-06-01 10:00:00' AS "time", 'web' AS site, 1.5::DOUBLE AS score
		 UNION ALL SELECT TIMESTAMPTZ '2026-06-01 11:00:00', 'ios', 2.5::DOUBLE`)

	if _, err := m.planSpecs("default.events"); err != nil {
		t.Fatalf("planSpecs: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "forced to metric") || !strings.Contains(out, "score") {
		t.Fatalf("F8a: want a warning that continuous column 'score' was forced to metric (per-dim/dim-rich coverage unavailable); logs:\n%s", out)
	}

	// Once per source per process.
	buf.Reset()
	if _, err := m.planSpecs("default.events"); err != nil {
		t.Fatalf("planSpecs 2nd: %v", err)
	}
	if strings.Contains(buf.String(), "forced to metric") {
		t.Fatalf("F8a: forced-to-metric warning must fire once per source; got again:\n%s", buf.String())
	}
}

// TestDimRichSkippedTooFewLowCard (F8b) — a table with >=2 medium-card dims
// (MaxDimCard < card <= MaxPerDimCard) but fewer than 2 low-card dims builds NO
// dim-rich cube (DimRichSpec needs >=2 low-card dims) and the old warn branch
// (len(lowCardDims) > DimRichMaxDims) never fires: total silence. It must warn,
// naming the excluded medium-card dims and the actual cause.
func TestDimRichSkippedTooFewLowCard(t *testing.T) {
	root := t.TempDir()
	var buf bytes.Buffer
	m := signalManager(t, root, &buf)
	m.cfg.DimRich = true
	m.cfg.MaxDimCard = 3 // low-card threshold
	m.cfg.MaxPerDimCard = 100

	// userhash & devicehash: card 10 -> medium (3 < 10 <= 100); no low-card dims.
	writeSourceDay(t, m.db, root, "2026/06/01",
		`SELECT TIMESTAMPTZ '2026-06-01 10:00:00' AS "time",
		        'u' || i::VARCHAR AS userhash, 'd' || i::VARCHAR AS devicehash
		 FROM range(10) t(i)`)

	if _, err := m.planSpecs("default.events"); err != nil {
		t.Fatalf("planSpecs: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "dim-rich") || !strings.Contains(out, "userhash") || !strings.Contains(out, "devicehash") {
		t.Fatalf("F8b: want a warning that NO dim-rich cube was built, naming excluded medium-card dims userhash+devicehash; logs:\n%s", out)
	}
	if !strings.Contains(out, "low-card") {
		t.Fatalf("F8b: warning must state the actual cause (too few low-cardinality dims), not a generic message; logs:\n%s", out)
	}
}

// TestDimRichSkippedTooManyLowCard (F8c) — the existing high-dimensionality skip
// must state the actual cause and list the eligible dims, so an operator can see
// exactly which multi-dim coverage is missing and what raising
// rollup.dim_rich_max_dims would buy.
func TestDimRichSkippedTooManyLowCard(t *testing.T) {
	root := t.TempDir()
	var buf bytes.Buffer
	m := signalManager(t, root, &buf)
	m.cfg.DimRich = true
	m.cfg.DimRichMaxDims = 2

	// Three low-card dims alpha/beta/gamma: 3 > DimRichMaxDims=2 -> skipped.
	writeSourceDay(t, m.db, root, "2026/06/01",
		`SELECT TIMESTAMPTZ '2026-06-01 10:00:00' AS "time", 'x' AS alpha, 'y' AS beta, 'z' AS gamma
		 UNION ALL SELECT TIMESTAMPTZ '2026-06-01 11:00:00', 'x2', 'y2', 'z2'`)

	if _, err := m.planSpecs("default.events"); err != nil {
		t.Fatalf("planSpecs: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "alpha") || !strings.Contains(out, "beta") || !strings.Contains(out, "gamma") {
		t.Fatalf("F8c: dim-rich skip warning must list the eligible low-card dims; logs:\n%s", out)
	}
	if !strings.Contains(out, "too many low-card") {
		t.Fatalf("F8c: warning must state the actual cause (too many low-cardinality dims vs dim_rich_max_dims); logs:\n%s", out)
	}
}
