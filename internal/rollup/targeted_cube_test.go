package rollup

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// TestTargetedSpec pins that an operator-declared targeted cube carries exactly the
// configured dims (sorted) plus COUNT and a Theta sketch per distinct column — the
// shape that lets a multi-dimension dashboard (Hammel Survey) roll up on a wide table.
func TestTargetedSpec(t *testing.T) {
	p := TableProfile{
		Source: "posthog.events", Grain: "hour",
		DimCard:    map[string]int{"event": 120, "survey_name": 3, "survey_response": 6, "os_name": 3, "app_version": 40},
		SketchCols: []string{"distinct_id"},
	}

	spec, ok := p.targetedSpec(
		[]string{"event", "survey_name", "survey_response", "os_name", "app_version"},
		[]string{"distinct_id"},
	)
	if !ok {
		t.Fatal("expected ok=true for a cube over known columns")
	}

	wantDims := []string{"app_version", "event", "os_name", "survey_name", "survey_response"}
	if !reflect.DeepEqual(spec.Dims, wantDims) {
		t.Fatalf("dims = %v, want %v (sorted)", spec.Dims, wantDims)
	}

	wantAggs := []Aggregate{{Kind: AggCount}, {Kind: AggCountDistinct, Col: "distinct_id"}}
	if !reflect.DeepEqual(spec.Aggs, wantAggs) {
		t.Fatalf("aggs = %v, want %v", spec.Aggs, wantAggs)
	}

	if spec.Source != "posthog.events" || spec.Grain != "hour" {
		t.Fatalf("source/grain = %q/%q, want posthog.events/hour", spec.Source, spec.Grain)
	}
}

// TestTargetedSpecRejectsUnknownColumns pins that a typo'd column (dim or distinct)
// or an empty dim set is skipped (ok=false) rather than producing a broken cube —
// so a config typo can't take the builder down.
func TestTargetedSpecRejectsUnknownColumns(t *testing.T) {
	p := TableProfile{
		Source: "posthog.events", Grain: "hour",
		DimCard:    map[string]int{"event": 120, "survey_name": 3},
		SketchCols: []string{"distinct_id"},
	}

	if _, ok := p.targetedSpec([]string{"event", "nope"}, nil); ok {
		t.Fatal("expected ok=false when a dim column is unknown")
	}
	if _, ok := p.targetedSpec([]string{"event"}, []string{"ghost"}); ok {
		t.Fatal("expected ok=false when a distinct column is unknown")
	}
	if _, ok := p.targetedSpec(nil, nil); ok {
		t.Fatal("expected ok=false for an empty dim set")
	}

	// A distinct column may be a high-card sketch column (not a dim) — that's valid.
	if _, ok := p.targetedSpec([]string{"event"}, []string{"distinct_id"}); !ok {
		t.Fatal("expected ok=true: distinct_id is a known sketch column")
	}
}

// TestPlanSpecsEmitsTargetedCube pins that a configured [[rollup.cube]] is planned for
// its table — and only its table.
func TestPlanSpecsEmitsTargetedCube(t *testing.T) {
	surveyDims := []string{"event", "survey_name", "survey_response", "os_name", "app_version"}
	sortedSurvey := []string{"app_version", "event", "os_name", "survey_name", "survey_response"}

	m := &Manager{
		log: zerolog.New(io.Discard),
		cfg: Config{
			TargetedCubes: []TargetedCube{
				{Source: "posthog.events", Dims: surveyDims, Distinct: []string{"distinct_id"}},
			},
		}.withDefaults(),
		profiles: map[string]TableProfile{
			"posthog.events": {
				Source: "posthog.events", Grain: "hour",
				DimCard:    map[string]int{"event": 120, "survey_name": 3, "survey_response": 6, "os_name": 3, "app_version": 40},
				SketchCols: []string{"distinct_id"},
			},
			"default.downloads": {
				Source: "default.downloads", Grain: "hour",
				DimCard: map[string]int{"site": 3319, "tag": 40},
			},
		},
		workload:  NewWorkload(),
		manifests: map[string]*Manifest{},
	}

	specs, err := m.planSpecs("posthog.events")
	if err != nil {
		t.Fatalf("planSpecs(posthog.events): %v", err)
	}
	if !hasCubeWithDims(specs, sortedSurvey) {
		t.Fatalf("targeted survey cube missing from plan; cube dims = %v", cubeDims(specs))
	}

	other, err := m.planSpecs("default.downloads")
	if err != nil {
		t.Fatalf("planSpecs(default.downloads): %v", err)
	}
	if hasCubeWithDims(other, sortedSurvey) {
		t.Fatal("targeted survey cube must not appear on default.downloads")
	}
}

// TestPlanSpecsWarnsOnceOnUnknownTargetedColumn pins that a typo'd targeted-cube column
// is skipped (no cube emitted) and warned — once per source, so a bad config can't crash
// the builder or spam the log every tick.
func TestPlanSpecsWarnsOnceOnUnknownTargetedColumn(t *testing.T) {
	var buf bytes.Buffer
	m := &Manager{
		log: zerolog.New(&buf),
		cfg: Config{
			TargetedCubes: []TargetedCube{{Source: "posthog.events", Dims: []string{"event", "typo_col"}}},
		}.withDefaults(),
		profiles: map[string]TableProfile{
			"posthog.events": {Source: "posthog.events", Grain: "hour", DimCard: map[string]int{"event": 120}},
		},
		workload:       NewWorkload(),
		manifests:      map[string]*Manifest{},
		targetedBailed: map[string]bool{},
	}

	specs, err := m.planSpecs("posthog.events")
	if err != nil {
		t.Fatalf("planSpecs: %v", err)
	}
	if hasCubeWithDims(specs, []string{"event", "typo_col"}) {
		t.Fatal("a cube with an unknown column must not be emitted")
	}
	if !strings.Contains(buf.String(), "targeted cube SKIPPED") {
		t.Fatalf("expected a skip warning; logs:\n%s", buf.String())
	}

	buf.Reset()
	if _, err := m.planSpecs("posthog.events"); err != nil {
		t.Fatalf("planSpecs 2nd: %v", err)
	}
	if strings.Contains(buf.String(), "targeted cube SKIPPED") {
		t.Fatalf("skip warning must fire once per source; fired again:\n%s", buf.String())
	}
}

func hasCubeWithDims(specs []CubeSpec, want []string) bool {
	for _, s := range specs {
		if reflect.DeepEqual(s.Dims, want) {
			return true
		}
	}
	return false
}

func cubeDims(specs []CubeSpec) [][]string {
	out := make([][]string, 0, len(specs))
	for _, s := range specs {
		out = append(out, s.Dims)
	}
	return out
}
