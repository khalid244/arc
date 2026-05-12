package rollup

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRollupSpec_JSONRoundtrip(t *testing.T) {
	original := RollupSpec{
		Name:           "analytics__events__1h",
		Database:       "analytics",
		SourceTable:    "events",
		BucketColumn:   "ts",
		BucketInterval: time.Hour,
		KeepDimensions: []string{"service", "region"},
		DroppedColumns: []DroppedColumn{
			{Name: "session_id", Reason: "high_cardinality:0.94"},
		},
		Aggregations: []Aggregation{
			{
				SourceColumn: "latency_ms",
				Functions:    []AggFunction{AggSum, AggMin, AggMax, AggTDigest},
				SketchConfig: &SketchConfig{HLLLgK: 12, TDigestK: 200},
			},
			{
				SourceColumn: "user_id",
				Functions:    []AggFunction{AggHLL},
				SketchConfig: &SketchConfig{HLLLgK: 12, TDigestK: 200},
			},
		},
	}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded RollupSpec
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Name != original.Name {
		t.Errorf("name: got %q want %q", decoded.Name, original.Name)
	}
	if decoded.BucketInterval != original.BucketInterval {
		t.Errorf("bucket interval: got %v want %v", decoded.BucketInterval, original.BucketInterval)
	}
	if len(decoded.Aggregations) != 2 {
		t.Fatalf("aggregations: got %d want 2", len(decoded.Aggregations))
	}
	if decoded.Aggregations[0].Functions[3] != AggTDigest {
		t.Errorf("functions[0][3]: got %v want %v", decoded.Aggregations[0].Functions[3], AggTDigest)
	}
}

func TestRollupSpec_Validate(t *testing.T) {
	tests := []struct {
		name    string
		spec    RollupSpec
		wantErr string
	}{
		{
			name:    "empty name",
			spec:    RollupSpec{Database: "d", SourceTable: "t", BucketColumn: "ts", BucketInterval: time.Hour},
			wantErr: "name is required",
		},
		{
			name:    "empty bucket column",
			spec:    RollupSpec{Name: "n", Database: "d", SourceTable: "t", BucketInterval: time.Hour},
			wantErr: "bucket_column is required",
		},
		{
			name:    "zero bucket interval",
			spec:    RollupSpec{Name: "n", Database: "d", SourceTable: "t", BucketColumn: "ts"},
			wantErr: "bucket_interval must be > 0",
		},
		{
			name: "valid",
			spec: RollupSpec{
				Name: "n", Database: "d", SourceTable: "t",
				BucketColumn: "ts", BucketInterval: time.Hour,
			},
			wantErr: "",
		},
		{
			name: "sketch func without config",
			spec: RollupSpec{
				Name: "n", Database: "d", SourceTable: "t",
				BucketColumn: "ts", BucketInterval: time.Hour,
				Aggregations: []Aggregation{
					{SourceColumn: "u", Functions: []AggFunction{AggHLL}},
				},
			},
			wantErr: "sketch_config is required",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.spec.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestRollupSpec_Fingerprint_StableAcrossNoiseFields confirms that the
// fingerprint only depends on shape (KeepDimensions, Aggregations,
// BucketColumn, BucketInterval) and not on identifying fields like Name
// or SourceTable. Renaming a variant must not invalidate its watermark.
func TestRollupSpec_Fingerprint_StableAcrossNoiseFields(t *testing.T) {
	a := &RollupSpec{
		Name: "v1", Database: "default", SourceTable: "events",
		BucketColumn: "time", BucketInterval: 24 * time.Hour,
		KeepDimensions: []string{"country", "os"},
		Aggregations: []Aggregation{
			{SourceColumn: "latency_ms", Functions: []AggFunction{AggSum, AggCount}},
		},
	}
	b := *a
	b.Name = "renamed"
	b.SourceTable = "events_renamed"
	if a.Fingerprint() != b.Fingerprint() {
		t.Errorf("rename should not change fingerprint: %s vs %s", a.Fingerprint(), b.Fingerprint())
	}
}

// TestRollupSpec_Fingerprint_DetectsShapeChange confirms that a real
// shape change (different dim list, different aggregates, different
// bucket interval) produces a different fingerprint. This is the
// drift-detection signal the scheduler uses.
func TestRollupSpec_Fingerprint_DetectsShapeChange(t *testing.T) {
	base := &RollupSpec{
		BucketColumn: "time", BucketInterval: 24 * time.Hour,
		KeepDimensions: []string{"country"},
		Aggregations: []Aggregation{
			{SourceColumn: "latency_ms", Functions: []AggFunction{AggSum}},
		},
	}
	cases := []struct {
		name string
		mut  func(*RollupSpec)
	}{
		{"added dim", func(s *RollupSpec) { s.KeepDimensions = append(s.KeepDimensions, "os") }},
		{"added agg", func(s *RollupSpec) {
			s.Aggregations = append(s.Aggregations, Aggregation{SourceColumn: "bytes", Functions: []AggFunction{AggSum}})
		}},
		{"bucket interval changed", func(s *RollupSpec) { s.BucketInterval = time.Hour }},
		{"bucket column changed", func(s *RollupSpec) { s.BucketColumn = "created_at" }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			b := *base
			b.KeepDimensions = append([]string(nil), base.KeepDimensions...)
			b.Aggregations = append([]Aggregation(nil), base.Aggregations...)
			tt.mut(&b)
			if base.Fingerprint() == b.Fingerprint() {
				t.Errorf("fingerprint did not change after %s", tt.name)
			}
		})
	}
}

// TestRollupSpec_Fingerprint_StableUnderDimReorder confirms that the
// fingerprint is order-independent on KeepDimensions and Aggregations
// (the scheduler emits dims/aggs in spec-declaration order which can
// flip across runs without anything actually changing).
func TestRollupSpec_Fingerprint_StableUnderDimReorder(t *testing.T) {
	a := &RollupSpec{
		BucketColumn: "time", BucketInterval: 24 * time.Hour,
		KeepDimensions: []string{"country", "os", "region"},
		Aggregations: []Aggregation{
			{SourceColumn: "a", Functions: []AggFunction{AggSum}},
			{SourceColumn: "b", Functions: []AggFunction{AggCount}},
		},
	}
	b := *a
	b.KeepDimensions = []string{"region", "os", "country"}
	b.Aggregations = []Aggregation{
		{SourceColumn: "b", Functions: []AggFunction{AggCount}},
		{SourceColumn: "a", Functions: []AggFunction{AggSum}},
	}
	if a.Fingerprint() != b.Fingerprint() {
		t.Errorf("reorder should not change fingerprint: %s vs %s", a.Fingerprint(), b.Fingerprint())
	}
}

// TestIsRollupTableName pins the discovery-filter contract: every rollup
// output directory name must report true, so callers walking the storage
// backend exclude them. Source-table-shaped names report false.
func TestIsRollupTableName(t *testing.T) {
	rollupNames := []string{
		"downloads__1d",
		"downloads_sketch__1d",
		"downloads_by_site__1d",
		"downloads_by_os_version__1d",
		"events__5m",
		"events__1h",
		"events__1w",
		"events__30s",
	}
	for _, n := range rollupNames {
		if !IsRollupTableName(n) {
			t.Errorf("expected IsRollupTableName(%q) = true", n)
		}
	}
	sourceNames := []string{
		"downloads",
		"events",
		"events_1d",     // single underscore — not the rollup suffix
		"events_v2",     // no digit-letter suffix
		"my__table",     // no digits after `__`
		"events__final", // letters not in [smhdw]
	}
	for _, n := range sourceNames {
		if IsRollupTableName(n) {
			t.Errorf("expected IsRollupTableName(%q) = false", n)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
