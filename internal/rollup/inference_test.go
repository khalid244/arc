package rollup

import (
	"testing"
)

func TestClassifyColumn(t *testing.T) {
	tests := []struct {
		name        string
		col         ColumnStats
		hints       TableConfig
		wantRole    ColumnRole
		wantSketch  bool
		wantTDigest bool
	}{
		{"timestamp", ColumnStats{Name: "time", Type: "TIMESTAMPTZ"}, TableConfig{}, RoleTime, false, false},
		{"low_card_string", ColumnStats{Name: "country", Type: "VARCHAR", Distinct: 193}, TableConfig{}, RoleDim, false, false},
		// Pure cardinality classification (no name patterns):
		//   ≤ 1024              → RoleDim
		//   1024..100000        → RoleSketch HLL
		//   > 100000            → RoleDrop
		{"midcard_string_hll", ColumnStats{Name: "device_id", Type: "VARCHAR", Distinct: 50000}, TableConfig{}, RoleSketch, true, false},
		{"midcard_no_id_suffix", ColumnStats{Name: "site", Type: "VARCHAR", Distinct: 19525}, TableConfig{}, RoleSketch, true, false},
		{"low_card_id_suffix_is_still_dim", ColumnStats{Name: "session_id", Type: "VARCHAR", Distinct: 500}, TableConfig{}, RoleDim, false, false},
		{"continuous_metric", ColumnStats{Name: "duration_seconds", Type: "DOUBLE", Distinct: 50000}, TableConfig{}, RoleMetric, false, true},
		{"enum_numeric_is_dim", ColumnStats{Name: "status_code", Type: "INTEGER", Distinct: 8}, TableConfig{}, RoleDim, false, false},
		{"midcard_numeric_is_metric", ColumnStats{Name: "request_size", Type: "INTEGER", Distinct: 50}, TableConfig{}, RoleMetric, false, false},
		{"drop_very_high_card_string", ColumnStats{Name: "url", Type: "VARCHAR", Distinct: 200000}, TableConfig{}, RoleDrop, false, false},
		{"force_sketch", ColumnStats{Name: "foo", Type: "VARCHAR", Distinct: 50}, TableConfig{SketchColumns: []string{"foo"}}, RoleSketch, true, false},
		{"force_ignore", ColumnStats{Name: "bar", Type: "VARCHAR", Distinct: 50}, TableConfig{IgnoreColumns: []string{"bar"}}, RoleDrop, false, false},
		{"restrict_tdigest", ColumnStats{Name: "score", Type: "DOUBLE", Distinct: 5000}, TableConfig{QuantileColumns: []string{"duration_seconds"}}, RoleMetric, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyColumn(tt.col, tt.hints, ThresholdConfig{})
			if got.Role != tt.wantRole {
				t.Errorf("Role = %v, want %v", got.Role, tt.wantRole)
			}
			if got.HLL != tt.wantSketch {
				t.Errorf("HLL = %v, want %v", got.HLL, tt.wantSketch)
			}
			if got.TDigest != tt.wantTDigest {
				t.Errorf("TDigest = %v, want %v", got.TDigest, tt.wantTDigest)
			}
		})
	}
}

func TestInferSchema_MultipleTimestampRejected(t *testing.T) {
	stats := []ColumnStats{
		{Name: "created_at", Type: "TIMESTAMPTZ"},
		{Name: "updated_at", Type: "TIMESTAMPTZ"},
		{Name: "status", Type: "VARCHAR", Distinct: 4},
	}
	_, err := InferSchema(stats, TableConfig{}, ThresholdConfig{})
	if err == nil {
		t.Fatal("expected error for multiple TIMESTAMPTZ columns")
	}
}

func TestInferSchema_TimeColumnOverride(t *testing.T) {
	stats := []ColumnStats{
		{Name: "created_at", Type: "TIMESTAMPTZ"},
		{Name: "updated_at", Type: "TIMESTAMPTZ"},
	}
	s, err := InferSchema(stats, TableConfig{TimeColumn: "updated_at"}, ThresholdConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.TimeColumn != "updated_at" {
		t.Errorf("TimeColumn = %q", s.TimeColumn)
	}
}

func TestClassifyColumn_KeepColumns(t *testing.T) {
	// keep_columns force-keeps regardless of cardinality, with HighCard=true
	// when the column is genuinely high-card (above dim_cardinality_max).
	cases := []struct {
		col  ColumnStats
		want ClassifiedColumn
	}{
		{ColumnStats{Name: "site", Type: "VARCHAR", Distinct: 19525}, ClassifiedColumn{Name: "site", Role: RoleDim, HighCard: true}},
		{ColumnStats{Name: "tier", Type: "VARCHAR", Distinct: 10}, ClassifiedColumn{Name: "tier", Role: RoleDim, HighCard: false}},
	}
	for _, c := range cases {
		got := ClassifyColumn(c.col, TableConfig{KeepColumns: []string{c.col.Name}}, ThresholdConfig{})
		if got != c.want {
			t.Errorf("col %s: got %+v, want %+v", c.col.Name, got, c.want)
		}
	}
}

// TestClassifyColumn_AutoHighCard pins the split: raising dim_cardinality_max
// above the safe cross-product cap (1024) keeps the column out of the
// dim-rich variant via HighCard=true, while still classifying it as RoleDim
// so it gets a per-dim variant.
func TestClassifyColumn_AutoHighCard(t *testing.T) {
	// Below the safe cap → plain dim, HighCard=false.
	got := ClassifyColumn(
		ColumnStats{Name: "country", Type: "VARCHAR", Distinct: 193},
		TableConfig{},
		ThresholdConfig{DimCardinalityMax: 25000},
	)
	if got.Role != RoleDim || got.HighCard {
		t.Errorf("country: got %+v, want Role=RoleDim HighCard=false", got)
	}

	// Above the safe cap, within user-raised dim_cardinality_max → dim with
	// HighCard=true so spec generation skips the cross-product variant.
	got = ClassifyColumn(
		ColumnStats{Name: "site", Type: "VARCHAR", Distinct: 19525},
		TableConfig{},
		ThresholdConfig{DimCardinalityMax: 25000},
	)
	if got.Role != RoleDim || !got.HighCard {
		t.Errorf("site: got %+v, want Role=RoleDim HighCard=true", got)
	}
}

// TestClassifyColumn_CustomThresholds verifies the two threshold knobs do
// shift the classifier output.
func TestClassifyColumn_CustomThresholds(t *testing.T) {
	col := ColumnStats{Name: "tier", Type: "VARCHAR", Distinct: 5000}

	// Default thresholds: 5000 > 1024, 5000 ≤ 100000 → RoleSketch
	got := ClassifyColumn(col, TableConfig{}, ThresholdConfig{})
	if got.Role != RoleSketch {
		t.Errorf("default thresholds: got %v, want RoleSketch", got.Role)
	}

	// Raise dim cutoff above the column's distinct count → RoleDim
	got = ClassifyColumn(col, TableConfig{}, ThresholdConfig{DimCardinalityMax: 10000})
	if got.Role != RoleDim {
		t.Errorf("raised dim cutoff: got %v, want RoleDim", got.Role)
	}

	// Lower the sketch cutoff below the column's distinct count → RoleDrop
	got = ClassifyColumn(col, TableConfig{}, ThresholdConfig{SketchCardinalityMax: 1000})
	if got.Role != RoleDrop {
		t.Errorf("lowered sketch cutoff: got %v, want RoleDrop", got.Role)
	}
}
