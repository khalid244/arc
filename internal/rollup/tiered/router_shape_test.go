package tiered

import "testing"

func TestQueryShape_ZeroValueIsUnsupported(t *testing.T) {
	var qs QueryShape
	if qs.Supported {
		t.Error("zero-value QueryShape should not be Supported")
	}
}

func TestQueryShape_AggKindString(t *testing.T) {
	tests := []struct {
		k    AggKind
		want string
	}{
		{AggUnknown, "unknown"},
		{AggCount, "count"},
		{AggCountStar, "count_star"},
		{AggSum, "sum"},
		{AggAvg, "avg"},
		{AggMin, "min"},
		{AggMax, "max"},
		{AggCountDistinct, "count_distinct"},
		{AggQuantile, "quantile_cont"},
	}
	for _, tc := range tests {
		if got := tc.k.String(); got != tc.want {
			t.Errorf("AggKind(%d).String() = %q want %q", tc.k, got, tc.want)
		}
	}
}

func TestFilterPredicate_ZeroOpIsInvalid(t *testing.T) {
	var fp FilterPredicate
	if fp.Op != "" {
		t.Errorf("zero FilterPredicate Op should be empty, got %q", fp.Op)
	}
}
