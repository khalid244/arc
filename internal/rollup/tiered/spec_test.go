package tiered

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSpec_JSONRoundTrip(t *testing.T) {
	s := Spec{
		Table:    "default.events",
		TZ:       "UTC",
		Dims: map[string]DimSpec{
			"dim_b":  {Role: "Dim", KeptValues: []string{"kept_1", "kept_2"}},
			"user_id": {Role: "Sketch"},
		},
		BuilderVersion: "abc123",
	}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Spec
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Table != s.Table || got.TZ != s.TZ {
		t.Errorf("round-trip mismatch: %+v vs %+v", got, s)
	}
	if !strings.Contains(string(b), "kept_1") {
		t.Errorf("kept values not serialized: %s", string(b))
	}
}

func TestSpec_SchemaHashDeterministic(t *testing.T) {
	s1 := Spec{Table: "t", TZ: "UTC", Dims: map[string]DimSpec{"a": {Role: "Dim", KeptValues: []string{"x", "y"}}}}
	s2 := Spec{Table: "t", TZ: "UTC", Dims: map[string]DimSpec{"a": {Role: "Dim", KeptValues: []string{"y", "x"}}}}
	h1, err := s1.SchemaHash()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := s2.SchemaHash()
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Errorf("kept-value order should not affect hash: %s vs %s", h1, h2)
	}
}

func TestSpec_SchemaHashChangesOnContentChange(t *testing.T) {
	s1 := Spec{Table: "t", TZ: "UTC"}
	s2 := Spec{Table: "t", TZ: "Asia/Riyadh"}
	h1, _ := s1.SchemaHash()
	h2, _ := s2.SchemaHash()
	if h1 == h2 {
		t.Error("schema hash must change when TZ changes")
	}
}
