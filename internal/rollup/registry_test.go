package rollup

import (
	"testing"
	"time"
)

func TestRegistry_LookupReturnsTiersFineFirst(t *testing.T) {
	specs := []RollupSpec{
		{Name: "d__t__1d", Database: "d", SourceTable: "t", BucketInterval: 24 * time.Hour},
		{Name: "d__t__1h", Database: "d", SourceTable: "t", BucketInterval: time.Hour},
		{Name: "d__t__5m", Database: "d", SourceTable: "t", BucketInterval: 5 * time.Minute},
	}
	r := NewRegistry(specs)

	got := r.ForTable("d", "t")
	if len(got) != 3 {
		t.Fatalf("expected 3 tiers, got %d", len(got))
	}
	if got[0].BucketInterval != 5*time.Minute {
		t.Errorf("first tier should be finest (5m), got %v", got[0].BucketInterval)
	}
	if got[2].BucketInterval != 24*time.Hour {
		t.Errorf("last tier should be coarsest (1d), got %v", got[2].BucketInterval)
	}
}

func TestRegistry_LookupMissingReturnsNil(t *testing.T) {
	r := NewRegistry(nil)
	if got := r.ForTable("missing", "table"); got != nil {
		t.Errorf("expected nil for missing, got %v", got)
	}
}
