package precalc

import (
	"encoding/json"
	"testing"
	"time"
)

func TestManifest_RoundTrip(t *testing.T) {
	m := Manifest{
		Table:      "default.downloads",
		Generation: 17,
		Entries: []ManifestEntry{
			{
				Tier: "1h", Variant: "sketch",
				Path: "tier=1h/year=2026/month=05/day=15/sketch/abc.parquet",
				BucketLo: time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC),
				BucketHi: time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
				SchemaHash: "deadbeef",
				BuilderVersion: "abc123",
			},
		},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var got Manifest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Table != m.Table || got.Generation != m.Generation {
		t.Errorf("round-trip mismatch")
	}
	if len(got.Entries) != 1 || got.Entries[0].Path != m.Entries[0].Path {
		t.Errorf("entries lost in round-trip")
	}
}

func TestManifest_FilesForTierVariant(t *testing.T) {
	m := Manifest{
		Entries: []ManifestEntry{
			{Tier: "1h", Variant: "sketch", Path: "a"},
			{Tier: "1h", Variant: "by_site", Path: "b"},
			{Tier: "1d", Variant: "sketch", Path: "c"},
		},
	}
	got := m.FilesForTierVariant("1h", "sketch")
	if len(got) != 1 || got[0] != "a" {
		t.Errorf("FilesForTierVariant = %v, want [a]", got)
	}
}
