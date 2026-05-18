package tiered

import (
	"encoding/json"
	"testing"
)

func TestManifest_RoundTrip(t *testing.T) {
	m := Manifest{
		Table:      "default.events",
		Generation: 17,
		Entries: []ManifestEntry{
			{
				Path:       "_arc/rollup/default/events/1h/2026/05/15/00/sketch/abc.parquet",
				SchemaHash: "deadbeef",
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
			{Path: "_arc/rollup/default/events/1h/2026/05/15/01/sketch/a.parquet"},
			{Path: "_arc/rollup/default/events/1h/2026/05/15/02/by_site/b.parquet"},
			{Path: "_arc/rollup/default/events/1d/2026/05/15/sketch/c.parquet"},
		},
	}
	got := m.FilesForTierVariant("1h", "sketch")
	if len(got) != 1 || got[0] != "_arc/rollup/default/events/1h/2026/05/15/01/sketch/a.parquet" {
		t.Errorf("FilesForTierVariant = %v, want 1 sketch entry", got)
	}
}
