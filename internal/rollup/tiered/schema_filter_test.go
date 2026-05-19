package tiered

import (
	"sort"
	"testing"
)

// filterPathsBySchemaHash partitions rollup file paths into (kept,
// dropped) using a caller-supplied lookup. The reader uses this to
// refuse files whose stamped schema_hash differs from the current
// Spec's hash — preventing silent wrong-schema reads after a spec
// change (the bug class A from the architecture review).
//
// Legacy files written before KV-metadata stamping (empty hash from
// the lookup) are KEPT — they pre-date the safety mechanism and
// failing every old file would just break the rollup wholesale.

func TestFilterPathsBySchemaHash_KeepsMatching(t *testing.T) {
	lookup := func(p string) (string, error) {
		return "deadbeef", nil // all files match
	}
	kept, dropped := filterPathsBySchemaHash([]string{"a.parquet", "b.parquet"}, "deadbeef", lookup)
	if len(kept) != 2 || len(dropped) != 0 {
		t.Errorf("expected all kept, got kept=%v dropped=%v", kept, dropped)
	}
}

func TestFilterPathsBySchemaHash_DropsMismatched(t *testing.T) {
	hashes := map[string]string{
		"a.parquet": "deadbeef",
		"b.parquet": "cafebabe", // mismatch
		"c.parquet": "deadbeef",
	}
	lookup := func(p string) (string, error) {
		return hashes[p], nil
	}
	kept, dropped := filterPathsBySchemaHash([]string{"a.parquet", "b.parquet", "c.parquet"}, "deadbeef", lookup)
	sort.Strings(kept)
	if len(kept) != 2 || kept[0] != "a.parquet" || kept[1] != "c.parquet" {
		t.Errorf("expected [a,c] kept, got %v", kept)
	}
	if len(dropped) != 1 || dropped[0] != "b.parquet" {
		t.Errorf("expected [b] dropped, got %v", dropped)
	}
}

func TestFilterPathsBySchemaHash_LegacyEmptyHash_Kept(t *testing.T) {
	hashes := map[string]string{
		"new.parquet":    "deadbeef",
		"legacy.parquet": "", // pre-stamping
	}
	lookup := func(p string) (string, error) {
		return hashes[p], nil
	}
	kept, dropped := filterPathsBySchemaHash([]string{"new.parquet", "legacy.parquet"}, "deadbeef", lookup)
	if len(kept) != 2 {
		t.Errorf("legacy empty-hash files must be kept (don't strand pre-stamped data): got kept=%v dropped=%v", kept, dropped)
	}
}

func TestFilterPathsBySchemaHash_LookupErrorKeepsFile(t *testing.T) {
	// If the hash lookup errors (transient S3 hiccup, DuckDB issue, …)
	// keep the file in the result set rather than silently dropping it.
	// The downstream read might still succeed; dropping would be worse.
	lookupErr := func(p string) (string, error) {
		return "", &lookupTestErr{}
	}
	kept, dropped := filterPathsBySchemaHash([]string{"x.parquet"}, "deadbeef", lookupErr)
	if len(kept) != 1 || len(dropped) != 0 {
		t.Errorf("transient errors must not drop: kept=%v dropped=%v", kept, dropped)
	}
}

func TestFilterPathsBySchemaHash_EmptyExpected_NoFiltering(t *testing.T) {
	// When the caller can't compute the spec hash (zero-value Spec,
	// startup race), don't filter — preserve current behaviour.
	lookup := func(p string) (string, error) {
		return "anything", nil
	}
	kept, dropped := filterPathsBySchemaHash([]string{"a.parquet", "b.parquet"}, "", lookup)
	if len(kept) != 2 || len(dropped) != 0 {
		t.Errorf("empty expected hash must skip filtering: kept=%v dropped=%v", kept, dropped)
	}
}

func TestFilterPathsBySchemaHash_NilLookup_NoFiltering(t *testing.T) {
	// Test/local mode without a DB handle for KV lookups must also
	// short-circuit instead of returning empty.
	kept, dropped := filterPathsBySchemaHash([]string{"a.parquet"}, "deadbeef", nil)
	if len(kept) != 1 || len(dropped) != 0 {
		t.Errorf("nil lookup must skip filtering: kept=%v dropped=%v", kept, dropped)
	}
}

type lookupTestErr struct{}

func (e *lookupTestErr) Error() string { return "synthetic lookup failure" }
