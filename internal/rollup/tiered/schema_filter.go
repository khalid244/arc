package tiered

// filterPathsBySchemaHash partitions rollup file paths into (kept,
// dropped) by comparing each file's stamped schema_hash to the
// current Spec's hash. Mismatching files are excluded from the read
// path so a stale schema-versioned spec can't silently produce wrong
// totals (schema-drift class, see architecture review proposal C).
//
// Defensive behaviour (don't strand existing data):
//   - Files written before KV-metadata stamping return empty hash from
//     the lookup; these are KEPT.
//   - Lookup errors (transient S3 / DuckDB issues) KEEP the file. A
//     downstream read may still work; dropping silently is worse.
//   - When the expected hash is empty (e.g. spec-hash computation
//     failed at startup) OR no lookup function is wired (test/local
//     mode), no filtering happens at all.
//
// The lookup function abstracts FileSchemaHash so callers can inject a
// real DuckDB-backed reader in prod and a deterministic stub in tests.
func filterPathsBySchemaHash(paths []string, expected string, lookup func(string) (string, error)) (kept, dropped []string) {
	if expected == "" || lookup == nil {
		// Pass-through: no filtering possible.
		kept = make([]string, len(paths))
		copy(kept, paths)
		return kept, nil
	}
	kept = make([]string, 0, len(paths))
	for _, p := range paths {
		got, err := lookup(p)
		if err != nil {
			// Transient lookup failure: keep the file. The downstream
			// read may still succeed.
			kept = append(kept, p)
			continue
		}
		if got == "" || got == expected {
			kept = append(kept, p)
			continue
		}
		dropped = append(dropped, p)
	}
	return kept, dropped
}
