package rollup

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/basekick-labs/arc/internal/compaction"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
)

// Compaction-manifest exclusion (QR-8). A compaction uploads its output into the
// same partition as its inputs, writes a manifest, flags it OutputUploaded and
// only then deletes the inputs. Until the delete finishes the partition holds
// every row twice. The API's source path hides the inputs of uploaded manifests;
// the rollup must too, on both of its source reads:
//
//   - the cube builder's exact day listing (listDaySource, shared by the day,
//     sketch-subprocess and month builds) drops the inputs from the files it
//     fingerprints and reads; and
//   - the merge-on-read source branches (fresh tail, head patch) wrap their
//     read_parquet in the same file-name exclusion the API applies.

// CompactionManifests is the read-only view of the compactor's manifests the
// rollup needs; *compaction.ManifestManager satisfies it. The rollup never
// writes, recovers or deletes manifests.
type CompactionManifests interface {
	ListManifests(ctx context.Context) ([]string, error)
	ReadManifest(ctx context.Context, manifestPath string) (*compaction.Manifest, error)
	GetInputFilesForUploadedOutputs(ctx context.Context) (map[string]struct{}, error)
}

// SetCompactionManifests wires the compaction manifest reader. Called once at
// startup, before serving (like SetSourcePruner); nil disables the exclusion.
func (m *Manager) SetCompactionManifests(c CompactionManifests) { m.compactionManifests = c }

// srcFileCol is the helper column naming each row's source file in an
// exclusion-wrapped read. Never "filename": a table may have a real column of
// that name (read_parquet's filename=true then fails with a Binder Error), and
// the wrapper drops it again, so it never reaches a result or a SELECT *.
const srcFileCol = "_arc_src_file"

// sourceRelation renders the FROM relation for a merge source branch over glob
// (a read_parquet list/glob argument), hiding the files in exclude (full URIs,
// as DuckDB reports them).
func sourceRelation(glob string, exclude []string) string {
	if len(exclude) == 0 {
		return readParquetFrom(glob)
	}
	q := make([]string, len(exclude))
	for i, u := range exclude {
		q[i] = "'" + esc(u) + "'"
	}
	return fmt.Sprintf("(SELECT * EXCLUDE (%s) FROM %s WHERE %s NOT IN (%s))",
		srcFileCol, sqlutil.ReadParquet(glob, "union_by_name=true", "filename='"+srcFileCol+"'"),
		srcFileCol, strings.Join(q, ", "))
}

// compactionExcludes is the Router's SourceExclude: the full URIs of source's
// compaction inputs whose output is already uploaded — the rule (and the cached
// manifest read) the API's source path uses. A manifest read error hides
// nothing, as on the API path.
func (m *Manager) compactionExcludes(source string) []string {
	c := m.compactionManifests
	if c == nil {
		return nil
	}
	all, err := c.GetInputFilesForUploadedOutputs(context.Background())
	if err != nil {
		m.log.Warn().Err(err).Str("source", source).
			Msg("Rollup: compaction manifests unreadable; merge source reads do not hide compaction inputs")
		return nil
	}
	db, meas := splitSource(source)
	prefix := db + "/" + meas + "/" // trailing slash: default/events must not match default/events_x
	var out []string
	for k := range all {
		if k = normKey(k); strings.HasPrefix(k, prefix) {
			out = append(out, m.rootURI()+"/"+k)
		}
	}
	sort.Strings(out)
	return out
}

func normKey(k string) string { return strings.TrimPrefix(filepath.ToSlash(k), "/") }

// unsettledSig prefixes the fingerprint of a day whose listing a compaction
// touched (or whose manifests could not all be read). Fingerprints compare
// without it (the same effective files are the same cube), but a CHANGED day
// whose cube was built unsettled is never held back by rebuild limits: that
// build may have raced the compactor.
const unsettledSig = "u"

// settledSig strips the unsettled marker for comparison.
func settledSig(sig string) string { return strings.TrimPrefix(sig, unsettledSig) }

// compactionSnapshot is one read of every compaction manifest. complete=false
// means the listing or a manifest read failed (nothing can be ruled out).
type compactionSnapshot struct {
	manifests []*compaction.Manifest
	complete  bool
}

func (m *Manager) readCompactionManifests(ctx context.Context) compactionSnapshot {
	c := m.compactionManifests
	if c == nil {
		return compactionSnapshot{complete: true}
	}
	paths, err := c.ListManifests(ctx)
	if err != nil {
		return compactionSnapshot{}
	}
	snap := compactionSnapshot{complete: true}
	for _, p := range paths {
		mf, err := c.ReadManifest(ctx, p)
		if err != nil {
			snap.complete = false // e.g. deleted between list and read: a compaction just finished
			continue
		}
		snap.manifests = append(snap.manifests, mf)
	}
	return snap
}

// dropCompactedInputs removes compaction inputs from a day listing taken
// between two manifest reads (before, after), and reports whether the day is
// unsettled. A manifest's inputs are dropped when its output is in the listing
// (then every input row is in the output, whatever the OutputUploaded flag — the
// compactor uploads before it writes the manifest), or when the manifest read
// BEFORE the listing was already flagged uploaded (the output existed before the
// listing; this also covers an output since re-compacted away). A flag seen only
// AFTER the listing is not trusted: the output may have landed after the listing
// passed its path, and dropping the inputs would lose their rows.
func dropCompactedInputs(files []storage.ObjectInfo, before, after compactionSnapshot) ([]storage.ObjectInfo, bool) {
	listed := make(map[string]bool, len(files))
	for _, f := range files {
		listed[f.Path] = true
	}
	unsettled := !before.complete || !after.complete
	drop := map[string]bool{}
	consider := func(mf *compaction.Manifest, flagBeforeListing bool) {
		out := normKey(mf.OutputPath)
		touches := listed[out]
		for _, in := range mf.InputFiles {
			touches = touches || listed[normKey(in)]
		}
		if !touches {
			return
		}
		unsettled = true
		if listed[out] || (flagBeforeListing && mf.OutputUploaded) {
			for _, in := range mf.InputFiles {
				drop[normKey(in)] = true
			}
		}
	}
	for _, mf := range before.manifests {
		consider(mf, true)
	}
	for _, mf := range after.manifests {
		consider(mf, false)
	}
	if len(drop) == 0 {
		return files, unsettled
	}
	kept := make([]storage.ObjectInfo, 0, len(files))
	for _, f := range files {
		if !drop[f.Path] {
			kept = append(kept, f)
		}
	}
	return kept, unsettled
}
