package api

import (
	"context"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
)

// queryExcludes is one query's view of the compaction inputs to hide (QR-8).
// The manifests are read once per query, and each table's NOT IN filter is
// rendered once, however many relations, passes or checks of the transform
// ask for it: the list runs to several hundred paths while compactions are
// stuck or frequent.
type queryExcludes struct {
	mu     sync.Mutex
	loaded bool
	all    map[string]struct{}
	err    error
	exprs  map[string]string
}

type queryExcludesKey struct{}

// withQueryExcludes attaches a per-query exclusion view to ctx, or returns the
// one already attached.
func withQueryExcludes(ctx context.Context) context.Context {
	if _, ok := ctx.Value(queryExcludesKey{}).(*queryExcludes); ok {
		return ctx
	}
	return context.WithValue(ctx, queryExcludesKey{}, &queryExcludes{})
}

// uploadedInputs returns the storage keys of every compaction input whose
// output is uploaded, read once per query when ctx carries a view.
func (h *QueryHandler) uploadedInputs(ctx context.Context) (map[string]struct{}, error) {
	qe, _ := ctx.Value(queryExcludesKey{}).(*queryExcludes)
	if qe == nil {
		return h.manifestManager.GetInputFilesForUploadedOutputs(ctx)
	}
	qe.mu.Lock()
	defer qe.mu.Unlock()
	if !qe.loaded {
		qe.all, qe.err = h.manifestManager.GetInputFilesForUploadedOutputs(ctx)
		qe.loaded = true
	}
	return qe.all, qe.err
}

func (qe *queryExcludes) expr(table string) (string, bool) {
	if qe == nil {
		return "", false
	}
	qe.mu.Lock()
	defer qe.mu.Unlock()
	e, ok := qe.exprs[table]
	return e, ok
}

func (qe *queryExcludes) setExpr(table, expr string) {
	if qe == nil {
		return
	}
	qe.mu.Lock()
	defer qe.mu.Unlock()
	if qe.exprs == nil {
		qe.exprs = map[string]string{}
	}
	qe.exprs[table] = expr
}

// excludeCacheKey keys a transform on the compaction exclusion it embeds. The
// transform cache outlives a compaction's flag by up to its TTL; a transform
// built before the flag has no exclusion and, reused after it, reads the
// inputs and the output (QR-8). "" when nothing is excluded, so keys are
// unchanged while no compaction is in its upload -> delete window. When the
// manifests cannot be read, the last key read is used: a cache hit serves
// what it served before this key existed, and a miss fails in the transform
// as an incomplete listing (P3Q-1).
func (h *QueryHandler) excludeCacheKey(ctx context.Context) string {
	if h.manifestManager == nil {
		return ""
	}
	all, err := h.uploadedInputs(ctx)
	if err != nil {
		k, _ := h.lastExcludeKey.Load().(string)
		return k
	}
	key := ""
	if len(all) > 0 {
		var sum uint64
		for k := range all {
			f := fnv.New64a()
			f.Write([]byte(k))
			sum += f.Sum64()
		}
		key = "ce" + strconv.Itoa(len(all)) + "." + strconv.FormatUint(sum, 16) + "|"
	}
	h.lastExcludeKey.Store(key)
	return key
}

// compactionExcludeExpr renders the WHERE fragment hiding files (full paths,
// as DuckDB reports them in the helper column); "" for none.
func compactionExcludeExpr(files []string) string {
	if len(files) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(compactionSourceFileColumn)
	b.WriteString(" NOT IN (")
	for i, f := range files {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(sqlutil.QuoteStringLiteral(f))
	}
	b.WriteString(")")
	return b.String()
}

// tableExcludePaths returns the full storage paths of database/measurement's
// uploaded compaction inputs, sorted. Storage keys become full S3/Azure/local
// paths (storage.GetFullPath unwraps the resilient wrapper and includes any
// s3_prefix) to match DuckDB's filename column.
func (h *QueryHandler) tableExcludePaths(all map[string]struct{}, database, measurement string) []string {
	files := filterExcludesForTable(all, database, measurement)
	out := make([]string, 0, len(files))
	for f := range files {
		out = append(out, storage.GetFullPath(h.storage, f))
	}
	sort.Strings(out)
	return out
}
