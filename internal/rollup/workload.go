package rollup

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Workload is the observed query signal that drives per-dim cube selection: it
// counts how often each dimension of each source is actually queried. Rollup
// then materializes a per-dim cube only for dimensions that are queried (above a
// cheap cardinality threshold, where building everything would waste storage) —
// so the cube set scales with the real workload, not with the table's column
// count. Safe for concurrent Record (query path) + reads (build path).
//
// Multi-pod model: queries land on route-only replicas while planSpecs runs on
// the single builder pod, so the signal has to cross pods through object
// storage. Counts live in three layers that DimCounts sums:
//
//   - dims:   what THIS process recorded (Record).
//   - seed:   what this pod recorded in PREVIOUS lives, resumed from its own
//     durable file at startup (LoadBytes).
//   - remote: other pods' files, merged under a per-file namespace
//     (MergeNamespaceBytes) with REPLACE semantics.
//
// Each observation has exactly ONE durable home — a reader pod's recordings in
// its per-pod file, the builder's own in _workload.json — enforced by Bytes
// serializing only seed+dims (never remote). That is what makes the whole
// scheme idempotent: the builder can re-merge every per-pod file every tick
// (replace, not add), restart and re-merge again (seed never contains remote),
// and a pod can never echo another pod's counts back through its own persist.
type Workload struct {
	mu      sync.Mutex
	entries map[string]*WorkloadEntry            // legacy shape index (used by the standalone planner)
	dims    map[string]map[string]int            // own recordings: source -> dim -> observation count
	seed    map[string]map[string]int            // this pod's resumed history (LoadBytes target)
	remote  map[string]map[string]map[string]int // namespace (per-pod file key) -> source -> dim -> count
	seq     uint64                               // bumped per Record; persisters compare it to skip no-new-data writes
}

// WorkloadEntry is one observed shape with its frequency.
type WorkloadEntry struct {
	Shape QueryShape
	Count int
}

func NewWorkload() *Workload {
	return &Workload{
		entries: map[string]*WorkloadEntry{},
		dims:    map[string]map[string]int{},
		seed:    map[string]map[string]int{},
		remote:  map[string]map[string]map[string]int{},
	}
}

// shapeSig is the planning identity of a shape: source, grain, required dims, and
// aggregate keys. Two queries with the same sig need the same materialization.
func shapeSig(q QueryShape) string {
	dims := append([]string(nil), q.requiredDims()...)
	sort.Strings(dims)
	aggs := make([]string, len(q.Aggs))
	for i, a := range q.Aggs {
		aggs[i] = aggKey(a)
	}
	sort.Strings(aggs)
	return strings.Join([]string{q.Source, q.Grain, strings.Join(dims, ","), strings.Join(aggs, ";")}, "|")
}

func aggKey(a Aggregate) string {
	return strings.Join([]string{
		strconv.Itoa(int(a.Kind)), a.Col,
		strconv.FormatFloat(a.P, 'g', -1, 64),
	}, ":")
}

// Record observes one query: bumps its shape frequency and each required
// dimension's count for the shape's source.
func (w *Workload) Record(q QueryShape) {
	if q.Source == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seq++
	sig := shapeSig(q)
	if e, ok := w.entries[sig]; ok {
		e.Count++
	} else {
		w.entries[sig] = &WorkloadEntry{Shape: q, Count: 1}
	}
	if w.dims[q.Source] == nil {
		w.dims[q.Source] = map[string]int{}
	}
	for _, d := range q.requiredDims() {
		w.dims[q.Source][d]++
	}
}

// Seq returns a monotonic count of recorded queries. Persisters compare it to
// the value at their last write and skip the PUT when nothing new was observed
// — every-tick unconditional writes are real pressure on object storage.
// LoadBytes/MergeNamespaceBytes do NOT advance it: resumed or remote counts are
// already durable elsewhere and never make THIS pod's file worth rewriting.
func (w *Workload) Seq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// DimCounts returns how often each dimension of source has been queried, summed
// across this pod's own recordings, its resumed history, and every merged
// per-pod namespace (cross-pod counts are additive — two replicas each seeing
// half the traffic rank a dim as high as one pod seeing all of it).
func (w *Workload) DimCounts(source string) map[string]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[string]int{}
	add := func(dm map[string]int) {
		for d, n := range dm {
			out[d] += n
		}
	}
	add(w.dims[source])
	add(w.seed[source])
	for _, pod := range w.remote {
		add(pod[source])
	}
	return out
}

// Hot returns shapes observed at least minCount times, most frequent first.
func (w *Workload) Hot(minCount int) []WorkloadEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []WorkloadEntry
	for _, e := range w.entries {
		if e.Count >= minCount {
			out = append(out, *e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return shapeSig(out[i].Shape) < shapeSig(out[j].Shape)
	})
	return out
}

// Bytes serializes the per-source dim counts this pod OWNS (resumed seed + own
// recordings) for persistence across restarts. Remote namespaces are
// deliberately excluded: those counts belong to other pods' files, and
// re-exporting them here would re-import them as seed on the next restart while
// the still-present per-pod files merge them again — counting them twice (and
// again every restart).
func (w *Workload) Bytes() ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	merged := map[string]map[string]int{}
	for _, layer := range []map[string]map[string]int{w.seed, w.dims} {
		for src, dm := range layer {
			if merged[src] == nil {
				merged[src] = map[string]int{}
			}
			for d, n := range dm {
				merged[src][d] += n
			}
		}
	}
	return json.MarshalIndent(map[string]any{"dims": merged}, "", "  ")
}

// LoadBytes resumes this pod's own persisted dim counts (called once at startup
// with the pod's OWN durable file). They land in the seed layer — separate from
// dims — so Bytes round-trips them while merges of other pods' files
// (MergeNamespaceBytes) stay out of what this pod persists as its own.
func (w *Workload) LoadBytes(b []byte) error {
	var doc struct {
		Dims map[string]map[string]int `json:"dims"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for src, dm := range doc.Dims {
		if w.seed[src] == nil {
			w.seed[src] = map[string]int{}
		}
		for d, n := range dm {
			w.seed[src][d] += n
		}
	}
	return nil
}

// MergeNamespaceBytes merges one per-pod workload file under ns, REPLACING any
// previous merge of the same namespace. Per-pod files carry ABSOLUTE counters
// (a pod's Bytes only grow within its lifetime), so replacement makes the
// builder's every-tick re-merge idempotent — additive merging (the old
// LoadBytes semantics) would double the same file's counts every tick. Files of
// pods that no longer exist keep contributing their final counts: that is
// recorded history, intentionally retained across deploy rollovers.
func (w *Workload) MergeNamespaceBytes(ns string, b []byte) error {
	var doc struct {
		Dims map[string]map[string]int `json:"dims"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.remote[ns] = doc.Dims
	return nil
}
