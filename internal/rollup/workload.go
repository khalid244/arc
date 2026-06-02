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
type Workload struct {
	mu      sync.Mutex
	entries map[string]*WorkloadEntry // legacy shape index (used by the standalone planner)
	dims    map[string]map[string]int // source -> dim -> observation count
}

// WorkloadEntry is one observed shape with its frequency.
type WorkloadEntry struct {
	Shape QueryShape
	Count int
}

func NewWorkload() *Workload {
	return &Workload{entries: map[string]*WorkloadEntry{}, dims: map[string]map[string]int{}}
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

// DimCounts returns how often each dimension of source has been queried.
func (w *Workload) DimCounts(source string) map[string]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[string]int{}
	for d, n := range w.dims[source] {
		out[d] = n
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

// Bytes serializes the per-source dim counts for persistence across restarts.
func (w *Workload) Bytes() ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return json.MarshalIndent(map[string]any{"dims": w.dims}, "", "  ")
}

// LoadBytes merges persisted dim counts back in (called once on startup).
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
		if w.dims[src] == nil {
			w.dims[src] = map[string]int{}
		}
		for d, n := range dm {
			w.dims[src][d] += n
		}
	}
	return nil
}
