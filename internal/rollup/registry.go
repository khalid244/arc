package rollup

import "sort"

// Registry is the read-side index of available rollups, populated once from
// config at process start. Lookups are O(1) by (database, source_table).
type Registry struct {
	byTable map[string][]RollupSpec
}

func NewRegistry(specs []RollupSpec) *Registry {
	r := &Registry{byTable: map[string][]RollupSpec{}}
	for _, s := range specs {
		key := s.Database + "." + s.SourceTable
		r.byTable[key] = append(r.byTable[key], s)
	}
	for k := range r.byTable {
		sort.Slice(r.byTable[k], func(i, j int) bool {
			return r.byTable[k][i].BucketInterval < r.byTable[k][j].BucketInterval
		})
	}
	return r
}

// ForTable returns all rollups for a source table, finest-first.
func (r *Registry) ForTable(database, table string) []RollupSpec {
	return r.byTable[database+"."+table]
}
