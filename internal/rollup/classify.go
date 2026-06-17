package rollup

import (
	"fmt"
	"sort"
	"strings"
)

// Auto-classification derives a cube set for ANY table from its Parquet schema
// and measured column cardinalities — no column names are hardcoded. This is what
// lets Rollup serve arbitrary tables/databases without per-table code.
//
// Classification per non-time column:
//   - numeric & high-cardinality            -> metric  (sum/min/max/count/avg + KLL p95)
//   - cardinality <= MaxPerDimCard           -> dimension (gets its own per-dim cube)
//   - string & very-high-cardinality         -> sketch column (HLL distinct in the coarse cube)
//
// Cube set produced:
//   - one COARSE cube (no dims): count + every metric's exact+sketch aggregates +
//     an HLL per high-card column. Low-dimensional => sketches stay accurate.
//   - one PER-DIM cube (exact aggregates only) for each dimension: cheap to build,
//     serves group-by/filter on that dimension.

type ClassifyConfig struct {
	MaxDimCard    int // <= this => can fold into a shared/low-card dim cube (default 1024)
	MaxPerDimCard int // <= this => worth a dedicated per-dim cube (default 50000)
	MaxDims       int // cap on per-dim cubes (default 16)
	SampleRows    int // cardinality probe sample cap (default 5,000,000)
}

func (c ClassifyConfig) withDefaults() ClassifyConfig {
	if c.MaxDimCard == 0 {
		c.MaxDimCard = 1024
	}
	if c.MaxPerDimCard == 0 {
		c.MaxPerDimCard = 50000
	}
	if c.MaxDims == 0 {
		c.MaxDims = 16
	}
	if c.SampleRows == 0 {
		c.SampleRows = 5_000_000
	}
	return c
}

type colInfo struct {
	name string
	typ  string // DuckDB column type
}

// isNumericType reports whether a DuckDB type is a numeric (aggregatable) type.
func isNumericType(t string) bool {
	t = strings.ToUpper(t)
	for _, p := range []string{"DOUBLE", "FLOAT", "DECIMAL", "BIGINT", "INTEGER", "HUGEINT", "SMALLINT", "TINYINT", "REAL", "NUMERIC"} {
		if strings.Contains(t, p) {
			return true
		}
	}
	return false
}

// isContinuousType reports whether a DuckDB type is a continuous floating/decimal
// measure (DOUBLE/FLOAT/REAL/DECIMAL) — as opposed to an integer, which can be a
// legitimate low-card dimension (e.g. a status code). Continuous columns are always
// metrics: a real-valued measure's distinct count is under-counted by the row-capped
// cardinality sample, and grouping a cube by it explodes the cube toward source size.
func isContinuousType(t string) bool {
	t = strings.ToUpper(t)
	for _, p := range []string{"DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"} {
		if strings.Contains(t, p) {
			return true
		}
	}
	return false
}

// colClass is the rollup role assigned to a source column.
type colClass int

const (
	classSkip   colClass = iota // all-NULL in the sample — ignored
	classMetric                 // numeric measure: sum/min/max/avg + p95
	classDim                    // categorical dimension: eligible for a per-dim cube
	classSketch                 // very-high-card string: HLL distinct in the coarse cube
)

// classifyColumn assigns a column's rollup role from its type and sampled
// cardinality. Continuous types are metrics regardless of the sampled cardinality
// (it under-counts a real-valued measure); integers split metric/dim/sketch by
// cardinality so low-card codes stay dimensions.
func classifyColumn(typ string, card int, cfg ClassifyConfig) colClass {
	switch {
	case card == 0:
		return classSkip
	case isContinuousType(typ):
		return classMetric
	case isNumericType(typ) && card > cfg.MaxDimCard:
		return classMetric
	case card <= cfg.MaxPerDimCard:
		return classDim
	default:
		return classSketch
	}
}

// continuousDimEligible reports whether a continuous column — already classified as a
// metric by classifyColumn — should ALSO be registered as a dimension. True only for a
// low-card, integer-valued float: a categorical code mis-typed as a float (e.g. an HTTP
// status stored as DOUBLE), never a fractional measure (price, duration, latency). The
// column STAYS a metric, so AVG/percentile are unaffected — this only ADDS group-by/
// filter coverage. Keeping it a metric is what preserves the continuous->metric rule the
// duration_seconds fix relies on (see TestClassifyColumn), so there is no regression:
// genuine measures keep metric-only treatment, mis-typed codes additionally get a dim.
func continuousDimEligible(typ string, card int, intValued bool, cfg ClassifyConfig) bool {
	cfg = cfg.withDefaults()
	return isContinuousType(typ) && intValued && card > 0 && card <= cfg.MaxDimCard
}

// describeColumnSet returns the columns readable via fromExpr (a read_parquet
// expression or a table name) as a lower-cased name -> DuckDB type map. This is
// THE schema probe for every build/manager site: errors always PROPAGATE (a
// failed probe must never read as "no columns", which would silently degrade or
// skip a build), and names are case-folded because DuckDB binds identifiers
// case-insensitively and union_by_name merges differently-cased columns — so a
// source column "UserId" satisfies a spec column "userid".
func describeColumnSet(db Execer, fromExpr string) (map[string]string, error) {
	cols, err := describeColumns(db, fromExpr)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(cols))
	for _, c := range cols {
		out[strings.ToLower(c.name)] = c.typ
	}
	return out, nil
}

// describeColumns lists (name, type) for every column readable via readExpr.
func describeColumns(db Execer, readExpr string) ([]colInfo, error) {
	r, qerr := db.Query("DESCRIBE SELECT * FROM " + readExpr)
	if qerr != nil {
		return nil, qerr
	}
	defer r.Close()
	cols, _ := r.Columns()
	var out []colInfo
	for r.Next() {
		raw := make([]any, len(cols))
		ptr := make([]any, len(cols))
		for i := range raw {
			ptr[i] = &raw[i]
		}
		if err := r.Scan(ptr...); err != nil {
			return nil, err
		}
		out = append(out, colInfo{name: fmt.Sprintf("%v", raw[0]), typ: fmt.Sprintf("%v", raw[1])})
	}
	return out, r.Err()
}

// cardinalities measures approximate distinct counts for the given columns in a
// single pass over a row-capped sample (approx_count_distinct is cheap).
func cardinalities(db Execer, readExpr string, cols []string, sampleRows int) (map[string]int, error) {
	if len(cols) == 0 {
		return map[string]int{}, nil
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("approx_count_distinct(%q) AS %q", c, c)
	}
	q := fmt.Sprintf("SELECT %s FROM (SELECT * FROM %s LIMIT %d)", strings.Join(parts, ", "), readExpr, sampleRows)
	r, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	out := map[string]int{}
	if r.Next() {
		vals := make([]any, len(cols))
		ptr := make([]any, len(cols))
		for i := range vals {
			ptr[i] = &vals[i]
		}
		if err := r.Scan(ptr...); err != nil {
			return nil, err
		}
		for i, c := range cols {
			out[c] = toInt(vals[i])
		}
	}
	return out, r.Err()
}

// integerValued reports, for each given column, whether every non-NULL value in a
// row-capped sample is a whole number (v == floor(v)). An integer-valued float is a
// categorical code mis-typed as a float — not a real-valued measure. A column with no
// non-NULL sampled values reads as false (no evidence it is categorical). One pass.
func integerValued(db Execer, readExpr string, cols []string, sampleRows int) (map[string]bool, error) {
	out := map[string]bool{}
	if len(cols) == 0 {
		return out, nil
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		// Count fractional (non-whole, non-NULL) values; 0 => integer-valued.
		parts[i] = fmt.Sprintf("COALESCE(SUM(CASE WHEN %q IS NOT NULL AND %q <> floor(%q) THEN 1 ELSE 0 END), 0) AS %q", c, c, c, c)
	}
	q := fmt.Sprintf("SELECT %s FROM (SELECT * FROM %s LIMIT %d)", strings.Join(parts, ", "), readExpr, sampleRows)
	r, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if r.Next() {
		vals := make([]any, len(cols))
		ptr := make([]any, len(cols))
		for i := range vals {
			ptr[i] = &vals[i]
		}
		if err := r.Scan(ptr...); err != nil {
			return nil, err
		}
		for i, c := range cols {
			out[c] = toInt(vals[i]) == 0
		}
	}
	return out, r.Err()
}

func toInt(v any) int {
	switch x := v.(type) {
	case int64:
		return int(x)
	case int32:
		return int(x)
	case float64:
		return int(x)
	default:
		var n int
		fmt.Sscanf(fmt.Sprintf("%v", x), "%d", &n)
		return n
	}
}

// TableProfile is the classified shape of a table: its metrics, high-card sketch
// columns, and the dimensions eligible for a per-dim cube (with their measured
// cardinality). Cube *selection* (which per-dim cubes to actually build) is a
// separate, workload-driven decision — see Manager.planSpecs.
type TableProfile struct {
	Source     string
	Grain      string
	Metrics    []string          // continuous numeric columns
	SketchCols []string          // very-high-card strings (HLL distinct)
	DimCard    map[string]int    // eligible dimension -> cardinality
	Types      map[string]string // lower-cased column name -> DuckDB type (for typed NULL drift casts)
	// ForcedMetrics lists continuous columns (-> sampled cardinality) that were
	// classified metric DESPITE a dim-eligible cardinality (card <= MaxPerDimCard).
	// Intentional (a continuous measure's sampled cardinality under-counts), but it
	// removes per-dim/dim-rich coverage those columns would otherwise have had —
	// the Manager warns once per source so the coverage loss is operator-visible.
	ForcedMetrics map[string]int
}

// ProfileTable inspects a table's schema + per-column cardinalities and classifies
// every column. It does NOT decide the cube set — that is workload-driven.
func ProfileTable(db Execer, source, timeCol, grain, readExpr string, cfg ClassifyConfig) (TableProfile, error) {
	cfg = cfg.withDefaults()
	cols, err := describeColumns(db, readExpr)
	if err != nil {
		return TableProfile{}, fmt.Errorf("describe %s: %w", source, err)
	}
	names := make([]string, 0, len(cols))
	typ := map[string]string{}
	for _, c := range cols {
		if c.name == timeCol {
			continue
		}
		names = append(names, c.name)
		typ[c.name] = c.typ
	}
	cards, err := cardinalities(db, readExpr, names, cfg.SampleRows)
	if err != nil {
		return TableProfile{}, fmt.Errorf("cardinalities %s: %w", source, err)
	}
	// Probe which low-card continuous columns are integer-valued in the sample. A
	// float that only ever holds whole numbers is a categorical code mis-typed as a
	// float (e.g. an HTTP status stored as DOUBLE); it gets a per-dim cube below in
	// ADDITION to staying a metric. Only low-card continuous columns are probed, so
	// this is at most one extra cheap aggregate pass (usually zero columns).
	var contCands []string
	for _, n := range names {
		if isContinuousType(typ[n]) && cards[n] > 0 && cards[n] <= cfg.MaxDimCard {
			contCands = append(contCands, n)
		}
	}
	intValued, err := integerValued(db, readExpr, contCands, cfg.SampleRows)
	if err != nil {
		return TableProfile{}, fmt.Errorf("integer-valued probe %s: %w", source, err)
	}
	p := TableProfile{Source: source, Grain: grain, DimCard: map[string]int{}, Types: map[string]string{}}
	for _, n := range names {
		p.Types[strings.ToLower(n)] = typ[n]
		switch classifyColumn(typ[n], cards[n], cfg) {
		case classMetric:
			p.Metrics = append(p.Metrics, n)
			if isContinuousType(typ[n]) && cards[n] <= cfg.MaxPerDimCard {
				// A continuous column that is dim-eligible by cardinality. If it's a
				// low-card, integer-valued float it's really a categorical code: ALSO
				// register it as a dimension so group-by/filter rolls up (it stays a
				// metric, so AVG/percentile are unaffected — no duration_seconds-bug
				// regression). Otherwise it's a genuine measure with no dim coverage:
				// record it so the Manager warns.
				if continuousDimEligible(typ[n], cards[n], intValued[n], cfg) {
					p.DimCard[n] = cards[n]
				} else {
					if p.ForcedMetrics == nil {
						p.ForcedMetrics = map[string]int{}
					}
					p.ForcedMetrics[n] = cards[n]
				}
			}
		case classDim:
			p.DimCard[n] = cards[n]
		case classSketch:
			p.SketchCols = append(p.SketchCols, n)
		}
		// classSkip: all-NULL in the sample — no useful dimension, metric, or sketch.
	}
	sort.Strings(p.Metrics)
	sort.Strings(p.SketchCols)
	return p, nil
}

// CoarseSpec is the always-on dimensionless cube: count + per-metric exact &
// percentile aggregates + an HLL per high-card column. Low-dimensional, so its
// sketches stay accurate.
func (p TableProfile) CoarseSpec() CubeSpec {
	aggs := []Aggregate{{Kind: AggCount}}
	for _, m := range p.Metrics {
		aggs = append(aggs,
			Aggregate{Kind: AggAvg, Col: m},
			Aggregate{Kind: AggMin, Col: m},
			Aggregate{Kind: AggMax, Col: m},
			Aggregate{Kind: AggCountCol, Col: m},
			Aggregate{Kind: AggPercentile, Col: m, P: 0.95},
		)
	}
	for _, s := range p.SketchCols {
		aggs = append(aggs, Aggregate{Kind: AggCountDistinct, Col: s})
	}
	return CubeSpec{Source: p.Source, Grain: p.Grain, Dims: nil, Aggs: aggs, ColTypes: p.colTypesFor(nil, aggs)}
}

// colTypesFor collects the profiled DuckDB types of every source column a cube
// references (dims + aggregate inputs), keyed lower-cased — the type source for
// the typed NULL casts a drifted period's build emits (see buildSelectFrom).
func (p TableProfile) colTypesFor(dims []string, aggs []Aggregate) map[string]string {
	if len(p.Types) == 0 {
		return nil
	}
	out := map[string]string{}
	add := func(col string) {
		if col == "" {
			return
		}
		lc := strings.ToLower(col)
		if t, ok := p.Types[lc]; ok {
			out[lc] = t
		}
	}
	for _, d := range dims {
		add(d)
	}
	for _, a := range aggs {
		add(a.Col)
		add(a.ThenCol)
		for _, c := range a.CondCols {
			add(c)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// exactAggs is the exact (sketch-free) aggregate payload used by per-dim and
// dim-rich cubes: count plus every metric's sum/min/max/count (enough for
// count, sum, avg, min, max — sketches stay in the coarse cube).
func (p TableProfile) exactAggs() []Aggregate {
	aggs := []Aggregate{{Kind: AggCount}}
	for _, m := range p.Metrics {
		aggs = append(aggs,
			Aggregate{Kind: AggAvg, Col: m},
			Aggregate{Kind: AggMin, Col: m},
			Aggregate{Kind: AggMax, Col: m},
			Aggregate{Kind: AggCountCol, Col: m},
		)
	}
	return aggs
}

// PerDimSpec is an exact-only per-dimension cube (cheap to build, accurate).
func (p TableProfile) PerDimSpec(dim string) CubeSpec {
	dims, aggs := []string{dim}, p.exactAggs()
	return CubeSpec{Source: p.Source, Grain: p.Grain, Dims: dims, Aggs: aggs, ColTypes: p.colTypesFor(dims, aggs)}
}

// targetedSpec builds an operator-declared cube over an explicit dim set (+ optional
// COUNT(DISTINCT) sketch columns) declared in [[rollup.cube]]. Returns ok=false (so the
// caller warns and skips) when the dim set is empty, a dim is not a profiled dimension,
// or a distinct column is unknown — a config typo can't produce a broken cube.
func (p TableProfile) targetedSpec(dims, distinct []string) (CubeSpec, bool) {
	if len(dims) == 0 {
		return CubeSpec{}, false
	}
	for _, d := range dims {
		if _, ok := p.DimCard[d]; !ok {
			return CubeSpec{}, false
		}
	}
	for _, d := range distinct {
		if !p.knownColumn(d) {
			return CubeSpec{}, false
		}
	}
	sorted := append([]string(nil), dims...)
	sort.Strings(sorted)
	aggs := []Aggregate{{Kind: AggCount}}
	for _, d := range distinct {
		aggs = append(aggs, Aggregate{Kind: AggCountDistinct, Col: d})
	}
	return CubeSpec{Source: p.Source, Grain: p.Grain, Dims: sorted, Aggs: aggs, ColTypes: p.colTypesFor(sorted, aggs)}, true
}

// knownColumn reports whether col was profiled on this table — as a dimension, a
// high-card sketch column, or a metric. Used to validate targeted-cube columns.
func (p TableProfile) knownColumn(col string) bool {
	if _, ok := p.DimCard[col]; ok {
		return true
	}
	for _, s := range p.SketchCols {
		if s == col {
			return true
		}
	}
	for _, m := range p.Metrics {
		if m == col {
			return true
		}
	}
	return false
}

// DimRichSpec is an exact cube over the LOW-cardinality dimensions (card <=
// lowCardMax). It serves any query whose group-by/filter dims are a subset of these
// — including multi-dimension queries (e.g. site × response) that no single-dim cube
// covers. Only low-card dims are unioned so the cross-product stays far below source
// size; a medium-card dim (which the sample under-counts and would explode the wide
// cube toward source size) is excluded and stays covered by its own per-dim cube.
// Returns ok=false when there are fewer than 2 or more than maxDims such dims.
func (p TableProfile) DimRichSpec(maxDims, lowCardMax int) (CubeSpec, bool) {
	return p.dimRichSpecFrom(p.lowCardDims(lowCardMax), maxDims)
}

// dimRichSpecFrom is DimRichSpec over a PRE-COMPUTED low-card dim list, so a
// caller that also needs the list for its skip warning (Manager.planSpecs)
// computes it once — the decision and the signal cannot diverge.
func (p TableProfile) dimRichSpecFrom(dims []string, maxDims int) (CubeSpec, bool) {
	if len(dims) < 2 || len(dims) > maxDims {
		return CubeSpec{}, false
	}
	aggs := p.exactAggs()
	return CubeSpec{Source: p.Source, Grain: p.Grain, Dims: dims, Aggs: aggs, ColTypes: p.colTypesFor(dims, aggs)}, true
}

// lowCardDims returns the sorted dimensions whose cardinality is at or below max —
// the only dims eligible for the dim-rich cross-product, so a medium-card dim can't
// blow the wide cube up toward source size.
func (p TableProfile) lowCardDims(max int) []string {
	dims := make([]string, 0, len(p.DimCard))
	for d, card := range p.DimCard {
		if card <= max {
			dims = append(dims, d)
		}
	}
	sort.Strings(dims)
	return dims
}

// Classify is a convenience used by the standalone build tool / tests: profile
// the table then build the coarse cube plus a per-dim cube for every eligible
// dimension (capped). The production Manager profiles separately and selects
// per-dim cubes from the observed workload instead.
func Classify(db Execer, source, timeCol, readExpr string, cfg ClassifyConfig) ([]CubeSpec, error) {
	cfg = cfg.withDefaults()
	p, err := ProfileTable(db, source, timeCol, "hour", readExpr, cfg)
	if err != nil {
		return nil, err
	}
	dims := make([]string, 0, len(p.DimCard))
	for d := range p.DimCard {
		dims = append(dims, d)
	}
	sort.Strings(dims)
	cubes := []CubeSpec{p.CoarseSpec()}
	for i, d := range dims {
		if i >= cfg.MaxDims {
			break
		}
		cubes = append(cubes, p.PerDimSpec(d))
	}
	return cubes, nil
}
