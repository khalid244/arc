package tiered

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Same EmitArgs → same SQL twice. If anything in the emit path iterates a
// map without sorting, this test will eventually fail across Go versions
// and CPU counts. Run with a hundred iterations to give the map iteration
// PRNG a chance to expose order drift.
func TestIR_DeterministicEmission(t *testing.T) {
	spec := &Spec{
		Table:      "downloads",
		TZ:         "UTC",
		TimeColumn: "time",
		Dims: map[string]DimSpec{
			"site":    {Role: "Dim", KeptValues: []string{"a", "b"}},
			"country": {Role: "Dim", KeptValues: []string{"US"}},
			"city":    {Role: "Dim", KeptValues: []string{"NYC"}},
		},
	}
	timeLo := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	timeHi := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC)
	files := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/default/downloads/1h/2026/05/10/by_site/a.parquet",
		},
	}

	shape := &QueryShape{
		OriginalSQL: "SELECT 1",
		Table:       "downloads",
		TimeColumn:  "time",
		TimeLo:      timeLo,
		TimeHi:      timeHi,
		BucketArg:   "hour",
		GroupDims:   []string{"site"},
		// 3 filter-only dims to give map iteration room to misbehave.
		Filters: map[string]FilterPredicate{
			"country": {Op: "=", Values: []string{"US"}},
			"city":    {Op: "=", Values: []string{"NYC"}},
			"site":    {Op: "IN", Values: []string{"a"}},
		},
		Aggregates: []Aggregate{{Kind: AggCountStar}},
	}
	args := EmitArgs{
		Ctx:               context.Background(),
		Shape:             shape,
		Tier:              Tier1h,
		TailLo:            timeHi,
		Variant:           "by_site",
		Files:             files,
		Spec:              spec,
		SkipCoverageCheck: true,
	}

	first, ok := EmitMergeOnRead(args)
	if !ok {
		t.Fatalf("first emit refused")
	}
	for i := 0; i < 100; i++ {
		got, ok := EmitMergeOnRead(args)
		if !ok {
			t.Fatalf("iteration %d emit refused", i)
		}
		if got != first {
			t.Fatalf("iteration %d differs from first\nfirst:\n%s\ngot:\n%s", i, first, got)
		}
	}
}

// Same Statement built twice gives the same string. This is a tighter unit
// check than the emit-level test above — it isolates the IR from the rest.
func TestIR_StatementBuildDeterministic(t *testing.T) {
	mk := func() *Statement {
		rollup := NewSelect(RollupMode).
			Project(FuncExpr("SUM", Col("cnt")), "v").
			From(ReadParquet([]string{"a.parquet"})).
			Where(BinOp(">=", Col("bucket"), TimestampLit(time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)))).
			Where(In(Col("site_class"), []string{"a", "b", "c"}, false)).
			GroupBy(Col("bucket"))
		return NewStatement().
			Setup("SET TimeZone = 'UTC'").
			WithCTE("rollup", rollup).
			Body(NewSelect(RollupMode).Project(Col("v"), "value").From(FromCTE("rollup")))
	}
	first, err := mk().Build()
	if err != nil {
		t.Fatalf("first build err: %v", err)
	}
	for i := 0; i < 100; i++ {
		got, err := mk().Build()
		if err != nil {
			t.Fatalf("iter %d err: %v", i, err)
		}
		if got != first {
			t.Fatalf("iter %d differs", i)
		}
	}
	if !strings.Contains(first, "SUM(cnt)") || !strings.Contains(first, "WITH rollup AS") {
		t.Fatalf("unexpected sql shape: %s", first)
	}
}
