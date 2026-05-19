package tiered

import (
	"strings"
	"testing"
	"time"
)

func makeFileIndex(tier, variant string, paths []string) *MemoryFileIndex {
	return &MemoryFileIndex{Paths: paths}
}

func makeSpec(tz string, dims map[string]DimSpec) *Spec {
	return &Spec{
		Table:      "db.events",
		TZ:         tz,
		TimeColumn: "time",
		Dims:       dims,
	}
}

func TestEmit_SketchVariant_NoOpenTail(t *testing.T) {
	timeLo := mustTime("2026-03-01")
	timeHi := mustTime("2026-05-15")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/03/01/sketch/f1.parquet"})
	spec := makeSpec("Asia/Riyadh", nil)
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     timeLo,
		TimeHi:     timeHi,
		BucketArg:  "day",
		Aggregates: []Aggregate{{Kind: AggCountStar, OutputAlias: "total"}},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  timeHi,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	if !strings.Contains(sql, "SET TimeZone = 'Asia/Riyadh'") {
		t.Errorf("missing timezone: %s", sql)
	}
	if !strings.Contains(sql, "WITH rollup AS") {
		t.Errorf("missing rollup CTE: %s", sql)
	}
	if strings.Contains(sql, "fresh AS") {
		t.Errorf("should not have fresh CTE when no open tail: %s", sql)
	}
	if !strings.Contains(sql, "f1.parquet") {
		t.Errorf("missing read_parquet: %s", sql)
	}
	if !strings.Contains(sql, "date_trunc('day', bucket)") {
		t.Errorf("missing date_trunc: %s", sql)
	}
	if !strings.Contains(sql, "FROM rollup") {
		t.Errorf("missing FROM rollup (no UNION): %s", sql)
	}
	if strings.Contains(sql, "UNION ALL") {
		t.Errorf("should not have UNION ALL without open tail: %s", sql)
	}
}

func TestEmit_SketchVariant_WithOpenTail(t *testing.T) {
	timeLo := mustTime("2026-05-01")
	tailLo := mustTime("2026-05-10")
	timeHi := mustTime("2026-05-15")

	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/db/events/1h/2026/05/01/sketch/main.parquet",
			"_arc/rollup/db/events/1h/2026/05/10/sketch/fresh.parquet",
		},
	}
	spec := makeSpec("UTC", nil)
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     timeLo,
		TimeHi:     timeHi,
		BucketArg:  "day",
		Aggregates: []Aggregate{{Kind: AggCountStar}},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  tailLo,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	if !strings.Contains(sql, "fresh AS") {
		t.Errorf("expected fresh CTE: %s", sql)
	}
	if !strings.Contains(sql, "UNION ALL") {
		t.Errorf("expected UNION ALL: %s", sql)
	}
	// Post single-tier migration: open tail always falls to source-scan, not
	// a finer rollup tier. The fresh CTE reads the source table directly.
	if !strings.Contains(sql, "FROM db.events") {
		t.Errorf("expected source-scan fresh CTE: %s", sql)
	}
	if !strings.Contains(sql, "time >= TIMESTAMP '2026-05-10") {
		t.Errorf("expected tailLo (source time column) in fresh CTE: %s", sql)
	}
}

func TestEmit_BySiteVariant_WithFilter(t *testing.T) {
	timeLo := mustTime("2026-04-01")
	timeHi := mustTime("2026-04-30")
	idx := makeFileIndex("1h", "by_country", []string{"_arc/rollup/db/events/1h/2026/04/01/by_country/f.parquet"})
	spec := makeSpec("Asia/Riyadh", map[string]DimSpec{
		"country": {Role: "PerDim", KeptValues: []string{"SA", "AE", "KW"}},
	})
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     timeLo,
		TimeHi:     timeHi,
		BucketArg:  "day",
		GroupDims:  []string{"country"},
		Filters:    map[string]FilterPredicate{"country": {Op: "=", Values: []string{"SA"}}},
		Aggregates: []Aggregate{{Kind: AggCountStar, OutputAlias: "cnt"}},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  timeHi,
		Variant: "by_country",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	if !strings.Contains(sql, "country_class = 'SA'") {
		t.Errorf("expected dim filter mapped to _class col: %s", sql)
	}
	if !strings.Contains(sql, "country_class AS country") {
		t.Errorf("expected outer SELECT mapping _class to user name: %s", sql)
	}
	if !strings.Contains(sql, "country_class") {
		t.Errorf("expected country_class in GROUP BY: %s", sql)
	}
}

func TestEmit_AllVariant_MultiDim(t *testing.T) {
	timeLo := mustTime("2026-01-01")
	timeHi := mustTime("2026-03-01")
	idx := makeFileIndex("1h", "all", []string{"_arc/rollup/db/events/1h/2026/01/15/all/f.parquet"})
	spec := makeSpec("Asia/Riyadh", map[string]DimSpec{
		"country":  {Role: "Dim", KeptValues: []string{"SA", "AE"}, EffectiveCard: 10},
		"platform": {Role: "Dim", KeptValues: []string{"ios", "android"}, EffectiveCard: 5},
	})
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     timeLo,
		TimeHi:     timeHi,
		BucketArg:  "month",
		GroupDims:  []string{"country", "platform"},
		Aggregates: []Aggregate{{Kind: AggSum, Column: "revenue", OutputAlias: "total_revenue"}},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  timeHi,
		Variant: "all",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	if !strings.Contains(sql, "country_class") {
		t.Errorf("expected country_class in query: %s", sql)
	}
	if !strings.Contains(sql, "platform_class") {
		t.Errorf("expected platform_class in query: %s", sql)
	}
	if !strings.Contains(sql, "GROUP BY _bkt, country_class, platform_class") {
		t.Errorf("expected multi-dim GROUP BY: %s", sql)
	}
}

func TestEmit_TimeZonePinned(t *testing.T) {
	timeHi := mustTime("2026-05-15")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/05/01/sketch/f.parquet"})
	spec := makeSpec("Asia/Riyadh", nil)
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     mustTime("2026-05-01"),
		TimeHi:     timeHi,
		BucketArg:  "day",
		Aggregates: []Aggregate{{Kind: AggCountStar}},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  timeHi,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if !ok {
		t.Fatalf("expected ok=true")
	}
	if !strings.Contains(sql, "SET TimeZone = 'Asia/Riyadh';") {
		t.Errorf("expected timezone pin as first statement: %s", sql)
	}
	if !strings.HasPrefix(strings.TrimSpace(sql), "SET TimeZone") {
		t.Errorf("SET TimeZone must be the first statement: %s", sql)
	}
}

func TestEmit_NoDimsNoFilters_SingleAgg(t *testing.T) {
	timeHi := mustTime("2026-05-15")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/05/01/sketch/f.parquet"})
	spec := makeSpec("UTC", nil)
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     mustTime("2026-05-01"),
		TimeHi:     timeHi,
		BucketArg:  "day",
		GroupDims:  nil,
		Filters:    nil,
		Aggregates: []Aggregate{{Kind: AggCountStar, OutputAlias: "total"}},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  timeHi,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if !ok {
		t.Fatalf("expected ok=true")
	}
	if strings.Contains(sql, "_class") {
		t.Errorf("no-dim query should have no _class columns: %s", sql)
	}
	if !strings.Contains(sql, "GROUP BY _bkt") {
		t.Errorf("expected GROUP BY _bkt: %s", sql)
	}
}

func TestEmit_RefusesWhenNoFiles(t *testing.T) {
	timeHi := mustTime("2026-05-15")
	idx := &MemoryFileIndex{}
	spec := makeSpec("UTC", nil)
	shape := &QueryShape{
		Table:       "db.events",
		TimeColumn:  "time",
		TimeLo:      mustTime("2026-05-01"),
		TimeHi:      timeHi,
		BucketArg:   "day",
		Aggregates:  []Aggregate{{Kind: AggCountStar}},
		OriginalSQL: "SELECT count(*) FROM db.events",
	}

	_, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  timeHi,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if ok {
		t.Fatal("expected ok=false when no files for tier/variant")
	}
}

func TestEmit_OuterAggsInOuterSelect(t *testing.T) {
	timeHi := mustTime("2026-05-15")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/05/01/sketch/f.parquet"})
	spec := makeSpec("UTC", nil)
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     mustTime("2026-05-01"),
		TimeHi:     timeHi,
		BucketArg:  "day",
		Aggregates: []Aggregate{
			{Kind: AggSum, Column: "revenue", OutputAlias: "rev"},
			{Kind: AggMax, Column: "duration", OutputAlias: "max_dur"},
		},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  timeHi,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if !ok {
		t.Fatalf("expected ok=true")
	}
	if !strings.Contains(sql, "SUM(_agg_0) AS rev") {
		t.Errorf("expected outer sum expr: %s", sql)
	}
	if !strings.Contains(sql, "MAX(_agg_1) AS max_dur") {
		t.Errorf("expected outer max expr: %s", sql)
	}
}

func TestEmit_HavingClausePreserved(t *testing.T) {
	timeHi := mustTime("2026-05-15")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/05/01/sketch/f.parquet"})
	spec := makeSpec("UTC", nil)
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     mustTime("2026-05-01"),
		TimeHi:     timeHi,
		BucketArg:  "day",
		Aggregates: []Aggregate{{Kind: AggCountStar, OutputAlias: "total"}},
		Having:     []HavingClause{{AggIndex: 0, Op: ">", Value: 100}},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  timeHi,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if !ok {
		t.Fatalf("expected ok=true")
	}
	if !strings.Contains(sql, "HAVING") {
		t.Errorf("expected HAVING clause: %s", sql)
	}
	if !strings.Contains(sql, "> 100") {
		t.Errorf("expected HAVING condition > 100: %s", sql)
	}
}

func TestEmit_OrderLimitPreserved(t *testing.T) {
	timeHi := mustTime("2026-05-15")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/05/01/sketch/f.parquet"})
	spec := makeSpec("UTC", nil)
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     mustTime("2026-05-01"),
		TimeHi:     timeHi,
		BucketArg:  "day",
		Aggregates: []Aggregate{{Kind: AggCountStar, OutputAlias: "total"}},
		OrderLimit: &OrderLimit{AggIndex: 0, Desc: true, Limit: 10},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  timeHi,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if !ok {
		t.Fatalf("expected ok=true")
	}
	if !strings.Contains(sql, "ORDER BY") {
		t.Errorf("expected ORDER BY: %s", sql)
	}
	if !strings.Contains(sql, "DESC") {
		t.Errorf("expected DESC: %s", sql)
	}
	if !strings.Contains(sql, "LIMIT 10") {
		t.Errorf("expected LIMIT 10: %s", sql)
	}
}

func TestEmit_OpenTail_1h_FallsToRaw(t *testing.T) {
	timeLo := mustTime("2026-05-01")
	tailLo := mustTime("2026-05-10")
	timeHi := mustTime("2026-05-15")

	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/db/events/1h/2026/05/01/sketch/main.parquet",
		},
	}
	spec := makeSpec("UTC", nil)
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "ts",
		TimeLo:     timeLo,
		TimeHi:     timeHi,
		BucketArg:  "hour",
		Aggregates: []Aggregate{{Kind: AggCountStar}},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  tailLo,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if !ok {
		t.Fatalf("expected ok=true")
	}
	if !strings.Contains(sql, "fresh AS") {
		t.Errorf("expected fresh CTE for 1h open tail: %s", sql)
	}
	if !strings.Contains(sql, "FROM db.events") {
		t.Errorf("expected raw source table in fresh CTE: %s", sql)
	}
	if !strings.Contains(sql, "date_trunc('hour', ts)") {
		t.Errorf("expected date_trunc on TimeColumn for raw source: %s", sql)
	}
	if !strings.Contains(sql, "UNION ALL") {
		t.Errorf("expected UNION ALL: %s", sql)
	}
}

func TestEmit_BucketArgWeek_PicksWeekFromBucket(t *testing.T) {
	timeLo := mustTime("2026-03-01")
	timeHi := mustTime("2026-05-01")
	idx := makeFileIndex("1h", "sketch", []string{"_arc/rollup/db/events/1h/2026/03/01/sketch/f.parquet"})
	spec := makeSpec("UTC", nil)
	shape := &QueryShape{
		Table:      "db.events",
		TimeColumn: "time",
		TimeLo:     timeLo,
		TimeHi:     timeHi,
		BucketArg:  "week",
		Aggregates: []Aggregate{{Kind: AggCountStar}},
	}

	sql, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  timeHi,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})

	if !ok {
		t.Fatalf("expected ok=true")
	}
	if !strings.Contains(sql, "date_trunc('week', bucket)") {
		t.Errorf("expected date_trunc('week', bucket): %s", sql)
	}
}

func TestEmit_RefusesWhenAllFilesHaveWrongTierOrVariant(t *testing.T) {
	spec := &Spec{Table: "t", TZ: "UTC", TimeColumn: "time"}
	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/default/t/1h/2026/05/14/by_other/stale.parquet",
		},
	}
	shape := &QueryShape{
		Supported:   true,
		Table:       "t",
		TimeColumn:  "time",
		TimeLo:      time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC),
		TimeHi:      time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC),
		BucketArg:   "day",
		Aggregates:  []Aggregate{{Kind: AggCountStar, OutputAlias: "c"}},
		OriginalSQL: "SELECT count(*) FROM t",
	}
	_, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  shape.TimeHi,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})
	if ok {
		t.Error("expected EmitMergeOnRead to refuse when no files match tier/variant")
	}
}

func TestEmit_AcceptsMatchingFiles(t *testing.T) {
	spec := &Spec{Table: "t", TZ: "UTC", TimeColumn: "time"}
	idx := &MemoryFileIndex{
		Paths: []string{
			"_arc/rollup/default/t/1h/2026/05/14/sketch/current.parquet",
		},
	}
	shape := &QueryShape{
		Supported:  true,
		Table:      "t",
		TimeColumn: "time",
		TimeLo:     time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC),
		TimeHi:     time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC),
		BucketArg:  "day",
		Aggregates: []Aggregate{{Kind: AggCountStar, OutputAlias: "c"}},
	}
	_, ok := EmitMergeOnRead(EmitArgs{
		Shape:   shape,
		Tier:    Tier1h,
		TailLo:  shape.TimeHi,
		Variant: "sketch",
		Files:   idx,
		Spec:              spec,
		SkipCoverageCheck: true,
	})
	if !ok {
		t.Error("expected EmitMergeOnRead to accept matching files")
	}
}

func TestBuildFilterExpr_NullSemanticsUseSentinel(t *testing.T) {
	// IS NULL must compare to the '_null_' sentinel because the builder
	// COALESCEs NULL source values to that sentinel; the class column is
	// never SQL-NULL.
	if got, want := buildFilterExpr("country_class", FilterPredicate{Op: "IS NULL"}), "country_class = '_null_'"; got != want {
		t.Errorf("IS NULL: got %q, want %q", got, want)
	}
	if got, want := buildFilterExpr("country_class", FilterPredicate{Op: "IS NOT NULL"}), "country_class <> '_null_'"; got != want {
		t.Errorf("IS NOT NULL: got %q, want %q", got, want)
	}
}
