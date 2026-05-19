package tiered

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

const createEvents = `CREATE TABLE events (
	time TIMESTAMPTZ,
	dim_a VARCHAR,
	dim_b VARCHAR,
	dim_c VARCHAR,
	dim_d VARCHAR,
	dim_e VARCHAR,
	dim_f VARCHAR,
	dim_g VARCHAR,
	dim_h VARCHAR,
	dim_i VARCHAR,
	dim_j VARCHAR,
	metric_a DOUBLE,
	metric_b DOUBLE,
	user_id VARCHAR,
	ip_addr VARCHAR,
	url_addr VARCHAR,
	title_str VARCHAR
)`

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatalf("OpenWithDataSketches: %v", err)
	}
	if _, err := db.Exec(createEvents); err != nil {
		db.Close()
		t.Fatalf("CREATE TABLE: %v", err)
	}
	return db
}

func TestExtractShape_BasicTimeBucketCount(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	sql := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	        WHERE time BETWEEN '2026-05-01' AND '2026-05-15'
	        GROUP BY 1 ORDER BY 1`
	qs, err := ExtractQueryShape(ctx, db, sql)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported, got: %s", qs.Reason)
	}
	if qs.Table != "events" {
		t.Errorf("Table=%q want events", qs.Table)
	}
	if qs.BucketArg != "day" {
		t.Errorf("BucketArg=%q want day", qs.BucketArg)
	}
	if qs.TimeColumn != "time" {
		t.Errorf("TimeColumn=%q want time", qs.TimeColumn)
	}
	if len(qs.Aggregates) != 1 || qs.Aggregates[0].Kind != AggCountStar {
		t.Errorf("Aggregates=%+v want one AggCountStar", qs.Aggregates)
	}
}

func TestExtractShape_DiscoversTimeBoundsFromBetween(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15'
	      GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.TimeLo.IsZero() {
		t.Error("TimeLo is zero")
	}
	if qs.TimeHi.IsZero() {
		t.Error("TimeHi is zero")
	}
	if got := qs.TimeLo.Format("2006-01-02"); got != "2026-05-01" {
		t.Errorf("TimeLo=%q want 2026-05-01", got)
	}
	if got := qs.TimeHi.Format("2006-01-02"); got != "2026-05-15" {
		t.Errorf("TimeHi=%q want 2026-05-15", got)
	}
}

func TestExtractShape_DiscoversTimeBoundsFromGEandLT(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE time >= '2026-05-01' AND time < '2026-05-16'
	      GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.TimeLo.IsZero() {
		t.Error("TimeLo is zero")
	}
	if qs.TimeHi.IsZero() {
		t.Error("TimeHi is zero")
	}
	if got := qs.TimeLo.Format("2006-01-02"); got != "2026-05-01" {
		t.Errorf("TimeLo=%q want 2026-05-01", got)
	}
	if got := qs.TimeHi.Format("2006-01-02"); got != "2026-05-16" {
		t.Errorf("TimeHi=%q want 2026-05-16", got)
	}
}

func TestExtractShape_AcceptsNoTimeFilter(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	// Queries without a time filter are parser-accepted; variant selection handles fallback.
	q := `SELECT COUNT(*) FROM events GROUP BY dim_a`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported=true for no-time-filter aggregate query, got Reason=%q", qs.Reason)
	}
}

func TestExtractShape_RefusesOpenEndedTimeFilter(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE time >= '2026-05-01'
	      GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for open-ended time filter")
	}
	if qs.Reason == "" {
		t.Error("expected non-empty Reason")
	}
}

func TestExtractShape_RefusesNoAggregate(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT * FROM events WHERE time BETWEEN '2026-05-01' AND '2026-05-15'`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for query with no aggregate")
	}
	if qs.Reason == "" {
		t.Error("expected non-empty Reason")
	}
}

func TestExtractShape_ExtractsBucketArgFromDateTruncDay(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.BucketArg != "day" {
		t.Errorf("BucketArg=%q want day", qs.BucketArg)
	}
}

func TestExtractShape_ExtractsBucketArgFromDateTruncHour(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT date_trunc('hour', time) AS h, COUNT(*) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.BucketArg != "hour" {
		t.Errorf("BucketArg=%q want hour", qs.BucketArg)
	}
}

func TestExtractShape_ExtractsBucketArgFromDateTruncWeek(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT date_trunc('week', time) AS w, COUNT(*) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.BucketArg != "week" {
		t.Errorf("BucketArg=%q want week", qs.BucketArg)
	}
}

func TestExtractShape_ExtractsBucketArgFromDateTruncMonth(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT date_trunc('month', time) AS m, SUM(metric_a) FROM events
	      WHERE time >= '2026-05-01' AND time < '2026-06-01' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if qs.BucketArg != "month" {
		t.Errorf("BucketArg=%q want month", qs.BucketArg)
	}
	if len(qs.Aggregates) != 1 || qs.Aggregates[0].Kind != AggSum {
		t.Errorf("Aggregates=%+v want one AggSum", qs.Aggregates)
	}
}

func TestExtractShape_CountStar(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 {
		t.Fatalf("want 1 aggregate, got %d", len(qs.Aggregates))
	}
	if qs.Aggregates[0].Kind != AggCountStar {
		t.Errorf("Kind=%v want AggCountStar", qs.Aggregates[0].Kind)
	}
	if qs.Aggregates[0].Column != "" {
		t.Errorf("Column=%q want empty", qs.Aggregates[0].Column)
	}
}

func TestExtractShape_CountColumn(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(dim_a) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 {
		t.Fatalf("want 1 aggregate, got %d", len(qs.Aggregates))
	}
	if qs.Aggregates[0].Kind != AggCount {
		t.Errorf("Kind=%v want AggCount", qs.Aggregates[0].Kind)
	}
	if qs.Aggregates[0].Column != "dim_a" {
		t.Errorf("Column=%q want dim_a", qs.Aggregates[0].Column)
	}
}

func TestExtractShape_Sum(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, SUM(metric_a) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 {
		t.Fatalf("want 1 aggregate, got %d", len(qs.Aggregates))
	}
	if qs.Aggregates[0].Kind != AggSum {
		t.Errorf("Kind=%v want AggSum", qs.Aggregates[0].Kind)
	}
	if qs.Aggregates[0].Column != "metric_a" {
		t.Errorf("Column=%q want metric_a", qs.Aggregates[0].Column)
	}
}

func TestExtractShape_Avg(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, AVG(metric_a) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 {
		t.Fatalf("want 1 aggregate, got %d", len(qs.Aggregates))
	}
	if qs.Aggregates[0].Kind != AggAvg {
		t.Errorf("Kind=%v want AggAvg", qs.Aggregates[0].Kind)
	}
	if qs.Aggregates[0].Column != "metric_a" {
		t.Errorf("Column=%q want metric_a", qs.Aggregates[0].Column)
	}
}

func TestExtractShape_Min(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, MIN(metric_a) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 {
		t.Fatalf("want 1 aggregate, got %d", len(qs.Aggregates))
	}
	if qs.Aggregates[0].Kind != AggMin {
		t.Errorf("Kind=%v want AggMin", qs.Aggregates[0].Kind)
	}
	if qs.Aggregates[0].Column != "metric_a" {
		t.Errorf("Column=%q want metric_a", qs.Aggregates[0].Column)
	}
}

func TestExtractShape_Max(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, MAX(metric_a) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 {
		t.Fatalf("want 1 aggregate, got %d", len(qs.Aggregates))
	}
	if qs.Aggregates[0].Kind != AggMax {
		t.Errorf("Kind=%v want AggMax", qs.Aggregates[0].Kind)
	}
	if qs.Aggregates[0].Column != "metric_a" {
		t.Errorf("Column=%q want metric_a", qs.Aggregates[0].Column)
	}
}

func TestExtractShape_CountDistinct(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(DISTINCT user_id) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 {
		t.Fatalf("want 1 aggregate, got %d", len(qs.Aggregates))
	}
	if qs.Aggregates[0].Kind != AggCountDistinct {
		t.Errorf("Kind=%v want AggCountDistinct", qs.Aggregates[0].Kind)
	}
	if qs.Aggregates[0].Column != "user_id" {
		t.Errorf("Column=%q want user_id", qs.Aggregates[0].Column)
	}
}

func TestExtractShape_QuantileCont(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, quantile_cont(metric_a, 0.95) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 {
		t.Fatalf("want 1 aggregate, got %d", len(qs.Aggregates))
	}
	agg := qs.Aggregates[0]
	if agg.Kind != AggQuantile {
		t.Errorf("Kind=%v want AggQuantile", agg.Kind)
	}
	if agg.Column != "metric_a" {
		t.Errorf("Column=%q want metric_a", agg.Column)
	}
	if agg.Quantile != 0.95 {
		t.Errorf("Quantile=%v want 0.95", agg.Quantile)
	}
}

func TestExtractShape_MultipleAggregates(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, SUM(metric_a), AVG(metric_a), COUNT(*) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 3 {
		t.Fatalf("want 3 aggregates, got %d: %+v", len(qs.Aggregates), qs.Aggregates)
	}
	kinds := [3]AggKind{qs.Aggregates[0].Kind, qs.Aggregates[1].Kind, qs.Aggregates[2].Kind}
	if kinds[0] != AggSum || kinds[1] != AggAvg || kinds[2] != AggCountStar {
		t.Errorf("aggregate kinds=%v want [AggSum AggAvg AggCountStar]", kinds)
	}
}

func TestExtractShape_RefusesStddev(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, stddev(metric_a) FROM events
	      WHERE time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for stddev aggregate")
	}
	if qs.Reason == "" {
		t.Error("expected non-empty Reason")
	}
	if !strings.Contains(qs.Reason, "stddev") && !strings.Contains(qs.Reason, "untranslatable") {
		t.Errorf("Reason=%q should mention stddev or untranslatable", qs.Reason)
	}
}

func TestExtractShape_FilterEqualsString(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE dim_a='val_a' AND time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	f, ok := qs.Filters["dim_a"]
	if !ok {
		t.Fatal("expected Filters[dim_a]")
	}
	if f.Op != "=" {
		t.Errorf("Op=%q want =", f.Op)
	}
	if len(f.Values) != 1 || f.Values[0] != "val_a" {
		t.Errorf("Values=%v want [val_a]", f.Values)
	}
}

func TestExtractShape_FilterIN(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE dim_a IN ('val_a','val_b','val_c') AND time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	f, ok := qs.Filters["dim_a"]
	if !ok {
		t.Fatal("expected Filters[dim_a]")
	}
	if f.Op != "IN" {
		t.Errorf("Op=%q want IN", f.Op)
	}
	if len(f.Values) != 3 || f.Values[0] != "val_a" || f.Values[1] != "val_b" || f.Values[2] != "val_c" {
		t.Errorf("Values=%v want [val_a val_b val_c]", f.Values)
	}
}

func TestExtractShape_FilterNotIN(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE dim_a NOT IN ('val_a','val_b') AND time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	f, ok := qs.Filters["dim_a"]
	if !ok {
		t.Fatal("expected Filters[dim_a]")
	}
	if f.Op != "NOT IN" {
		t.Errorf("Op=%q want NOT IN", f.Op)
	}
	if len(f.Values) != 2 || f.Values[0] != "val_a" || f.Values[1] != "val_b" {
		t.Errorf("Values=%v want [val_a val_b]", f.Values)
	}
}

func TestExtractShape_FilterIsNotNull(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE dim_a IS NOT NULL AND time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	f, ok := qs.Filters["dim_a"]
	if !ok {
		t.Fatal("expected Filters[dim_a]")
	}
	if f.Op != "IS NOT NULL" {
		t.Errorf("Op=%q want IS NOT NULL", f.Op)
	}
	if len(f.Values) != 0 {
		t.Errorf("Values=%v want nil/empty", f.Values)
	}
}

func TestExtractShape_MultipleEqualityFilters(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE dim_a='val_a' AND dim_d='no' AND dim_e='tag_a' AND time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if len(qs.Filters) != 3 {
		t.Fatalf("expected 3 filters, got %d: %+v", len(qs.Filters), qs.Filters)
	}
	check := func(col, wantVal string) {
		t.Helper()
		f, ok := qs.Filters[col]
		if !ok {
			t.Errorf("missing Filters[%s]", col)
			return
		}
		if f.Op != "=" || len(f.Values) != 1 || f.Values[0] != wantVal {
			t.Errorf("Filters[%s]=%+v want {Op:= Values:[%s]}", col, f, wantVal)
		}
	}
	check("dim_a", "val_a")
	check("dim_d", "no")
	check("dim_e", "tag_a")
}

func TestExtractShape_RefusesORedFilters(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE (dim_a='val_a' OR dim_a='val_b') AND time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for OR filter")
	}
	if qs.Reason == "" {
		t.Error("expected non-empty Reason")
	}
	if !strings.Contains(strings.ToUpper(qs.Reason), "OR") && !strings.Contains(qs.Reason, "unsupported") {
		t.Errorf("Reason=%q should mention OR or unsupported", qs.Reason)
	}
}

func TestExtractShape_RefusesLIKE(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE dim_a LIKE 'S%' AND time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for LIKE filter")
	}
	if qs.Reason == "" {
		t.Error("expected non-empty Reason")
	}
}

func TestExtractShape_RefusesJoin(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT COUNT(*) FROM events a JOIN events b ON a.dim_b=b.dim_b`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for JOIN query")
	}
	if qs.Reason == "" {
		t.Error("expected non-empty Reason")
	}
	if !strings.Contains(strings.ToLower(qs.Reason), "join") {
		t.Errorf("Reason=%q should mention join", qs.Reason)
	}
}

func TestExtractShape_RefusesSelectStar(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT * FROM events LIMIT 10`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for SELECT * query")
	}
	if qs.Reason == "" {
		t.Error("expected non-empty Reason")
	}
	lowerReason := strings.ToLower(qs.Reason)
	if !strings.Contains(lowerReason, "select_star") && !strings.Contains(lowerReason, "no aggregate") && !strings.Contains(lowerReason, "limit") {
		t.Errorf("Reason=%q should mention select_star, no aggregate, or limit", qs.Reason)
	}
}

func TestExtractShape_RefusesWindowFunction(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT dim_b, COUNT(*), LAG(COUNT(*)) OVER (ORDER BY dim_b) FROM events GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for window function query")
	}
	if qs.Reason == "" {
		t.Error("expected non-empty Reason")
	}
	if !strings.Contains(strings.ToLower(qs.Reason), "window") {
		t.Errorf("Reason=%q should mention window", qs.Reason)
	}
}

func TestExtractShape_RefusesSubqueryInWHERE(t *testing.T) {
	ctx := context.Background()
	db, err := OpenWithDataSketches("UTC")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(createEvents); err != nil {
		t.Fatal(err)
	}
	q := `SELECT COUNT(*) FROM events WHERE dim_a IN (SELECT dim_a FROM events GROUP BY dim_a HAVING COUNT(*) > 1000000)`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for subquery in WHERE")
	}
	if qs.Reason == "" {
		t.Error("expected non-empty Reason")
	}
	lowerReason := strings.ToLower(qs.Reason)
	if !strings.Contains(lowerReason, "subquery") && !strings.Contains(lowerReason, "join") && !strings.Contains(lowerReason, "nested") {
		t.Errorf("Reason=%q should mention subquery, join, or nested", qs.Reason)
	}
}

func TestExtractShape_TimeRangeNotInFilters(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events
	      WHERE dim_a='val_a' AND time BETWEEN '2026-05-01' AND '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported: %s", qs.Reason)
	}
	if _, ok := qs.Filters["time"]; ok {
		t.Error("time column must not appear in Filters; it belongs to TimeLo/TimeHi")
	}
	if qs.TimeLo.IsZero() || qs.TimeHi.IsZero() {
		t.Error("TimeLo/TimeHi must be set")
	}
}

// ---- focused unit tests for cases surfaced by catalog ----

func TestExtractShape_RefusesMinuteBucket(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('minute', time) AS t, COUNT(*) FROM events
	      WHERE time >= '2026-05-14' AND time < '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for minute bucket")
	}
	if !strings.Contains(qs.Reason, "minute") {
		t.Errorf("Reason=%q should mention minute", qs.Reason)
	}
}

func TestExtractShape_RefusesSecondBucket(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('second', time) AS t, COUNT(*) FROM events
	      WHERE time >= '2026-05-14' AND time < '2026-05-15' GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for second bucket")
	}
	if !strings.Contains(qs.Reason, "second") {
		t.Errorf("Reason=%q should mention second", qs.Reason)
	}
}

func TestExtractShape_AcceptsNoTimeBounds(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, COUNT(*) FROM events GROUP BY 1 ORDER BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported=true for no-time-bounds aggregate: %s", qs.Reason)
	}
	if qs.BucketArg != "day" {
		t.Errorf("BucketArg=%q want day", qs.BucketArg)
	}
}

func TestExtractShape_AcceptsAggregateWithoutDateTrunc(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT COUNT(*) AS value FROM events WHERE time >= '2026-03-17' AND time < '2026-05-15'`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported=true for time-filtered aggregate without date_trunc: %s", qs.Reason)
	}
	if qs.BucketArg != "" {
		t.Errorf("BucketArg=%q want empty (no date_trunc)", qs.BucketArg)
	}
}

func TestExtractShape_AcceptsQuantileWithoutTimeBounds(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT quantile_cont(metric_a, 0.95) FROM events`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported=true for timeless quantile: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 || qs.Aggregates[0].Kind != AggQuantile {
		t.Errorf("expected single AggQuantile, got %+v", qs.Aggregates)
	}
}

func TestExtractShape_AcceptsMultipleQuantiles(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT quantile_cont(metric_a, 0.5) AS p50, quantile_cont(metric_a, 0.95) AS p95 FROM events`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported=true for multiple quantiles: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 2 {
		t.Fatalf("expected 2 aggregates, got %d", len(qs.Aggregates))
	}
}

func TestExtractShape_AcceptsLimitWithAggregate(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('hour', time) AS h, COUNT(*) FROM events GROUP BY 1 ORDER BY 1 LIMIT 5`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported=true for aggregate with LIMIT: %s", qs.Reason)
	}
}

func TestExtractShape_AcceptsTopKWithLimitNoTime(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT dim_b, COUNT(*) AS c FROM events GROUP BY 1 ORDER BY c DESC LIMIT 10`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported=true for top-K dim query: %s", qs.Reason)
	}
}

func TestExtractShape_RefusesFloorGroupBy(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT FLOOR(metric_a/10)*10 AS bin, COUNT(*) FROM events
	      WHERE time >= '2026-05-01' AND time < '2026-05-15' GROUP BY 1 ORDER BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for FLOOR in GROUP BY")
	}
	if !strings.Contains(qs.Reason, "unsupported GROUP BY") {
		t.Errorf("Reason=%q should mention unsupported GROUP BY", qs.Reason)
	}
}

func TestExtractShape_AcceptsSumCaseWhen(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, SUM(CASE WHEN dim_a='val_a' THEN 1 ELSE 0 END) FROM events GROUP BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported=true for SUM(CASE WHEN ...): %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 || qs.Aggregates[0].Kind != AggSum {
		t.Errorf("expected AggSum, got %+v", qs.Aggregates)
	}
}

// TestExtractShape_RefusesHaving — HAVING is a post-aggregation filter the
// router doesn't translate (no HAVING-aware emit). Earlier versions of the
// parser silently dropped the predicate, accepting the query and returning
// extra rows. The router now refuses to keep results truthful; callers fall
// back to the source scan.
func TestExtractShape_RefusesHaving(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT dim_b, COUNT(*) AS c FROM events GROUP BY 1 HAVING c > 1000000 ORDER BY c DESC`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if qs.Supported {
		t.Fatal("expected Supported=false for HAVING clause (silent-drop avoided)")
	}
	if qs.Reason == "" {
		t.Error("expected non-empty Reason")
	}
}

func TestExtractShape_AcceptsArithmeticInProjection(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, AVG(metric_a) * 100 FROM events GROUP BY 1 ORDER BY 1`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported=true for arithmetic in projection: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 || qs.Aggregates[0].Kind != AggAvg {
		t.Errorf("expected AggAvg, got %+v", qs.Aggregates)
	}
}

func TestExtractShape_AcceptsDimGroupByWithDateTrunc(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT date_trunc('day', time) AS d, dim_a, COUNT(*) FROM events
	      WHERE dim_a IN ('val_a','val_b','val_c') GROUP BY 1,2 ORDER BY 1,2`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported=true for date_trunc + dim GROUP BY: %s", qs.Reason)
	}
	if qs.BucketArg != "day" {
		t.Errorf("BucketArg=%q want day", qs.BucketArg)
	}
}

func TestExtractShape_CountDistinctNoTimeBounds(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defer db.Close()
	q := `SELECT COUNT(DISTINCT user_id) FROM events WHERE dim_a='val_a'`
	qs, err := ExtractQueryShape(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !qs.Supported {
		t.Fatalf("expected Supported=true for COUNT DISTINCT without time: %s", qs.Reason)
	}
	if len(qs.Aggregates) != 1 || qs.Aggregates[0].Kind != AggCountDistinct {
		t.Errorf("expected AggCountDistinct, got %+v", qs.Aggregates)
	}
}

