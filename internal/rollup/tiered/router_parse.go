package tiered

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ExtractQueryShape runs DuckDB json_serialize_plan on the user SQL and
// walks the resulting logical plan to populate QueryShape. Returns shape with
// Supported=false when the query doesn't fit a known pattern.
func ExtractQueryShape(ctx context.Context, db *sql.DB, userSQL string) (*QueryShape, error) {
	qs := &QueryShape{OriginalSQL: userSQL, Filters: make(map[string]FilterPredicate)}

	escaped := strings.ReplaceAll(userSQL, "'", "''")
	stmt := fmt.Sprintf("SELECT json_serialize_plan('%s')::VARCHAR", escaped)

	row := db.QueryRowContext(ctx, stmt)
	var planJSON string
	if err := row.Scan(&planJSON); err != nil {
		qs.Reason = fmt.Sprintf("serialize_plan failed: %v", err)
		return qs, nil
	}

	var root planRoot
	if err := json.Unmarshal([]byte(planJSON), &root); err != nil {
		qs.Reason = "plan JSON parse failed"
		return qs, nil
	}
	if root.Error {
		qs.Reason = "plan reported error"
		return qs, nil
	}
	if len(root.Plans) == 0 {
		qs.Reason = "empty plan"
		return qs, nil
	}

	walkNode(root.Plans[0], qs)

	// Second pass: apply LOGICAL_PROJECTION arithmetic (e.g. avg*100, sum/count).
	applyProjectionArithmetic(root.Plans[0], qs)

	if qs.Reason != "" {
		return qs, nil
	}
	// When Arc tables are backed by a read_parquet view (the production case),
	// DuckDB inlines the view at plan time so LOGICAL_GET points at the
	// read_parquet table function with no `table` field. Extract the bare
	// table name from the SQL's FROM clause as a fallback — qs.Table is
	// only used by the emitter for the open-tail source-scan FROM, which
	// Arc's downstream rewrite converts to read_parquet anyway.
	if qs.Table == "" {
		qs.Table = extractFromTable(userSQL)
	}
	if qs.Table == "" {
		qs.Reason = "no source table found"
		return qs, nil
	}
	// If a time bound was found on one side only, require the other side too.
	if (!qs.TimeLo.IsZero() || !qs.TimeHi.IsZero()) && (qs.TimeLo.IsZero() || qs.TimeHi.IsZero()) {
		qs.Reason = "missing two-sided time filter"
		return qs, nil
	}
	if len(qs.Aggregates) == 0 {
		qs.Reason = "no aggregate in SELECT"
		return qs, nil
	}
	qs.Supported = true
	return qs, nil
}

// ---- plan JSON types ----

type planRoot struct {
	Error bool       `json:"error"`
	Plans []planNode `json:"plans"`
}

type planNode struct {
	Type     string                 `json:"type"`
	Children []planNode             `json:"children"`
	Extra    map[string]interface{} `json:"-"`

	// LOGICAL_GET
	FunctionData *logicalGetFnData `json:"function_data"`
	Names        []string          `json:"names"`

	// LOGICAL_FILTER / LOGICAL_AGGREGATE_AND_GROUP_BY / LOGICAL_PROJECTION
	Expressions []planExpr `json:"expressions"`

	// LOGICAL_AGGREGATE_AND_GROUP_BY
	Groups []planExpr `json:"groups"`
	// `grouping_sets` for a vanilla GROUP BY is `[[0,1,...]]` — one entry per
	// distinct grouping. ROLLUP, CUBE, and explicit GROUPING SETS produce
	// multiple entries (each subtotal level). The router only supports a
	// single grouping set; multiple → refuse.
	GroupingSets [][]int `json:"grouping_sets"`
}

type logicalGetFnData struct {
	Table string `json:"table"`
}

type planExpr struct {
	Type            string          `json:"type"`
	ExpressionClass string          `json:"expression_class"`
	Name            string          `json:"name"`           // BOUND_FUNCTION / BOUND_AGGREGATE
	AggregateType   string          `json:"aggregate_type"` // NON_DISTINCT / DISTINCT
	Alias           string          `json:"alias"`
	Children        []planExpr      `json:"children"`
	Left            *planExpr       `json:"left"`
	Right           *planExpr       `json:"right"`
	Child           *planExpr       `json:"child"` // BOUND_CAST wraps BOUND_CONSTANT
	Value           *planValue      `json:"value"` // VALUE_CONSTANT
	Index           *int            `json:"index"` // BOUND_REF
	ReturnType      *planType       `json:"return_type"`
	FunctionData    *aggFunctionData `json:"function_data"` // quantile_cont carries quantiles here
}

type aggFunctionData struct {
	Quantiles []planValue `json:"quantiles"`
}

type planValue struct {
	IsNull bool        `json:"is_null"`
	Value  interface{} `json:"value"`
	Type   planType    `json:"type"`
}

type planType struct {
	ID       string        `json:"id"`
	TypeInfo *planTypeInfo `json:"type_info"`
}

type planTypeInfo struct {
	Scale int `json:"scale"`
}

// ---- walker ----

func walkNode(n planNode, qs *QueryShape) {
	switch n.Type {
	case "LOGICAL_COMPARISON_JOIN", "LOGICAL_JOIN", "LOGICAL_DELIM_JOIN", "LOGICAL_ANY_JOIN":
		qs.Supported = false
		qs.Reason = "unsupported query shape: join"
		return

	case "LOGICAL_WINDOW":
		qs.Supported = false
		qs.Reason = "unsupported query shape: window function"
		return

	case "LOGICAL_GET":
		if n.FunctionData != nil && n.FunctionData.Table != "" {
			qs.Table = n.FunctionData.Table
		}

	case "LOGICAL_FILTER":
		for _, expr := range n.Expressions {
			gotTime := extractTimeFilter(expr, qs)
			if qs.Reason != "" {
				return
			}
			gotDim := extractDimFilter(expr, qs)
			if qs.Reason != "" {
				return
			}
			if !gotTime && !gotDim {
				qs.Supported = false
				// DuckDB lowers `WHERE col IN (subquery)` to a MARK join whose
				// result becomes a top-level BOUND_REF in the outer
				// LOGICAL_FILTER (alias often "SUBQUERY"). Surface that as a
				// subquery refusal rather than the generic class/type tag.
				if expr.ExpressionClass == "BOUND_REF" {
					qs.Reason = "unsupported filter: nested subquery"
				} else {
					qs.Reason = fmt.Sprintf("unsupported filter expression: %s/%s", expr.ExpressionClass, expr.Type)
				}
				return
			}
		}

	case "LOGICAL_AGGREGATE_AND_GROUP_BY":
		if len(n.GroupingSets) > 1 {
			qs.Supported = false
			qs.Reason = "unsupported GROUP BY: ROLLUP/CUBE/GROUPING SETS"
			return
		}
		for _, grp := range n.Groups {
			if reason := checkGroupExpr(grp); reason != "" {
				qs.Supported = false
				qs.Reason = reason
				return
			}
			extractDateTruncGroup(grp, qs)
			if qs.Reason != "" {
				return
			}
			extractToTimestampGroup(grp, qs)
			if qs.Reason != "" {
				return
			}
			extractDimGroup(grp, qs)
		}
		for _, agg := range n.Expressions {
			a, reason := extractAggregate(agg)
			if reason != "" {
				qs.Supported = false
				qs.Reason = reason
				return
			}
			if a != nil {
				qs.Aggregates = append(qs.Aggregates, *a)
			}
		}
	}

	for _, child := range n.Children {
		walkNode(child, qs)
	}
}

// extractTimeFilter inspects a LOGICAL_FILTER expression and updates TimeLo / TimeHi.
// Returns true if it recognised the expression as a time bound (handled).
func extractTimeFilter(expr planExpr, qs *QueryShape) bool {
	if expr.ExpressionClass != "BOUND_COMPARISON" {
		return false
	}
	if expr.Left == nil || expr.Right == nil {
		return false
	}

	// Left must be a BOUND_REF (the time column)
	if expr.Left.ExpressionClass != "BOUND_REF" {
		return false
	}
	colName := expr.Left.Alias
	if colName == "" {
		return false
	}

	// Right side: may be BOUND_CAST wrapping a BOUND_CONSTANT, or directly BOUND_CONSTANT
	val := extractConstantValue(expr.Right)
	if val == "" {
		return false
	}

	ts, err := parseTimestamp(val)
	if err != nil {
		return false
	}

	switch expr.Type {
	case "COMPARE_GREATERTHANOREQUALTO", "COMPARE_GREATERTHAN":
		if qs.TimeLo.IsZero() {
			qs.TimeColumn = colName
			qs.TimeLo = ts
		}
		return true
	case "COMPARE_LESSTHANOREQUALTO", "COMPARE_LESSTHAN":
		if qs.TimeHi.IsZero() {
			qs.TimeColumn = colName
			qs.TimeHi = ts
		}
		return true
	}
	return false
}

// extractDimFilter inspects a LOGICAL_FILTER expression and, if it is a recognised
// dim predicate (=, IN, NOT IN, IS NOT NULL), adds it to qs.Filters.
// Unrecognised or OR-combined predicates set Supported=false.
// Returns true if it recognised the expression shape (either classified as a
// dim filter or as an explicitly refused/known shape). Returns false to let
// the caller decide whether to refuse — typically when the expression has a
// shape we don't understand (e.g., EXTRACT() comparisons).
func extractDimFilter(expr planExpr, qs *QueryShape) bool {
	switch expr.Type {
	case "COMPARE_EQUAL":
		if expr.ExpressionClass != "BOUND_COMPARISON" {
			return false
		}
		if expr.Left == nil || expr.Left.ExpressionClass != "BOUND_REF" {
			return false
		}
		col := expr.Left.Alias
		if col == "" {
			return false
		}
		val := extractConstantValue(expr.Right)
		if val == "" {
			return false
		}
		if !isTimeBound(expr) {
			qs.Filters[col] = FilterPredicate{Op: "=", Values: []string{val}}
		}
		return true

	case "COMPARE_IN", "COMPARE_NOT_IN":
		if expr.ExpressionClass != "BOUND_OPERATOR" {
			return false
		}
		if len(expr.Children) < 2 {
			return false
		}
		ref := expr.Children[0]
		if ref.ExpressionClass != "BOUND_REF" {
			return false
		}
		col := ref.Alias
		if col == "" {
			return false
		}
		var vals []string
		for _, child := range expr.Children[1:] {
			v := extractConstantValue(&child)
			if v == "" {
				return false
			}
			vals = append(vals, v)
		}
		op := "IN"
		if expr.Type == "COMPARE_NOT_IN" {
			op = "NOT IN"
		}
		qs.Filters[col] = FilterPredicate{Op: op, Values: vals}
		return true

	case "OPERATOR_IS_NOT_NULL", "OPERATOR_IS_NULL":
		if expr.ExpressionClass != "BOUND_OPERATOR" {
			return false
		}
		if len(expr.Children) < 1 {
			return false
		}
		ref := expr.Children[0]
		if ref.ExpressionClass != "BOUND_REF" {
			return false
		}
		col := ref.Alias
		if col == "" {
			return false
		}
		op := "IS NOT NULL"
		if expr.Type == "OPERATOR_IS_NULL" {
			op = "IS NULL"
		}
		qs.Filters[col] = FilterPredicate{Op: op}
		return true

	case "CONJUNCTION_OR":
		qs.Supported = false
		qs.Reason = "unsupported filter predicate: OR"
		return true

	case "COMPARE_GREATERTHANOREQUALTO", "COMPARE_GREATERTHAN",
		"COMPARE_LESSTHANOREQUALTO", "COMPARE_LESSTHAN":
		// time-bound comparisons — already handled by extractTimeFilter when
		// left is a BOUND_REF over a column with a timestamp constant. Report
		// "not handled here" so the caller can refuse when the comparison is
		// over a non-time expression (e.g., EXTRACT(HOUR FROM time) > 9).
		return false

	default:
		if expr.ExpressionClass == "BOUND_COMPARISON" || expr.ExpressionClass == "BOUND_OPERATOR" || expr.ExpressionClass == "BOUND_FUNCTION" || expr.ExpressionClass == "BOUND_CONJUNCTION" {
			qs.Supported = false
			qs.Reason = fmt.Sprintf("unsupported filter predicate: %s", expr.Type)
			return true
		}
		return false
	}
}

// isTimeBound reports whether a COMPARE_EQUAL expression is a time-column equality
// that was already handled as a time bound (so we should not also store it as a dim filter).
func isTimeBound(expr planExpr) bool {
	if expr.Left == nil {
		return false
	}
	if expr.Right == nil {
		return false
	}
	_, err := parseTimestamp(extractConstantValue(expr.Right))
	return err == nil
}

// extractConstantValue digs through BOUND_CAST -> BOUND_CONSTANT to get the string value.
func extractConstantValue(expr *planExpr) string {
	if expr == nil {
		return ""
	}
	switch expr.ExpressionClass {
	case "BOUND_CAST":
		return extractConstantValue(expr.Child)
	case "BOUND_CONSTANT":
		if expr.Value != nil && !expr.Value.IsNull {
			if s, ok := expr.Value.Value.(string); ok {
				return s
			}
		}
	}
	return ""
}

// checkGroupExpr returns a non-empty reason string if the GROUP BY expression is unsupported.
// BOUND_REF (plain column), BOUND_FUNCTION date_trunc, and BOUND_FUNCTION
// to_timestamp((epoch_ns(col)//1e9//N)*N) are the supported group-by shapes.
func checkGroupExpr(grp planExpr) string {
	switch grp.ExpressionClass {
	case "BOUND_REF":
		return ""
	case "BOUND_FUNCTION":
		if grp.Name == "date_trunc" || grp.Name == "to_timestamp" {
			return ""
		}
		return fmt.Sprintf("unsupported GROUP BY expression: %s", grp.Name)
	default:
		return fmt.Sprintf("unsupported GROUP BY expression class: %s", grp.ExpressionClass)
	}
}

// extractToTimestampGroup recognises the Grafana plugin's bucketing idiom:
//
//	to_timestamp((epoch_ns(<col>) // 1000000000 // N) * N)
//
// where N is the bucket size in seconds. Maps N to BucketArg and (when N
// doesn't match an existing date_trunc unit) sets qs.UserBucketSecs so the
// emit applies an outer to_timestamp wrap.
//
// Refuses sub-hour buckets and non-aligned (non-multiple-of-3600 for
// sub-day, non-multiple-of-86400 for super-day) values — those can't be
// served correctly from the hourly rollup.
func extractToTimestampGroup(grp planExpr, qs *QueryShape) {
	if grp.ExpressionClass != "BOUND_FUNCTION" || grp.Name != "to_timestamp" {
		return
	}
	if len(grp.Children) != 1 {
		return
	}
	// child = optional BOUND_CAST wrapping the * expression
	mul := unwrapCast(&grp.Children[0])
	if mul == nil || mul.Name != "*" || len(mul.Children) != 2 {
		return
	}
	// mul.Children[0] = // expression (outer floor-div), [1] = N constant (must match the inner outer //'s N)
	outerDiv := unwrapCast(&mul.Children[0])
	outerN, okN := extractConstantInt(&mul.Children[1])
	if outerDiv == nil || outerDiv.Name != "//" || !okN {
		return
	}
	if len(outerDiv.Children) != 2 {
		return
	}
	innerDiv := unwrapCast(&outerDiv.Children[0])
	div2N, okN2 := extractConstantInt(&outerDiv.Children[1])
	if innerDiv == nil || innerDiv.Name != "//" || !okN2 || div2N != outerN {
		return
	}
	if len(innerDiv.Children) != 2 {
		return
	}
	epochCall := unwrapCast(&innerDiv.Children[0])
	nsPerSec, okNS := extractConstantInt(&innerDiv.Children[1])
	if epochCall == nil || epochCall.Name != "epoch_ns" || !okNS || nsPerSec != 1000000000 {
		return
	}
	if len(epochCall.Children) != 1 || epochCall.Children[0].ExpressionClass != "BOUND_REF" {
		return
	}
	timeCol := epochCall.Children[0].Alias

	// Map bucket seconds → BucketArg + UserBucketSecs.
	bucketArg, userBucketSecs, ok := mapBucketSecs(outerN)
	if !ok {
		qs.Reason = fmt.Sprintf("unsupported bucket size: %d seconds", outerN)
		return
	}
	if qs.BucketArg == "" {
		qs.BucketArg = bucketArg
	}
	qs.UserBucketSecs = userBucketSecs
	if qs.TimeColumn == "" && timeCol != "" {
		qs.TimeColumn = timeCol
	}
}

// mapBucketSecs converts a bucket size in seconds to (BucketArg, UserBucketSecs).
// Returns ok=false when the bucket is unsupported (sub-hour, non-aligned).
//
//	3600         → hour,  0          (use date_trunc('hour') unchanged)
//	86400        → day,   0
//	604800       → week,  0
//	multiple of 86400 (>1 day) → day, N (outer wraps to N-second bucket)
//	multiple of 3600 (>1 hour) → hour, N
//	anything else → refuse
func mapBucketSecs(n int64) (bucketArg string, userBucketSecs int64, ok bool) {
	if n <= 0 {
		return "", 0, false
	}
	switch n {
	case 3600:
		return "hour", 0, true
	case 86400:
		return "day", 0, true
	case 604800:
		return "week", 0, true
	}
	if n%86400 == 0 {
		return "day", n, true
	}
	if n%3600 == 0 {
		return "hour", n, true
	}
	return "", 0, false
}

// unwrapCast peels BOUND_CAST wrappers and returns the inner expression.
// Returns nil if expr is nil.
func unwrapCast(expr *planExpr) *planExpr {
	for expr != nil && expr.ExpressionClass == "BOUND_CAST" {
		expr = expr.Child
	}
	return expr
}

// extractConstantInt unwraps casts and returns an integer constant value.
// Handles JSON numbers (float64) and explicit int types.
func extractConstantInt(expr *planExpr) (int64, bool) {
	e := unwrapCast(expr)
	if e == nil || e.ExpressionClass != "BOUND_CONSTANT" || e.Value == nil || e.Value.IsNull {
		return 0, false
	}
	switch v := e.Value.Value.(type) {
	case float64:
		return int64(v), true
	case int:
		return int64(v), true
	case int64:
		return v, true
	case string:
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n, true
		}
	}
	return 0, false
}

// extractDateTruncGroup checks if a GROUP BY expression is date_trunc and extracts bucket + time col.
// Sets qs.Reason if the bucket granularity is sub-hour (minute, second, etc.).
func extractDateTruncGroup(grp planExpr, qs *QueryShape) {
	if grp.ExpressionClass != "BOUND_FUNCTION" || grp.Name != "date_trunc" {
		return
	}
	if len(grp.Children) < 2 {
		return
	}
	// children[0] = BOUND_CONSTANT with the bucket string
	bucketArg := ""
	if c := grp.Children[0]; c.ExpressionClass == "BOUND_CONSTANT" && c.Value != nil {
		if s, ok := c.Value.Value.(string); ok {
			bucketArg = strings.ToLower(s)
		}
	}
	if bucketArg == "" {
		return
	}

	switch bucketArg {
	case "minute", "second", "millisecond", "microsecond", "nanosecond":
		qs.Reason = fmt.Sprintf("unsupported bucket granularity: %s", bucketArg)
		return
	}

	// children[1] = BOUND_REF referencing the time column
	timeCol := ""
	if c := grp.Children[1]; c.ExpressionClass == "BOUND_REF" {
		timeCol = c.Alias
	}

	if qs.BucketArg == "" {
		qs.BucketArg = bucketArg
	}
	if qs.TimeColumn == "" && timeCol != "" {
		qs.TimeColumn = timeCol
	}
}

// extractDimGroup appends a plain BOUND_REF GROUP BY column to qs.GroupDims,
// skipping the time column (already captured by extractDateTruncGroup).
func extractDimGroup(grp planExpr, qs *QueryShape) {
	if grp.ExpressionClass != "BOUND_REF" {
		return
	}
	col := grp.Alias
	if col == "" || col == qs.TimeColumn {
		return
	}
	for _, d := range qs.GroupDims {
		if d == col {
			return
		}
	}
	qs.GroupDims = append(qs.GroupDims, col)
}

// extractAggregate converts a BOUND_AGGREGATE expression to an Aggregate.
// Returns (agg, "") on success or (nil, reason) when the aggregate is unsupported.
func extractAggregate(expr planExpr) (*Aggregate, string) {
	if expr.ExpressionClass != "BOUND_AGGREGATE" {
		return nil, ""
	}
	agg := &Aggregate{}
	switch expr.Name {
	case "count_star":
		agg.Kind = AggCountStar
	case "count":
		if expr.AggregateType == "DISTINCT" {
			agg.Kind = AggCountDistinct
		} else {
			agg.Kind = AggCount
		}
	case "sum":
		agg.Kind = AggSum
	case "avg":
		agg.Kind = AggAvg
	case "min":
		agg.Kind = AggMin
	case "max":
		agg.Kind = AggMax
	case "quantile_cont":
		agg.Kind = AggQuantile
		if expr.FunctionData != nil && len(expr.FunctionData.Quantiles) > 0 {
			q := expr.FunctionData.Quantiles[0]
			if !q.IsNull {
				switch v := q.Value.(type) {
				case float64:
					if q.Type.ID == "DECIMAL" && q.Type.TypeInfo != nil && q.Type.TypeInfo.Scale > 0 {
						scale := 1.0
						for i := 0; i < q.Type.TypeInfo.Scale; i++ {
							scale *= 10
						}
						agg.Quantile = v / scale
					} else if v > 1 {
						agg.Quantile = v / 100
					} else {
						agg.Quantile = v
					}
				}
			}
		}
	default:
		return nil, fmt.Sprintf("untranslatable aggregate: %s", expr.Name)
	}
	if len(expr.Children) > 0 {
		agg.Column = expr.Children[0].Alias
	}
	agg.OutputAlias = expr.Alias
	return agg, ""
}

// parseTimestamp parses a date/datetime string into time.Time. Layouts
// cover DuckDB's serialised TIMESTAMP-WITH-TIME-ZONE forms:
//   - "2006-01-02 15:04:05-07:00" (full hh:mm offset, e.g. +03:00)
//   - "2006-01-02 15:04:05-07"    (DuckDB short-hour form, e.g. +00)
//   - fractional-second variants of each
//   - "2006-01-02 15:04:05"       (no offset)
//   - "2006-01-02"                (date-only)
//   - RFC3339                     ("2006-01-02T15:04:05Z" and variants)
func parseTimestamp(s string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05-07",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp %q", s)
}

// applyProjectionArithmetic finds the LOGICAL_PROJECTION node in the plan tree
// and processes its expressions to set OuterExpr on aggregates. Handles:
//   - agg_ref * constant → OuterExpr = "_agg * N"
//   - agg_ref / constant → OuterExpr = "_agg / N"
//   - agg_ref1 / agg_ref2 → OuterExpr on agg0 set to "_agg / SUM(_agg_1)"
//     (div-by-count pattern like SUM/COUNT)
func applyProjectionArithmetic(n planNode, qs *QueryShape) {
	if n.Type == "LOGICAL_PROJECTION" {
		applyProjExprs(n.Expressions, qs)
		return
	}
	for _, child := range n.Children {
		applyProjectionArithmetic(child, qs)
	}
}

// applyProjExprs processes LOGICAL_PROJECTION expressions to detect arithmetic
// wrappers around aggregate BOUND_REFs.
func applyProjExprs(exprs []planExpr, qs *QueryShape) {
	if len(qs.Aggregates) == 0 {
		return
	}
	// The LOGICAL_AGGREGATE_AND_GROUP_BY produces: [bucket_columns..., agg_columns...]
	// The number of bucket columns depends on whether there's a date_trunc group.
	// We can identify aggregate refs by BOUND_REF index >= numGroups.
	// Actually, we use the BOUND_REF's alias to identify which aggregate it refers to.

	for _, expr := range exprs {
		if expr.ExpressionClass != "BOUND_FUNCTION" {
			continue
		}
		op := expr.Name
		if op != "*" && op != "/" && op != "+" && op != "-" {
			continue
		}
		if len(expr.Children) != 2 {
			continue
		}
		left := expr.Children[0]
		right := expr.Children[1]

		// Case 1: agg_ref OP constant
		if left.ExpressionClass == "BOUND_REF" && left.Index != nil {
			constVal := extractConstantNumericString(&right)
			if constVal != "" {
				aggIdx := projAggIndex(left, qs)
				if aggIdx >= 0 {
					qs.Aggregates[aggIdx].OuterExpr = fmt.Sprintf("_agg %s %s", op, constVal)
					if expr.Alias != "" {
						qs.Aggregates[aggIdx].OutputAlias = expr.Alias
					}
				}
				continue
			}
		}

		// Case 2: constant OP agg_ref
		if right.ExpressionClass == "BOUND_REF" && right.Index != nil {
			constVal := extractConstantNumericString(&left)
			if constVal != "" {
				aggIdx := projAggIndex(right, qs)
				if aggIdx >= 0 {
					qs.Aggregates[aggIdx].OuterExpr = fmt.Sprintf("%s %s _agg", constVal, op)
					if expr.Alias != "" {
						qs.Aggregates[aggIdx].OutputAlias = expr.Alias
					}
				}
				continue
			}
		}

		// Case 3: agg_ref OP agg_ref (e.g. SUM(x) / COUNT(*))
		// The emitter would need to combine two aggregate outputs into one expression,
		// which requires structural changes. Refuse this pattern for now.
		if left.ExpressionClass == "BOUND_REF" && right.ExpressionClass == "BOUND_REF" &&
			left.Index != nil && right.Index != nil {
			numIdx := projAggIndex(left, qs)
			denIdx := projAggIndex(right, qs)
			if numIdx >= 0 && denIdx >= 0 && numIdx != denIdx {
				qs.Supported = false
				qs.Reason = fmt.Sprintf("unsupported projection: aggregate %s aggregate", op)
				return
			}
		}
	}
}

// projAggIndex returns the index of the aggregate in qs.Aggregates that a
// BOUND_REF expression (from LOGICAL_PROJECTION) refers to. The BOUND_REF's
// alias matches the aggregate's source expression name. Returns -1 if not found.
func projAggIndex(ref planExpr, qs *QueryShape) int {
	if ref.Index == nil {
		return -1
	}
	// DuckDB sets the alias of projection BOUND_REFs to the aggregate function name.
	// Match by alias against known aggregate output names.
	refAlias := strings.ToLower(ref.Alias)
	for i, agg := range qs.Aggregates {
		switch agg.Kind {
		case AggAvg:
			if strings.Contains(refAlias, "avg") {
				return i
			}
		case AggSum:
			if strings.Contains(refAlias, "sum") {
				return i
			}
		case AggCount:
			if strings.Contains(refAlias, "count") && !strings.Contains(refAlias, "distinct") && !strings.Contains(refAlias, "star") {
				return i
			}
		case AggCountStar:
			if strings.Contains(refAlias, "count_star") || refAlias == "count(*)" {
				return i
			}
		case AggCountDistinct:
			if strings.Contains(refAlias, "distinct") {
				return i
			}
		case AggMin:
			if strings.Contains(refAlias, "min") {
				return i
			}
		case AggMax:
			if strings.Contains(refAlias, "max") {
				return i
			}
		case AggQuantile:
			if strings.Contains(refAlias, "quantile") {
				return i
			}
		}
	}
	return -1
}

// extractConstantNumericString digs into a BOUND_CAST/BOUND_CONSTANT to get
// the numeric value as a string (for use in OuterExpr).
func extractConstantNumericString(expr *planExpr) string {
	if expr == nil {
		return ""
	}
	switch expr.ExpressionClass {
	case "BOUND_CAST":
		return extractConstantNumericString(expr.Child)
	case "BOUND_CONSTANT":
		if expr.Value != nil && !expr.Value.IsNull {
			switch v := expr.Value.Value.(type) {
			case float64:
				if v == float64(int(v)) {
					return fmt.Sprintf("%d", int(v))
				}
				return fmt.Sprintf("%g", v)
			case string:
				return v
			}
		}
	}
	return ""
}

// extractFromTable returns the first FROM-clause identifier in sql, or "".
// Used as a fallback for ExtractQueryShape when the DuckDB plan inlines a
// view to read_parquet (no table name in function_data).
func extractFromTable(sql string) string {
	lower := strings.ToLower(sql)
	idx := strings.Index(lower, "from ")
	if idx < 0 {
		return ""
	}
	i := idx + 5
	for i < len(sql) && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n') {
		i++
	}
	// Reject openings that aren't a plain identifier (function call, subquery).
	if i >= len(sql) || sql[i] == '(' {
		return ""
	}
	start := i
	for i < len(sql) {
		c := sql[i]
		// Allow letters, digits, underscore, and ONE dot for db.table.
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.' {
			i++
			continue
		}
		break
	}
	// Reject when followed by '(' — that's a function call (e.g., read_parquet),
	// not a table reference.
	if i < len(sql) && sql[i] == '(' {
		return ""
	}
	return sql[start:i]
}
