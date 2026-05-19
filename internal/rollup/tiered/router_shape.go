package tiered

import "time"

// AggKind identifies one of the translatable aggregate functions.
// Anything outside this set causes the parser to refuse the query.
type AggKind int

const (
	AggUnknown AggKind = iota
	AggCount
	AggCountStar
	AggSum
	AggAvg
	AggMin
	AggMax
	AggCountDistinct
	AggQuantile
)

var aggKindNames = [...]string{
	"unknown", "count", "count_star", "sum", "avg", "min", "max", "count_distinct", "quantile_cont",
}

func (k AggKind) String() string {
	if int(k) < 0 || int(k) >= len(aggKindNames) {
		return "invalid"
	}
	return aggKindNames[k]
}

// Aggregate is one aggregate expression appearing in SELECT or HAVING.
type Aggregate struct {
	Kind        AggKind
	Column      string  // empty for COUNT(*)
	Quantile    float64 // only for AggQuantile
	OutputAlias string
	OuterExpr   string // wrapper like "_agg * 100"; "" if pure aggregate
}

// HavingClause is one HAVING predicate against an aggregate.
type HavingClause struct {
	AggIndex int // index into QueryShape.Aggregates
	Op       string
	Value    float64
}

// OrderLimit captures `ORDER BY <agg> [DESC] LIMIT N`.
type OrderLimit struct {
	AggIndex int
	Desc     bool
	Limit    int
}

// FilterPredicate is a top-level WHERE predicate on a dim column.
// Op is one of "=", "IN", "NOT IN", "IS NOT NULL".
type FilterPredicate struct {
	Op     string
	Values []string // for "=" length 1; for IN / NOT IN the list
}

// QueryShape is the parser's extracted understanding of a user query.
// All fields zero/false unless the parser succeeded.
type QueryShape struct {
	Supported   bool
	Reason      string // when Supported=false, why
	Table       string // resolved "db.table"
	TimeColumn  string
	TimeLo      time.Time // WHERE bound, inclusive
	TimeHi      time.Time // WHERE bound, exclusive
	BucketArg   string    // "hour" | "day" | "week" | "month" — from date_trunc in SELECT/GROUP BY
	// UserBucketSecs is the user's desired bucket size in seconds when the SQL
	// uses the to_timestamp(epoch_ns//1e9//N*N) idiom (Grafana plugin form).
	// 0 means "use BucketArg literal" (date_trunc form). When non-zero, the
	// emit applies an outer-SELECT to_timestamp wrap so the result honors the
	// user-requested bucket size (e.g. 3h, 6h, 12h).
	UserBucketSecs int64
	GroupDims   []string  // ordered, non-time dims in GROUP BY
	Filters     map[string]FilterPredicate
	Aggregates  []Aggregate
	Having      []HavingClause
	OrderLimit  *OrderLimit
	OriginalSQL string
}
