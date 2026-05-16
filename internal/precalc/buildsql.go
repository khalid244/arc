package precalc

import (
	"fmt"
	"strings"
)

// Tier identifies a precalc bucket size.
type Tier string

const (
	Tier1h  Tier = "1h"
	Tier1d  Tier = "1d"
	Tier1w  Tier = "1w"
	Tier1mo Tier = "1mo"
)

// DateTruncArg returns the `date_trunc` argument for the tier ("hour", "day",
// "week", "month").
func (t Tier) DateTruncArg() string {
	switch t {
	case Tier1h:
		return "hour"
	case Tier1d:
		return "day"
	case Tier1w:
		return "week"
	case Tier1mo:
		return "month"
	}
	return ""
}

// MetricCol describes a numeric column the builder should aggregate.
type MetricCol struct {
	Name    string
	Numeric bool
}

// BuildArgs is the input to SQL generators.
type BuildArgs struct {
	Tier       Tier
	Source     string // "read_parquet('...')" — fully qualified
	TimeColumn string // default "time" if empty
	MetricCols []MetricCol
	HLLCols    []string
	KLLCols    []string
	HLLLgK     int
	KLLk       int
}

func (a BuildArgs) timeCol() string {
	if a.TimeColumn == "" {
		return "time"
	}
	return a.TimeColumn
}

// BuildSketchVariantSQL emits the SQL for the no-dim sketch variant of one
// tier, built from a raw source. Returns a SELECT statement (no COPY wrapper);
// the caller wraps with COPY when ready to write.
func BuildSketchVariantSQL(a BuildArgs) string {
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT\n  date_trunc('%s', %s) AS bucket,\n  COUNT(*) AS cnt", a.Tier.DateTruncArg(), a.timeCol())
	for _, m := range a.MetricCols {
		if !m.Numeric {
			continue
		}
		fmt.Fprintf(&b, ",\n  COUNT(%s) AS cnt_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(%s) AS sum_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  SUM(%s * %s) AS sum_sq_%s", m.Name, m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MIN(%s) AS min_%s", m.Name, m.Name)
		fmt.Fprintf(&b, ",\n  MAX(%s) AS max_%s", m.Name, m.Name)
	}
	for _, c := range a.HLLCols {
		fmt.Fprintf(&b, ",\n  datasketch_hll(%d, %s) AS hll_%s", a.HLLLgK, c, c)
	}
	for _, c := range a.KLLCols {
		fmt.Fprintf(&b, ",\n  datasketch_kll(%d, %s) AS kll_%s", a.KLLk, c, c)
	}
	fmt.Fprintf(&b, "\nFROM %s\nGROUP BY 1", a.Source)
	return b.String()
}
