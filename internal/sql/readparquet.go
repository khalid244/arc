package sql

import "strings"

// HivePartitioningOff disables DuckDB's Hive partition inference.
//
// DuckDB derives a column from any `key=value` DIRECTORY component of the path
// it is reading. When that name collides with a real column in the file, the
// path's value REPLACES the stored value and its type, in place; when the file
// has no such column, one is appended. Neither case raises an error, and
// union_by_name=true does not mitigate it.
//
// Arc never wants this. The storage layout is parsed explicitly
// ({database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/file.parquet) and no code
// consumes an inferred column, so disabling inference removes a corruption
// mode and costs nothing.
//
// There is no global or session setting — `SET hive_partitioning` and
// `SET parquet_hive_partitioning` are both unrecognized configuration
// parameters — so this has to travel with every single read. That is why reads
// are built through ReadParquet rather than assembled at each call site, and
// why TestNoUnscopedReadParquetLiterals pins where such literals may live.
const HivePartitioningOff = "hive_partitioning=false"

// ReadParquet renders a DuckDB read_parquet(...) call with Hive partition
// inference disabled.
//
// pathExpr is the already-quoted path expression: a single quoted literal from
// QuoteStringLiteral, or a bracketed list of them. It is interpolated verbatim,
// so every caller must have quoted it already.
//
// extraOpts are appended after the inference flag, each as a bare `k=v`
// fragment (for example "union_by_name=true", "filename=true").
//
// NOT used by internal/arcxrouter, and it must not be: the arcx engine's SQL
// recognizer accepts only paths between the parentheses and rejects ANY option
// argument, so routing arcx's generated SQL through here would make every arcx
// query decline — silently in serve mode. arcx reads Parquet itself and
// performs no Hive inference, so it needs no flag; the DuckDB shadow oracle
// gets it through the normal query path. See #1005.
func ReadParquet(pathExpr string, extraOpts ...string) string {
	var b strings.Builder
	// "read_parquet(" + ", " + ")" is 16 bytes; leave room for the options too.
	n := len(pathExpr) + len(HivePartitioningOff) + 16
	for _, opt := range extraOpts {
		n += len(opt) + 2
	}
	b.Grow(n)
	b.WriteString("read_parquet(")
	b.WriteString(pathExpr)
	b.WriteString(", ")
	b.WriteString(HivePartitioningOff)
	for _, opt := range extraOpts {
		if opt == "" {
			continue
		}
		b.WriteString(", ")
		b.WriteString(opt)
	}
	b.WriteByte(')')
	return b.String()
}

// ReadParquetList renders a read_parquet call over several already-quoted
// paths, as the bracketed list form DuckDB requires.
func ReadParquetList(quotedPaths []string, extraOpts ...string) string {
	return ReadParquet("["+strings.Join(quotedPaths, ", ")+"]", extraOpts...)
}

// readParquetOpen is the call prefix ReadParquet renders. WrapReadParquetCalls
// matches it, so every read the query layer builds through this file is found.
const readParquetOpen = "read_parquet("

// WrapReadParquetCalls rewrites every read_parquet(...) call in sqlText with
// wrap(call, n): call is the complete call text, from the function name to its
// matching ')' (parentheses inside single-quoted literals ignored), and n counts
// the calls from 0. Text outside the calls is kept byte for byte, and an
// unbalanced call leaves the remainder untouched.
//
// It is the matcher counterpart of ReadParquet and lives beside it for the same
// reason (#1018 guard): the call is spelled in this file only. The query layer
// uses it to give schema-drifted source reads NULL columns.
func WrapReadParquetCalls(sqlText string, wrap func(call string, n int) string) string {
	var b strings.Builder
	rest := sqlText
	n := 0
	for {
		i := strings.Index(rest, readParquetOpen)
		if i < 0 {
			b.WriteString(rest)
			break
		}
		open := i + len(readParquetOpen) - 1 // index of '('
		end := MatchParen(rest, open)
		if end < 0 { // unbalanced (shouldn't happen) — leave the remainder as-is
			b.WriteString(rest)
			break
		}
		b.WriteString(rest[:i])
		b.WriteString(wrap(rest[i:end+1], n))
		n++
		rest = rest[end+1:]
	}
	return b.String()
}

// CountReadParquetCalls returns the number of read_parquet(...) calls in
// sqlText, matched exactly as WrapReadParquetCalls matches them. The query
// layer uses it to tell a statement that reads one relation from one that
// reads several (P3Q-2).
func CountReadParquetCalls(sqlText string) int {
	n := 0
	WrapReadParquetCalls(sqlText, func(call string, _ int) string {
		n++
		return call
	})
	return n
}

// MatchParen returns the index of the ')' matching the '(' at open, ignoring
// parentheses inside single-quoted string literals (so a path/glob with a "'" or
// "(" never throws off the depth count). Returns -1 if unbalanced.
func MatchParen(s string, open int) int {
	depth := 0
	inStr := false
	for i := open; i < len(s); i++ {
		switch c := s[i]; {
		case inStr:
			if c == '\'' {
				inStr = false
			}
		case c == '\'':
			inStr = true
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}
