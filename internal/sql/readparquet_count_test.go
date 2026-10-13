package sql

import "testing"

func TestCountReadParquetCalls(t *testing.T) {
	cases := map[string]int{
		"SELECT 1": 0,
		"SELECT * FROM " + ReadParquet("'a/**/*.parquet'", "union_by_name=true"):                                           1,
		"SELECT * FROM " + ReadParquetList([]string{"'a/1/*.parquet'", "'a/2/*.parquet'"}):                                 1,
		"SELECT * FROM " + ReadParquet("'a/**/*.parquet'") + " UNION ALL SELECT * FROM " + ReadParquet("'b/**/*.parquet'"): 2,
		// A ')' or '(' inside a path literal does not end or open a call.
		"SELECT * FROM " + ReadParquet("'a/x(1)/*.parquet'") + " l LEFT JOIN " + ReadParquet("'b/y)/*.parquet'") + " r ON true": 2,
	}
	for sqlText, want := range cases {
		if got := CountReadParquetCalls(sqlText); got != want {
			t.Errorf("CountReadParquetCalls(%q) = %d, want %d", sqlText, got, want)
		}
	}
}
