package api

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// dayGlobs renders the day-level globs of table for the days of 2026-<month>.
func dayGlobs(table string, month int, days ...int) string {
	var out []string
	for _, d := range days {
		out = append(out, fmt.Sprintf("'s3://b/%s/2026/%02d/%02d/*.parquet'", table, month, d))
	}
	return strings.Join(out, ", ")
}

func daysRange(from, to int) []int {
	var out []int
	for d := from; d <= to; d++ {
		out = append(out, d)
	}
	return out
}

func TestMergeDayGlobs(t *testing.T) {
	// Today is 2026-11-17: the 16th and the 17th still hold raw files.
	protectedFrom := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	read := func(paths string) string {
		return "SELECT * FROM read_parquet([" + paths + "], hive_partitioning=false, union_by_name=true) WHERE x = 1"
	}
	for _, c := range []struct{ name, sql, want string }{
		{
			"days sharing their first digit become one glob",
			read(dayGlobs("db/m", 5, daysRange(1, 12)...)),
			read("'s3://b/db/m/2026/05/0[123456789]/*.parquet', 's3://b/db/m/2026/05/1[012]/*.parquet'"),
		},
		{
			"a day alone in its group is kept",
			read(dayGlobs("db/m", 5, 9, 10, 11, 30, 31)),
			read("'s3://b/db/m/2026/05/09/*.parquet', 's3://b/db/m/2026/05/1[01]/*.parquet', 's3://b/db/m/2026/05/3[01]/*.parquet'"),
		},
		{
			"every day of a month becomes one glob",
			read(dayGlobs("db/m", 4, 29, 30) + ", " + dayGlobs("db/m", 5, daysRange(1, 31)...) + ", " + dayGlobs("db/m", 6, 1)),
			read("'s3://b/db/m/2026/04/29/*.parquet', 's3://b/db/m/2026/04/30/*.parquet', 's3://b/db/m/2026/05/[0-3][0-9]/*.parquet', 's3://b/db/m/2026/06/01/*.parquet'"),
		},
		{
			"February",
			read(dayGlobs("db/m", 2, daysRange(1, 28)...)),
			read("'s3://b/db/m/2026/02/[0-3][0-9]/*.parquet'"),
		},
		{
			// The LIST behind 1[0-5] walks every key of days 10 to 19, the raw
			// files of the 16th and 17th included: those days keep their globs.
			"a group whose LIST would reach a day with raw files is kept",
			read(dayGlobs("db/m", 10, daysRange(1, 31)...) + ", " + dayGlobs("db/m", 11, daysRange(1, 15)...)),
			read("'s3://b/db/m/2026/10/[0-3][0-9]/*.parquet', 's3://b/db/m/2026/11/0[123456789]/*.parquet', " + dayGlobs("db/m", 11, daysRange(10, 15)...)),
		},
		{
			// The pruner emits the day globs in no fixed order.
			"days in any order, with other paths among them",
			read("'/anchor/a.parquet', " + dayGlobs("db/m", 5, 12, 3) + ", 's3://b/db/m/2026/05/02/07/*.parquet', " + dayGlobs("db/m", 5, 1, 11, 2, 20)),
			read("'/anchor/a.parquet', 's3://b/db/m/2026/05/1[12]/*.parquet', 's3://b/db/m/2026/05/0[123]/*.parquet', 's3://b/db/m/2026/05/02/07/*.parquet', " + dayGlobs("db/m", 5, 20)),
		},
		{
			"each table and month on its own",
			read(dayGlobs("db/m", 5, 1, 2) + ", " + dayGlobs("db/n", 5, 3, 4) + ", " + dayGlobs("db/n", 6, 5, 6)),
			read("'s3://b/db/m/2026/05/0[12]/*.parquet', 's3://b/db/n/2026/05/0[34]/*.parquet', 's3://b/db/n/2026/06/0[56]/*.parquet'"),
		},
		{
			"the compaction exclusion wrapper",
			"SELECT * FROM (SELECT * EXCLUDE (_arc_src_file) FROM read_parquet([" + dayGlobs("db/m", 5, 1, 2) + "], hive_partitioning=false, filename='_arc_src_file') WHERE _arc_src_file NOT IN ('s3://b/db/m/2026/05/01/a.parquet'))",
			"SELECT * FROM (SELECT * EXCLUDE (_arc_src_file) FROM read_parquet(['s3://b/db/m/2026/05/0[12]/*.parquet'], hive_partitioning=false, filename='_arc_src_file') WHERE _arc_src_file NOT IN ('s3://b/db/m/2026/05/01/a.parquet'))",
		},
		{
			"every read of the statement",
			"SELECT * FROM read_parquet([" + dayGlobs("db/m", 5, 1, 2) + "], hive_partitioning=false) a JOIN read_parquet([" + dayGlobs("db/n", 5, 21, 22) + "], hive_partitioning=false) b USING (k)",
			"SELECT * FROM read_parquet(['s3://b/db/m/2026/05/0[12]/*.parquet'], hive_partitioning=false) a JOIN read_parquet(['s3://b/db/n/2026/05/2[12]/*.parquet'], hive_partitioning=false) b USING (k)",
		},
		{
			"a single path",
			"SELECT * FROM read_parquet(" + dayGlobs("db/m", 5, 1) + ", hive_partitioning=false)",
			"SELECT * FROM read_parquet(" + dayGlobs("db/m", 5, 1) + ", hive_partitioning=false)",
		},
		{
			"local folders, hour folders and other literals",
			"SELECT * FROM read_parquet(['/data/db/m/2026/05/01/*.parquet', '/data/db/m/2026/05/02/*.parquet', 's3://b/db/m/2026/05/03/01/*.parquet', 's3://b/db/m/2026/05/03/02/*.parquet'], hive_partitioning=false) WHERE note IN (" + dayGlobs("db/m", 5, 1, 2) + ")",
			"SELECT * FROM read_parquet(['/data/db/m/2026/05/01/*.parquet', '/data/db/m/2026/05/02/*.parquet', 's3://b/db/m/2026/05/03/01/*.parquet', 's3://b/db/m/2026/05/03/02/*.parquet'], hive_partitioning=false) WHERE note IN (" + dayGlobs("db/m", 5, 1, 2) + ")",
		},
	} {
		if got := mergeDayGlobs(c.sql, protectedFrom); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.name, got, c.want)
		}
	}
}

// A list that names a day twice reads its files twice; merging would read them
// once, so that month is left alone.
func TestMergeDayGlobsLeavesAMonthThatNamesADayTwice(t *testing.T) {
	sql := "SELECT * FROM read_parquet([" + dayGlobs("db/m", 5, 1, 1, 2) + ", " + dayGlobs("db/m", 6, 1, 2) + "], hive_partitioning=false)"
	want := "SELECT * FROM read_parquet([" + dayGlobs("db/m", 5, 1, 1, 2) + ", 's3://b/db/m/2026/06/0[12]/*.parquet'], hive_partitioning=false)"
	if got := mergeDayGlobs(sql, time.Time{}); got != want {
		t.Errorf(" got %s\nwant %s", got, want)
	}
}
