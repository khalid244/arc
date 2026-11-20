package api

import (
	gosql "database/sql"
	"testing"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// Every character DuckDB's lexer reads as a space between tokens must be one
// sqlutil.IsUnicodeSpace accepts: the keyword checks fold them
// (foldUnicodeSpace) and the partition pruner's range check refuses SQL
// holding one outside literals. A character missing from the set is a FROM
// both DuckDB and the storage-path rewrite read, and the range check never
// sees (`id<U+00A0>FROM mem`). Scans the Basic Multilingual Plane (DuckDB
// 1.5.6 reads 18 such characters, all in it; none above it).
func TestDuckDBUnicodeSpacesAreKnown(t *testing.T) {
	if testing.Short() {
		t.Skip("scans 63k statements")
	}
	db, err := gosql.Open("duckdb", "")
	if err != nil {
		t.Skipf("duckdb unavailable: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	spaces := 0
	for r := rune(0x80); r <= 0xFFFF; r++ {
		if r >= 0xD800 && r <= 0xDFFF {
			continue
		}
		// A space: FROM range(3) t WHERE false. Otherwise t<r>WHERE is an
		// alias and false a syntax error.
		var n int
		if err := db.QueryRow("SELECT count(*) FROM range(3) t" + string(r) + "WHERE false").Scan(&n); err != nil {
			continue
		}
		spaces++
		if !sqlutil.IsUnicodeSpace(r) {
			t.Errorf("DuckDB reads %U as a space; sqlutil.IsUnicodeSpace does not", r)
		}
	}
	if spaces == 0 {
		t.Fatal("DuckDB read no character as a space: the probe is broken")
	}
	for _, r := range []rune{0x00A0, 0x200B, 0x2003} {
		var n int
		if err := db.QueryRow("SELECT count(*) FROM range(3) t" + string(r) + "WHERE false").Scan(&n); err != nil {
			t.Errorf("DuckDB no longer reads %U as a space (%v): update the comments that say it does", r, err)
		}
	}
}
