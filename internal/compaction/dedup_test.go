package compaction

import (
	"strings"
	"testing"
)

// buildCompactionSQL joins the statement(s) buildCompactionQuery returns into a
// single string for the string-assertion unit tests below. The tagged form
// returns three statements (stage + keys + copy); the tagless form one.
func buildCompactionSQL(fileListSQL, orderByClause, outputFile string, tagColumns []string) string {
	return strings.Join(buildCompactionQuery(fileListSQL, orderByClause, outputFile, tagColumns), "\n;\n")
}

// A batch without tag metadata — every production batch — is still
// deduplicated: a whole-row DISTINCT, which is exact-row dedup by definition.
// There is no "no dedup" form any more (upstream #1234 left tagless raw ingest
// undeduplicated, which would keep every crash-replay duplicate forever).
func TestBuildCompactionQuery_TaglessIsWholeRowDistinct(t *testing.T) {
	stmts := buildCompactionQuery("['a.parquet', 'b.parquet']", `ORDER BY "time"`, "/tmp/out.parquet", nil)
	if len(stmts) != 1 {
		t.Fatalf("got %d statements, want a single COPY", len(stmts))
	}
	query := stmts[0]
	if !strings.Contains(query, "SELECT DISTINCT * REPLACE") {
		t.Errorf("tagless batch is not deduplicated to whole rows:\n%s", query)
	}
	if strings.Contains(query, "ROW_NUMBER") || strings.Contains(query, "PARTITION BY") {
		t.Errorf("tagless batch keeps one row per key instead of exact rows:\n%s", query)
	}
	if !strings.Contains(query, "read_parquet") {
		t.Error("expected read_parquet in query")
	}
	if !strings.Contains(query, `ORDER BY "time"`) {
		t.Error("expected ORDER BY clause")
	}
}

func TestBuildCompactionQueryDisablesHivePartitioning(t *testing.T) {
	for name, tags := range map[string][]string{"tagged": {"host"}, "tagless": nil} {
		query := buildCompactionSQL("['a.parquet', 'b.parquet']", `ORDER BY "time"`, "/tmp/out.parquet", tags)
		if strings.Count(query, "read_parquet") != strings.Count(query, "hive_partitioning=false") {
			t.Fatalf("%s: every read_parquet call must disable Hive partition inference, got:\n%s", name, query)
		}
	}
}

func TestBuildCompactionQuery_EmptyDedup(t *testing.T) {
	query := buildCompactionSQL("['a.parquet']", "", "/tmp/out.parquet", []string{})

	if strings.Contains(query, "ROW_NUMBER") {
		t.Error("expected no ROW_NUMBER with empty dedup keys")
	}
	if !strings.Contains(query, "SELECT DISTINCT *") {
		t.Errorf("empty tag list must still deduplicate whole rows:\n%s", query)
	}
}

func TestBuildCompactionQuery_SpecialCharsInPath(t *testing.T) {
	query := buildCompactionSQL("['a.parquet']", "", "/tmp/it's out.parquet", []string{"host"})

	if !strings.Contains(query, "it''s") {
		t.Error("expected escaped single quote in output path")
	}
}

func TestBuildCompactionQuery_IdentifierEscaping(t *testing.T) {
	// Defense-in-depth: even if validation is bypassed, identifiers are properly escaped
	query := buildCompactionSQL("['a.parquet']", "", "/tmp/out.parquet", []string{`host"injection`})

	// Double-quote inside identifier should be doubled
	if !strings.Contains(query, `"host""injection"`) {
		t.Errorf("expected escaped double-quote in identifier, got: %s", query)
	}
}

func TestIsValidIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"simple", "host", true},
		{"with_underscore", "host_name", true},
		{"with_hyphen", "host-name", true},
		{"with_numbers", "sensor123", true},
		{"mixed_case", "HostName", true},
		{"empty", "", false},
		{"with_double_quote", `host"`, false},
		{"with_space", "host name", false},
		{"with_semicolon", "host;DROP", false},
		{"with_paren", "host()", false},
		{"with_comma", "a,b", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("isValidIdentifier(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestReadTagColumnsValidation(t *testing.T) {
	// isValidIdentifier is called internally by readTagColumnsFromParquet
	// but we can't easily test that without a DuckDB instance.
	// Instead, test the validation logic directly.

	// Valid tags should pass
	for _, tag := range []string{"host", "region", "sensor_id", "host-name"} {
		if !isValidIdentifier(tag) {
			t.Errorf("expected %q to be valid", tag)
		}
	}

	// Injection attempts should fail
	for _, tag := range []string{`host" OR 1=1--`, `"; DROP TABLE`, `host,region`} {
		if isValidIdentifier(tag) {
			t.Errorf("expected %q to be invalid", tag)
		}
	}
}

// TestBuildCompactionQuery_BothFormsNormalizeTime asserts both forms (with and
// without tag columns) normalize time to TIMESTAMPTZ so a VARCHAR-time file
// never wedges compaction. String-only (no DuckDB), so it lives here in the
// untagged test file rather than the duckdb_arrow-gated integration test.
func TestBuildCompactionQuery_BothFormsNormalizeTime(t *testing.T) {
	withTags := buildCompactionSQL("['a.parquet']", "", "/tmp/o.parquet", []string{"host"})
	noTags := buildCompactionSQL("['a.parquet']", "", "/tmp/o.parquet", nil)
	for name, q := range map[string]string{"tagged": withTags, "tagless": noTags} {
		if !strings.Contains(q, "make_timestamptz") || !strings.Contains(q, "TIMESTAMPTZ") {
			t.Errorf("%s form does not normalize time to TIMESTAMPTZ:\n%s", name, q)
		}
	}
}

// Exact-row dedup (#1232): rows whose (tags, time) key is unique are copied as
// they are; for keys that repeat, one copy of each distinct row is kept. The
// key never decides on its own that rows are duplicates.
func TestBuildCompactionQuery_ExactRows(t *testing.T) {
	stmts := buildCompactionQuery("['a.parquet']", `ORDER BY "time"`, "/tmp/o.parquet", []string{"host", "region"})
	if len(stmts) != 3 {
		t.Fatalf("got %d statements, want stage + keys + copy", len(stmts))
	}
	q := strings.Join(stmts, "\n;\n")
	for _, want := range []string{
		`GROUP BY ALL HAVING count(*) > 1`,
		`SELECT "host", "region", "time" FROM ` + dedupStagingTable,
		`k."host" IS NOT DISTINCT FROM s."host" AND k."region" IS NOT DISTINCT FROM s."region" AND k."time" IS NOT DISTINCT FROM s."time"`,
		`WHERE NOT EXISTS`,
		`SELECT DISTINCT * FROM ` + dedupStagingTable + ` s WHERE EXISTS`,
		`make_timestamptz`,
	} {
		if !strings.Contains(q, want) {
			t.Errorf("exact-row query lacks %q:\n%s", want, q)
		}
	}
	if strings.Contains(q, "ROW_NUMBER") || strings.Contains(q, "PARTITION BY") {
		t.Errorf("exact-row query still keeps one row per key:\n%s", q)
	}
	if strings.Contains(q, "KV_METADATA") {
		t.Errorf("tagged exact-row output is marked:\n%s", q)
	}
}

// The tagless form is marked with arc:dedup_exact (so a build that gates dedup
// on footer metadata keeps deduplicating the output) and never with the legacy
// arc:dedup_time key, which builds before #1232 read as "one row per time".
// DISTINCT keeps no order, so the output is ordered by time when no sort keys
// are configured, and by the sort keys when they are — in both forms.
func TestBuildCompactionQuery_OrderAndMarker(t *testing.T) {
	q := buildCompactionSQL("['a.parquet']", "", "/tmp/o.parquet", nil)
	if !strings.Contains(q, `KV_METADATA {'arc:dedup_exact': 'true'}`) {
		t.Errorf("tagless exact-row output is not marked:\n%s", q)
	}
	for name, tags := range map[string][]string{"tagged": {"host"}, "tagless": nil} {
		q := buildCompactionSQL("['a.parquet']", "", "/tmp/o.parquet", tags)
		if strings.Contains(q, "arc:dedup_time") {
			t.Errorf("%s output carries the legacy arc:dedup_time key:\n%s", name, q)
		}
		if !strings.Contains(q, `ORDER BY "time"`) {
			t.Errorf("%s: no ORDER BY fallback without sort keys:\n%s", name, q)
		}
		q = buildCompactionSQL("['a.parquet']", `ORDER BY "host", "time"`, "/tmp/o.parquet", tags)
		if !strings.Contains(q, `ORDER BY "host", "time"`) {
			t.Errorf("%s: configured sort keys ignored:\n%s", name, q)
		}
	}
}
