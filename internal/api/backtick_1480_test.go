package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// DuckDB does not quote identifiers with backticks. The validators refuse an
// unquoted one, and mask the SQL exactly as written (#1480).

func TestValidateSQLRefusesUnquotedBacktick(t *testing.T) {
	for _, sql := range []string{
		// The issue: a backtick span around a FROM. DuckDB parses the FROM.
		"SELECT 1 ` FROM '/data/db2/**/*.parquet' WHERE ` 1 = 1",
		"SELECT `value` FROM cpu",
		"SELECT * FROM `cpu`",
		"SELECT 2 ` 3",
		"SELECT 1 /* a */ ` 2",
	} {
		err := ValidateSQLRequest(sql)
		if err == nil || !strings.Contains(err.Error(), "backticks are not identifier quotes") {
			t.Errorf("ValidateSQLRequest(%q) = %v, want the backtick refusal", sql, err)
		}
	}
}

func TestValidateSQLAllowsBacktickInQuotedText(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM cpu WHERE msg = 'a`b'",
		`SELECT "a` + "`" + `b" FROM cpu`,
		"SELECT 1 -- a `comment`",
		"SELECT 1 /* a ` comment */",
		"SELECT $$a`b$$",
		"SELECT $t$a`b$t$",
		"SELECT E'a\\'`b'",
		// Answered by Arc itself, never by DuckDB.
		"SHOW TABLES FROM `mydb`",
		"SHOW TABLES FROM `my--db`",
		"SHOW MEASUREMENTS FROM `mydb`;",
	} {
		if err := ValidateSQLRequest(sql); err != nil {
			t.Errorf("ValidateSQLRequest(%q) = %v, want nil", sql, err)
		}
	}
}

// A backtick inside a quoted identifier is part of the name.
func TestValidateSQLReadsBacktickInIdentifierAsWritten(t *testing.T) {
	for _, sql := range []string{
		`SELECT 1 AS "x` + "`" + `/*"; SELECT 2 --*/`,
		`SELECT 1 AS "x` + "`" + `--"; SELECT 2`,
		`SHOW TABLES FROM "a` + "`" + `/*"; SELECT 2 --*/`,
		`SELECT "x` + "`" + `" , 1 FROM '/data/db2/x.parquet' "` + "`" + `"`,
	} {
		if err := ValidateSQLRequest(sql); err == nil {
			t.Errorf("ValidateSQLRequest(%q) accepted", sql)
		}
	}
}

// This fork keeps its own DELETE predicate validator (the delete.go hunks of
// #1480 were not taken), so only the measurement query path is checked here.
func TestWhereValidatorsRefuseUnquotedBacktick(t *testing.T) {
	for _, where := range []string{
		"`host` = 'a'",
		"host = 'a' ` 1",
		`"x` + "`" + `/*" = 1; SELECT 2 --*/`,
		"`a'` = 1 OR glob('/etc/passwd') IS NOT NULL OR `'` = 1",
		`"x` + "`" + `" = 1 OR v IN (FROM '/data/db2/x.parquet') OR "` + "`" + `" = 1`,
	} {
		// The /query/:measurement path checks the fragment, then the assembled
		// statement; either may refuse.
		if validateWhereClauseQuery(where) == nil && ValidateSQLRequest("SELECT * FROM cpu WHERE "+where) == nil {
			t.Errorf("the measurement query path accepted %q", where)
		}
	}
	// The fragment check on its own reads the clause as written.
	if err := validateWhereClauseQuery(`"x` + "`" + `" = 1 UNION SELECT 2 OR "` + "`" + `" = 1`); err == nil {
		t.Error("validateWhereClauseQuery accepted a keyword between two identifiers holding backticks")
	}
	for _, where := range []string{
		"msg = 'a`b'",
		`"a` + "`" + `b" = 1`,
	} {
		if err := validateWhereClauseQuery(where); err != nil {
			t.Errorf("validateWhereClauseQuery(%q) = %v", where, err)
		}
	}
}

// The issue's query is refused with the reason, not answered with an empty
// success.
func TestQueryEndpointRefusesBacktickSpan(t *testing.T) {
	logger := zerolog.Nop()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: dir}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)

	for _, hdr := range []string{"", "default"} {
		sql := "SELECT 1 ` FROM '" + dir + "/other/**/*.parquet' WHERE ` 1 = 1"
		body, _ := json.Marshal(map[string]string{"sql": sql})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if hdr != "" {
			req.Header.Set("x-arc-database", hdr)
		}
		resp, err := app.Test(req, 10000)
		if err != nil {
			t.Fatal(err)
		}
		out, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(out), "backticks are not identifier quotes") {
			t.Errorf("header %q: status %d, body %s", hdr, resp.StatusCode, out)
		}
	}
}
