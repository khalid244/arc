package rollup

import (
	"strings"
	"testing"
)

func TestStripURLScheme(t *testing.T) {
	cases := map[string]string{
		"http://minio:9000":  "minio:9000",
		"https://minio:9000": "minio:9000",
		"HTTP://Minio:9000":  "Minio:9000", // scheme match is case-insensitive
		"minio:9000":         "minio:9000",
		"":                   "",
		"  http://h:1  ":     "h:1",
	}
	for in, want := range cases {
		if got := stripURLScheme(in); got != want {
			t.Errorf("stripURLScheme(%q) = %q, want %q", in, got, want)
		}
	}
}

func endpointStmt(stmts []string) string {
	for _, s := range stmts {
		if strings.Contains(s, "s3_endpoint") {
			return s
		}
	}
	return ""
}

// TestBuildConnStmts_StripsEndpointScheme is the regression for the rollup
// "http://http://host" double-scheme: DuckDB's s3_endpoint must be host[:port]
// only (it derives http/https from s3_use_ssl). A scheme'd endpoint produced an
// unresolvable URL and broke rollup glob/build.
func TestBuildConnStmts_StripsEndpointScheme(t *testing.T) {
	stmts := buildConnStmts(S3Params{Endpoint: "http://minio:9000", UseSSL: false}, "1GB", 0, false)
	ep := endpointStmt(stmts)
	if ep != "SET GLOBAL s3_endpoint='minio:9000'" {
		t.Fatalf("endpoint stmt = %q, want scheme-stripped 'minio:9000'", ep)
	}
	if strings.Contains(ep, "://") {
		t.Fatalf("endpoint still carries a scheme (double-scheme bug): %q", ep)
	}
}

func TestBuildConnStmts_BareHostUnchanged(t *testing.T) {
	stmts := buildConnStmts(S3Params{Endpoint: "minio:9000"}, "1GB", 0, false)
	if ep := endpointStmt(stmts); ep != "SET GLOBAL s3_endpoint='minio:9000'" {
		t.Fatalf("bare host mangled: %q", ep)
	}
}
