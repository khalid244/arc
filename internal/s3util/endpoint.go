// Package s3util holds small, dependency-free helpers for S3/MinIO endpoint
// handling shared across the components that hand an endpoint to DuckDB's
// httpfs extension (the query engine and the rollup builder). Keeping one
// implementation avoids the drift that produced the rollup "http://http://"
// double-scheme bug, where a local copy lacked the trailing-slash trim.
package s3util

import "strings"

// StripURLScheme normalises a configured S3/MinIO endpoint into the bare
// "host[:port]" form DuckDB's httpfs extension expects (it derives http/https
// from the separate s3_use_ssl setting). The AWS SDK accepts either
// "host:port" or "scheme://host:port[/]"; DuckDB does not — passing a scheme'd
// or trailing-slashed value through verbatim produces malformed
// "http://http://..." URLs that fail to resolve.
//
// Strips, in order:
//   - leading and trailing whitespace (paste artefacts),
//   - a leading "http://" or "https://" (case-insensitive — RFC 3986 schemes
//     are case-insensitive and users routinely paste mixed-case),
//   - trailing slashes ("host:port/" -> "host:port").
//
// The case of the remainder is preserved (bucket names and path components can
// be case-sensitive depending on the S3 implementation).
func StripURLScheme(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	lower := strings.ToLower(endpoint)
	switch {
	case strings.HasPrefix(lower, "https://"):
		endpoint = endpoint[len("https://"):]
	case strings.HasPrefix(lower, "http://"):
		endpoint = endpoint[len("http://"):]
	}
	return strings.TrimRight(endpoint, "/")
}
