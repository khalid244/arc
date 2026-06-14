package s3util

import "testing"

func TestStripURLScheme(t *testing.T) {
	cases := []struct{ in, want string }{
		{"http://minio:9000", "minio:9000"},
		{"https://minio:9000", "minio:9000"},
		{"HTTP://Minio:9000", "Minio:9000"},   // scheme match is case-insensitive
		{"https://S3.US-EAST-1.io", "S3.US-EAST-1.io"}, // remainder case preserved
		{"minio:9000", "minio:9000"},          // bare host unchanged
		{"minio:9000/", "minio:9000"},         // trailing slash, no scheme
		{"http://minio:9000/", "minio:9000"},  // trailing slash trimmed
		{"http://minio:9000///", "minio:9000"},
		{"  http://h:1  ", "h:1"},             // surrounding whitespace trimmed
		{"  https://s3.amazonaws.com/  ", "s3.amazonaws.com"}, // whitespace + slash
		{"weird-host-http://name", "weird-host-http://name"}, // scheme not at start: no match
		{"", ""},
		{"   ", ""}, // whitespace-only collapses to empty
		{"localhost:9000", "localhost:9000"},
		{"s3.amazonaws.com", "s3.amazonaws.com"},
	}
	for _, c := range cases {
		if got := StripURLScheme(c.in); got != c.want {
			t.Errorf("StripURLScheme(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// Idempotence: stripping an already-bare endpoint is a no-op.
func TestStripURLScheme_Idempotent(t *testing.T) {
	for _, in := range []string{"minio:9000", "http://minio:9000/", "https://x"} {
		once := StripURLScheme(in)
		if twice := StripURLScheme(once); twice != once {
			t.Errorf("not idempotent for %q: %q -> %q", in, once, twice)
		}
	}
}
