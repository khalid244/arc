package sql

import "testing"

func TestIsUnicodeSpace(t *testing.T) {
	// The characters DuckDB 1.5.6 reads as a space between tokens.
	for _, r := range []rune{0x00A0, 0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007,
		0x2008, 0x2009, 0x200A, 0x200B, 0x202F, 0x205F, 0x2060, 0x3000, 0xFEFF} {
		if !IsUnicodeSpace(r) {
			t.Errorf("%U not a Unicode space", r)
		}
	}
	for _, r := range []rune{' ', '\t', '\n', 'a', '_', '$', 0x00E9, 0x0639, 0x00B5} {
		if IsUnicodeSpace(r) {
			t.Errorf("%U taken for a Unicode space", r)
		}
	}
	for s, want := range map[string]bool{
		"SELECT id FROM cpu":                 false,
		"SELECT id AS \u0639\u062f\u062f":    false,
		"SELECT id\u00a0FROM mem":            true,
		"SELECT id\u200bFROM mem":            true,
		"SELECT \u00e9t\u00e9\u2003FROM mem": true,
		"\ufeffSELECT 1":                     true,
	} {
		if got := ContainsUnicodeSpace(s); got != want {
			t.Errorf("ContainsUnicodeSpace(%q) = %v, want %v", s, got, want)
		}
	}
}
