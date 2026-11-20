package sql

import (
	"unicode"
	"unicode/utf8"
)

// IsUnicodeSpace reports whether r is a non-ASCII space (or a zero-width or
// BOM character). DuckDB's lexer reads some of them between tokens
// (DuckDB 1.5.6: U+00A0, U+2000-U+200B, U+202F, U+205F, U+2060, U+3000,
// U+FEFF), while the `\s` and `\b` of Go's regexps do not see them as spaces.
// The set is wider than DuckDB's: it also holds U+0085, U+1680, U+180E,
// U+200C, U+200D, U+2028 and U+2029, which DuckDB 1.5.6 reads as identifier
// characters.
func IsUnicodeSpace(r rune) bool {
	return r >= 0x80 && (unicode.IsSpace(r) || unicode.In(r, unicode.Zs, unicode.Zl, unicode.Zp) ||
		r == 0xFEFF || r == 0x180E || r == 0x2060 || (r >= 0x200B && r <= 0x200D))
}

// ContainsUnicodeSpace reports whether s holds a rune IsUnicodeSpace accepts.
func ContainsUnicodeSpace(s string) bool {
	for i := 0; i < len(s); {
		if s[i] < utf8.RuneSelf {
			i++
			continue
		}
		r, n := utf8.DecodeRuneInString(s[i:])
		if IsUnicodeSpace(r) {
			return true
		}
		i += n
	}
	return false
}
