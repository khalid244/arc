package pruning

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/sql"
)

// rangeForEveryRead returns the range that may leave partitions out of every
// measurement sqlStr reads, given tr, the candidate the regex reader took
// from it: tr itself when it holds for every read, otherwise the smallest
// range that does (the hull of what each read's WHERE proves), otherwise nil.
//
// The regex reader takes the first time bound it finds anywhere in the first
// WHERE clause and the pruner applies it to every table reference, so on its
// own it returned too few rows (with success) for, among others: a bound
// under a top-level OR or NOT, inside CASE, a comment or QUALIFY, arithmetic
// after the bound, a column merely named like time ("timestamp", "ts::timestamp"),
// a JOIN or UNION where one table is not bounded, and a bound in a subquery
// applied to the outer query (or the other way round).
//
// The rule: for every measurement reference (a name after FROM, JOIN or a
// FROM-list comma that is not a CTE), its own query block must have a WHERE,
// and that WHERE's bare top-level AND terms on its time column (unqualified,
// or qualified by its name or alias) must bound the rows at least as tightly
// as tr. A term is bare when it is exactly `time OP operand` or `time BETWEEN
// operand AND operand`, the operand a literal (optionally TIMESTAMP,
// TIMESTAMPTZ or DATE typed or cast) or now()/current_timestamp, plus or
// minus intervals. Every other term is ignored, which only widens what is
// proven; a top-level OR proves nothing. Anything this check does not
// understand answers nil: the query then reads the whole measurement,
// correctly but unpruned.
//
// When tr does not hold for every read but each read's WHERE proves a start,
// the hull of the proven ranges is returned instead (two subqueries over 7
// and 30 days read 30 days, where tr alone read 7 for both).
//
// A start-only range keeps its assumed end (now + 24h), as before: rows
// stamped more than a day in the future are not read by such a query.
func rangeForEveryRead(sqlStr string, tr *TimeRange, now time.Time) *TimeRange {
	if tr == nil || tr.StartAssumed {
		return nil
	}
	g, ok := newShapeGate(sqlStr, now)
	if !ok {
		return nil
	}
	return g.rangeFor(tr)
}

// newShapeGate tokenizes sqlStr for the checks above. ok is false for SQL
// they do not read: caller text spelling a masker placeholder, an
// unterminated comment, unbalanced nesting, a non-ASCII space outside
// literals and comments, a word the storage-path rewrite's `\b` splits, a
// keyword after a dot.
func newShapeGate(sqlStr string, now time.Time) (*shapeGate, bool) {
	if sql.ContainsReservedPlaceholder(sqlStr) {
		return nil, false
	}
	masked, masks := sql.MaskStringLiterals(sqlStr, sql.HasQuotes(sqlStr))
	// The storage-path rewrite finds CTE names in its own comment-stripped
	// text, where a block comment leaves nothing behind.
	rewriteForm, _ := stripMaskedSQLComments(masked, "")
	masked, ok := stripMaskedSQLComments(masked, " ")
	if !ok {
		return nil, false
	}
	// A non-ASCII space outside literals and comments is not read. DuckDB
	// reads U+00A0, U+200B, U+2003 and others as a space between tokens, and
	// the storage-path rewrite's `\b` sees a word boundary next to any
	// non-ASCII byte, so `id<U+00A0>FROM mem` reads mem in both, while the
	// tokenizer below takes non-ASCII bytes for word bytes and never saw that
	// FROM. Folding them to spaces is not safe either: DuckDB reads U+200C,
	// U+2028 and others of the set as identifier characters, so a folded
	// `x<U+200C>union` would end a query block DuckDB does not end.
	if sql.ContainsUnicodeSpace(masked) {
		return nil, false
	}
	// FROM inside EXTRACT/TRIM/SUBSTRING/OVERLAY and IS [NOT] DISTINCT FROM is
	// not a relation's FROM.
	masked, _ = sql.MaskFromKeywordsInFunctionBodies(masked)
	toks, ok := tokenizeShape(masked)
	if !ok {
		return nil, false
	}
	g := &shapeGate{toks: toks, literals: literalTexts(masks), idents: sql.IdentifierNames(masks), now: now}
	g.ctes = g.cteNames(rewriteCTENames(rewriteForm))
	return g, true
}

// CTENamePatternText is the storage-path rewrite's CTE-name pattern
// (internal/api patternCTENames, which a test holds equal to it).
const CTENamePatternText = `(?i)\bWITH\s+(?:RECURSIVE\s+)?(\w+)(?:\s*\([^)]*\))?\s+AS\s*\(|,\s*(\w+)(?:\s*\([^)]*\))?\s+AS\s*\(`

var cteNamePattern = regexp.MustCompile(CTENamePatternText)

var withWordPattern = regexp.MustCompile(`(?i)\bwith\b`)

// rewriteCTENames are the names the storage-path rewrite leaves alone as
// CTEs (internal/api extractCTENames), lower-cased.
func rewriteCTENames(s string) map[string]bool {
	names := map[string]bool{}
	if !withWordPattern.MatchString(s) {
		return names
	}
	for _, m := range cteNamePattern.FindAllStringSubmatch(s, -1) {
		for _, n := range m[1:] {
			if n != "" {
				names[strings.ToLower(n)] = true
			}
		}
	}
	return names
}

// stripMaskedSQLComments removes -- and /* */ comments from SQL whose string
// literals and quoted identifiers are already masked, so every comment marker
// left is a real one. A comment becomes replacement. ok is false for an
// unterminated or nested block comment: the masker ends a block comment at
// its first */ and DuckDB nests them, so the two would disagree on what is
// code (ValidateSQLRequest refuses nested comments for the same reason).
func stripMaskedSQLComments(s, replacement string) (string, bool) {
	if !strings.Contains(s, "--") && !strings.Contains(s, "/*") {
		return s, true
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		switch {
		case s[i] == '-' && i+1 < len(s) && s[i+1] == '-':
			j := i + 2
			for j < len(s) && s[j] != '\n' && s[j] != '\r' {
				j++
			}
			b.WriteString(replacement)
			i = j
		case s[i] == '/' && i+1 < len(s) && s[i+1] == '*':
			j := i + 2
			for j+1 < len(s) && !(s[j] == '*' && s[j+1] == '/') {
				if s[j] == '/' && s[j+1] == '*' {
					return "", false
				}
				j++
			}
			if j+1 >= len(s) {
				return "", false
			}
			j += 2
			b.WriteString(replacement)
			i = j
		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String(), true
}

func literalTexts(masks []sql.StringMask) map[string]string {
	out := make(map[string]string, len(masks))
	for _, m := range masks {
		if !m.Identifier {
			out[m.Placeholder] = m.Original
		}
	}
	return out
}

type shapeTok struct {
	text  string // as written (masked form)
	lower string
	word  bool // identifier, keyword, placeholder or number
	depth int  // nesting depth: parentheses and CASE ... END
}

// tokenizeShape splits masked SQL into words and operator/punctuation tokens
// and records each token's nesting depth. An opening "(" or CASE carries the
// outer depth and a closing ")" or END the outer depth again, so a block's
// own tokens all share one depth. ok is false on unbalanced nesting.
func tokenizeShape(s string) ([]shapeTok, bool) {
	var toks []shapeTok
	depth := 0
	isWordStart := func(c byte) bool {
		return c == '_' || c == '$' || (c|0x20 >= 'a' && c|0x20 <= 'z') || c >= 0x80
	}
	isWordByte := func(c byte) bool {
		return isWordStart(c) || (c >= '0' && c <= '9')
	}
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
			continue
		case c == '_' && placeholderAt(s, i) > 0:
			// A masked literal or identifier is one token even when a word
			// follows with no space ('a'END is a string, then END).
			n := placeholderAt(s, i)
			toks = append(toks, shapeTok{text: s[i : i+n], lower: s[i : i+n], word: true, depth: depth})
			i += n
			continue
		case c >= '0' && c <= '9':
			// A number ends where its digits do: DuckDB lexes 1OR as 1 OR.
			j := i + 1
			for j < len(s) && (s[j] >= '0' && s[j] <= '9' || s[j] == '.') {
				j++
			}
			toks = append(toks, shapeTok{text: s[i:j], lower: s[i:j], word: true, depth: depth})
			i = j
			continue
		case isWordStart(c):
			j := i + 1
			for j < len(s) && isWordByte(s[j]) && !(s[j] == '_' && placeholderAt(s, j) > 0) {
				j++
			}
			w := s[i:j]
			if regexSplitsWord(w) {
				return nil, false
			}
			lw := strings.ToLower(w)
			if len(toks) > 0 && toks[len(toks)-1].text == "." && hasKeywordRole(lw) {
				// DuckDB reads any word after a dot as a name (g.group,
				// s.limit, s.case), where this check would end a list or
				// a WHERE body, nest a CASE or split a term on it.
				return nil, false
			}
			t := shapeTok{text: w, lower: lw, word: true, depth: depth}
			switch lw {
			case "case":
				depth++
			case "end":
				depth--
				t.depth = depth
			}
			if depth < 0 {
				return nil, false
			}
			toks = append(toks, t)
			i = j
			continue
		}
		// Operators and punctuation.
		n := 1
		if i+1 < len(s) {
			switch s[i : i+2] {
			case ">=", "<=", "<>", "!=", "::", "||", "->", "=>", "==", ">>", "<<", "**", "//", "!~", "~~":
				n = 2
			}
		}
		t := shapeTok{text: s[i : i+n], lower: s[i : i+n], depth: depth}
		switch t.text {
		case "(":
			depth++
		case ")":
			depth--
			t.depth = depth
		}
		if depth < 0 {
			return nil, false
		}
		toks = append(toks, t)
		i += n
	}
	return toks, depth == 0
}

// hasKeywordRole reports whether the checks below read lw (lower-cased) as
// a keyword: CASE/END nesting, a word that ends a FROM list or a WHERE body,
// joins relations, splits a WHERE into terms, or opens a query, a CTE or an
// alias. The operand words (now, interval, cast, timestamp, ...) and time
// are read only at fixed places in a bound, where a qualified name never
// matches.
func hasKeywordRole(lw string) bool {
	switch lw {
	case "case", "end", "from", "join", "with", "as", "and", "or", "not", "between", "select", "values":
		return true
	}
	return fromListEnd[lw] || joinWord[lw] || unmodelledWord[lw]
}

// regexSplitsWord reports whether the storage-path rewrite's `\b`, which
// counts only ASCII letters, digits and _ as word characters, sees a word
// boundary inside w, a word to the tokenizer (and an identifier to DuckDB):
// w mixes those with $ or non-ASCII bytes. The rewrite reads `FROM mem` in
// `id<U+00E9>FROM mem`, a table no FROM of this check names.
func regexSplitsWord(w string) bool {
	regexWord, other := false, false
	for i := 0; i < len(w); i++ {
		c := w[i]
		if c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			regexWord = true
		} else {
			other = true
		}
	}
	return regexWord && other
}

// placeholderAt returns the length of the masker placeholder (__STR_n__,
// __IDENT_n__, __FROM_MASK_n__) starting at s[i], or 0.
func placeholderAt(s string, i int) int {
	for _, prefix := range []string{"__STR_", "__IDENT_", "__FROM_MASK_"} {
		if !strings.HasPrefix(s[i:], prefix) {
			continue
		}
		j := i + len(prefix)
		k := j
		for k < len(s) && s[k] >= '0' && s[k] <= '9' {
			k++
		}
		if k > j && strings.HasPrefix(s[k:], "__") {
			return k + 2 - i
		}
	}
	return 0
}

type shapeGate struct {
	toks     []shapeTok
	literals map[string]string // string-literal placeholder -> original text, quotes included
	idents   map[string]string // quoted-identifier placeholder -> name
	now      time.Time
	ctes     map[string]bool
}

// measurementRef is one table reference the storage-path rewrite can turn
// into a read of a measurement.
type measurementRef struct {
	names    map[string]bool // lower-cased names a column can be qualified by
	listEnd  int             // index of the token that ends its FROM list
	depth    int
	blockEnd int // index one past its query block's last token
	// joined: its FROM list holds more than one relation. An unqualified
	// time column in the WHERE then still bounds it: the measurement has a
	// time column, so a successful bind means the name is either its column
	// or one merged with it by USING/NATURAL, and a WHERE comparison on it
	// rejects the NULL-extended rows an outer join makes.
	joined bool
}

// Words that end a FROM list, and (except WHERE) a WHERE body, at a block's
// depth.
var fromListEnd = map[string]bool{
	"where": true, "group": true, "having": true, "qualify": true, "window": true,
	"order": true, "limit": true, "offset": true, "fetch": true, "select": true,
	"union": true, "intersect": true, "except": true,
}

// Words that put a second relation into a FROM list, or end a relation in
// one.
var joinWord = map[string]bool{
	"join": true, "natural": true, "cross": true, "left": true, "right": true,
	"full": true, "inner": true, "outer": true, "semi": true, "anti": true,
	"asof": true, "using": true, "on": true,
}

// Relation forms this check does not model. Any of them anywhere means no
// pruning.
var unmodelledWord = map[string]bool{
	"recursive": true, "lateral": true, "positional": true, "pivot": true, "unpivot": true,
	"pivot_wider": true, "pivot_longer": true, "tablesample": true, "summarize": true,
	"describe": true, "show": true, "table": true, "sample": true,
}

func (g *shapeGate) rangeFor(tr *TimeRange) *TimeRange {
	for _, t := range g.toks {
		if t.word && unmodelledWord[t.lower] {
			return nil
		}
	}
	refs, ok := g.measurementRefs()
	if !ok || len(refs) == 0 {
		return nil
	}
	holds := true
	var hullLo, hullHi time.Time
	openEnd := false
	for i, r := range refs {
		lo, hi, ok := g.provenBounds(r)
		if !ok || lo == nil {
			return nil
		}
		if i == 0 || lo.Before(hullLo) {
			hullLo = *lo
		}
		if hi == nil {
			openEnd = true
		} else if i == 0 || hi.After(hullHi) {
			hullHi = *hi
		}
		// The candidate must not start after what the WHERE proves...
		if tr.Start.After(*lo) {
			holds = false
		}
		// ...nor end before it. An assumed end (start-only predicate) stands
		// when the WHERE states no end, as before this check existed.
		if hi == nil {
			if !tr.EndAssumed {
				holds = false
			}
		} else if tr.End.Before(*hi) {
			holds = false
		}
	}
	if holds {
		return tr
	}
	hull := &TimeRange{Start: hullLo, End: hullHi}
	if openEnd {
		// A read with no end gets the assumed one, but a read whose WHERE
		// states a later end keeps it.
		if assumed := g.now.Add(24 * time.Hour); assumed.After(hull.End) {
			hull.End = assumed
		}
		hull.EndAssumed = true
	}
	if !hull.End.After(hull.Start) {
		return nil
	}
	return hull
}

// cteNames returns the lower-cased names every WITH clause defines that the
// storage-path rewrite also takes for CTEs (rewrite): a name only one of the
// two sees as a CTE is checked as a measurement.
func (g *shapeGate) cteNames(rewrite map[string]bool) map[string]bool {
	names := map[string]bool{}
	t := g.toks
	for i := 0; i < len(t); i++ {
		if !t[i].word || t[i].lower != "with" {
			continue
		}
		// Only the forms the storage-path rewrite also takes for a CTE
		// (patternCTENames: an unquoted name, AS directly before the
		// parenthesis) count. A name it would rewrite as a measurement read
		// must be checked as one here.
		j := i + 1
		for j < len(t) {
			name := t[j].text
			if !t[j].word || strings.HasPrefix(name, "__") || name[0] >= '0' && name[0] <= '9' {
				break
			}
			j++
			if j < len(t) && t[j].text == "(" { // column list
				j = g.matchClose(j) + 1
			}
			if j >= len(t) || t[j].lower != "as" {
				break
			}
			j++
			if j >= len(t) || t[j].text != "(" {
				break
			}
			if rewrite[strings.ToLower(name)] {
				names[strings.ToLower(name)] = true
			}
			j = g.matchClose(j) + 1
			if j < len(t) && t[j].text == "," {
				j++
				continue
			}
			break
		}
	}
	return names
}

// matchClose returns the index of the ")" closing the "(" at i, or len-1.
func (g *shapeGate) matchClose(i int) int {
	d := g.toks[i].depth
	for j := i + 1; j < len(g.toks); j++ {
		if g.toks[j].text == ")" && g.toks[j].depth == d {
			return j
		}
	}
	return len(g.toks) - 1
}

// identName reads the token at i as a plain or quoted identifier.
func (g *shapeGate) identName(i int) (string, bool) {
	if i >= len(g.toks) || !g.toks[i].word {
		return "", false
	}
	w := g.toks[i].text
	if name, ok := g.idents[w]; ok {
		return name, true
	}
	if strings.HasPrefix(w, "__STR_") || (w[0] >= '0' && w[0] <= '9') {
		return "", false
	}
	return w, true
}

// measurementRefs finds every relation reference after FROM, JOIN or a FROM
// list comma that is a name (not a subquery, a table function or a CTE). ok
// is false for a shape this check does not read: a name on the right of a
// SEMI or ANTI join (its columns are not visible to the WHERE), a relation
// followed by anything but an alias, or a join in parentheses.
func (g *shapeGate) measurementRefs() ([]measurementRef, bool) {
	t := g.toks
	var refs []measurementRef
	// listJoins are the JOIN words that join relations of a FROM list read
	// here.
	listJoins := map[int]bool{}
	for i := 0; i < len(t); i++ {
		if !t[i].word || t[i].lower != "from" {
			continue
		}
		d := t[i].depth
		end := i + 1
		joined := false
		for end < len(t) {
			x := t[end]
			// The ")" closing an enclosing block carries the outer depth, so
			// depth < d ends the list; a ")" at depth d closes a nested group.
			if x.depth < d || (x.depth == d && (x.text == ";" || (x.word && fromListEnd[x.lower]))) {
				break
			}
			if x.depth == d && (x.text == "," || (x.word && x.lower == "join")) {
				joined = true
				if x.word {
					listJoins[end] = true
				}
			}
			end++
		}
		// Each relation starts after FROM, a comma or JOIN, and runs to the
		// next comma, join word or the end of the list.
		k := i + 1
		for k < end {
			itemEnd := k
			for itemEnd < end && !(t[itemEnd].depth == d && (t[itemEnd].text == "," || (t[itemEnd].word && joinWord[t[itemEnd].lower]))) {
				itemEnd++
			}
			ref, isRef, ok := g.readRef(k, itemEnd, d)
			if !ok {
				return nil, false
			}
			if isRef {
				if k > i+1 && (t[k-1].lower == "join" && k >= 2 && (t[k-2].lower == "semi" || t[k-2].lower == "anti")) {
					return nil, false
				}
				ref.listEnd = end
				ref.depth = d
				ref.blockEnd = g.blockEnd(end, d)
				ref.joined = joined
				refs = append(refs, ref)
			}
			// Skip the join condition (ON/USING ...) and join words to the
			// next relation.
			k = itemEnd
			for k < end && !(t[k].depth == d && (t[k].text == "," || t[k].lower == "join")) {
				k++
			}
			k++
		}
	}
	// A JOIN outside every FROM list read above (one in parentheses, say)
	// joins a relation the rewrite reads and this check never saw.
	for j, x := range t {
		if x.word && x.lower == "join" && !listJoins[j] {
			return nil, false
		}
	}
	return refs, true
}

// readRef reads the relation in tokens [k,end) of a FROM list at depth d.
// isRef is false for a subquery, a table function or a CTE name; ok is false
// when anything but [AS] alias follows a name, and for a parenthesised group
// that is not a query (a join in parentheses).
func (g *shapeGate) readRef(k, end, d int) (measurementRef, bool, bool) {
	t := g.toks
	if k >= end {
		return measurementRef{}, false, false
	}
	if t[k].text == "(" {
		// A subquery's own FROM is read where it stands. A parenthesised
		// join, `FROM (c JOIN mem m ON ...)`, is not modelled: the rewrite's
		// JOIN pattern still turns mem into a read, which no FROM of this
		// check names.
		return measurementRef{}, false, g.isQueryGroup(k)
	}
	var parts []string
	j := k
	for {
		name, ok := g.identName(j)
		if !ok || j >= end {
			return measurementRef{}, false, false
		}
		parts = append(parts, strings.ToLower(name))
		j++
		if j < end && t[j].text == "." {
			j++
			continue
		}
		break
	}
	if j < end && t[j].text == "(" {
		return measurementRef{}, false, true // table function
	}
	if len(parts) == 1 && g.ctes[parts[0]] {
		return measurementRef{}, false, true
	}
	if len(parts) > 2 {
		return measurementRef{}, false, false
	}
	names := map[string]bool{parts[len(parts)-1]: true}
	if len(parts) == 2 {
		names[parts[0]+"."+parts[1]] = true
	}
	// Optional alias: [AS] name. An alias hides the table's own name.
	if j < end && t[j].lower == "as" {
		j++
	}
	if j < end {
		alias, ok := g.identName(j)
		if !ok {
			return measurementRef{}, false, false
		}
		names = map[string]bool{strings.ToLower(alias): true}
		j++
	}
	if j != end {
		return measurementRef{}, false, false
	}
	return measurementRef{names: names}, true, true
}

// isQueryGroup reports whether the "(" at i opens a query: (SELECT ...),
// (WITH ...), (FROM ...), (VALUES ...), or a parenthesised query in
// parentheses, alone or before a set operation, ORDER BY or LIMIT. A join in
// parentheses, `(a JOIN b ON ...)` or `((SELECT ...) x JOIN b ON ...)`, is
// not one.
func (g *shapeGate) isQueryGroup(i int) bool {
	t := g.toks
	j := i + 1
	if j >= len(t) {
		return false
	}
	if t[j].word {
		switch t[j].lower {
		case "select", "with", "from", "values":
			return true
		}
		return false
	}
	if t[j].text != "(" || !g.isQueryGroup(j) {
		return false
	}
	n := g.matchClose(j) + 1
	if n >= len(t) {
		return false
	}
	if t[n].text == ")" && t[n].depth == t[i].depth {
		return true
	}
	if t[n].word && t[n].depth == t[i].depth+1 {
		switch t[n].lower {
		case "union", "intersect", "except", "order", "limit", "offset":
			return true
		}
	}
	return false
}

// blockEnd returns the index one past the last token of the query block
// whose FROM list ends at from.
func (g *shapeGate) blockEnd(from, d int) int {
	t := g.toks
	for j := from; j < len(t); j++ {
		x := t[j]
		if x.depth < d || (x.depth == d && x.text == ";") {
			return j
		}
		if x.depth == d && x.word && (x.lower == "union" || x.lower == "intersect" || x.lower == "except") {
			return j
		}
	}
	return len(t)
}

// provenBounds returns the tightest start and exclusive end the bare
// top-level AND terms of r's WHERE state. ok is false when the block has no
// WHERE or its WHERE is a top-level disjunction.
func (g *shapeGate) provenBounds(r measurementRef) (lo, hi *time.Time, ok bool) {
	t := g.toks
	w := r.listEnd
	if w >= r.blockEnd || t[w].lower != "where" || t[w].depth != r.depth {
		return nil, nil, false
	}
	bodyEnd := w + 1
	for bodyEnd < r.blockEnd {
		x := t[bodyEnd]
		if x.depth == r.depth && x.word && x.lower != "where" && fromListEnd[x.lower] {
			break
		}
		bodyEnd++
	}
	terms, ok := g.conjuncts(w+1, bodyEnd, r.depth)
	if !ok {
		return nil, nil, false
	}
	for _, term := range terms {
		g.addTerm(term[0], term[1], r, &lo, &hi)
	}
	return lo, hi, true
}

// conjuncts splits tokens [from,to) at depth d on AND (the AND of a BETWEEN
// excepted). ok is false when a top-level OR joins them.
func (g *shapeGate) conjuncts(from, to, d int) ([][2]int, bool) {
	var out [][2]int
	start := from
	between := false
	for k := from; k < to; k++ {
		x := g.toks[k]
		if x.depth != d || !x.word {
			continue
		}
		switch x.lower {
		case "or":
			return nil, false
		case "between":
			between = true
		case "and":
			if between {
				between = false
				continue
			}
			out = append(out, [2]int{start, k})
			start = k + 1
		}
	}
	out = append(out, [2]int{start, to})
	return out, true
}

// addTerm narrows lo/hi by the term [from,to) when it is a bare time bound.
// A parenthesised group that is itself an AND of terms is read term by term.
func (g *shapeGate) addTerm(from, to int, r measurementRef, lo, hi **time.Time) {
	t := g.toks
	if from >= to {
		return
	}
	if t[from].text == "(" && g.matchClose(from) == to-1 {
		inner, ok := g.conjuncts(from+1, to-1, t[from].depth+1)
		if !ok {
			return
		}
		for _, term := range inner {
			g.addTerm(term[0], term[1], r, lo, hi)
		}
		return
	}
	l, h, ok := g.readBound(from, to, r)
	if !ok {
		return
	}
	if l != nil && (*lo == nil || l.After(**lo)) {
		*lo = l
	}
	if h != nil && (*hi == nil || h.Before(**hi)) {
		*hi = h
	}
}

// readBound parses `time OP operand` or `time BETWEEN operand AND operand`
// filling exactly [from,to). The end it returns is exclusive.
func (g *shapeGate) readBound(from, to int, r measurementRef) (lo, hi *time.Time, ok bool) {
	j, bare, ok := g.readTimeColumn(from, to, r)
	if !ok || j >= to {
		return nil, nil, false
	}
	op := g.toks[j].lower
	j++
	switch op {
	case ">=", ">", "<=", "<":
		v, next, ok := g.readOperand(j, to, op == "<=" || op == "<", bare)
		if !ok || next != to {
			return nil, nil, false
		}
		switch op {
		case ">=", ">":
			return &v, nil, true
		case "<=":
			e := v.Add(time.Microsecond)
			return nil, &e, true
		default:
			return nil, &v, true
		}
	case "between":
		a, next, ok := g.readOperand(j, to, false, bare)
		if !ok || next >= to || g.toks[next].lower != "and" {
			return nil, nil, false
		}
		b, next, ok := g.readOperand(next+1, to, true, bare)
		if !ok || next != to {
			return nil, nil, false
		}
		e := b.Add(time.Microsecond)
		return &a, &e, true
	}
	return nil, nil, false
}

// readTimeColumn reads the time column, optionally qualified by r's name or
// alias and optionally cast to timestamp/timestamptz, and returns the index
// after it and how DuckDB reads a bare string compared with it: "tz" (an
// offset is honoured) or, for time::timestamp, "naive" (the string becomes a
// TIMESTAMP, its offset dropped).
func (g *shapeGate) readTimeColumn(from, to int, r measurementRef) (int, string, bool) {
	t := g.toks
	var parts []string
	j := from
	for j < to {
		name, ok := g.identName(j)
		if !ok {
			return 0, "", false
		}
		parts = append(parts, strings.ToLower(name))
		j++
		if j < to && t[j].text == "." {
			j++
			continue
		}
		break
	}
	if len(parts) == 0 || parts[len(parts)-1] != "time" {
		return 0, "", false
	}
	if q := parts[:len(parts)-1]; len(q) > 0 && !r.names[strings.Join(q, ".")] {
		return 0, "", false
	}
	bare := "tz"
	if j+1 < to && t[j].text == "::" && (t[j+1].lower == "timestamp" || t[j+1].lower == "timestamptz") {
		if t[j+1].lower == "timestamp" {
			bare = "naive"
		}
		j += 2
	}
	return j, bare, true
}

// readOperand evaluates the operand starting at j and returns the instant
// DuckDB compares the time column with and the index after it. The operand
// is a base value followed by any number of `+|- INTERVAL ...` terms. The
// base is now() or current_timestamp, a string literal (optionally typed
// TIMESTAMP/TIMESTAMPTZ/DATE '…', cast with ::type, or CAST('…' AS type)).
// An untyped string takes no interval (DuckDB refuses VARCHAR - INTERVAL)
// and is read as bare says (readTimeColumn). upper rounds a sub-microsecond
// literal up instead of down, so the bound is never tighter than DuckDB's.
func (g *shapeGate) readOperand(j, to int, upper bool, bare string) (time.Time, int, bool) {
	v, j, typed, ok := g.readBase(j, to, upper, bare)
	if !ok {
		return time.Time{}, 0, false
	}
	for j < to && (g.toks[j].text == "-" || g.toks[j].text == "+") {
		if !typed {
			return time.Time{}, 0, false
		}
		v, j, ok = g.readInterval(v, j, to)
		if !ok {
			return time.Time{}, 0, false
		}
	}
	return v, j, true
}

var castKinds = map[string]string{"timestamp": "naive", "timestamptz": "tz", "date": "date"}

// readBase reads the base value of an operand. typed is false for a bare
// string literal.
func (g *shapeGate) readBase(j, to int, upper bool, bare string) (time.Time, int, bool, bool) {
	t := g.toks
	if j >= to {
		return time.Time{}, 0, false, false
	}
	switch t[j].lower {
	case "now":
		if j+2 >= to || t[j+1].text != "(" || t[j+2].text != ")" {
			return time.Time{}, 0, false, false
		}
		return g.now, j + 3, true, true
	case "current_timestamp":
		return g.now, j + 1, true, true
	case "cast":
		// CAST ( 'literal' AS type )
		if j+5 >= to+1 || t[j+1].text != "(" || t[j+3].lower != "as" || t[j+5].text != ")" {
			return time.Time{}, 0, false, false
		}
		kind := castKinds[t[j+4].lower]
		val, ok := g.stringLiteral(j + 2)
		if kind == "" || !ok {
			return time.Time{}, 0, false, false
		}
		v, ok := duckLiteralInstant(val, kind, upper)
		return v, j + 6, true, ok
	}
	kind, typed := bare, false
	if k := castKinds[t[j].lower]; k != "" {
		kind, typed = k, true
		j++
	}
	if j >= to {
		return time.Time{}, 0, false, false
	}
	val, ok := g.stringLiteral(j)
	if !ok {
		return time.Time{}, 0, false, false
	}
	j++
	if j+1 < to && t[j].text == "::" {
		cast := castKinds[t[j+1].lower]
		if cast == "" || (typed && cast != kind) {
			return time.Time{}, 0, false, false
		}
		kind, typed = cast, true
		j += 2
	}
	v, ok := duckLiteralInstant(val, kind, upper)
	return v, j, typed, ok
}

// stringLiteral returns the text of the single-quoted string literal masked
// at token j.
func (g *shapeGate) stringLiteral(j int) (string, bool) {
	if j >= len(g.toks) {
		return "", false
	}
	orig, ok := g.literals[g.toks[j].text]
	if !ok || len(orig) < 2 || orig[0] != '\'' || orig[len(orig)-1] != '\'' {
		return "", false
	}
	return strings.ReplaceAll(orig[1:len(orig)-1], "''", "'"), true
}

// readInterval applies the `+|- INTERVAL '<n> <unit>'`, `INTERVAL <n> <unit>`
// or `INTERVAL '<n>' <unit>` at j to v.
func (g *shapeGate) readInterval(v time.Time, j, to int) (time.Time, int, bool) {
	t := g.toks
	add := t[j].text == "+"
	j++
	if j >= to || t[j].lower != "interval" {
		return time.Time{}, 0, false
	}
	j++
	var amount, unit string
	if lit, ok := g.stringLiteral(j); ok && j < to {
		f := strings.Fields(lit)
		switch {
		case len(f) == 2:
			amount, unit = f[0], f[1]
			j++
		case len(f) == 1 && j+1 < to && t[j+1].word:
			amount, unit = f[0], t[j+1].text
			j += 2
		}
	} else if j+1 < to && t[j].word && t[j+1].word {
		amount, unit = t[j].text, t[j+1].text
		j += 2
	}
	if amount == "" {
		return time.Time{}, 0, false
	}
	if _, err := strconv.Atoi(amount); err != nil {
		return time.Time{}, 0, false
	}
	r, err := evaluateRelativeTimeAt(amount, unit, add, v)
	if err != nil {
		return time.Time{}, 0, false
	}
	return r, j, true
}

// duckLiteralInstant is the instant DuckDB compares a TIMESTAMPTZ column
// with for a string literal of the given kind, in Arc's UTC session time
// zone (database/duckdb.go pins it):
//   - "tz" (plain string, TIMESTAMPTZ '…', '…'::timestamptz): an explicit
//     offset is honoured, none means UTC;
//   - "naive" (TIMESTAMP '…', '…'::timestamp): an offset is dropped and the
//     wall clock read as UTC;
//   - "date": midnight UTC of the date.
//
// Only the spellings parseDateTime also reads are accepted.
func duckLiteralInstant(val, kind string, upper bool) (time.Time, bool) {
	val = strings.TrimSpace(val)
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	for _, layout := range layouts {
		ts, err := time.Parse(layout, val)
		if err != nil {
			continue
		}
		switch kind {
		case "naive":
			ts = time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), ts.Nanosecond(), time.UTC)
		case "date":
			ts = time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
		default:
			ts = ts.UTC()
		}
		if r := ts.Truncate(time.Microsecond); !r.Equal(ts) {
			if upper {
				ts = r.Add(time.Microsecond)
			} else {
				ts = r
			}
		}
		return ts, true
	}
	return time.Time{}, false
}
