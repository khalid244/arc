package rollup

import (
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// Manifest is the per-cube index that makes the read path S3-latency-friendly.
// It lists every materialized day file with its bucket bounds, schema hash, and
// row count, so resolving a query's time range to a set of files is ONE object
// GET — no S3 LIST, no per-file parquet-metadata round-trips (the N+1 pattern
// that dominated latency in prior rollup generations).
type Manifest struct {
	CubeID     string      `json:"cube_id"`
	Source     string      `json:"source"`
	Grain      string      `json:"grain"`
	Dims       []string    `json:"dims"`
	Aggs       []Aggregate `json:"aggs"` // makes the manifest self-describing
	SchemaHash string      `json:"schema_hash"`
	Days       []DayEntry  `json:"days"`
	// Superseded lists cube files whose manifest entry was REPLACED by a rebuild
	// (a new unique-name file took its place) but which must not be deleted yet.
	// Day cube files carry a per-build nonce so a rebuild never overwrites a live
	// object in place (the ETag-changed-mid-read 500); the old object is parked
	// here and deleted only on a LATER builder pass (sweepSuperseded), giving a
	// full-tick grace so in-flight queries (≤300s) and route-only pods (5m manifest
	// cache) never lose a file they were handed. Empty in steady state.
	Superseded []SupersededFile `json:"superseded,omitempty"`
}

// SupersededFile is one cube object pending deferred deletion. At is the unix-nano
// wall time it was superseded; sweepSuperseded deletes it once at least one
// ForwardTick has elapsed (so it was recorded in a strictly earlier pass).
type SupersededFile struct {
	URI string `json:"uri"`
	At  int64  `json:"at"`
}

// clone returns a deep copy of the manifest whose every mutable field is freshly
// allocated, so subsequent in-place mutation of the original (the build's working
// copy — Upsert's append+sort, the Days = kept filters, purge) can never reach
// what the clone holds. The read-path Router publishes ONLY clones (see
// NewRouter), so a query iterating m.Days / m.Coverage / m.HasInteriorGap touches
// a frozen snapshot and never races the build mutating its own copy.
//
// Deep-copies every slice the router can read: Dims, Aggs (incl. each
// Aggregate's CondCols slice — the only nested slice on Aggregate), and the Days
// slice PLUS each DayEntry's Covers slice (the nested mutable field compactMonth
// appends to). Scalar fields copy by value via the struct copy.
func (m *Manifest) clone() *Manifest {
	if m == nil {
		return nil
	}
	c := *m // copies all scalar fields; slice headers are re-pointed below
	c.Dims = append([]string(nil), m.Dims...)
	if m.Aggs != nil {
		c.Aggs = make([]Aggregate, len(m.Aggs))
		for i := range m.Aggs {
			a := m.Aggs[i] // value copy of every scalar Aggregate field
			if m.Aggs[i].CondCols != nil {
				a.CondCols = append([]string(nil), m.Aggs[i].CondCols...)
			}
			c.Aggs[i] = a
		}
	}
	if m.Days != nil {
		c.Days = make([]DayEntry, len(m.Days))
		for i := range m.Days {
			d := m.Days[i] // value copy of every scalar DayEntry field
			if m.Days[i].Covers != nil {
				d.Covers = append([]string(nil), m.Days[i].Covers...)
			}
			c.Days[i] = d
		}
	}
	// Superseded is builder-only state (the read path never reads it), but clone the
	// slice so a sweep clearing the original's list can never reach a published copy.
	if m.Superseded != nil {
		c.Superseded = append([]SupersededFile(nil), m.Superseded...)
	}
	return &c
}

// BuiltDays returns the set of UTC dates (YYYY-MM-DD) already materialized,
// expanding compacted month files via their Covers list so the build loop does
// not re-build (and thus duplicate) days that were folded into a monthly file.
func (m *Manifest) BuiltDays() map[string]bool {
	s := make(map[string]bool, len(m.Days))
	for _, d := range m.Days {
		if len(d.Covers) > 0 {
			for _, c := range d.Covers {
				s[c] = true
			}
			continue
		}
		s[d.Date] = true
	}
	return s
}

// CompactedDays returns the UTC dates (YYYY-MM-DD) folded into a COMPACTED month
// file — i.e. listed in some entry's Covers. These days are FINAL and must never be
// re-materialized as a loose daily file: a loose entry keyed "YYYY-MM-DD" cannot
// replace the month entry keyed "YYYY-MM" (supersedeUpsert matches on date), so it
// is appended ALONGSIDE the month, and DaysInRange then selects BOTH files for that
// day — the read path sums the two and double-counts. The compacted month is the
// sole authority for its days. (This is the invariant whose violation, when a
// widened rebuild_days floor pulled compacted days back into the rebuild window,
// caused the May-2026 cube duplication.)
func (m *Manifest) CompactedDays() map[string]bool {
	s := make(map[string]bool)
	for _, d := range m.Days {
		for _, c := range d.Covers {
			s[c] = true
		}
	}
	return s
}

// Spec reconstructs the CubeSpec from a manifest, so the router can be rebuilt
// from object storage alone (no separate registry).
func (m *Manifest) Spec() CubeSpec {
	return CubeSpec{Source: m.Source, Grain: m.Grain, Dims: m.Dims, Aggs: m.Aggs}
}

// DayEntry describes one cube Parquet file. Normally one UTC day; after
// compaction a single file holds a whole month, with Covers listing the daily
// dates it absorbed (so the build loop still treats them as materialized).
type DayEntry struct {
	Date       string   `json:"date"` // YYYY-MM-DD (daily) or YYYY-MM (compacted month)
	URI        string   `json:"uri"`  // s3://... or local path
	SchemaHash string   `json:"schema_hash"`
	BucketLo   string   `json:"bucket_lo"` // min bucket in the file (inclusive)
	BucketHi   string   `json:"bucket_hi"` // max bucket + grain (exclusive)
	Rows       int64    `json:"rows"`
	Covers     []string `json:"covers,omitempty"` // daily dates merged into a compacted file; empty => single-day
}

// Bytes serializes the manifest for storage (one small object per cube).
func (m *Manifest) Bytes() ([]byte, error) { return json.MarshalIndent(m, "", "  ") }

func ParseManifest(b []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// Remove deletes the entry whose Date == date; no-op when absent. Used to retire
// a coverage-only '-empty' marker once a real build lands for the same period.
func (m *Manifest) Remove(date string) {
	for i := range m.Days {
		if m.Days[i].Date == date {
			m.Days = append(m.Days[:i], m.Days[i+1:]...)
			return
		}
	}
}

// Upsert adds or replaces the entry for a day (idempotent rebuilds), keeping Days
// sorted by date.
func (m *Manifest) Upsert(e DayEntry) {
	for i := range m.Days {
		if m.Days[i].Date == e.Date {
			m.Days[i] = e
			return
		}
	}
	m.Days = append(m.Days, e)
	sort.Slice(m.Days, func(i, j int) bool { return m.Days[i].Date < m.Days[j].Date })
}

// supersedeUpsert is Upsert for a rebuilt day whose file name carries a fresh
// nonce: if it replaces an existing entry whose URI DIFFERS (the common rebuild
// case, including a legacy fixed-name file), the old URI is parked on the
// Superseded list for deferred deletion (sweepSuperseded) instead of being deleted
// in place — the read path may still be handing it out. `at` is the wall time the
// supersession happened (the grace clock). A replaced entry with NO URI (an
// '-empty' coverage marker) or an unchanged URI parks nothing.
func (m *Manifest) supersedeUpsert(e DayEntry, at time.Time) {
	for i := range m.Days {
		if m.Days[i].Date == e.Date {
			if old := m.Days[i].URI; old != "" && old != e.URI {
				m.Superseded = append(m.Superseded, SupersededFile{URI: old, At: at.UnixNano()})
			}
			m.Days[i] = e
			return
		}
	}
	m.Days = append(m.Days, e)
	sort.Slice(m.Days, func(i, j int) bool { return m.Days[i].Date < m.Days[j].Date })
}

// DaysInRange returns the day files whose stored bucket span overlaps [lo,hi).
// This is the whole pruning decision — made from the manifest alone, in memory.
func (m *Manifest) DaysInRange(lo, hi string) []DayEntry {
	loT, ok1 := parseTS(lo)
	hiT, ok2 := parseTS(hi)
	if !ok1 || !ok2 {
		return m.Days
	}
	var out []DayEntry
	for _, d := range m.Days {
		bl, _ := parseTS(d.BucketLo)
		bh, _ := parseTS(d.BucketHi)
		// overlap iff bl < hi AND bh > lo
		if bl.Before(hiT) && bh.After(loT) {
			out = append(out, d)
		}
	}
	return out
}

// ReadExpr renders the read_parquet(...) list argument for a set of day entries.
// Returns "" when empty so the caller can fall through to source.
func ReadExpr(days []DayEntry) string {
	if len(days) == 0 {
		return ""
	}
	parts := make([]string, len(days))
	for i, d := range days {
		parts[i] = "'" + d.URI + "'"
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// Coverage reports the earliest and latest bucket the manifest covers, used by
// the router to compute the merge boundary / detect gaps. Coverage-only '-empty'
// markers carry no bucket span and are skipped — a marker at either end of the
// Days list must not zero out the coverage bounds.
func (m *Manifest) Coverage() (lo, hi time.Time, ok bool) {
	for _, d := range m.Days {
		l, ok1 := parseTS(d.BucketLo)
		h, ok2 := parseTS(d.BucketHi)
		if !ok1 || !ok2 {
			continue
		}
		if !ok || l.Before(lo) {
			lo = l
		}
		if !ok || h.After(hi) {
			hi = h
		}
		ok = true
	}
	return lo, hi, ok
}

// coveredDays returns the set of UTC dates (YYYY-MM-DD) that this manifest's
// files physically cover. A daily/range entry covers every UTC day its
// [BucketLo,BucketHi) span touches; a compacted month additionally covers every
// date in its Covers list (which records the daily dates it absorbed, including
// ones that produced no rows so the day phase doesn't rebuild them). This is the
// authoritative "which days does the cube actually contain data for" set, used by
// HasInteriorGap to detect a day that is missing from the middle of the cube's
// coverage (e.g. a purged or lifecycle-expired file) — a silent-undercount risk.
func (m *Manifest) coveredDays() map[string]bool {
	days := map[string]bool{}
	for _, d := range m.Days {
		if len(d.Covers) > 0 {
			// A compacted file's Covers list is the AUTHORITATIVE day set: the file's
			// [BucketLo,BucketHi) span is contiguous across the whole month even when an
			// interior day was dropped, so trusting the span would hide exactly the gap we
			// must catch. Use ONLY the explicit Covers dates for compacted entries.
			for _, c := range d.Covers {
				days[c] = true
			}
			continue
		}
		lo, ok1 := parseTS(d.BucketLo)
		hi, ok2 := parseTS(d.BucketHi)
		if !ok1 || !ok2 || !hi.After(lo) {
			continue
		}
		// A loose single-file entry (no Covers): every UTC date its [lo,hi) bucket span
		// touches is covered. hi is exclusive, so step from the lo date up to (but
		// excluding) the date hi lands on.
		for t := dayFloorUTC(lo); t.Before(hi); t = t.AddDate(0, 0, 1) {
			days[t.Format("2006-01-02")] = true
		}
	}
	return days
}

// HasInteriorGap reports whether the cube has a day with NO coverage that lies in
// the interior of its overall coverage AND within the queried [lo,hi) range — the
// silent-undercount condition. The read path must fall such a query to source: the
// selected files would otherwise be re-aggregated as if the missing day's rows
// never existed, returning fewer rows than source with no error raised.
//
// Only INTERIOR days count: a day outside [covLo,covHi] is the fresh tail / leading
// edge (handled by manifestCoversStart + the merge boundary), not a hole. A day on
// the boundary contributes via covLo/covHi. We scan each full UTC day strictly
// inside the cube's coverage and inside the query window; the first one absent from
// coveredDays() is a gap.
//
// NOTE: a genuinely-empty source day (no rows ever written) also has no cube file,
// so it reads as a gap here and the query conservatively falls to source. That is
// always CORRECT (an empty day contributes nothing, so source returns the same
// rows) — at worst one such query is slower. The build side (compaction) backfills
// real holes from source before compacting, so this net rarely fires in practice;
// it is the defense-in-depth catch for holes from ANY cause (purge, lifecycle
// expiry, partial writes), independent of how they arose.
func (m *Manifest) HasInteriorGap(lo, hi string) bool {
	covLo, covHi, ok := m.Coverage()
	if !ok {
		return false
	}
	loT, ok1 := parseTS(lo)
	hiT, ok2 := parseTS(hi)
	if !ok1 || !ok2 {
		return false
	}
	// Intersect the query window with the cube's interior coverage. Days before
	// covLo are a leading gap (manifestCoversStart handles that); days at/after covHi
	// are the fresh tail (merge-on-read patches them from source).
	from := loT
	if covLo.After(from) {
		from = covLo
	}
	to := hiT
	if covHi.Before(to) {
		to = covHi
	}
	if !to.After(from) {
		return false
	}
	covered := m.coveredDays()
	// Check each full UTC day the [from,to) span touches. A day is "interior" iff its
	// whole span lies within [covLo,covHi); a partial boundary day is covered by the
	// covLo/covHi entries themselves, so we only flag a day with zero coverage.
	for t := dayFloorUTC(from); t.Before(to); t = t.AddDate(0, 0, 1) {
		date := t.Format("2006-01-02")
		if covered[date] {
			continue
		}
		// Uncovered day. Only a fully-interior day is a true hole: its entire
		// [dayStart,dayEnd) must sit inside the cube's coverage span. (A day that
		// only partially overlaps the coverage edge is the boundary, not a gap.)
		dayStart := t
		dayEnd := t.AddDate(0, 0, 1)
		if !dayStart.Before(covLo) && !dayEnd.After(covHi) {
			return true
		}
	}
	return false
}

// dayFloorUTC truncates a timestamp down to the start of its UTC calendar day.
func dayFloorUTC(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
