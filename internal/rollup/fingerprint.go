package rollup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Source fingerprints.
//
// The pass used to re-roll EVERY day inside the rebuild window on every pass, to
// absorb late events. With rebuild_days=30 and a ~15h pass that rebuilt each
// posthog.events day ~48 times over its life — every time re-reading the whole
// day (~2.3 GB) and rewriting ~180 cube files, often for nothing.
//
// Ingest, the late-event reorganizer and compaction only ADD new uniquely named
// source files and DELETE old ones; the delete API rewrites a file in place,
// which changes its size and/or mtime (a same-size rewrite within the same second
// would be missed — the listing carries no ETag). So the set of objects under a
// day's partition (path, size, mtime) changes whenever the day's data may have
// changed. Each daily cube entry records the fingerprint of the files its build
// read; a later pass lists the day (one LIST, no data read) and rebuilds the day
// only if the fingerprint differs. Compaction rewriting files costs one rebuild
// with identical results — the safe direction.
//
// The build reads EXACTLY the fingerprinted file list (not a fresh glob), so the
// recorded fingerprint always describes the data in the cube: a file landing or
// vanishing after the listing changes the next pass's fingerprint and triggers a
// rebuild. A day that lists cleanly with no Parquet files is EMPTY: a cube that
// never built it gets an empty marker (nothing to retry); a cube that did keeps
// its file — rollups outlive raw retention. Whenever the
// fingerprint is unavailable (backend without object listing, a listing error, a
// file name DuckDB would treat as a glob, an entry from before fingerprints) the
// day is read through the day glob and rebuilt as before: a missing fingerprint
// never causes a skip.

// cubeBuildVersion is folded into every fingerprint. Bump it whenever the cube
// build itself changes in a way the stored cubes must pick up (aggregation SQL,
// null-fill, bucketing): every day inside the rebuild window is then rebuilt once
// by the next pass, as the old every-pass re-roll used to do implicitly. The
// DuckDB library version is folded in too, for the same reason.
const cubeBuildVersion = 1

// objectLister is the optional backend capability fingerprints need (S3, Azure
// and local backends implement it).
type objectLister interface {
	ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error)
}

// daySource is how a day build reads its source.
type daySource struct {
	sig   string // fingerprint of the listed files; "" when !ok
	expr  string // read_parquet source expression (exact list, or the day glob when !ok); "" when empty
	ok    bool   // listed cleanly: sig describes exactly what expr reads
	empty bool   // listed cleanly and found no Parquet files: nothing to read
}

// fingerprintHeader is what every fingerprint starts with: anything that changes
// how the same source files become a cube.
func (m *Manager) fingerprintHeader() string {
	return fmt.Sprintf("build=%d duckdb=%s time=%s\n", cubeBuildVersion, m.duckVersion, m.cfg.TimeCol)
}

// listDaySource lists every .parquet object under the day's partition — the set
// sourceDayGlob matches — drops compaction inputs whose output is in place
// (dropCompactedInputs: the compaction manifests are read before and after the
// listing), and fingerprints the rest (fingerprintHeader, then sorted
// path|size|mtime, hashed; prefixed unsettledSig when a compaction touched it).
func (m *Manager) listDaySource(ctx context.Context, source string, day time.Time) daySource {
	fallback := daySource{expr: m.sourceDayGlob(source, day)}
	l, can := m.stg.(objectLister)
	if !can {
		return fallback
	}
	db, meas := splitSource(source)
	prefix := fmt.Sprintf("%s/%s/%s/", db, meas, day.Format("2006/01/02"))
	before := m.readCompactionManifests(ctx)
	objs, err := l.ListObjects(ctx, prefix)
	if err != nil {
		m.log.Warn().Str("source", source).Str("day", day.Format("2006-01-02")).Err(err).
			Msg("Rollup source fingerprint listing failed; rebuilding the day from its glob")
		return fallback
	}
	var files []storage.ObjectInfo
	for _, o := range objs {
		p := strings.TrimPrefix(o.Path, "/")
		if !strings.HasPrefix(p, prefix) || !strings.HasSuffix(p, ".parquet") {
			continue // outside the day (defence in depth) or not a source file
		}
		if strings.ContainsAny(p, "*?[") {
			// DuckDB expands list elements as globs: such a name would be read
			// as a pattern. Fall back to the plain day glob for this day.
			return fallback
		}
		o.Path = p
		files = append(files, o)
	}
	if len(files) == 0 && m.cfg.TieredStorage {
		// With tiering, a day's files may have moved to the cold tier: an empty
		// hot listing does not mean the day has no data.
		return fallback
	}
	files, unsettled := dropCompactedInputs(files, before, m.readCompactionManifests(ctx))
	mark := ""
	if unsettled {
		mark = unsettledSig
	}
	h := sha256.New()
	h.Write([]byte(m.fingerprintHeader()))
	if len(files) == 0 {
		sum := h.Sum(nil)
		return daySource{sig: mark + "0-" + hex.EncodeToString(sum[:16]), ok: true, empty: true}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	uris := make([]string, len(files))
	for i, f := range files {
		fmt.Fprintf(h, "%s|%d|%d\n", f.Path, f.Size, f.LastModified.Unix())
		uris[i] = "'" + esc(m.rootURI()+"/"+f.Path) + "'"
	}
	sum := h.Sum(nil)
	return daySource{
		sig:  fmt.Sprintf("%s%d-%s", mark, len(files), hex.EncodeToString(sum[:16])),
		expr: "[" + strings.Join(uris, ", ") + "]",
		ok:   true,
	}
}

// sourceUnchanged reports whether cube cb's existing daily entry for date was
// built from exactly the source files fingerprinted in ds, and its cube file is
// still on storage.
func sourceUnchanged(cb *cubeBuild, date string, ds daySource) bool {
	if !ds.ok || !cb.built[date] || cb.missing[date] {
		return false
	}
	recorded := cb.man.DailySourceSig(date)
	return recorded != "" && settledSig(recorded) == settledSig(ds.sig)
}

// skipReason is why a candidate cube-day is not built now.
type skipReason int

const (
	buildNow          skipReason = iota
	skipUnchanged                // source unchanged since its build (or gone: the cube is kept)
	skipBuiltThisTick            // built this tick, nothing proves it changed since
	skipDeferred                 // changed, but its cube is younger than its rebuild limit (rebuild_limit.go)
)

// skipDay decides, at build time, whether cube cb leaves date alone given the
// day's current listing.
func (m *Manager) skipDay(cb *cubeBuild, date string, ds daySource, mode needMode) skipReason {
	if sourceUnchanged(cb, date, ds) {
		return skipUnchanged
	}
	if ds.empty && cb.built[date] && !cb.missing[date] {
		// Every source file of a built day is gone (retention, a whole-day
		// delete): keep the cube. Rollups are meant to outlive raw retention, as
		// they always did; only a day never built gets an empty marker. (If its
		// cube file is gone too, the day is emptied: the entry would dangle.)
		return skipUnchanged
	}
	if m.tickBuilt[tickBuiltKey(cb.spec, date)] && (mode == needMissing || !ds.ok) {
		return skipBuiltThisTick
	}
	if mode == needPass && m.rebuildDeferred(cb, date) {
		return skipDeferred
	}
	return buildNow
}

// markMissingCubeFiles lists cb's cube directory once and flags every daily
// entry inside the rebuild window whose file is no longer on storage, so the pass
// rebuilds it instead of trusting an unchanged source fingerprint (the old
// every-pass re-roll healed such holes implicitly).
func (m *Manager) markMissingCubeFiles(ctx context.Context, cb *cubeBuild, rebuildFloor time.Time) {
	l, can := m.stg.(objectLister)
	if !can {
		return
	}
	objs, err := l.ListObjects(ctx, m.cfg.StoragePrefix+"/"+cubeDir(cb.spec)+"/")
	if err != nil {
		return // unknown: keep trusting the manifest this pass
	}
	present := make(map[string]bool, len(objs))
	for _, o := range objs {
		present[strings.TrimPrefix(o.Path, "/")] = true
	}
	floor := rebuildFloor.Format("2006-01-02")
	var missing []string
	for _, d := range cb.man.Days {
		if d.URI == "" || len(d.Covers) > 0 || len(d.Date) != len("2006-01-02") || d.Date < floor {
			continue // markers, months, and days outside the window are not re-checked
		}
		if !present[m.keyFromURI(d.URI)] {
			if cb.missing == nil {
				cb.missing = map[string]bool{}
			}
			cb.missing[d.Date] = true
			missing = append(missing, d.Date)
		}
	}
	if len(missing) > 0 {
		m.log.Warn().Str("cube", CubeID(cb.spec)).Strs("days", missing).
			Msg("Rollup cube files missing from storage; rebuilding those days")
	}
}
