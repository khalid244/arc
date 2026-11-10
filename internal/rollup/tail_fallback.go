package rollup

import "time"

// TailPlan is the cube-only answer of an auto-mode merge, for a request that
// accepts one when the merge's tail fails (X-Arc-Tail-Fallback: cube-only):
// the tail is the source branch past the cube's coverage, today's raw files,
// and the part that times out or fails when object storage is slow.
type TailPlan struct {
	// CubeOnlySQL is the query without its tail branch: the cube buckets and
	// any head patch, exactly as the merge reads them.
	CubeOnlySQL string
	// Lo, Hi bound the window only the tail reads, which a CubeOnlySQL answer
	// is missing.
	Lo, Hi time.Time
}

// withTail attaches a filled tail plan to d. A declined decision names its
// cube only with a plan (the cube CubeOnlySQL reads).
func withTail(d Decision, tail *TailPlan) Decision {
	if tail != nil && tail.CubeOnlySQL != "" {
		d.Tail = tail
	} else if !d.Served {
		d.Cube = ""
	}
	return d
}

// RouteTail is Route for a request that accepts a cube-only answer when the
// tail fails. Decision.Tail is set when the served query is a merge with a
// tail branch, and when a merge was declined only because its tail could not
// be planned (the tail's existence listing failed, or the compaction
// manifests could not be read and the cube-only answer reads no source): the
// caller may then answer Tail.CubeOnlySQL, labelled as missing [Lo, Hi).
// Nil when the query has no tail (fully sealed) or the cube-only answer
// itself needs a source read that could not be planned.
func (r *Router) RouteTail(sql, headerDB string) Decision {
	return r.routeTail(sql, headerDB, true, false, true)
}

// RouteTailHTTP adapts RouteTail to the api's optional tail-fallback router
// interface (matched structurally, like RouteHTTP). rewritten, served and cube
// are RouteHTTP's; cubeOnly, tailLo and tailHi are Decision.Tail's ("" and
// zero when unset). When served is false and cubeOnly is set, cube names the
// cube cubeOnly reads.
func (r *Router) RouteTailHTTP(sql, headerDB string) (rewritten string, served bool, cube, cubeOnly string, tailLo, tailHi time.Time) {
	d := r.RouteTail(sql, headerDB)
	if d.Tail != nil {
		cubeOnly, tailLo, tailHi = d.Tail.CubeOnlySQL, d.Tail.Lo, d.Tail.Hi
	}
	return d.SQL, d.Served, d.Cube, cubeOnly, tailLo, tailHi
}

// RouteTailHTTP forwards the tail-fallback decision to the live router.
func (m *Manager) RouteTailHTTP(sql, headerDB string) (rewritten string, served bool, cube, cubeOnly string, tailLo, tailHi time.Time) {
	m.mu.RLock()
	r := m.router
	m.mu.RUnlock()
	if r == nil {
		return "", false, "", "", time.Time{}, time.Time{}
	}
	return r.RouteTailHTTP(sql, headerDB)
}
