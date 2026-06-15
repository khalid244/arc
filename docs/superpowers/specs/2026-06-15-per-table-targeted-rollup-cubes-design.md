# Per-table targeted rollup cubes — design

- **Date:** 2026-06-15
- **Status:** Approved (brainstorm) → ready for implementation plan
- **Area:** `internal/rollup` (cube planner/builder) + `internal/config` (TOML loader)

## Problem

Rollup cubes are auto-derived per table: a `coarse` cube, one `by_<dim>` cube per
low-cardinality dimension, and — only when a table has `≤ dim_rich_max_dims` low-card
dims — a single "dim-rich" cube over *all* of them. A query rolls up only when one cube
stores **every** column it groups or filters by (`coverage.go: requiredDims`).

This leaves a gap for **multi-dimension queries on wide tables**. The motivating case is
the Hammel Survey Grafana dashboard, which queries `posthog.events` with shapes like
`WHERE event = 'survey sent' AND survey_name IN (…) GROUP BY survey_response` (and an
`os_name × app_version` breakdown). Serving these needs one cube holding
`{event, survey_name, survey_response, os_name, app_version}` together. But:

- `posthog.events` has **137 low-card dims**, far above `dim_rich_max_dims` (default 12),
  so its dim-rich cube is permanently skipped — no multi-dim cube exists.
- The `by_event` cube (event dim only) cannot serve a query that also filters/ groups by
  `survey_name` etc., so every survey-filtered panel falls back to a full source scan
  (works at ≤6h, times out at 30d).

### Why not just raise `dim_rich_max_dims`

It was tried in prod (12 → 24) and **OOM-killed the builder**: 24 caught
`crashlytics.ios` (exactly 24 low-card dims), whose high-volume 24-dim dim-rich build
ballooned memory to the 32 GiB pod limit and crash-looped. `dim_rich_max_dims` is a
**global** knob — it cannot target one table without catching others. Reverted to 12.

So we need a way to declare **one specific cube for one specific table**, with no global
side effects.

## Goal

Let an operator declare targeted cube(s) per table in config. The Hammel Survey dashboard's
11 aggregate panels then roll up at any time range, with a build cost bounded by a dim set
the operator chooses — structurally immune to the global-knob OOM.

### Non-goals

- No ingest routing, no `surveys` table, no historical data backfill, no dashboard SQL
  changes. The cube builds over existing `posthog.events` data; the dashboard stays
  `FROM events`.
- No change to `dim_rich_max_dims` or any global cardinality knob.
- Numeric measures (SUM/MIN/MAX/percentile of a metric column) are **not** in scope for the
  initial cube — `count` + `count_distinct` sketches cover every survey panel. A `metrics`
  field can be added later if a future cube needs it.

## Design

### Config schema

A new repeatable block under `[rollup]` in `arc.toml`:

```toml
[[rollup.cube]]
table    = "posthog.events"                                       # db-qualified source
dims     = ["event","survey_name","survey_response","os_name","app_version"]
distinct = ["distinct_id"]                                        # optional: Theta-sketch cols
```

- `table` (required): db-qualified source (`<db>.<measurement>`), matched against the
  builder's `source` identity.
- `dims` (required, ≥1): columns the cube stores as group-by/filter dimensions.
- `distinct` (optional): columns to add as `COUNT(DISTINCT col)` Theta sketches (for
  `Unique Users`-style panels).

TOML array-of-tables → **one block = one cube**. Multiple blocks are independent and may
target the same table with different dim sets or different tables. Build cost scales
linearly with the number of cubes; each cube's size is bounded by *its own* dims, so there
is no global blast radius.

### The Hammel Survey cube

One cube on `posthog.events`:

- `dims = [event, survey_name, survey_response, os_name, app_version]`
- aggs  = `count` + `count_distinct(distinct_id)`
- hourly grain (inherited), backfilled + daily-rebuilt + monthly-compacted like every cube.

Its dim set is a superset of every aggregate panel's needs:

| Panels | Needs | Served |
|---|---|---|
| Shown / Responses / Dismissed / Funnel / Response-Rate (2,3,4,9,5) | filter `{event, survey_name}` + count / conditional-count | ✓ |
| Avg Rating / Distribution / Avg-over-time (6,8,11) | + group `survey_response` | ✓ |
| Rating by App × OS (17) | + group `os_name, app_version` | ✓ |
| Unique Users (7) | `count_distinct(distinct_id)` filtered by `{event, survey_name}` | ✓ (sketch union) |
| Responses / Avg over time (10,11) | hourly grain at ≥1h bucket | ✓ |

**Stays on source (by design):** Panel 16 (Response Details) is a raw `SELECT … LIMIT 200`,
not an aggregate — rollup never applies. Unchanged.

### Code changes (`internal/rollup`, `internal/config`)

1. **`internal/rollup/config.go`** — add
   ```go
   type TargetedCube struct { Source string; Dims []string; Distinct []string }
   ```
   and `TargetedCubes []TargetedCube` on `Config`. No default (empty = none).

2. **`internal/config/config.go`** — parse the `rollup.cube` array (viper `UnmarshalKey`)
   into `[]rollup.TargetedCube` and attach to the `rollup.Config` built alongside the other
   `rollup.*` keys.

3. **`internal/rollup/classify.go`** — add
   ```go
   func (p TableProfile) targetedSpec(dims, distinct []string) (CubeSpec, bool)
   ```
   It validates every `dim`/`distinct` column is known to the profile; returns `ok=false`
   if any is unknown. Builds `CubeSpec{Source, Grain, Dims: sortedCopy(dims),
   Aggs: [count] + [AggCountDistinct(col) for each distinct]}`.

4. **`internal/rollup/manager.go` `planSpecs`** — after the coarse/per-dim/dim-rich block
   (before `recordPlan`), append a spec for each configured cube whose `Source == source`:
   ```go
   for _, tc := range m.cfg.TargetedCubes {
       if tc.Source != source { continue }
       if spec, ok := p.targetedSpec(tc.Dims, tc.Distinct); ok {
           cubes = append(cubes, spec)
       } else {
           m.warnTargetedCubeSkipped(source, tc)   // unknown column(s); once-per-source
       }
   }
   ```
   Appending before `recordPlan(source, cubes)` means the cube is tracked (not swept) and
   built by the normal build loop.

5. **`coverage.go`** — **no change.** `Covers()` already matches any cube whose dims ⊇ a
   query's `requiredDims` and whose aggs are derivable, so the router selects the targeted
   cube automatically. (For the survey panel shapes the targeted cube is the only cover; if
   future overlapping cubes make several cover one query, confirm the selector prefers the
   narrower/cheaper one — a selection detail to verify in implementation, not a correctness
   issue, since any cover returns the right answer.)

### Validation & failure modes

- **Unknown table / column** → the block is warned-and-skipped (logged once per source);
  the builder never crashes on a typo.
- **Empty `dims`** → skipped with a warning (a 0-dim cube is just the coarse cube).
- Duplicate/overlapping cubes are allowed; the router picks the cheapest cover per query.

### Safety analysis

- **No global knob touched** → `crashlytics.ios`, `posthog.events`' auto-derivation, and
  every other table are unaffected. The class of failure that caused the OOM is impossible
  here by construction.
- **Bounded size**: the survey cube's row count is bounded by
  `card(event) × card(survey_name) × card(survey_response) × card(os_name) × card(app_version)`
  per hour — low tens of thousands of rows worst case, orders of magnitude below the 24-dim
  `crashlytics` cube that OOM'd. The build reads `posthog.events` once (same as the existing
  `by_event` build); the 5-dim aggregation is cheap.

## Testing

- **Unit** (`internal/rollup/planner_unit_test.go`):
  - a configured `TargetedCube` is emitted by `planSpecs` for its table and not for others;
  - an unknown-column cube is skipped (no panic, warn path hit);
  - `Covers()` returns true for each survey panel's query shape against the targeted spec,
    including the `count_distinct(distinct_id)` panel.
- **Local-sim e2e** (the `arc-autocube` + minio harness used for the `dim_rich` test):
  ingest synthetic survey rows into `posthog.events`-shaped data, configure the
  `[[rollup.cube]]` block, build, and confirm the panel queries return `servedBy: rollup`
  (rollup-only mode → HTTP 200) — and that builder memory stays bounded.

## Rollout

1. Build `registry.gyoom.sa/arc:<sha>` (linux/amd64) from this branch.
2. Bump the rollup builder deployment to the new image.
3. Add the `[[rollup.cube]]` block (above) to
   `argocd-server-config/manifest/rollup-config.yaml`; ArgoCD syncs; restart the rollup pod
   so it reads the new config.
4. Let it backfill the survey cube across the events range; watch builder memory stays flat.
5. Verify each survey panel rolls up at 30d via the `rollup-only` probe
   (`X-Arc-Rollup-Only: true` → HTTP 200, `X-Arc-Rollup-Cube` header set).
6. Lift the dashboard's `now-6h` default cap (the panels now roll up at any range).

## Future (out of scope now)

- Optional `metrics = [...]` field for exact SUM/MIN/MAX/percentile aggregates.
- A second targeted cube for the `downloads` "by site" panel (also currently uncubable) —
  just another `[[rollup.cube]]` block, no code change.
