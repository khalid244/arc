# Rollup — Operator Guide

Rollup gives Arc transparent, **zero-configuration** rollups. It chooses what to
pre-aggregate with a *hybrid* policy — a cube for every low-cardinality dimension is
built eagerly from the schema (no cold-start blind spot), while high-cardinality
dimensions are added only when queries actually group by them — then rewrites
matching queries onto the small pre-aggregated cubes, falling through to source
whenever it can't help.

Design rationale and the post-mortem of four prior rollup generations:
`docs/superpowers/specs/2026-05-30-autocube-rollup-design.md`.

## What you configure (`[rollup]` in arc.toml)

Cube *definitions* are derived from each table's schema at runtime — you never
write column names. The section only governs whether/where/how often to build:

```toml
[rollup]
enabled = true                 # master switch (default false)
# databases = ["default"]      # allow-list; empty = every database discovered
# exclude_measurements = []    # measurements to skip
# grace_seconds = 21600        # 6h: seal delay before a day is built
# forward_tick_seconds = 300   # 5m: build cadence
# rebuild_days = 2             # re-roll the last N days each pass for late data
# max_dim_cardinality = 1024   # shared-dim cap
# max_per_dim_cardinality = 50000  # a dim at/under this gets its own per-dim cube
# max_dims = 16                # max per-dim cubes per table
# memory_limit = "2GB"         # DuckDB memory_limit per build subprocess
# storage_prefix = "_arc/rollup"
```

Requires the **s3** storage backend. Also set a bounded query memory so wide
source scans don't OOM a small node: `[database] memory_limit = "4GB"`.

There is **no per-table cube list, no per-panel registration, no sketch tuning** —
the cube set is auto-derived per table.

## How it runs (production)

A background **Manager** (started from `main.go`, stopped on shutdown):

1. **Discovers** every `db.measurement` and its available UTC days by globbing the
   partition tree (one S3 LIST per database — no full scan).
2. **Auto-classifies** each table once: `DESCRIBE` + `approx_count_distinct` per
   column → low/medium-card columns become dimensions (one per-dim cube each),
   high-card id-like strings become HLL sketches, numeric columns become metrics
   (sum/min/max/avg + KLL p95). A coarse (no-dim) cube carries the sketches.
3. **Builds day-by-day**, newest sealed day first, **each day in its own
   subprocess** (`arc rollup-buildday`). One UTC day per DuckDB `COPY` bounds
   memory; the subprocess isolates the DuckDB *datasketches* extension, whose
   large HLL/KLL aggregations can SIGSEGV on Linux — a crash kills only that
   child, never the query server. The manifest is persisted after every day, so a
   restart resumes.
4. **Serves** matching queries from cubes (copy-on-write Router) and re-rolls the
   last `rebuild_days` days each tick to absorb late data.

## How it works

1. **Observe.** Each aggregate query that hits a source has its *shape* extracted
   (source, time grain, group-by dims, aggregates) and recorded. Filters are
   captured but are **not** part of a cube's identity.
2. **Plan (hybrid: eager-cheap + lazy-expensive).** Each tick the planner recomputes
   the cube set: a *coarse* cube (also home to the percentile/distinct sketches, which
   lose accuracy when merged across many tiny cells) plus a per-dim cube for every
   *low-cardinality* dimension (≤ `max_dim_cardinality`, default 1024) — built eagerly
   from the schema so common group-bys are covered from day one — and a per-dim cube for
   a *high-cardinality* dimension only once a query has grouped by it (workload-gated,
   capped at `max_dims`). With `dim_rich`, one wide exact cube over all eligible dims is
   also built for multi-dimension queries. Tables with no data are never built.
3. **Build.** Each cube materializes one Parquet file per UTC day (hourly buckets
   inside) holding mergeable counters: `_cnt`, `_sum/_min/_max/_cnt` per metric,
   HLL sketches for `COUNT(DISTINCT)`, KLL sketches for percentiles. A per-cube
   `manifest.json` indexes the day files.
4. **Match & rewrite.** An incoming query is matched by **dimensional coverage**:
   the narrowest cube whose stored dims ⊇ the query's required dims, whose grain
   nests inside the query's grain, and whose stored columns can derive every
   aggregate. Extra predicates are applied post-aggregation on the (tiny) cube
   output. Anything not covered falls through to source — always correct, just
   slower.
5. **Stay fresh.** The sealed range is served from the cube; the `[watermark,now)`
   tail and any partial leading bucket are patched from source and merged with
   `UNION ALL BY NAME`. Late data is healed by a rolling rebuild scan.

## S3 latency

Arc's hot path is object-store-bound, so Rollup is designed around round-trips:

- the per-cube **manifest is one GET** that resolves any time range to a file list
  — no S3 `LIST`, no per-file metadata reads on the query path;
- **daily** cube files (hourly buckets inside) mean ~1/24 the object count;
- the fresh-tail source read is skipped entirely when the post-watermark window
  is empty;
- cube files are tiny (a month of `downloads` is 60M source rows → ~2.2k cube
  rows), so GETs are fast and few.

Measured on the 6-month local corpus (December, reading both cube and source from
MinIO): a daily-count-and-average-by-status query over the month ran in **39 ms
from the cube vs 6.42 s from source — a 165× speedup** with byte-exact results.

## Correctness

Every query shape is validated against a full source scan (`internal/rollup/
*_test.go`, plus a live Grafana-style sweep against the 6-month MinIO corpus).
Live results over the 22-day `downloads` corpus (`2026-05-01 … 2026-05-23`,
≈48M rows; rollup vs `X-Arc-No-Rollup` source):

- **exact** (`count/sum/min/max/avg`, group-by, multi-dim, filters): row-for-row
  identical (sum/avg of floats to ~1e-8 relative — double summation order);
- **conditional aggregates** — `SUM(CASE WHEN <pred over dims> THEN x ELSE y END)`
  and `COUNT(CASE WHEN <pred> THEN … END)` (success/error/conversion rates): exact.
  The predicate must reference stored dimensions; each cube row aggregates rows
  with identical dim values, so it takes one CASE branch and re-sums exactly;
- **TopN** — `ORDER BY <agg> LIMIT n`: exact. The user ORDER BY is reproduced by
  select-list position in the cube read; a LIMIT whose ordering can't be
  reproduced falls through to source rather than return wrong rows;
- **CTE base-rewrite** — `WITH base AS (<servable aggregate>) <outer CASE/TopN/
  ratio>`: the base is served from a cube, the lightweight outer SQL runs in
  DuckDB. This is the supported form for success-rate ratio panels;
- **sketches** — `COUNT(DISTINCT)` via HLL within ~2% (99.6% of buckets); KLL
  p50/p95 within ~3% (100% of buckets). **KLL p99 and other extreme-tail
  percentiles drift more (≈4–5% mean over long ranges)** — the inherent floor of
  rank sketches; use the rollup switch to fall back to source when an exact tail
  percentile is required, or raise `kllK` (rebuilds sketch cubes, ~×k storage);
- merge-on-read across the watermark (sealed + fresh tail + partial-bucket head
  patch) matches source, including for sketch aggregates.

> Time bucketing: Grafana's `$__timeGroup` expands to epoch-floor arithmetic
> (`to_timestamp((epoch_ns(t)//1e9//N)*N)`), which floors deterministically and
> matches cubes exactly. A *hand-written* `date_trunc('hour', t)` on a TIMESTAMPTZ
> column can disagree with itself by a few boundary rows per hour (a DuckDB
> vectorized-`date_trunc` rounding quirk, present in source too); real Grafana
> queries do not hit it.

## Coverage

The model serves the common dashboard shapes — time series, group-by (incl.
multi-dimension via the dim-rich cube), TopN, success/error-rate conditional
aggregates, CTE ratio panels, and `OR`/inequality/`IS NULL` filters (applied
post-aggregation over stored dims). Live sweep: **12/12 shapes served and within
tolerance, 0 fall-throughs**, at 5–40× the source latency. A query falls through
to source (no `X-Arc-Rollup-Cube` header) only when no cube covers its dims or it
uses an unsupported construct — never returning a wrong answer.

## Local testing recipe

Verified end-to-end against the 6-month MinIO corpus (`arc-minio` on `:9000`).

1. **Build cubes into object storage** (one-time; the production scheduler will
   do this on a tick):
   ```sh
   go build -o /tmp/rollup-build ./cmd/rollup-build
   ARC_S3_ENDPOINT=localhost:9000 ARC_S3_KEY=arcadmin ARC_S3_SECRET=arcpassword \
   ARC_S3_BUCKET=arc-test /tmp/rollup-build -from 2025-12-01 -to 2026-01-01 \
     -only downloads__coarse,downloads__by_status
   ```
   Each cube is laid out by database and table:
   `s3://arc-test/_arc/rollup/<database>/<table>/<cube-kind>/<date>.parquet` plus a
   `manifest.json` per cube (e.g. `…/default/downloads/by_status/2026-05-10.parquet`,
   `…/default/downloads/coarse/manifest.json`). Cube-kinds are `coarse` (no dims),
   `by_<dim>` (per-dim), and the dim-rich `by_<all-dims>` cube. (Drop `-from/-to`
   to build the full span; drop `-only` for all cubes. Day-pruned source globs keep
   each build to seconds.)

2. **Run arc with Rollup enabled.** Cubes are built in **UTC**, so run arc with
   `TZ=UTC` for cube/source day-boundary consistency, and provide S3 creds:
   ```sh
   # arc.toml: [rollup] enabled = true   (and [storage] backend="s3" endpoint=…)
   ARC_STORAGE_S3_ACCESS_KEY=arcadmin ARC_STORAGE_S3_SECRET_KEY=arcpassword ./arc
   ```
   Startup logs `Rollup enabled` then `Rollup manager started`. The query and build
   DuckDB connections both pin `TimeZone='UTC'` so cube/source day boundaries align
   without an external `TZ`. arc's DuckDB loads the `datasketches` community
   extension automatically so HLL/KLL cube queries resolve.

3. **Query and observe.** A served query carries an `X-Arc-Rollup-Cube` response
   header; a miss has none (it ran against source). Measured live:
   - `count(*)` for December: **59,997,205** (cube) == source, 87 ms.
   - daily count by status: row-for-row identical to source.
   - `count(DISTINCT device_id)`: 721,200 vs source 715,217 (0.84%).
   - `quantile_cont(duration_seconds,0.95)`: 10.195 vs source 10.199 (0.04%).
   - a `GROUP BY region` (no region cube) correctly falls through to source.

## Production integration status

Implemented and TDD-verified against the 6-month corpus:

- **Read path, end to end** — `Parse` (raw SQL → `QueryShape` via DuckDB
  `json_serialize_sql`), coverage matcher, manifest range-pruning, merge-on-read
  emit, and the `Router` that ties them together. Proven by `TestRouter_EndToEnd`:
  a raw SQL string in, a rewritten cube query out, byte-identical to the original
  run against source — with correct fallthrough for uncovered/unsupported SQL.
- **Write path** — mergeable rewriter, cube builder, per-day manifest, workload
  tracker, and the zero-config planner.
- **Query seam** — `internal/api/query.go` `executeQuery` calls an optional
  `RollupRouter` (set via `SetRollupRouter`) just before the normal storage-path
  conversion; a hit uses the rewritten SQL and sets `X-Arc-Rollup-Cube`, a miss
  falls through unchanged. The whole arc binary builds with the hook in place and
  it is nil-safe (disabled until wired).

The operational glue is also done: `cmd/arc/main.go` constructs the `Manager`
from `[rollup]` config, runs the background tick loop (discover → classify →
build day-by-day → compact), persists cube Parquet + manifests + the learned
workload to object storage, and registers the router on the query seam.

## Production deployment

**Go-live checklist (Hetzner dedicated + Hetzner Object Storage):**

1. **Config** — in `arc.toml`:
   ```toml
   [storage]
   backend = "s3"; s3_endpoint = "<region>.your-objectstorage.com"; s3_bucket = "<bucket>"
   s3_use_ssl = true; s3_path_style = true
   [database]
   memory_limit = "<~60% of RAM>"; thread_count = <8–16>   # threads ≥ cores: S3 reads are I/O-bound
   [rollup]
   enabled = true
   databases = ["default"]          # allow-list; omit to discover all
   exclude_measurements = ["..."]   # high-churn / raw tables to skip
   grace_seconds = 21600            # 6h seal delay before a day is built
   forward_tick_seconds = 300       # build cadence
   rebuild_days = 2                 # re-roll last N days for late data
   dim_rich = true                  # multi-dimension coverage
   # compact_min_days, dim_rich_max_dims, max_dims have safe defaults
   ```
   Put the object-store keys in `ARC_STORAGE_S3_ACCESS_KEY`/`_SECRET_KEY` (env, not
   the file). `thread_count` is the one tuning that matters: object-store GETs are
   I/O-wait-bound, so set it well above core count (8–16) even on a small box.

2. **First-run backfill is a one-time cost.** On first start the Manager builds
   every cube for all sealed history, day by day (memory-bounded — one day's COPY
   at a time, sketch builds isolated in a subprocess so a `datasketches` crash
   can't take down arc). Over months of data this takes minutes-to-an-hour
   depending on cores and object-store latency; queries serve from source until
   their range is covered, then transparently switch. Compaction folds sealed
   daily files into monthly ones afterward (cuts long-range read round-trips ~30×).

3. **Verify** before pointing dashboards at it: pick representative panels, run
   each with the **Use rollups** toggle on vs off — results must match (exact for
   counts/sums/group-bys; ≤2% distinct, ≤3% p50/p95). The query editor's pre-run
   badge tells you which panels will roll up at their current range.

## Operating it

**It degrades safely by construction.** Every uncertain case — unparseable SQL,
no covering cube, a coverage gap, a build failure — returns *not served*, and the
query runs against source unchanged. A wrong rollup answer is never returned; the
worst failure mode is "no acceleration," not "wrong data."

**Signals to watch (structured `component=rollup` logs):**
- `Rollup manager started` / `Rollup profiled table` — healthy startup.
- `Rollup compacted month` — compaction progressing.
- `Rollup ... build failed` / `sketch batch ended early` — a day failed; it retries
  next tick and partial progress is kept (not fatal).
- `Rollup dim-rich cube SKIPPED (too high-dimensional)` — a table exceeds
  `dim_rich_max_dims`; its multi-dimension queries fall through to source until you
  raise the cap.
- Per-query: the `X-Arc-Rollup-Cube` response header (present ⇒ served from that
  cube). The Grafana badge surfaces both this (post-run) and the pre-run prediction.

**Resource bounds:** DuckDB `memory_limit` + an on-disk spill dir cap query memory;
each build subprocess gets `rollup.memory_limit` (default 2 GB) and processes one
day at a time. Cube storage is tiny — measured **2.3% of source** (43× compression)
on the 6-month corpus.

## Known limitations (honest list)

- **Time-series rolls up only for ranges that pick an ≥1h bucket.** The Grafana
  plugin's `$__interval` is hourly above 7 days; shorter ranges request 10m/1m/10s
  buckets the hour-grain cubes can't serve, so they hit source (which is cheap at
  short ranges anyway). Finer-grain cubes would close this at a storage cost.
- **KLL tail percentiles (p99) drift ~4–5%** over long ranges — inherent to rank
  sketches; p50/p95 are within 3%. Raise `kllK` (rebuilds sketch cubes) or use the
  rollup toggle to fall back to source for exact tails.
- **Theta set algebra** (intersect/difference across cubes) is supported at the
  sketch layer but not yet exposed as query syntax, and needs theta sketches in
  dimensioned cubes to be useful for `A but not B` panels.
- **TopN-by-distinct** (`ORDER BY COUNT(DISTINCT …)`) is not yet rollup-able — a
  Top-K/frequent-items sketch would cover it.
- **Unrelated arc-core risk:** a query returning ~1M+ rows can fatal arc's Arrow
  result encoder and invalidate the DuckDB connection until restart (reproduced in
  `internal/api/query_arrow*.go`; standalone DuckDB runs the same SQL fine). The
  rollup path avoids it (small results); the exposure is on large *source* scans.
  Worth a row-cap / encoder fix in arc core before heavy production use.
