# Rollups

Rollups are pre-aggregated time bucket parquet files Arc maintains alongside source data, and a query rewriter that transparently redirects compatible aggregate queries from source scans onto them. A multi-tier pyramid (1h → 1d → 1w → 1mo) with mergeable HLL/KLL sketches delivers 1,000–5,000× speedup on long-horizon time-bucket queries with no app changes.

Disabled by default. Opt in per-deployment.

## Enabling

Minimum config:

```toml
[rollup]
enabled = true
tz      = "Asia/Riyadh"   # REQUIRED — bucket alignment timezone
builder = true             # exactly one node per cluster materializes
```

`builder = false` lets a node use rollups built elsewhere (reader-only). Exactly one node in a cluster should be the builder.

On first start with a non-empty source table, Arc samples the schema, infers per-column roles, and backfills tier buckets from the earliest source timestamp up to the grace window. Backfill runs at ~3–5 s per daily window on the local bench.

## Configuration reference

```toml
[rollup]
enabled = true             # default: false
tz      = "..."            # REQUIRED when enabled — bucket alignment timezone
builder = false            # this node materializes tier files

tiers = ["1h", "1d", "1w", "1mo"]   # default
grace_window = "6h"        # default. Buckets sealed only when bucket_end + grace ≤ now.
coverage_threshold = 0.99  # default. A dim is kept if N values cover ≥ this fraction of rows.
dim_rich_cap = 100         # default. Effective cardinality cap for dim-rich cross-product.
hll_lg_k = 14              # default. Raise to 16 (4× sketch size) for higher accuracy at long horizons.
kll_k    = 200             # default. KLL precision.
obsolete_grace = "168h"    # default 7d. How long to keep replaced variants for rollback.

[rollup.tables."default.events"]
time_column = "ts"           # default: auto-discover TIMESTAMPTZ column
force_keep   = ["region"]    # force-include in classifier kept-set regardless of cardinality
force_sketch = ["user_id"]   # force HLL on high-card cols
ignore_cols  = ["url_addr"]  # exclude from all variants
```

## Variants

Per `(table, tier)` the system maintains three storage variants:

| Variant | Shape | Used by router when |
|---|---|---|
| `sketch` | one row per bucket; counts + sums + min/max + HLL/KLL | query touches no dims |
| `by_<col>` | per-dim variant; one row per bucket × kept value + `_OTHER_` | query touches one dim |
| `all` (dim-rich) | one row per bucket × cross-product of kept low-card dims | query touches multiple dims, all in dim-rich cap |

The classifier decides each column's role on first start (`spec.json`):
- **Dim** (≤ `dim_rich_cap` effective cardinality): goes into dim-rich + own per-dim variant
- **PerDim** (between cap and `coverage_threshold`): per-dim variant only
- **Sketch**: HLL only, never grouped on
- **Drop**: not stored

## Storage layout

Hive-partitioned on the configured storage backend:

```
precalc/table=default.events/
  spec.json                              # classifier output (per-table)
  manifest.json                          # source of truth: file list + watermarks
  tier=1h/year=2026/month=05/day=15/
    sketch/<uuid>.parquet
    by_dim_a/<uuid>.parquet
    by_dim_b/<uuid>.parquet
    all/<uuid>.parquet
  tier=1d/year=2026/month=05/day=15/...
  tier=1w/year=2026/week=20/...
  tier=1mo/year=2026/month=05/...
```

Every Parquet file carries KV-metadata stamped at build time: `schema_hash`, `tier_tz`, `builder_version`, `bucket_lo`, `bucket_hi`. Readers verify `schema_hash` matches the current spec before merging — schema drift fails loudly instead of corrupting results.

## What gets rewritten

The router accepts:
- `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT)`, `percentile_cont`/`quantile_cont`
- Nested aggregates (`AVG(x) * 100`, `SUM(x) / COUNT(*)`) — decomposed to stored columns
- `quantile_cont(x, p)` without `GROUP BY` — uses sketch variant's KLL
- IN / NOT IN / IS NOT NULL filters
- Multiple aggregates in one query (multi-stat Grafana panels)
- Topk / HAVING / ORDER BY <agg> LIMIT N

What falls back to source (router refuses, query runs against raw):
- JOINs, window functions, subqueries in WHERE
- `CASE WHEN` inside an aggregate argument (e.g., `SUM(CASE WHEN x THEN 1 ELSE 0 END)`)
- Sub-hourly granularity (`date_trunc('minute', ...)`) — no 1m tier
- Per-row expressions (`SELECT FLOOR(x/10), COUNT(*) ... GROUP BY 1`)
- Filter values outside the dim's kept-set (e.g., niche site filters)
- Schema-less `FROM`
- Open-ended or missing time filter

## Late-arriving data

Events whose timestamp falls more than `grace_window` before `now` are **invisible to precalc** — they remain in source, raw queries see them correctly, precalc undercounts by that volume. Default grace window is 6 hours. For workloads where this matters, raise `grace_window` further (12h, 24h).

## Rollback

Set `rollup.enabled = false` and restart. The router goes dormant, queries fall back to source. On-disk tier files stay intact for re-enable.

## Known limitations

- **`CASE WHEN` inside aggregate arguments** — refuse, source fallback. The aggregate-argument expression isn't a column reference, so the router can't translate it.
- **Sub-hourly granularity** — no 1m or 5m tier; queries like `date_trunc('minute', ...)` fall back to source.
- **HLL accuracy at very long horizons** — at `lg_k=14`, 60-day distinct-count merges can exceed 5% error in the worst case. Raise `hll_lg_k = 16` for ~0.4% RSE at 4× sketch size.
- **First builds after spec change** — readers refuse files with stale `schema_hash` until the builder catches up. During the catch-up window, queries fall through to source.
- **Sketch-only scheduler** — automatic builds cover the `sketch` variant; per-dim and `all` variant publishing is currently driver-mediated (operator tool or future scheduler enhancement).

## Observability

The tiered subsystem exports Prometheus-format metrics via Arc's `/metrics` endpoint.

### Metrics

| Metric | Type | Meaning |
|---|---|---|
| `arc_tiered_rewrite_attempts_total` | counter | Total `tiered.Rewrite()` calls |
| `arc_tiered_rewrite_accepted_total` | counter | Calls where the router successfully rewrote |
| `arc_tiered_rewrite_refused_parser_total` | counter | Refused at parser stage (no time filter, JOIN, untranslatable agg, …) |
| `arc_tiered_rewrite_refused_variant_total` | counter | Refused at PickVariant (filter value not in kept-set, etc.) |
| `arc_tiered_rewrite_refused_tier_total` | counter | Refused at PickTier (watermark not caught up) |
| `arc_tiered_rewrite_refused_emit_total` | counter | Refused at emit (no files match current schema_hash) |
| `arc_tiered_rewrite_nano_total` | counter | Cumulative nanoseconds spent in `tiered.Rewrite()`. Divide by `attempts_total` for average latency. |
| `arc_tiered_build_success_total` | counter | Successful per-bucket builds (any variant) |
| `arc_tiered_build_errors_total` | counter | Failed builds |
| `arc_tiered_build_nano_total` | counter | Cumulative nanoseconds spent inside `Publisher.publishWith`. |
| `arc_tiered_watermark_lag_max_seconds` | gauge | Largest `now − watermark` across all `(tier, variant)` per table. Stuck builder shows up as a rising value. |

### Example alerts

```yaml
- alert: TieredWatermarkStuck
  expr: arc_tiered_watermark_lag_max_seconds > 7200  # 2 hours
  for: 10m
  annotations:
    summary: "Tiered builder watermark hasn't advanced in 2+ hours"

- alert: TieredBuildErrorsRising
  expr: rate(arc_tiered_build_errors_total[5m]) > 0
  for: 10m
  annotations:
    summary: "Tiered builder errors occurring"

- alert: TieredRouterRefusalSpike
  expr: |
    rate(arc_tiered_rewrite_refused_emit_total[5m])
      > 0.01 * rate(arc_tiered_rewrite_attempts_total[5m])
  for: 10m
  annotations:
    summary: "Tiered emit-stage refusals >1% of attempts (likely schema_hash drift)"
```

### Hit rate

```
sum(rate(arc_tiered_rewrite_accepted_total[5m]))
/ sum(rate(arc_tiered_rewrite_attempts_total[5m]))
```

Healthy production: 0.85+. Lower than 0.5 suggests configuration drift or workload changed.
