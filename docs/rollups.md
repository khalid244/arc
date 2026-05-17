# Rollups

Rollups are pre-aggregated time bucket parquet files Arc maintains alongside source data, and a query rewriter that transparently redirects compatible aggregate queries from source scans onto them. Long-window dashboard queries (`GROUP BY day`, `COUNT(DISTINCT)`, `WHERE dim = X`) become 10–80× faster with no app changes.

Disabled by default. Opt in per-deployment.

## Enabling

Minimum config:

```toml
[rollup]
enabled = true
builder = true      # this node materializes rollups
```

`builder = false` lets a node use rollups built elsewhere (reader-only). Exactly one node in a cluster should be the builder.

On first start with a non-empty source table, Arc samples the schema, infers per-column roles, and backfills daily buckets from the earliest source timestamp up to a 5-minute grace window. Backfill runs at ~3–5 s per daily window on the local bench.

## What gets built

Three variant kinds per source table:

| Variant | Shape | Use |
|---|---|---|
| `<table>__1d` | dim-rich, GROUP BY (time_bucket, all low-card dims) | filter/group-by on any low-card dim |
| `<table>_by_<col>__1d` | per-dim, GROUP BY (time_bucket, col) | filter/group-by on `col`, including high-card cols |
| `<table>_sketch__1d` | no-dim, one row per time bucket | global HLL `COUNT(DISTINCT)` per day |

The classifier assigns each column a role based on type and cardinality:

| Role | Trigger | What it gets |
|---|---|---|
| Time | `TIMESTAMP` / `TIMESTAMPTZ` | bucket column |
| Dim | string ≤ `dim_cardinality_max`, or numeric ≤ 32 distinct | GROUP BY column in dim-rich + per-dim variants |
| Metric | numeric > 32 distinct | `SUM`/`AVG`/`MIN`/`MAX`, t-digest if > 100 distinct |
| Sketch | string in (`dim_cardinality_max`, `sketch_cardinality_max`] | HLL for approximate `COUNT(DISTINCT)` |
| Drop | string > `sketch_cardinality_max`, or unsupported type | ignored |

Cols in (1024, `dim_cardinality_max`] that classify as `Dim` get a per-dim variant but stay out of the dim-rich cross-product (auto-HighCard), so raising the knob doesn't multiply dim-rich row count.

## Knobs

### `[rollup]`

| Key | Default | Notes |
|---|---|---|
| `enabled` | `false` | Master switch. Off = no inference, no builder, no rewrite. |
| `builder` | `false` | This node builds rollups. Reader-only nodes leave `false`. |
| `dim_cardinality_max` | `1024` | Max distinct values for a column to count as a Dim. Cols above this and ≤ `sketch_cardinality_max` become Sketches. |
| `sketch_cardinality_max` | `100000` | Above this, columns are dropped from rollups. Matches industry HLL "degraded above" threshold. |

### `[rollup.tables."db.table"]` (optional, per-table overrides)

| Key | Effect |
|---|---|
| `sketch_columns = [...]` | Force-HLL these columns regardless of classifier. Use for identity columns above `sketch_cardinality_max` when you ask `COUNT(DISTINCT)` on them. |
| `keep_columns = [...]` | Force-Dim these columns regardless of cardinality. High-card kept cols get a per-dim variant but stay out of dim-rich. Use when you filter/group by a column outside the threshold band. |
| `ignore_columns = [...]` | Exclude from rollups entirely. |
| `quantile_columns = [...]` | Restrict t-digest emission to this allow-list (default: all continuous numerics). |
| `time_column = "name"` | Disambiguate when the table has multiple timestamp columns. |

Example:

```toml
[rollup.tables."<database>.<table>"]
sketch_columns = ["<high_card_id>"]      # distinct above sketch_cardinality_max → would Drop; force HLL
keep_columns   = ["<mid_card_dim>"]      # distinct above dim_cardinality_max → would Sketch; we filter on it
```

## What queries get rewritten

A query is rewritten onto a rollup only if **all three** guards pass:

1. **Time filter:** WHERE has both an upper and lower bound on the time column at the top level (not nested in OR/NOT).
2. **Aggregate translatability:** every aggregate in SELECT/HAVING is one of `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT)`, `percentile_cont`/`quantile_cont`. Nested aggregates (`AVG(x)*100`, `SUM(x)/COUNT(*)`) refuse.
3. **Variant coverage:** a variant exists whose kept dims cover every column referenced in WHERE / GROUP BY (excluding aggregated columns).

If any guard fails, the query runs against source. Same correctness, no error.

The emitter produces merge-on-read SQL: `WITH rollup AS (...), fresh AS (...) SELECT ... FROM (UNION ALL) merged`. The `fresh` CTE serves rows from the in-flight bucket (after the rollup's upper bound, up to the user's `time < Hi`). Counts are exact even at bucket boundaries.

## Storage cost

Measured against a representative 1.8 GB source table (14 days, 14 dims, ~27M rows):

| Threshold | Variants | Total rollup size | % of source |
|---|---|---|---|
| `dim_cardinality_max=1024` (default) | 11 | 215 MB | 11.9 % |
| `dim_cardinality_max=25000` (admits two mid-card dims) | 13 | 233 MB | 12.9 % |

Per-dim variants are cheap (~0.5–10 MB each). The dim-rich variant dominates and grows with the cross-product of the low-card dims.

## Query speedups (same dataset)

| Query shape | Source ms | Rollup ms | Speedup |
|---|---:|---:|---:|
| `COUNT(*) GROUP BY day` (7 days, no filter) | 410 | 9 | 45× |
| `COUNT(*) GROUP BY day WHERE <dim> = X` (7 days, low-card filter) | 460 | 18 | 25× |
| `COUNT(*) GROUP BY day WHERE <dim> = X` (7 days, mid-card filter) | 280 | 17 | 16× |
| `COUNT(DISTINCT <id>) GROUP BY day` (14 days) | ≈3500 | 100 | 35× |
| `SUM(CASE WHEN <flag> = 'ok' THEN 1 ELSE 0 END)` | 78 | 10 | 7.8× |

Approximate `COUNT(DISTINCT)` via HLL has ~1.6 % error at lg_k=12 (fixed).

## Rollback

Set `rollup.enabled = false`, restart Arc. The rewriter goes dormant, queries fall back to source. On-disk rollup parquet stays intact; re-enable at any time and the builder resumes from the last watermark.

## Known limitations

- **Schema-less FROM** (`SELECT … FROM table`, not `db.table`) falls back to source.
- **Nested aggregates** in SELECT/HAVING (`AVG(x) * 100`, `SUM(x) / COUNT(*)`) refuse.
- **`percentile_cont` without GROUP BY** refuses (an upstream DuckDB datasketches t-digest crash drove this).
- **Late-arriving data** past the 5-minute grace window is not retroactively rebuilt; flush WAL within the grace window to avoid undercount.
- **Schema changes** are picked up on Arc restart (re-inference). Adding a column requires a restart to be reflected in new rollup variants.

---

# Tiered rollups (v2)

The v2 tiered subsystem extends the legacy single-tier rollup with a **pyramid of granularities** (1h → 1d → 1w → 1mo) plus a more permissive query rewriter. Year-long aggregate queries that used to scan billions of source rows complete in tens of milliseconds.

v1 and v2 coexist. The query router tries v2 first when a table has been opted into tiered; if v2 declines, the query falls through to the legacy v1 path; if v1 also declines, the query runs against source. **Existing rollup behavior is unchanged for tables not opted into tiered.**

## What it adds over v1

| | v1 (legacy) | v2 (tiered) |
|---|---|---|
| Tiers | 1d only | 1h, 1d, 1w, 1mo (configurable) |
| Bucket alignment | UTC (latent bug on local-TZ queries) | Pinned timezone via `tz` config — calendar-aligned |
| Query coverage | ~75% of aggregate shapes | ~95% empirically validated (50-query catalog, see commit history) |
| Year-horizon speedup | ~35-45× | **1,000–5,000×** on time-bucket aggregations |
| HLL precision | `lg_k=12` (~1.6% RSE) | `lg_k=14` (~0.8% RSE), configurable |
| Late-data handling | 5-minute grace; data past it lost | 6-hour grace by default; documented boundary |

## Enabling

Minimum config to opt in:

```toml
[rollup]
enabled = true

[rollup.tiered]
enabled = true
tz      = "Asia/Riyadh"   # REQUIRED — bucket alignment timezone
builder = true             # exactly one node per cluster
```

That's all. Defaults are documented below; auto-classification handles per-dim decisions.

## Configuration reference

```toml
[rollup.tiered]
enabled = true             # default: false
tz      = "..."            # REQUIRED when enabled
builder = false            # this node materializes tier files

tiers = ["1h", "1d", "1w", "1mo"]   # default
grace_window = "6h"        # default. Buckets sealed only when bucket_end + grace ≤ now.
coverage_threshold = 0.99  # default. A dim is kept if N values cover ≥ this fraction of rows.
dim_rich_cap = 100         # default. Effective cardinality cap for dim-rich cross-product.
hll_lg_k = 14              # default. Raise to 16 (4× sketch size) for higher accuracy at long horizons.
kll_k    = 200             # default. KLL precision.
obsolete_grace = "168h"    # default 7d. How long to keep replaced variants for rollback.

[rollup.tiered.tables."default.events"]
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

Beyond the v1 list, the tiered router additionally accepts:
- Nested aggregates (`AVG(x) * 100`, `SUM(x) / COUNT(*)`) — decomposed to stored columns
- `quantile_cont(x, p)` without `GROUP BY` — uses sketch variant's KLL
- IN / NOT IN / IS NOT NULL filters
- Multiple aggregates in one query (multi-stat Grafana panels)
- Topk / HAVING / ORDER BY <agg> LIMIT N

What still falls back to source (router refuses, query runs against raw):
- JOINs, window functions, subqueries in WHERE
- `CASE WHEN` inside an aggregate argument (e.g., `SUM(CASE WHEN x THEN 1 ELSE 0 END)`)
- Sub-hourly granularity (`date_trunc('minute', ...)`) — no 1m tier in v1
- Per-row expressions (`SELECT FLOOR(x/10), COUNT(*) ... GROUP BY 1`)
- Filter values outside the dim's kept-set (e.g., niche site filters)
- Schema-less `FROM` (still falls back, same as v1)
- Open-ended or missing time filter

## Late-arriving data

v1 grace window: 5 minutes. v2 default: 6 hours. Events whose timestamp falls more than `grace_window` before `now` are **invisible to precalc** — they remain in source, raw queries see them correctly, precalc undercounts by that volume. For workloads where this matters, raise `grace_window` further (12h, 24h).

If your real lateness distribution has a long tail past the grace window, file a feature request — the design admits an append-only bucket-file scheme (write a second parquet for late events; reader unions them) as a future v2.1 path.

## Migration

| Scenario | Behavior |
|---|---|
| `rollup.enabled = true`, `rollup.tiered.enabled = false` (default) | v1 behavior unchanged. No tiered work. |
| Both enabled, table not in `[rollup.tiered.tables]` | v1 handles the table. |
| Both enabled, table opted in via `[rollup.tiered.tables."db.tbl"]` | v2 tries first; v1 fallback if v2 declines; source if both decline. |
| Tiered enabled but no spec/manifest yet | Classifier runs on first builder cycle, then builds catch up. Queries fall through to v1 (or source) until the first tiered build completes. |

## Rollback

Set `[rollup.tiered].enabled = false` and restart. The router stops trying the v2 path; v1 continues. On-disk tier files stay intact for re-enable. Per-table opt-out via the `[rollup.tiered.tables.*]` block.

## Known limitations

- **`CASE WHEN` inside aggregate arguments** — refuse, source fallback. The aggregate-argument expression isn't a column reference, so the router can't translate it.
- **Sub-hourly granularity** — no 1m or 5m tier; queries like `date_trunc('minute', ...)` fall back to source.
- **HLL accuracy at very long horizons** — at `lg_k=14`, 60-day distinct-count merges can exceed 5% error in the worst case. Raise `hll_lg_k = 16` for ~0.4% RSE at 4× sketch size.
- **First builds after spec change** — readers refuse files with stale `schema_hash` until the builder catches up. During the catch-up window, queries fall through to v1 or source.
- **Sketch-only scheduler in v1** — automatic builds cover the `sketch` variant; per-dim and `all` variant publishing is currently driver-mediated (operator tool or future scheduler enhancement).
