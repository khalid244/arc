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
