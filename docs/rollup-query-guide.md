# Rollup — Query Guide

How to write queries that get **accelerated** by the rollup cubes, and how to tell
when one will. This is for query authors (dashboards, analysts). For configuring
and operating rollup, see [`rollup.md`](./rollup.md).

Rollup pre-aggregates your source data into small "cube" files (hourly buckets ×
group-by dimensions). When a query matches a cube, Arc rewrites it to read the
cube instead of scanning raw parquet — often 5–40× faster. **A query that doesn't
match simply runs against source: always correct, just not accelerated.** You
never get a wrong answer; you only ever lose the speedup.

---

## TL;DR — a query rolls up when ALL of these hold

1. It's an **aggregate** query — `GROUP BY` with supported aggregate functions (or a single grand-total aggregate).
2. The aggregates are **mergeable** (see the list below).
3. Every column in `GROUP BY` (and in any `WHERE`/`CASE` predicate) is **stored by a cube**.
4. The time bucket is **≥ 1 hour** (cubes are hourly).
5. The time range is **fully covered** by built cube data (no interior gaps).

Miss any one and it falls back to source. The query editor tells you which, before you run.

---

## How to tell

**In the Grafana editor**, a badge under the SQL shows the prediction and the result:

| Badge | Meaning |
|---|---|
| `◷ Will roll up · by_status` | covered — will be served from that cube |
| `⚠ Won't roll up — <reason>` | will hit source; the reason says why (e.g. *"time bucket is under 1h"*) |
| `⚡ Rolled up · by_status · 12 ms` | last run **was** served from the cube |
| `▤ Source · 1.3 s` | last run hit source |

**Rollups selector** (in *Options*): `Auto` (use a cube when one covers, else source — the default), `Rollup only` (force the cube; **errors** if nothing covers — use it to *verify* a panel is accelerated), `Off` (always source).

**API users**: the response carries `X-Arc-Rollup-Cube: <cube>` when served from a cube, and `X-Arc-Rollup-Fallback: source` if a cube was selected but its files were missing and it fell back. Send `X-Arc-No-Rollup: true` to force source.

---

## Supported aggregates

**Exact** (bit-identical to source):

- `COUNT(*)`, `COUNT(col)`
- `SUM(col)`, `MIN(col)`, `MAX(col)`
- `AVG(col)` (derived from `SUM`/`COUNT`)
- **Conditional aggregates** — success/error/conversion rates:
  `SUM(CASE WHEN <pred> THEN x ELSE y END)`, `COUNT(CASE WHEN <pred> THEN 1 END)`
- **Top-N** — `ORDER BY <agg> [DESC] LIMIT n`

**Approximate** (sketch-backed, served from the `coarse` cube):

- `COUNT(DISTINCT col)` — Theta sketch, ~1% typical error
- `quantile(col, p)` / percentiles (p50/p95/p99) — KLL sketch, ~1–3% error (p99/extreme tails drift more over long ranges)

> The predicate columns in a `CASE` count as group-by dimensions for coverage — they must be stored by a cube too.

---

## The #1 rule: bucket at ≥ 1 hour

Cubes are **hourly**. Your time bucket must be ≥ 1h or the query hits source.

```sql
-- ✅ rolls up: hourly (or coarser) bucket
SELECT $__timeGroup(time, '1h') AS time, status, COUNT(*)
FROM downloads WHERE $__timeFilter(time) GROUP BY 1, 2

-- ❌ falls to source: 30-minute bucket is finer than the hourly cube
SELECT $__timeGroup(time, '30m') AS time, status, COUNT(*) ...
```

With `$__timeGroup(time, $__interval)`, Grafana picks the interval from the panel
width and range. To force ≥ 1h: **widen the time range**, or set the panel's
**Min interval** to `1h`. (Sub-hour ranges are cheap to scan from source anyway.)

---

## Group-by & filter columns must be stored

The cube has to contain every column you group or filter by:

```sql
-- ✅ if 'region' and 'os' are stored dims
SELECT region, os, COUNT(*) FROM downloads WHERE $__timeFilter(time) GROUP BY 1, 2

-- ❌ falls to source if 'user_agent' is high-cardinality / has no cube
SELECT user_agent, COUNT(*) FROM downloads ... GROUP BY 1
```

Low/medium-cardinality columns get their own cube automatically; very
high-cardinality ones are gated (covering them would cost too much storage).
`WHERE` on a stored dim and `WHERE $__timeFilter(time)` are both pushed into the
cube read.

**Multi-dimension queries need ONE cube holding all those columns together.** A query
that filters by one column and groups by another (e.g. `WHERE event='survey sent' GROUP
BY survey_response, os_name`) needs a single cube storing *all* of them — the separate
single-dim cubes can't serve it. On a narrow table the `dim_rich` cube covers this; on a
**wide** table (where `dim_rich` is too large and skipped) the operator declares a
**targeted cube** (`[[rollup.cube]]`) for exactly that dim set. So if a multi-dim panel
won't roll up even though its columns *are* stored individually, that's the case — ask
the operator to add a targeted cube (see `rollup.md`).

---

## Patterns that fall to source — and the fix

| Pattern | Why | Fix |
|---|---|---|
| `30m`/`10m`/`1m` bucket | finer than the hourly cube | widen range or Min interval `1h` |
| `ROUND(AVG(x), 2)` directly in the aggregate select | a scalar **wrapping** the aggregate isn't a mergeable agg | aggregate **bare** in a base CTE, do the scalar math in an outer query |
| `SELECT * FROM downloads ...` | not an aggregate | rollup only accelerates aggregates — this is by design |
| `JOIN` / `db.table` cross-table | `FROM` must be a single base table | aggregate each side separately, or query source |
| `GROUP BY <high-card col>` | no cube stores it | group by a stored dim, or accept the source scan |
| range includes un-built / deleted days | a coverage gap would undercount | Arc detects the gap and runs source automatically (correct); the data re-materializes on the next build pass |

**Scalar-over-aggregate fix** — push the scalar to an outer layer:

```sql
-- ❌ won't roll up
SELECT $__timeGroup(time,'1h') t, ROUND(AVG(latency_ms),2) FROM events ... GROUP BY 1

-- ✅ rolls up: the base aggregates bare; ROUND runs on the tiny cube output
WITH base AS (
  SELECT $__timeGroup(time,'1h') AS t, AVG(latency_ms) AS avg_lat
  FROM events WHERE $__timeFilter(time) GROUP BY 1
)
SELECT t, ROUND(avg_lat, 2) FROM base ORDER BY t
```

---

## The CTE-base pattern (for complex panels)

For normalization, Top-N, ratios, or `CASE` reshaping, put a **simple, servable
aggregation in a base CTE**, then do the fancy work in outer CTEs over the (small)
cube output. Arc rewrites just the base onto the cube:

```sql
WITH base AS (                                   -- served by the by_site cube
  SELECT $__timeGroup(time, $__interval) AS t, site, COUNT(*) AS c
  FROM downloads WHERE $__timeFilter(time) GROUP BY 1, 2
),
normalized AS (                                  -- CASE runs on the cube output
  SELECT t,
    CASE WHEN site LIKE '%.youtube.com' THEN 'youtube.com'
         WHEN site LIKE '%.tiktok.com'  THEN 'tiktok.com'
         ELSE site END AS metric,
    SUM(c) AS value
  FROM base GROUP BY 1, 2
),
top AS ( SELECT metric FROM normalized GROUP BY metric ORDER BY SUM(value) DESC LIMIT 10 )
SELECT t AS time, metric, value
FROM normalized WHERE metric IN (SELECT metric FROM top) ORDER BY 1
```

The base CTE must be servable on its own (the 5 TL;DR rules); the outer CTEs can do
anything DuckDB supports.

---

## Freshness — you always get current data

Recent data inside the seal/grace window isn't cubed yet. Arc **stitches it from
source automatically** (merge-on-read): the sealed bulk comes from the cube, the
fresh tail from raw parquet, merged into one result. You never see stale numbers —
only the older, sealed portion is accelerated.

---

## Quick checklist

```
□ aggregate query (GROUP BY + COUNT/SUM/MIN/MAX/AVG/DISTINCT/quantile/CASE)
□ bucket ≥ 1h  ($__timeGroup with $__interval ≥ 1h; widen range / Min interval 1h)
□ group-by + predicate columns are stored dims
□ aggregates are bare (scalars/ROUND/ratios in an outer CTE, not wrapping the agg)
□ single base table (no JOIN)
□ check the editor badge says "Will roll up"
```
