# Arc E2E durability harness (`benchmarks/e2e/`)

Proves - or disproves - that **no acknowledged row is lost or duplicated**
when the object store misbehaves the way Hetzner Object Storage (Ceph RGW)
does in production: hard outages, 503 SlowDown storms, connections cut
mid-transfer, black-holed requests, and PUTs that are stored but whose
response never arrives.

It runs the real Arc binary natively, in the production topology (2 ingest,
1 query, 1 compactor, 1 rollup - separate processes with their own WAL,
stager and temp dirs), against a local MinIO behind a programmable fault
proxy, drives prod-shaped load, and checks every acknowledged row against
the bucket.

```
 loadgen ──msgpack/LP──▶ ingest1 ─┐                       ┌──▶ MinIO (sync-minio :29000)
 (ledger of acked uuids) ingest2 ─┤                       │        ▲
                         query  ──┼──▶ faultproxy ────────┘        │ direct, no proxy
                         compactor┤   :29010 ingest                │
                         rollup ──┘   :29012 compactor        verifier (DuckDB httpfs)
                                      :29013 query            + Arc query API
                                      :29014 rollup
                                      :29019 control (POST /mode)
```

## Safety

* `lib/guard.py` refuses any endpoint that is not loopback and anything
  matching `*your-objectstorage.com*` or `*.gyoom.sa`, refuses to run
  `kubectl`/`helm`/`argocd`, and strips every `ARC_*` / `AWS_*` / proxy
  variable from child environments (viper `AutomaticEnv` would otherwise let
  a stray shell variable redirect Arc). The Go proxy independently refuses a
  non-loopback or production-looking upstream and only binds loopback.
* Docker containers are named `sync-*` (`sync-minio`, throwaway `sync-mc-*`),
  host ports are 29000-29999 only. The harness refuses to start if one of
  its ports is already taken (a stale proxy would silently answer instead).
* Arc processes run with `TZ=UTC` (prod containers are UTC; see findings).

## Layout

| path | purpose |
|---|---|
| `run.py` | CLI: run scenarios (`S1`, `S1,S3`, `all`), `list`, `cleanup` |
| `faultproxy/main.go` | Go S3 fault proxy: per-role listeners + control API |
| `profiles/scaled.toml` | prod ConfigMaps scaled down (buffers/flush ages/schedules), prod value in a comment on every changed line |
| `profiles/prod.toml` | the same file with the production values (long soaks) |
| `lib/guard.py` | production guard (endpoints, env, commands) |
| `lib/config.py` | ports, profile loading, per-process `arc.toml` rendering |
| `lib/infra.py` | MinIO container + bucket (mc), fault-proxy build/start/control, port preflight |
| `lib/supervisor.py` | Arc process start / SIGTERM / SIGKILL / restart same-or-wiped dirs, health timing, 1 s RSS + dir-size sampler, timeline |
| `lib/loadgen.py` | async load generator, uuid per row, retries, ledger of acked rows |
| `lib/ledger.py` | SQLite ledger schema (batches + rows) |
| `lib/verify.py` | DuckDB reads the bucket directly; missing / dups / unexpected / time / partition checks; query-API cross-check |
| `lib/seed.py` | back-dated backlog (hourly, daily, late-sidecar objects) so compaction and reorg have work in minutes |
| `lib/env.py` | one test environment: bring-up, load, drain detection, teardown |
| `lib/steps.py` | shared scenario steps: fault on/off, drain + verify + standard checks, teardown guard |
| `lib/scenarios.py` | S0-S8, S11 |
| `lib/compaction_scenarios.py` | S9, S10 |
| `lib/logscan.py` | JSON-log scan: error counts, notable durability events, crash lines |
| `lib/report.py` | checks, `result.json`, `summary.txt`, exit code |
| `requirements.txt` | Python deps |

## Prerequisites

* Python 3.11+ with `pip install -r requirements.txt`
  (aiohttp, requests, duckdb, msgpack, pyarrow, psutil). The verifier
  installs the DuckDB `httpfs` and `sqlite` extensions on first use.
* Go 1.22+ (the proxy is built automatically into `.build/faultproxy`).
* Docker with `minio/minio:latest` and `quay.io/minio/mc:latest`.
* An Arc binary built with `-tags=duckdb_arrow`.

## Commands

```bash
cd benchmarks/e2e
python -m venv .venv && .venv/bin/pip install -r requirements.txt

# build the binary under test (any commit)
go build -tags=duckdb_arrow -o /tmp/arc-under-test ../../cmd/arc

.venv/bin/python run.py list
.venv/bin/python run.py S0 --binary /tmp/arc-under-test          # 1-min self-test
.venv/bin/python run.py S1 --binary /tmp/arc-under-test          # hard outage
.venv/bin/python run.py S1,S3,S4 --binary /tmp/arc-under-test    # several
.venv/bin/python run.py all --binary /tmp/arc-under-test         # S0..S10 (~1.5 h)
.venv/bin/python run.py cleanup                                  # kill strays, rm sync-mc-*
.venv/bin/python run.py cleanup --remove-minio                   # ...and sync-minio
```

**Pointing at a different binary:** `--binary PATH` (or `export
ARC_E2E_BINARY=PATH`). Nothing else changes; the binary's sha256 is recorded
in every result. To compare two builds, run the same scenario with the same
`--seed` against each.

Exit code: `0` PASS, `1` FAIL, `2` NOT_SUPPORTED (S6 when the binary has no
disk cap), `3` harness error. With several scenarios, the worst code wins.

### Knobs (all scenarios)

| flag | default | meaning |
|---|---|---|
| `--profile` | `scaled` | `profiles/<name>.toml` or a path; `prod` = production values |
| `--set k=v` | | override an arc.toml key for every role, e.g. `--set ingest.flush_timeout_seconds=30` |
| `--rate` | 1500 | offered rows/s across the ingest processes |
| `--warmup` / `--outage-s` / `--post-restore-s` | per scenario | phase lengths; raise for soaks |
| `--kill-at-s` | per scenario | S4/S5/S8/S11: seconds into the outage before the kill |
| `--drain-timeout` | 300 | **T**: after S3 recovers, Arc must empty buffers + stager dirs within T, with no restart |
| `--client-drain-timeout` | 300 | how long the client keeps retrying unacked batches after load stops |
| `--max-rss-mb` | 4000 | RSS bound per ingest process |
| `--pct --latency-ms --jitter-ms` | | S2/S10 fault intensity |
| `--every-n --ctt-response --ctt-delay-ms --sticky-ms` | 3, 504, 2000, 60000 | S3 commit-then-timeout shape |
| `--cap k=v --cap-keys --evict-at-mb` | | S6 |
| `--outage-mode` | `down` | shape of "S3 is down" in S1/S4-S9: `down` (refused), `hang` (black hole), `http503` |
| `--burst-rate` | 5000 | S8: load rate used to grow the WAL before the kill |
| `--head-pct` | 100 | S10: % of compactor HEADs answered 503 with `404` in the RequestId |
| `--stagger-ms` | 1700 | delay between ingest process starts (prod pods never start in the same ms; `0` makes cross-process key collisions likely) |
| `--arc-tz` | UTC | TZ for Arc processes |
| `--seed` | 42 | load generator / seeder RNG |

## Scenarios

Every scenario: bring up MinIO + proxy + Arc roles, start load, inject the
fault, restore, keep loading, stop load (the client keeps retrying unacked
batches), wait for Arc to drain **without restarting it**, verify. Standard
checks (all must pass):

* `zero_missing` - every acked uuid is in the bucket
* `zero_duplicates` - no uuid stored twice (copies on batches whose client
  retry was ambiguous - timeout/reset after send - are excluded and shown
  separately, because the client may have caused them)
* `zero_unexpected` - nothing in the bucket the client never sent
* `timestamps_preserved` - stored `time` equals the time that was sent
* `rows_in_correct_partition` - base rows sit under the `Y/M/D/H` of their time
* `query_api_matches_storage` - Arc's query process returns the same counts
* `no_silent_object_overwrites` - the proxy counts successful whole-object PUTs
  per key; a data key stored twice means the second PUT replaced the first
  (commit-then-timeout re-commits and `_arc/` objects excluded)
* `harness_ledger_uuids_unique` - self-check: the harness never sent a uuid twice
* `drained_within_T`, `ingest_rss_bounded`, `all_processes_alive`, `no_restart_needed`

| id | what | default timeline | scenario-specific checks |
|---|---|---|---|
| S0 | smoke, no faults | 60 s load | - |
| S1 | hard outage: proxy refuses connections (all roles) | 30 s warmup, 300 s down, 60 s load | drained within T without restart |
| S2 | 30 % `503 SlowDown` + 200-1000 ms latency | 30 s, 600 s fault, 30 s | |
| S3 | commit-then-timeout: every 3rd PUT is stored in MinIO, then the client gets a 504 after 2 s; retries of the same key within 60 s get the same treatment (`--sticky-ms 0` = only every Nth request) | 30 s, 300 s, 30 s | duplicate files are matched against the objects the proxy committed-then-failed (`dup_groups_vs_ctt`) |
| S4 | `kill -9` ingest1 60 s into a 180 s outage, restart with the same dirs | 60 s, 180 s, 60 s | restart healthy |
| S5 | SIGTERM during an outage: ingest1 restarted with the same dirs (container restart), ingest2 with wiped dirs (pod deletion) | 60 s, 180 s, 60 s | same-dirs restart lossless; rows acked after SIGTERM never lost; wiped-dirs loss reported as informational |
| S11 | `--handoff` required. Ingest `server.shutdown_timeout=20` (unless `--set`). SIGTERM ingest2 150 s into an outage: its shutdown handoff is cut while ingest1 is still storing (fsyncing) a unit, so the 2xx never reaches ingest2, which keeps its copy; ingest2 then restarts on the SAME dirs (container restart: liveness kill, crash, OOM after SIGTERM), S3 stays down 30 s, restore | 60 s, 150 s + 30 s, 60 s | `precondition_answer_lost_at_deadline` (from both logs: units the peer stored minus units the sender got a 2xx for > 0); zero missing AND zero duplicates; in-doubt re-offers / fallbacks reported |
| S6 | local disk cap reached during a long outage | 600 s | NOT_SUPPORTED unless the binary has a cap key (`--cap-keys`); with `--cap k=v`: 503 back-pressure seen, no crash, no loss. `--evict-at-mb N` simulates the kubelet emptyDir eviction instead |
| S7 | 300 s outage, then drain while ingest continues at full rate | 45 s, 300 s, 240 s | staged backlog back to baseline under load; p99 ack latency recorded |
| S8 | `kill -9` with a large WAL during an outage | 30 s, 120 s, +60 s | `/health` within `--max-startup-s` (dense /health + /ready probe recorded) |
| S9 | seeded backlog + compactor (hourly, daily, reorg) + rollup; outage starts 3 s after a triggered cycle | 45 s, 180 s, 150 s | hourly compaction and reorg actually ran (object counts, logs); verification with compactor idle and again with compactor/rollup stopped |
| S10 | compactor only, ordered rules: GETs of one seeded partition always fail (reset mid-body / 503 with `404` in the RequestId), other GETs fail 40 %, HEADs (`Exists`) get 503 with `404` in the RequestId | 30 s, 240 s | integrity + per-partition count of present files the compactor skipped as "not found" (informational) |

### Load

`lib/loadgen.py`: `default.downloads` (msgpack columnar, sort key
`site,response,tag`), `posthog.events` (msgpack columnar, wide and sparse - one
event type per batch with its own property columns, so the schema changes
batch to batch like posthog), `default.lp_metrics` (line protocol,
precision s/ms/us/ns). Each row has a unique `uuid`. Timestamps: 70 % real
time, 10 % inside the late window, 2 % slightly future, 12 % older than
`late_window_seconds`, 5 % 1-3 days old, 1 % beyond `future_skew_seconds`;
msgpack batch units 80 % us, 10 % ms, 10 % s. Batches go round-robin to the
ingest processes; a refused connection or 5xx fails over to the next one
(like the k8s Service).

### Fault proxy API

```bash
curl -XPOST 127.0.0.1:29019/mode -d '{"scope":"all","mode":"down"}'
curl -XPOST 127.0.0.1:29019/mode -d '{"scope":"compactor","mode":"flaky","pct":40,
      "methods":["GET","HEAD"],"mix":{"reset_midbody":1,"http503":1},"requestid_404":true}'
curl -XPOST 127.0.0.1:29019/mode -d '{"scope":"ingest","mode":"commit_then_timeout",
      "every_n":3,"ctt_response":"504","ctt_delay_ms":2000,"sticky_ms":60000}'
curl -XPOST 127.0.0.1:29019/mode -d '{"scope":"compactor","mode":"normal","rules":[
      {"methods":["GET"],"path_regex":"/default/downloads/2026/10/12/19/","mode":"flaky","pct":100,
       "mix":{"reset_midbody":1,"http503":1},"requestid_404":true},
      {"methods":["HEAD"],"mode":"http503","requestid_404":true}]}'
curl 127.0.0.1:29019/stats        # per listener counters, committed-then-failed keys, keys stored twice
curl -XPOST 127.0.0.1:29019/mode -d '{"scope":"all","mode":"normal"}'
```

Modes: `normal`, `down` (listener closed + open connections reset = refused),
`http503` (`status`, `error_code`, `requestid_404`), `reset_midbody`
(PUT body cut before MinIO sees it / GET body cut at `cut_fraction`, TCP RST),
`hang` (never answer; `hang_ms` > 0 = 504 after that), `commit_then_timeout`
(`every_n`, `ctt_response` 504|hang|reset, `ctt_delay_ms`, `sticky_ms`),
`latency` (`latency_ms` + `jitter_ms`), `flaky` (`pct` % of matching requests
get a fault drawn from `mix`). Filters: `methods`, `path_regex`. `rules` is
an ordered list of complete configs; the first whose methods and path regex
match handles the request, otherwise the parent config does. Scope is a
listener name, a list, or `all`. Changing the mode resets requests parked in
`hang` (like a load balancer coming back). Fault events are appended to
`<run>/proxy-events.jsonl`.

## Outputs

`runs/<scenario>-<timestamp>/`:

* `result.json` - status, headline numbers, every check, verifications
  (per-measurement counts + evidence samples: missing by target / class /
  minute, duplicate file groups, time mismatches), max RSS and dir sizes per
  process, load-generator stats, proxy counters, log summaries, timeline
* `summary.txt` - the human summary printed at the end
* `samples.csv` - 1 s samples: RSS, children RSS, CPU, WAL/fresh/late
  stager bytes and file counts per process
* `timeline.jsonl` - every harness action with relative time
* `ledger.sqlite` - every batch and row sent (`status` acked|never_acked)
* `logs/<proc>.log` - Arc JSON logs (restarts appended with a marker line),
  `faultproxy.log`, `loadgen.log`; `proxy-events.jsonl`
* `procs/<proc>/arc.toml` - the exact config each process ran with

Re-verify a finished run: `python -m lib.verify --bucket <bucket> --ledger
runs/<run>/ledger.sqlite [--query-url http://127.0.0.1:29110]` (MinIO must
still be up).

## Scaling notes (profiles/scaled.toml)

Buffers, flush ages, flush timeout, WAL rotation, compaction and reorg
schedules are scaled down ~10x so a scenario takes minutes. Two values can
NOT be scaled: `ingest.late_window_seconds` stays 7200 (Arc measures a row's
age from its **hour-bucket start**, so a window below 3600 s sends the
current hour to `<m>_late/`), and therefore `reorg.min_age_seconds` stays
7200 too; S9/S10 seed back-dated sidecar objects so reorg has closed buckets
to drain. `hourly_min_age_hours` is 0 (prod 1).

## Fork validation (binary `arc-fork` @ 3879d488, sha256 2304c87151ec…)

Full-length defaults for S1, S3, S4; the others are shortened shakedowns that
prove each scenario's code path (phase lengths in brackets).

| scenario | acked rows | missing | dup rows | max ingest RSS | drain after restore | result |
|---|---:|---:|---:|---:|---:|---|
| S1 hard outage 300 s | 585,181 | 0 | 0 | 1,816 MB | 68 s | PASS |
| S3 commit-then-timeout 300 s | 540,163 | 0 | 94,247 (74,388 uuids) | 1,130 MB | 38 s | FAIL |
| S4 kill -9 in outage | 482,091 | 0 | 89,896 (+20,835 rows with wrong time) | 1,874 MB | 66 s | FAIL |
| S2 [60 s 503+latency] | 135,109 | 0 | 0 | 680 MB | 23 s | PASS |
| S5 [SIGTERM in 100 s outage] | 273,501 | 65,789 (all ingest2, wiped dirs, acked before SIGTERM) | 0 | 1,130 MB | 32 s | PASS (loss documented) |
| S6 | - | - | - | - | - | NOT_SUPPORTED (no cap keys) |
| S6 `--evict-at-mb 6` [90 s] | 190,734 | 97,286 (2 simulated evictions) | 0 | 738 MB | 28 s | FAIL (eviction = loss) |
| S7 [90 s outage, 90 s full-rate drain] | 300,694 | 0 | 0 | 1,020 MB | backlog cleared under load in 28 s | PASS |
| S8 [5 000 rows/s, kill after 60 s] | 729,296 | 0 | 184,187 (+32,708 wrong time) | 2,418 MB | 32 s | FAIL; /health 18.8 s |
| S9 [60 s outage mid-cycle] | 297,616 | 0 | 0 | 903 MB | 100 s | PASS (hourly 6→1, daily 12→1, 8 reorg buckets, 192 cube objects) |
| S10 [90 s compactor faults] | 228,392 | 0 | 0 | 615 MB | 37 s | PASS (10 present files skipped as "not found") |
| S10 `--stagger-ms 0` | 237,677 | 290 | 0 | 587 MB | 44 s | FAIL (object key overwritten) |

### Findings on the fork

1. **Stager re-upload duplicates after commit-then-timeout (S3).** When a
   stager PUT is stored but the client sees failure, `FreshStager` /
   `LateStager` keep the staged sources and re-upload them on the next tick
   under a NEW key; the committed object is never cleaned up. Every duplicate
   group pairs an object the proxy committed-then-failed with its re-upload
   (`dup_groups_vs_ctt` in result.json), and 5 "rollback delete failed" log
   lines show the partial-walk rollback failing too. 94,247 duplicate rows in
   5 minutes.
2. **WAL replay duplicates after a crash (S4, S8).** Startup recovery replays
   every non-active WAL file, including the previous active file whose rows
   were already staged/uploaded (`WAL recovery complete files=1
   entries=90660`, then 89,896 duplicate rows - all from the killed process).
3. **WAL replay ignores the msgpack time unit (S4, S8).** The WAL stores the
   raw msgpack payload before timestamp normalisation; replay feeds raw
   seconds/milliseconds to the writer as microseconds, so replayed `s`/`ms`
   rows land in January 1970 (`time_mismatch_sample`: stored `1791833871`
   vs sent `1791833871000000`, in `events_late`), logged as "Data timestamp
   is >7 days old".
4. **Graceful shutdown with S3 down purges the WAL although staged data was
   never uploaded (S5).** The stager's shutdown flush fails, yet Arc logs
   "Purged WAL files after clean buffer flush"; 352 staged files are the only
   copy. Same-dirs restart recovers them (0 lost); pod deletion loses them
   (65,789 rows). An emptyDir eviction during a long outage (S6
   `--evict-at-mb`) loses everything staged the same way. The binary has no
   local disk cap, so the 20 Gi / 50 Gi emptyDir limits are the only bound.
5. **Late-stager object keys can collide across ingest processes (S10
   `--stagger-ms 0`).** Keys are `<m>_late_<second>_<UnixNano % 1e9>`
   with no per-process component; two processes wrote
   `default/downloads_late/downloads_late_20261012_213349_493116000.parquet`
   in the same microsecond and the second PUT silently replaced the first
   (290 rows lost). Requires same-µs uploads: likely on macOS (µs clock,
   aligned tickers), rare on Linux prod, but the key is not unique by
   construction.
6. **`Exists()` treats any error containing "404" as not-found (S10).** A
   503 whose RequestId contains `404` makes the compactor skip present
   source files as "already compacted" (all 6 objects of the poisoned
   partition). No loss on this path (skipped files are not deleted), but the
   same `Exists()` is used by orphaned-manifest recovery
   (`manifest.go`: output "missing" -> manifest deleted -> recompaction ->
   duplicates; input "already gone" -> manifest deleted with inputs still
   present). That path only runs on manifests older than 30 min
   (`ManifestMinRecoveryAge`) left by a crashed job; it is NOT automated
   yet (it needs a compactor kill between output upload and source delete,
   then a >= 30 min soak under the S10 HEAD rule).
7. **Startup blocks on S3 even for ingest (S4, S5, S8).** After WAL recovery
   the ingest process reads `_arc/rollup/_workload/*.json` from S3 with
   retries before the HTTP listener opens: /health is refused for 13-19 s
   with S3 refusing connections (0.2 s normally). With a black-holed S3
   (`--outage-mode hang`) there is no client timeout on that read.
8. **FreshStager partitions by the host's local timezone.** With `TZ` unset on
   a UTC+3 host, rows of 20:xx UTC were written under hour `23`
   (`rows_in_correct_partition` catches it; reproduce with `--arc-tz
   Asia/Riyadh`). Prod containers run in UTC, so the harness pins `TZ=UTC`.
9. Stager merges are unbounded in-process DuckDB: draining ~1,200 staged
   files after a 5-minute outage peaked ingest RSS at 1.8 GB (S1); S8 at
   5,000 rows/s reached 2.4 GB.
