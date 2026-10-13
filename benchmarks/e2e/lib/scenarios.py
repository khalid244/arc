"""Scenario definitions S0-S10. Each returns an exit code (see report.EXIT).

Shared shape:  bring up MinIO + proxy + Arc roles -> start load -> inject the
fault / kill / restart -> restore -> keep loading -> stop load (client keeps
retrying unacked batches) -> wait for Arc to drain its local backlog WITHOUT
any restart -> verify ledger vs bucket (+ query API) -> JSON result.
"""
from __future__ import annotations

import csv
import json
import time
from pathlib import Path

import requests

from . import logscan
from .env import Env
from .report import Checks, write_result
from .steps import d, finish_ingest_run, guarded, outage, outage_mode, restore, run_verify

REGISTRY: dict[str, tuple] = {}


def scenario(name: str, doc: str):
    def deco(fn):
        REGISTRY[name] = (fn, doc)
        return fn
    return deco


# ---------------------------------------------------------------- S0 smoke

@scenario("S0", "smoke: clean ingest, no faults (harness self-test)")
def s0(args):
    return guarded(_s0, "S0")(args)


def _s0(env: Env):
    a = env.args
    env.up(("ingest1", "ingest2", "query"))
    env.start_load()
    env.sleep(d(a.outage_s, 60), "clean load")
    checks, extra = Checks(), {}
    vs = finish_ingest_run(env, checks, extra, None, a.drain_timeout)
    return write_result(env, checks, vs, extra)


# ---------------------------------------------------------------- S1 hard outage

@scenario("S1", "hard S3 outage (connection refused) while ingesting; full drain without restart")
def s1(args):
    return guarded(_s1, "S1")(args)


def _s1(env: Env):
    a = env.args
    env.up(("ingest1", "ingest2", "query"))
    env.start_load()
    env.sleep(d(a.warmup, 30), "warmup")
    outage(env)
    env.sleep(d(a.outage_s, 300), "S3 down")
    t_restore = restore(env)
    env.sleep(d(a.post_restore_s, 60), "post-restore load")
    checks, extra = Checks(), {}
    vs = finish_ingest_run(env, checks, extra, t_restore, a.drain_timeout)
    return write_result(env, checks, vs, extra)


# ---------------------------------------------------------------- S2 503 storm + latency

@scenario("S2", "30% HTTP 503 SlowDown + latency for 10 min")
def s2(args):
    return guarded(_s2, "S2")(args)


def _s2(env: Env):
    a = env.args
    env.up(("ingest1", "ingest2", "query"))
    env.start_load()
    env.sleep(d(a.warmup, 30), "warmup")
    outage_mode(env, "flaky", pct=d(a.pct, 30), mix={"http503": 1}, latency_ms=d(a.latency_ms, 200),
                jitter_ms=d(a.jitter_ms, 800))
    env.sleep(d(a.outage_s, 600), "503 + latency")
    t_restore = restore(env)
    env.sleep(d(a.post_restore_s, 30), "post-restore load")
    checks, extra = Checks(), {}
    vs = finish_ingest_run(env, checks, extra, t_restore, a.drain_timeout)
    return write_result(env, checks, vs, extra)


# ---------------------------------------------------------------- S3 commit-then-timeout

@scenario("S3", "commit_then_timeout on every Nth PUT (PUT stored, client sees failure) - duplicate trap")
def s3(args):
    return guarded(_s3, "S3")(args)


def _s3(env: Env):
    a = env.args
    env.up(("ingest1", "ingest2", "query"))
    env.start_load()
    env.sleep(d(a.warmup, 30), "warmup")
    outage_mode(env, "commit_then_timeout", methods=["PUT", "POST"], every_n=a.every_n,
                ctt_response=a.ctt_response, ctt_delay_ms=a.ctt_delay_ms, sticky_ms=a.sticky_ms)
    env.sleep(d(a.outage_s, 300), "commit-then-timeout")
    t_restore = restore(env)
    env.sleep(d(a.post_restore_s, 30), "post-restore load")
    checks, extra = Checks(), {}
    vs = finish_ingest_run(env, checks, extra, t_restore, a.drain_timeout)
    # Evidence: do duplicate files coincide with objects the proxy committed-then-failed?
    try:
        ctt_keys = set()
        for lst in env.proxy.stats().values():
            if isinstance(lst, dict):
                ctt_keys.update(k.split("/", 2)[-1] for k in (lst.get("ctt_committed_keys") or []))
        overlap = []
        for files, copies, n in vs[-1]["evidence"].get("dup_file_groups") or []:
            overlap.append({"files": files, "uuids": n,
                            "files_committed_then_failed": [f for f in files if f in ctt_keys]})
        extra["ctt_committed_objects"] = len(ctt_keys)
        extra["dup_groups_vs_ctt"] = overlap
        checks.add("ctt_faults_injected", len(ctt_keys) > 0, f"{len(ctt_keys)} objects committed-then-failed",
                   informational=True)
    except Exception as e:  # noqa: BLE001 - evidence must never cost us the result
        extra["evidence_error"] = repr(e)
    return write_result(env, checks, vs, extra)


# ---------------------------------------------------------------- S4 kill -9 during outage

@scenario("S4", "kill -9 an ingest process during an outage, restart with the same dirs, restore")
def s4(args):
    return guarded(_s4, "S4")(args)


def _s4(env: Env):
    a = env.args
    env.up(("ingest1", "ingest2", "query"))
    env.start_load()
    env.sleep(d(a.warmup, 60), "warmup")
    outage(env)
    kill_at = d(a.kill_at_s, 60)
    env.sleep(kill_at, "S3 down (pre-kill)")
    victim = env.procs["ingest1"]
    pre = victim.local_state()
    env.timeline.mark("pre_kill_local_state", proc=victim.name, **pre)
    victim.sigkill()
    time.sleep(3)
    victim.start()
    startup = victim.wait_healthy(timeout=a.startup_timeout)
    env.sleep(max(0, d(a.outage_s, 180) - kill_at), "S3 down (post-restart)")
    t_restore = restore(env)
    env.sleep(d(a.post_restore_s, 60), "post-restore load")
    checks, extra = Checks(), {"pre_kill_local_state": pre, "restart_startup": startup}
    checks.add("restart_became_healthy", startup.get("health_s") is not None, json.dumps(startup))
    vs = finish_ingest_run(env, checks, extra, t_restore, a.drain_timeout, require_no_restart=False)
    extra["wal_recovery_log"] = logscan.grep(victim.log_path, "WAL recovery complete", 10)
    return write_result(env, checks, vs, extra)


# ---------------------------------------------------------------- S5 SIGTERM during outage

@scenario("S5", "SIGTERM (graceful) during an outage: same-dirs restart (must be lossless) "
                "and wiped-dirs restart (pod deletion, loss documented)")
def s5(args):
    return guarded(_s5, "S5")(args)


def _s5(env: Env):
    a = env.args
    env.up(("ingest1", "ingest2", "query"))
    env.start_load()
    env.sleep(d(a.warmup, 60), "warmup")
    outage(env)
    kill_at = d(a.kill_at_s, 45)
    env.sleep(kill_at, "S3 down")
    # Phase A: container restart (same dirs)
    pA = env.procs["ingest1"]
    preA = pA.local_state()
    tA = time.time()
    stopA = pA.sigterm()
    postA = pA.local_state()
    pA.start()
    hA = pA.wait_healthy(timeout=a.startup_timeout)
    env.sleep(30, "S3 down")
    # Phase B: pod deletion (wiped dirs)
    pB = env.procs["ingest2"]
    preB = pB.local_state()
    tB = time.time()
    stopB = pB.sigterm()
    postB = pB.local_state()
    pB.wipe()
    pB.start()
    hB = pB.wait_healthy(timeout=a.startup_timeout)
    env.sleep(max(0, d(a.outage_s, 180) - kill_at - 30), "S3 down")
    t_restore = restore(env)
    env.sleep(d(a.post_restore_s, 60), "post-restore load")

    checks, extra = Checks(), {}
    lg = env.stop_load()
    drain = env.wait_drained(a.drain_timeout, since=t_restore)
    extra["drain"] = drain
    checks.add("drained_within_T", drain["drained"], f"drain_s={drain['drain_s']}")
    v = run_verify(env, "final")
    t = v["totals"]
    # Attribute missing rows per ingest and per ack time vs. the SIGTERM instants.
    missing = missing_rows(env)
    a_missing = [r for r in missing if r["ack_target"] == pA.url]
    b_before = [r for r in missing if r["ack_target"] == pB.url and r["acked_at"] < tB]
    b_after = [r for r in missing if r["ack_target"] == pB.url and r["acked_at"] >= tB]
    other = [r for r in missing if r["ack_target"] not in (pA.url, pB.url)]
    extra["phaseA_same_dirs"] = {"sigterm_wall": tA, "pre_stop_local": preA, "stop": stopA, "post_stop_local": postA, "startup": hA,
                                 "missing": len(a_missing)}
    extra["phaseB_wiped_dirs"] = {"sigterm_wall": tB, "pre_stop_local": preB, "stop": stopB, "post_stop_local": postB, "startup": hB,
                                  "missing_acked_before_sigterm": len(b_before),
                                  "missing_acked_after_sigterm": len(b_after)}
    checks.add("same_dirs_restart_lossless", len(a_missing) == 0,
               f"{len(a_missing)} rows acked by {pA.name} missing after graceful stop + same-dirs restart; "
               f"local state after shutdown: {postA}")
    if getattr(a, "handoff", False):
        checks.add("wiped_restart_lossless_with_handoff", len(b_before) == 0,
                   f"{len(b_before)} rows acked by {pB.name} before SIGTERM lost with the wiped dirs although "
                   f"staged work is handed to the peer at shutdown; local state left at shutdown: {postB}")
    else:
        checks.add("wiped_restart_loss_documented", True,
                   f"{len(b_before)} rows acked by {pB.name} before SIGTERM lost with the wiped dirs "
                   f"(expected: emptyDir lost on pod deletion); local state left at shutdown: {postB}",
                   informational=True)
    checks.add("no_loss_of_rows_acked_after_shutdown_began", len(b_after) == 0,
               f"{len(b_after)} rows acked by {pB.name} after SIGTERM were lost")
    checks.add("no_other_loss", len(other) == 0, f"{len(other)} missing rows acked by other processes")
    arc_dups = t["dup_rows"] - t["dup_rows_client_ambiguous"]
    checks.add("zero_duplicates", arc_dups == 0, f"{t['dup_rows']} dup rows ({t['dup_rows_client_ambiguous']} "
               f"client-ambiguous)")
    checks.add("zero_unexpected", t["unexpected"] == 0, f"{t['unexpected']}")
    checks.add("timestamps_preserved", t["time_mismatch_rows"] == 0, f"{t['time_mismatch_rows']} rows")
    checks.add("client_never_acked", lg.get("rows_never_acked", 0) == 0, f"{lg.get('rows_never_acked', 0)}",
               informational=True)
    checks.add("graceful_shutdown_within_grace", all(s.get("escalated_to_kill") is False for s in (stopA, stopB)),
               f"A={stopA} B={stopB}")
    return write_result(env, checks, [v], extra)


def missing_rows(env: Env) -> list[dict]:
    """Missing acked rows with their ack target / time (re-queried from the verify tables)."""
    import duckdb
    from .verify import _attach_ledger, _connect, load_storage
    con = _connect(env.minio.endpoint)
    _attach_ledger(con, env.ledger_path)
    meas = [tuple(r) for r in con.execute("SELECT DISTINCT db, m FROM led").fetchall()]
    load_storage(con, env.bucket, meas)
    rows = con.execute("""SELECT l.db, l.m, l.uuid, l.ack_target, l.acked_at, l.batch_id, l.tclass
        FROM led l WHERE l.status='acked' AND NOT EXISTS
        (SELECT 1 FROM s3 s WHERE s.db=l.db AND s.m=l.m AND s.uuid=l.uuid)""").fetchall()
    con.close()
    out = [dict(zip(["db", "m", "uuid", "ack_target", "acked_at", "batch_id", "tclass"], r)) for r in rows]
    with open(env.run_dir / "missing.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["db", "m", "uuid", "ack_target", "acked_at", "batch_id", "tclass"])
        w.writeheader()
        w.writerows(out)
    _ = duckdb
    return out


# ---------------------------------------------------------------- S6 local disk cap

DEFAULT_CAP_KEYS = [
    "ingest.max_stager_bytes", "ingest.stager_max_bytes", "ingest.fresh_stager_max_bytes",
    "ingest.local_disk_max_bytes", "ingest.late_stager_max_bytes", "ingest.stager_max_size", "ingest.max_local_backlog_bytes",
    "ingest.local_backlog_max_bytes", "wal.max_total_size_mb", "wal.max_total_bytes", "wal.max_disk_bytes",
]


def binary_has_strings(binary: str, keys: list[str]) -> list[str]:
    """Arc registers every config key with viper.SetDefault("<section>.<key>", ...), so a
    supported key exists verbatim as a string literal in the binary."""
    data = Path(binary).read_bytes()
    return [k for k in keys if k.encode() in data]


@scenario("S6", "local disk cap reached during a long outage: 503 back-pressure, no crash, no loss")
def s6(args):
    keys = args.cap_keys.split(",") if args.cap_keys else DEFAULT_CAP_KEYS
    found = binary_has_strings(args.binary, keys)
    if not found and not args.evict_at_mb:
        env = Env("S6", args)
        checks = Checks()
        checks.add("disk_cap_supported", False,
                   f"not supported by binary: none of the cap config keys {keys} exist in {args.binary}; "
                   "WAL and stager dirs grow without bound during an outage (prod: emptyDir sizeLimit "
                   "20Gi/50Gi -> kubelet EVICTS the pod = pod deletion = staged data lost). "
                   "Re-run with --evict-at-mb N to measure that eviction loss.")
        env.sampler = None
        try:
            return write_result(env, checks, [], {"cap_keys_searched": keys}, status="NOT_SUPPORTED")
        finally:
            env.proxy.stop()
    return guarded(lambda env: _s6(env, found), "S6")(args)


def _s6(env: Env, found: list[str]):
    a = env.args
    extra_ov = {}
    if found:
        caps = dict(c.split("=", 1) for c in a.cap)
        if not caps:
            raise SystemExit(f"S6: binary supports {found}; pass --cap KEY=VALUE (small, e.g. 50MB)")
        extra_ov["ingest"] = caps
    env.up(("ingest1", "ingest2", "query"), extra_overrides=extra_ov)
    env.start_load()
    env.sleep(d(a.warmup, 30), "warmup")
    outage(env)
    t_out = time.time()
    evicted = []
    limit_s = d(a.outage_s, 600)
    while time.time() - t_out < limit_s:
        time.sleep(2)
        for p in env.ingest_procs():
            if not p.alive():
                continue
            ls = p.local_state()
            tot = ls["wal_bytes"] + ls["fresh_bytes"] + ls["late_bytes"]
            if a.evict_at_mb and not found and tot > a.evict_at_mb * 1e6:
                env.timeline.mark("kubelet_eviction_simulated", proc=p.name, local_bytes=tot)
                p.sigkill()
                p.wipe()
                p.start()
                p.wait_healthy(timeout=a.startup_timeout)
                evicted.append({"proc": p.name, "local_bytes": tot, "t": round(time.time() - t_out, 1)})
        st = env.lg_stats()
        print(f"   [S3 down] {time.time() - t_out:5.0f}s acked={st.get('rows_acked')} "
              f"errors={st.get('errors')}", flush=True)
    t_restore = restore(env)
    env.sleep(d(a.post_restore_s, 60), "post-restore load")
    checks, extra = Checks(), {"cap_keys_supported": found, "evictions": evicted}
    vs = finish_ingest_run(env, checks, extra, t_restore, a.drain_timeout, require_no_restart=not evicted)
    errs = env.lg_stats().get("errors", {})
    if found:
        checks.add("backpressure_503_seen", errs.get("http_503", 0) > 0, f"client errors: {errs}")
    else:
        checks.add("eviction_simulation", True, f"{len(evicted)} simulated evictions: {evicted}",
                   informational=True)
    return write_result(env, checks, vs, extra)


# ---------------------------------------------------------------- S7 drain at full rate

@scenario("S7", "drain after a long outage while ingest continues at full rate")
def s7(args):
    return guarded(_s7, "S7")(args)


def _s7(env: Env):
    a = env.args
    env.up(("ingest1", "ingest2", "query"))
    env.start_load()
    env.sleep(d(a.warmup, 45), "warmup")
    base = [p.local_state() for p in env.ingest_procs()]
    baseline = max(b["fresh_bytes"] + b["late_bytes"] for b in base)
    outage(env)
    env.sleep(d(a.outage_s, 300), "S3 down")
    peak = {p.name: p.local_state() for p in env.ingest_procs()}
    t_restore = restore(env)
    # Backlog cleared = staged bytes back to <= 2x pre-outage baseline + 2 MB on every ingest.
    thresh = 2 * baseline + 2_000_000
    cleared_at = None
    post = d(a.post_restore_s, 240)
    lat = []
    while time.time() - t_restore < post:
        time.sleep(2)
        st = env.lg_stats()
        lat.append((round(time.time() - t_restore, 1), st.get("lat_p99_s"), st.get("acked_rows_per_s_10s")))
        sizes = {p.name: p.local_state() for p in env.ingest_procs()}
        if cleared_at is None and all(v["fresh_bytes"] + v["late_bytes"] <= thresh for v in sizes.values()):
            cleared_at = round(time.time() - t_restore, 1)
            env.timeline.mark("backlog_cleared_under_load", seconds=cleared_at, sizes=sizes)
        if int(time.time() - t_restore) % 10 == 0:
            print(f"   [drain under load] {time.time() - t_restore:4.0f}s staged="
                  f"{ {n: (v['fresh_files'] + v['late_files'], (v['fresh_bytes'] + v['late_bytes']) // 1_000_000) for n, v in sizes.items()} } "
                  f"p99={st.get('lat_p99_s')} rate={st.get('acked_rows_per_s_10s')}", flush=True)
    checks, extra = Checks(), {"baseline_staged_bytes": baseline, "peak_local_state": peak,
                               "backlog_cleared_under_load_s": cleared_at, "latency_during_drain": lat}
    checks.add("backlog_cleared_under_full_load", cleared_at is not None,
               f"cleared {cleared_at}s after restore (window {post}s, threshold {thresh} bytes)")
    worst_p99 = max((x[1] or 0) for x in lat) if lat else None
    checks.add("ingest_latency_during_drain", True, f"worst 10s p99 ack latency {worst_p99}s", informational=True)
    vs = finish_ingest_run(env, checks, extra, t_restore, a.drain_timeout)
    return write_result(env, checks, vs, extra)


# ---------------------------------------------------------------- S8 restart with a large WAL

@scenario("S8", "restart during an outage with a large WAL: startup recovery time, /health and /ready")
def s8(args):
    return guarded(_s8, "S8")(args)


def _s8(env: Env):
    a = env.args
    env.up(("ingest1", "ingest2", "query"))
    env.start_load(rate=a.burst_rate)
    env.sleep(d(a.warmup, 30), "warmup")
    outage(env)
    env.sleep(d(a.kill_at_s, 120), "S3 down, WAL growing")
    victim = env.procs["ingest1"]
    pre = victim.local_state()
    env.timeline.mark("pre_kill_local_state", proc=victim.name, **pre)
    victim.sigkill()
    time.sleep(2)
    victim.start()
    # Poll /health and /ready densely to time the recovery window.
    t0 = time.time()
    probe = []
    health_s = ready_s = None
    while time.time() - t0 < a.startup_timeout:
        row = {"t": round(time.time() - t0, 2)}
        for ep in ("health", "ready"):
            try:
                row[ep] = requests.get(f"{victim.url}/{ep}", timeout=1).status_code
            except requests.RequestException as e:
                row[ep] = type(e).__name__
        probe.append(row)
        if health_s is None and row["health"] == 200:
            health_s = row["t"]
        if ready_s is None and row["ready"] == 200:
            ready_s = row["t"]
            break
        if not victim.alive():
            break
        time.sleep(0.25)
    env.timeline.mark("restart_probe", health_s=health_s, ready_s=ready_s)
    env.sleep(max(0, d(a.outage_s, 180) - d(a.kill_at_s, 120)), "S3 down (post-restart)")
    t_restore = restore(env)
    env.sleep(d(a.post_restore_s, 60), "post-restore load")
    rec = logscan.grep(victim.log_path, "WAL recovery complete", 10)
    checks, extra = Checks(), {"pre_kill_local_state": pre, "startup_probe": probe[:400],
                               "health_s": health_s, "ready_s": ready_s, "wal_recovery_log": rec}
    checks.add("startup_health_within_limit", health_s is not None and health_s <= a.max_startup_s,
               f"/health 200 after {health_s}s, /ready 200 after {ready_s}s (limit {a.max_startup_s}s); "
               f"WAL at kill: {pre['wal_bytes'] / 1e6:.1f} MB in {pre['wal_files']} files; "
               f"/health was unreachable for the whole recovery window" if health_s else "never healthy")
    vs = finish_ingest_run(env, checks, extra, t_restore, a.drain_timeout, require_no_restart=False)
    return write_result(env, checks, vs, extra)


# ---------------------------------------------------------------- S9 / S10 compaction

from .compaction_scenarios import s9 as _s9_impl, s10 as _s10_impl  # noqa: E402


@scenario("S9", "compactor (hourly+daily+reorg) and rollup running through an outage: no loss, no dups")
def s9(args):
    return guarded(_s9_impl, "S9")(args)


@scenario("S10", "reset_midbody + 503-with-404-RequestId on compactor GETs: storage error classification")
def s10(args):
    return guarded(_s10_impl, "S10")(args)
