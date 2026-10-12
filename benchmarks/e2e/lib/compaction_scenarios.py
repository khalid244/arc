"""S9 / S10: compactor (hourly + daily + reorg) and rollup under storage faults.

Both scenarios seed a back-dated backlog (lib/seed.py) so compaction and
reorg have real candidates within minutes, run live ingest at the same time,
and finish with the compactor and rollup STOPPED before the final
verification (a consistent snapshot: no object is being rewritten while the
verifier reads).
"""
from __future__ import annotations

import time

import requests

from . import logscan
from .env import Env
from .ledger import Ledger
from .report import Checks, write_result
from .steps import d, outage, outage_mode, restore, run_verify
from .seed import object_counts, seed_backlog


def _post(url: str, path: str) -> dict:
    try:
        r = requests.post(url + path, timeout=10)
        return {"status": r.status_code, "body": r.text[:200]}
    except requests.RequestException as e:
        return {"error": str(e)}


def _get(url: str, path: str) -> dict:
    try:
        return requests.get(url + path, timeout=10).json()
    except (requests.RequestException, ValueError) as e:
        return {"error": str(e)}


def trigger_all(env: Env, label: str):
    c = env.procs["compactor"].url
    res = {"compaction": _post(c, "/api/v1/compaction/trigger"), "reorg": _post(c, "/api/v1/reorg/trigger")}
    env.timeline.mark("compaction_trigger", label=label, **{k: v.get("status", v.get("error")) for k, v in res.items()})
    return res


def compactor_idle(env: Env) -> bool:
    c = env.procs["compactor"].url
    st = _get(c, "/api/v1/compaction/stats")
    rg = _get(c, "/api/v1/reorg/status")
    if "error" in st or "error" in rg:
        return False
    return (st.get("active_jobs") in (0, None)) and not st.get("cycle_running") and not rg.get("is_running")


def wait_compactor_idle(env: Env, timeout: float) -> bool:
    end = time.time() + timeout
    time.sleep(3)
    while time.time() < end:
        if compactor_idle(env):
            # double-check after a pause (a scheduled cycle may be starting)
            time.sleep(3)
            if compactor_idle(env):
                return True
        time.sleep(3)
    return False


def final_compaction_round(env: Env, rounds: int = 2, timeout: float = 240) -> list:
    out = []
    for i in range(rounds):
        wait_compactor_idle(env, timeout)
        out.append(trigger_all(env, f"final-{i}"))
        ok = wait_compactor_idle(env, timeout)
        env.timeline.mark("compactor_idle" if ok else "compactor_busy_timeout", round=i)
    return out


def compaction_evidence(env: Env) -> dict:
    ev = {}
    for name in ("compactor", "rollup"):
        if name not in env.procs:
            continue
        lp = env.procs[name].log_path
        sc = logscan.scan(lp)
        counts = {}
        for needle in ("Starting compaction subprocess", "Compaction cycle complete", "Found hourly compaction candidate",
                       "Found daily compaction candidate", "Scheduled compaction failed", "Scheduled compaction completed",
                       "Reorg bucket drained", "Reorg bucket failed", "Reorganizer run completed",
                       "Reorganizer run failed", "Reorganize failed", "ROW COUNT MISMATCH",
                       "Skipped already-compacted files", "Skipping corrupted file", "Compaction job failed",
                       "Recovered orphaned compaction manifests", "delete sources failed", "Splitting batch after",
                       "Retrying compaction", "Failed to find candidates", "Rollup glob failed", "Failed to write to S3"):
            counts[needle] = len(logscan.grep(lp, needle, 100000))
        ev[name] = {"counts": counts, "top_errors": sc.get("top_errors"), "levels": sc.get("levels")}
    return ev


def _seed(env: Env) -> dict:
    led = Ledger(env.ledger_path)
    st = seed_backlog(env.bucket, env.minio.endpoint, led, seed=env.args.seed)
    led.close()
    env.timeline.mark("seeded", objects=st["objects"], rows=st["rows"])
    return st


def _prefix_counts(env: Env, seed_stats: dict) -> dict:
    prefixes = list(seed_stats["hourly_partitions"]) + list(seed_stats["daily_partitions"]) + \
        ["default/downloads_late", "posthog/events_late", "_arc/rollup", "_arc/compaction", "_arc/reorg"]
    return object_counts(env.bucket, env.minio.endpoint, prefixes)


def _compaction_checks(checks: Checks, before: dict, after: dict, seed_stats: dict, ev: dict, rollup: bool):
    hourly_reduced = [p for p in seed_stats["hourly_partitions"] if after.get(p, 0) < before.get(p, 0)]
    checks.add("hourly_compaction_ran", bool(hourly_reduced),
               f"seeded hourly partitions with fewer objects after the run: {len(hourly_reduced)}/"
               f"{len(seed_stats['hourly_partitions'])} ({ {p: (before.get(p), after.get(p)) for p in seed_stats['hourly_partitions']} })")
    daily_reduced = [p for p in seed_stats["daily_partitions"] if after.get(p, 0) < before.get(p, 0)]
    checks.add("daily_compaction_ran", bool(daily_reduced),
               f"{ {p: (before.get(p), after.get(p)) for p in seed_stats['daily_partitions']} }", informational=True)
    late_b = before.get("default/downloads_late", 0) + before.get("posthog/events_late", 0)
    late_a = after.get("default/downloads_late", 0) + after.get("posthog/events_late", 0)
    drained = ev.get("compactor", {}).get("counts", {}).get("Reorg bucket drained", 0)
    checks.add("reorg_ran", drained > 0, f"{drained} reorg buckets drained; sidecar objects {late_b} -> {late_a}")
    if rollup:
        checks.add("rollup_wrote_cubes", after.get("_arc/rollup", 0) > 0,
                   f"{after.get('_arc/rollup', 0)} objects under _arc/rollup", informational=True)


# ---------------------------------------------------------------- S9

def s9(env: Env):
    a = env.args
    env.minio.ensure_running()
    env.minio.create_bucket(env.bucket)
    seed_stats = _seed(env)
    before = _prefix_counts(env, seed_stats)
    env.up(("ingest1", "ingest2", "query", "compactor", "rollup"))
    env.start_load()
    env.sleep(d(a.warmup, 45), "warmup")
    trigger_all(env, "pre-outage")
    time.sleep(3)  # let the cycle get going, then pull the plug mid-cycle
    outage(env)
    env.sleep(d(a.outage_s, 180), "S3 down (all roles)")
    t_restore = restore(env)
    env.sleep(d(a.post_restore_s, 150), "post-restore: scheduled cycles")
    checks, extra = Checks(), {"seed": seed_stats}
    lg = env.stop_load()
    drain = env.wait_drained(a.drain_timeout, since=t_restore)
    extra["drain"] = drain
    checks.add("drained_within_T", drain["drained"], f"drain_s={drain['drain_s']}")
    extra["final_rounds"] = final_compaction_round(env)
    v_mid = run_verify(env, "compactor-idle")
    env.procs["compactor"].sigterm()
    env.procs["rollup"].sigterm()
    after = _prefix_counts(env, seed_stats)
    ev = compaction_evidence(env)
    extra.update({"objects_before": before, "objects_after": after, "compaction_evidence": ev})
    v = run_verify(env, "final")
    from .report import standard_integrity_checks
    standard_integrity_checks(checks, v)
    _compaction_checks(checks, before, after, seed_stats, ev, rollup=True)
    checks.add("client_never_acked", lg.get("rows_never_acked", 0) == 0, f"{lg.get('rows_never_acked', 0)}",
               informational=True)
    dead = [p.name for p in env.procs.values() if p.name not in ("compactor", "rollup") and not p.alive()]
    checks.add("all_processes_alive", not dead, f"dead: {dead}" if dead else "all alive")
    return write_result(env, checks, [v_mid, v], extra)


# ---------------------------------------------------------------- S10

def s10(env: Env):
    a = env.args
    env.minio.ensure_running()
    env.minio.create_bucket(env.bucket)
    seed_stats = _seed(env)
    before = _prefix_counts(env, seed_stats)
    env.up(("ingest1", "ingest2", "query", "compactor"))
    env.start_load()
    env.sleep(d(a.warmup, 30), "warmup")
    # Ordered rules (first match wins):
    #  1. GETs of one seeded hourly partition ALWAYS fail (reset mid-body / 503 with "404" in the
    #     RequestId) - so the download really fails and compaction falls back to Exists();
    #  2. every other GET fails pct% of the time;
    #  3. HEADs (Exists checks) get a 503 whose RequestId contains "404" (head_pct%).
    poisoned = seed_stats["hourly_partitions"][0]           # e.g. default/downloads/2026/10/12/19
    extra_fault = {"poisoned_partition": poisoned}
    mix = {"reset_midbody": 1, "http503": 1}
    outage_mode(env, "normal", scope="compactor", rules=[
        {"mode": "flaky", "methods": ["GET"], "path_regex": "/" + poisoned + "/", "pct": 100, "mix": mix,
         "requestid_404": True},
        {"mode": "flaky", "methods": ["GET"], "pct": d(a.pct, 40), "mix": mix, "requestid_404": True},
        {"mode": "http503", "methods": ["HEAD"], "pct": a.head_pct, "requestid_404": True},
    ])
    t_end = time.time() + d(a.outage_s, 240)
    while time.time() < t_end:
        trigger_all(env, "under-fault")
        env.sleep(min(30, max(0, t_end - time.time())), "compactor GET faults")
    t_restore = restore(env, scope="compactor")
    checks, extra = Checks(), {"seed": seed_stats, **extra_fault}
    extra["final_rounds"] = final_compaction_round(env)
    lg = env.stop_load()
    drain = env.wait_drained(a.drain_timeout, since=t_restore)
    extra["drain"] = drain
    checks.add("drained", drain["drained"], f"drain_s={drain['drain_s']}")
    env.procs["compactor"].sigterm()
    after = _prefix_counts(env, seed_stats)
    ev = compaction_evidence(env)
    extra.update({"objects_before": before, "objects_after": after, "compaction_evidence": ev})
    # "File not found (already compacted), skipping" is DEBUG; the subprocess's INFO summary
    # ("Skipped already-compacted files", skipped=N, partition=...) carries the count.
    skipped_lines = logscan.grep(env.procs["compactor"].log_path, "Skipped already-compacted files", 1000)
    skipped_by_part: dict[str, int] = {}
    for x in skipped_lines:
        part = x.get("partition", "?")
        skipped_by_part[part] = skipped_by_part.get(part, 0) + int(x.get("skipped", 0) or 0)
    skipped_total = sum(skipped_by_part.values())
    extra["skipped_as_not_found_by_partition"] = skipped_by_part
    extra["skipped_as_not_found_examples"] = skipped_lines[:10]
    v = run_verify(env, "final")
    from .report import standard_integrity_checks
    standard_integrity_checks(checks, v)
    _compaction_checks(checks, before, after, seed_stats, ev, rollup=False)
    ps = env.proxy.stats().get("compactor", {}).get("counters", {})
    checks.add("faults_injected_on_compactor_gets", ps.get("fault:reset_midbody", 0) + ps.get("fault:http503", 0) > 0,
               f"proxy compactor counters: {ps}", informational=True)
    pz = skipped_by_part.get(poisoned, 0)
    checks.add("no_503_misclassified_as_404", skipped_total == 0,
               f"{skipped_total} source files skipped as 'not found (already compacted)' while S3 answered "
               f"503/reset with '404' in the RequestId; by partition: {skipped_by_part}. Poisoned partition "
               f"{poisoned}: {pz} skipped although its objects existed ({before.get(poisoned)} before, "
               f"{after.get(poisoned)} after the run = compacted once the fault ended)", informational=True)
    checks.add("client_never_acked", lg.get("rows_never_acked", 0) == 0, f"{lg.get('rows_never_acked', 0)}",
               informational=True)
    return write_result(env, checks, [v], extra)
