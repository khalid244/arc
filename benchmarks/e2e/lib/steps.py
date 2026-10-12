"""Shared scenario steps: fault on/off, drain + verify + standard checks, teardown guard."""
from __future__ import annotations

import json
import time

from .env import Env
from .report import Checks, standard_integrity_checks
from .verify import verify


def d(v, default):
    return default if v is None else v


# ---------------------------------------------------------------- common helpers

def run_verify(env: Env, label: str, with_query: bool = True) -> dict:
    env.timeline.mark("verify_start", label=label)
    q = env.procs["query"].url if with_query and "query" in env.procs and env.procs["query"].alive() else None
    v = verify(env.bucket, env.minio.endpoint, env.ledger_path, q, label=label)
    (env.run_dir / f"verify-{label}.json").write_text(json.dumps(v, indent=2, default=str))
    t = v["totals"]
    env.timeline.mark("verify_done", label=label, missing=t.get("missing"), dup_rows=t.get("dup_rows"),
                      acked=t.get("acked_rows"), seconds=v["verify_seconds"])
    return v


def finish_ingest_run(env: Env, checks: Checks, extra: dict, t_restore: float | None,
                      drain_timeout: float, require_no_restart: bool = True) -> list[dict]:
    """Stop load, wait for drain, verify, add the standard checks."""
    lg = env.stop_load()
    drain = env.wait_drained(drain_timeout, since=t_restore)
    extra["drain"] = drain
    if t_restore is not None:
        checks.add("drained_within_T", drain["drained"],
                   f"drained {drain['drain_s']}s after restore (T={drain_timeout}s)" if drain["drained"]
                   else f"NOT drained {drain_timeout}s after restore; backlog={drain['backlog']}")
    else:
        checks.add("drained", drain["drained"], f"backlog={drain['backlog']}")
    v = run_verify(env, "final")
    standard_integrity_checks(checks, v)
    mx = env.sampler.max if env.sampler else {}
    worst = max((m.get("rss_mb", 0) for n, m in mx.items() if n.startswith("ingest")), default=0)
    checks.add("ingest_rss_bounded", worst <= env.args.max_rss_mb, f"max ingest RSS {worst:.0f} MB "
               f"(limit {env.args.max_rss_mb:.0f})")
    dead = [p.name for p in env.procs.values() if not p.alive()]
    checks.add("all_processes_alive", not dead, f"dead: {dead}" if dead else "all alive")
    if require_no_restart:
        unexpected = [p.name for p in env.procs.values() if p.starts != 1]
        checks.add("no_restart_needed", not unexpected, f"restarted: {unexpected}" if unexpected else "none")
    checks.add("client_never_acked", lg.get("rows_never_acked", 0) == 0,
               f"{lg.get('rows_never_acked', 0)} rows never acknowledged by Arc "
               f"(errors seen by client: {lg.get('errors')})", informational=True)
    wal_dropped = {}
    for p in env.ingest_procs():
        bs = p.buffer_stats() or {}
        wal_dropped[p.name] = bs.get("total_wal_dropped")
    extra["ingest_buffer_stats_end"] = {p.name: p.buffer_stats() for p in env.ingest_procs()}
    checks.add("wal_never_dropped", not any(wal_dropped.values()),
               f"total_wal_dropped per ingest: {wal_dropped}", informational=True)
    return [v]


def guarded(fn, name: str):
    """Run a scenario body with guaranteed teardown."""
    def wrapper(args):
        env = Env(name, args)
        try:
            return fn(env)
        finally:
            env.down()
    wrapper.__name__ = fn.__name__
    return wrapper


def outage_mode(env: Env, mode: str, scope="all", **cfg):
    env.proxy.set(mode, scope=scope, **cfg)
    env.timeline.mark("fault_on", mode=mode, scope=scope, **cfg)


def outage(env: Env, scope="all"):
    """The 'S3 is down' fault, shaped by --outage-mode."""
    m = getattr(env.args, "outage_mode", "down") or "down"
    if m == "hang":
        outage_mode(env, "hang", scope=scope)          # accept, never answer
    elif m == "http503":
        outage_mode(env, "http503", scope=scope)        # every request: 503 SlowDown
    else:
        outage_mode(env, "down", scope=scope)           # connection refused


def restore(env: Env, scope="all") -> float:
    env.proxy.normal(scope)
    t = time.time()
    env.timeline.mark("fault_off", scope=scope)
    return t


