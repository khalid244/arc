"""Result assembly: JSON result file, human summary, exit code."""
from __future__ import annotations

import json
import time

EXIT = {"PASS": 0, "FAIL": 1, "NOT_SUPPORTED": 2, "ERROR": 3}


class Checks:
    def __init__(self):
        self.items: list[dict] = []

    def add(self, name: str, ok: bool, detail="", informational: bool = False):
        self.items.append({"check": name, "pass": bool(ok), "detail": detail, "informational": informational})
        return ok

    @property
    def failed(self):
        return [c for c in self.items if not c["pass"] and not c["informational"]]


def standard_integrity_checks(checks: Checks, v: dict, allow_client_dups: bool = True):
    t = v["totals"]
    checks.add("harness_ledger_uuids_unique", t.get("ledger_duplicate_uuids", 0) == 0,
               f"{t.get('ledger_duplicate_uuids', 0)} uuids were SENT more than once by the harness "
               "(harness bug - duplicate/time results below are not trustworthy)")
    checks.add("zero_missing", t["missing"] == 0, f"{t['missing']} acked rows not in storage")
    arc_dups = t["dup_rows"] - (t["dup_rows_client_ambiguous"] if allow_client_dups else 0)
    checks.add("zero_duplicates", arc_dups == 0,
               f"{t['dup_rows']} duplicate rows ({t['dup_uuids']} uuids); "
               f"{t['dup_rows_client_ambiguous']} on batches with an ambiguous client retry")
    checks.add("zero_unexpected", t["unexpected"] == 0, f"{t['unexpected']} uuids in storage never sent")
    checks.add("timestamps_preserved", t["time_mismatch_rows"] == 0,
               f"{t['time_mismatch_rows']} stored rows whose time differs from what was sent")
    checks.add("rows_in_correct_partition", t.get("misplaced_rows", 0) == 0,
               f"{t.get('misplaced_rows', 0)} rows stored under a Y/M/D/H path that does not match their time")
    if "query_api_all_match" in v:
        bad = {k: (x["api_rows"], x["s3_rows"]) for k, x in v["query_api"].items() if not x["match"]}
        checks.add("query_api_matches_storage", v["query_api_all_match"],
                   "all measurements match" if not bad else f"api vs s3 rows: {bad}")


def overwrite_check(checks: Checks, proxy_stats: dict) -> dict:
    """Data objects (not _arc/) that received more than one successful whole-object PUT.
    Each one is a silent overwrite: whatever the first PUT stored is gone. Keys the proxy
    itself re-committed (commit_then_timeout retries of the same key) are excluded."""
    over, ctt = {}, set()
    for lst in proxy_stats.values():
        if isinstance(lst, dict):
            ctt.update(lst.get("ctt_committed_keys") or [])
    for name, lst in proxy_stats.items():
        if not isinstance(lst, dict):
            continue
        for k, n in (lst.get("keys_stored_more_than_once") or {}).items():
            parts = k.split("/", 2)
            if len(parts) == 3 and not parts[2].startswith("_arc/") and k not in ctt:
                over[f"{name}:{parts[2]}"] = n
    checks.add("no_silent_object_overwrites", not over,
               f"{len(over)} data objects stored more than once (second PUT replaced the first): "
               f"{dict(list(over.items())[:6])}" if over else "every data object key was written once")
    return over


def max_stats(sampler) -> dict:
    return {k: dict(v) for k, v in (sampler.max.items() if sampler else [])}


def write_result(env, checks: Checks, verifies: list[dict], extra: dict, status: str | None = None) -> int:
    pstats = env.proxy.stats()
    if verifies and isinstance(pstats, dict) and "error" not in pstats:
        extra["object_overwrites"] = overwrite_check(checks, pstats)
    if status is None:
        status = "FAIL" if checks.failed else "PASS"
    final_v = verifies[-1] if verifies else {}
    tot = final_v.get("totals", {})
    mx = max_stats(env.sampler)
    ingest_rss = max((v.get("rss_mb", 0) for n, v in mx.items() if n.startswith("ingest")), default=0)
    res = {
        "status": status,
        "scenario": env.scenario,
        "meta": {**env.meta, "finished": time.strftime("%Y-%m-%dT%H:%M:%S%z")},
        "params": {k: v for k, v in vars(env.args).items() if k not in ("func",)},
        "headline": {
            "acked_rows": tot.get("acked_rows"), "never_acked_rows": tot.get("never_acked_rows"),
            "missing": tot.get("missing"), "dup_rows": tot.get("dup_rows"),
            "dup_rows_client_ambiguous": tot.get("dup_rows_client_ambiguous"),
            "unexpected": tot.get("unexpected"), "time_mismatch_rows": tot.get("time_mismatch_rows"),
            "max_ingest_rss_mb": ingest_rss,
            "max_local_bytes": {n: v.get("local_total_bytes") for n, v in mx.items() if n.startswith("ingest")},
            "drain_s": extra.get("drain", {}).get("drain_s"),
        },
        "checks": checks.items,
        "verifications": verifies,
        "max_by_process": mx,
        "loadgen": env.lg_stats(),
        "proxy_stats": pstats,
        "logs": env.logs_summary(),
        "timeline": env.timeline.events,
        **extra,
    }
    out = env.run_dir / "result.json"
    out.write_text(json.dumps(res, indent=2, default=str))
    summary = human_summary(res)
    (env.run_dir / "summary.txt").write_text(summary)
    print(summary, flush=True)
    return EXIT[status]


def human_summary(res: dict) -> str:
    h = res["headline"]
    L = []
    L.append("=" * 78)
    L.append(f"{res['scenario']}  ->  {res['status']}")
    L.append(f"binary {res['meta']['binary']} (sha256 {res['meta']['binary_sha256'][:12]})")
    L.append(f"run dir {res['meta']['run_dir']}")
    L.append(f"acked rows {h['acked_rows']}  never-acked {h['never_acked_rows']}  missing {h['missing']}  "
             f"dup rows {h['dup_rows']} (client-ambiguous {h['dup_rows_client_ambiguous']})  "
             f"unexpected {h['unexpected']}  time-mismatch {h['time_mismatch_rows']}")
    L.append(f"max ingest RSS {h['max_ingest_rss_mb']:.0f} MB   drain {h['drain_s']} s   "
             f"max local bytes {h['max_local_bytes']}")
    for c in res["checks"]:
        mark = "PASS" if c["pass"] else ("info" if c["informational"] else "FAIL")
        L.append(f"  [{mark:4}] {c['check']}: {c['detail']}")
    final = res["verifications"][-1] if res["verifications"] else None
    if final:
        ev = final.get("evidence", {})
        if h.get("missing"):
            L.append(f"  missing by target: {ev.get('missing_by_target')}")
            L.append(f"  missing by class:  {ev.get('missing_by_class')[:6]}")
            L.append(f"  missing by ack minute: {ev.get('missing_by_ack_minute')[:12]}")
        if h.get("dup_rows"):
            L.append(f"  dups by target/class: {ev.get('dup_by_target_class')[:6]}")
            for g in (ev.get("dup_file_groups") or [])[:4]:
                L.append(f"  dup files {g}")
        if h.get("time_mismatch_rows"):
            L.append(f"  time mismatch sample: {(ev.get('time_mismatch_sample') or [])[:3]}")
    for name, lg in res.get("logs", {}).items():
        errs = lg.get("top_errors") or []
        if errs:
            L.append(f"  {name} top errors: {errs[:4]}")
        if lg.get("crash_lines"):
            L.append(f"  {name} CRASH: {lg['crash_lines'][:2]}")
    L.append("=" * 78)
    return "\n".join(L)
