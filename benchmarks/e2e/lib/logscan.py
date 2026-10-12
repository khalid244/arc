"""Scan Arc JSON logs (zerolog) for errors and durability-relevant events."""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

# Messages worth surfacing as evidence even at info/warn level.
NOTABLE = [
    "WAL recovery complete", "WAL recovery: replayed", "WAL file recovered and deleted",
    "Recovering WAL file", "Purged WAL files", "Purged old WAL files", "Periodic WAL cleanup complete",
    "Skipping WAL purge", "FreshStager merged", "LateStager", "merge failed", "rolling back partial uploads",
    "rollback delete failed", "Write returned error but file exists", "Flush failed", "Partial multi-hour",
    "buffers still unflushed", "Failed to flush buffer during close", "shutdown flush failed",
    "circuit breaker", "Circuit breaker", "quarantin", "Reorg", "Reorganizer", "compaction", "Compaction",
    "WAL write dropped", "backpressure", "Storage write", "panic", "fatal",
]

_num = re.compile(r"\b\d+\b")
_hex = re.compile(r"\b[0-9a-f]{8,}\b")


def unwrap(ev: dict) -> dict:
    """The compactor parent re-logs its subprocess's JSON lines as the message string.
    Return the inner event (merged over the outer one) when that is the case."""
    msg = ev.get("message")
    if isinstance(msg, str) and msg.startswith("{") and msg.endswith("}"):
        try:
            inner = json.loads(msg)
            if isinstance(inner, dict):
                return {**ev, **inner, "_wrapped_by": ev.get("component")}
        except json.JSONDecodeError:
            pass
    return ev


def _norm(msg: str) -> str:
    return _hex.sub("<h>", _num.sub("<n>", msg))[:160]


def scan(log_path: Path, max_examples: int = 4) -> dict:
    levels = Counter()
    errors = Counter()
    warns = Counter()
    notable = Counter()
    examples: dict[str, list] = {}
    nonjson = []
    starts = 0
    if not log_path.exists():
        return {"missing_log": str(log_path)}
    with open(log_path, errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                if re.search(r"panic|fatal error|SIGSEGV|goroutine \d+", line):
                    nonjson.append(line[:300])
                continue
            if ev.get("harness") == "start":
                starts += 1
                continue
            ev = unwrap(ev)
            lvl = ev.get("level", "?")
            levels[lvl] += 1
            msg = str(ev.get("message", ev.get("msg", "")))
            err = str(ev.get("error", ""))
            key = _norm(msg)
            if lvl in ("error", "fatal", "panic"):
                errors[key] += 1
            elif lvl == "warn":
                warns[key] += 1
            for n in NOTABLE:
                if n in msg:
                    notable[key] += 1
                    ex = examples.setdefault(key, [])
                    if len(ex) < max_examples:
                        ex.append({k: ev.get(k) for k in ("time", "level", "component", "message", "error",
                                                         "entries", "files", "deleted", "rows", "records",
                                                         "partial_uploads", "staged_files", "key", "path",
                                                         "storage_path", "duration")
                                   if ev.get(k) is not None} | ({"error": err[:300]} if err else {}))
                    break
            if lvl in ("error", "fatal", "panic"):
                ex = examples.setdefault(key, [])
                if len(ex) < max_examples:
                    ex.append({"time": ev.get("time"), "level": lvl, "component": ev.get("component"),
                               "message": msg, "error": err[:300]})
    return {
        "process_starts": starts,
        "levels": dict(levels),
        "top_errors": errors.most_common(15),
        "top_warnings": warns.most_common(10),
        "notable": notable.most_common(25),
        "examples": examples,
        "crash_lines": nonjson[:20],
    }


def grep(log_path: Path, needle: str, limit: int = 20) -> list[dict]:
    out = []
    if not log_path.exists():
        return out
    with open(log_path, errors="replace") as f:
        for line in f:
            if needle in line:
                try:
                    ev = unwrap(json.loads(line))
                    if needle not in json.dumps(ev):
                        continue
                    out.append(ev)
                except json.JSONDecodeError:
                    out.append({"raw": line[:300]})
                if len(out) >= limit:
                    break
    return out
