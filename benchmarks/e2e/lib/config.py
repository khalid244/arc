"""Profiles, ports and arc.toml rendering.

A profile (profiles/*.toml) holds the role configs ([<role>.<section>]).
The harness overlays everything that is per run or per process: listen
port, bucket, S3 endpoint (the fault proxy listener for that role), local
MinIO credentials, the process's own WAL / stager / temp directories and
JSON logging. The result is written to <run>/procs/<name>/arc.toml and Arc is
started with that directory as its cwd (Arc reads ./arc.toml).
"""
from __future__ import annotations

import copy
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from . import guard

HARNESS_DIR = Path(__file__).resolve().parent.parent
PROFILES_DIR = HARNESS_DIR / "profiles"


@dataclass
class Ports:
    """All host ports live in 29000-29999 (harness rule)."""
    minio: int = 29000
    minio_console: int = 29001
    proxy_control: int = 29019
    service: int = 29150  # S12: kube-proxy-like Service in front of the ingest processes
    proxy: dict = field(default_factory=lambda: {
        "ingest": 29010, "compactor": 29012, "query": 29013, "rollup": 29014,
    })
    arc: dict = field(default_factory=lambda: {
        "ingest1": 29101, "ingest2": 29102, "ingest3": 29103,
        "query": 29110, "compactor": 29120, "rollup": 29130,
    })

    def validate(self):
        allp = [self.minio, self.minio_console, self.proxy_control, self.service, *self.proxy.values(),
                *self.arc.values()]
        for p in allp:
            if not (29000 <= p <= 29999):
                raise ValueError(f"port {p} outside 29000-29999")
        if len(set(allp)) != len(allp):
            raise ValueError("duplicate ports")


MINIO_USER = "arce2eadmin"
MINIO_PASSWORD = "arce2e-not-a-secret-123"

ROLE_OF = {
    "ingest1": "ingest", "ingest2": "ingest", "ingest3": "ingest",
    "query": "query", "compactor": "compactor", "rollup": "rollup",
}


def load_profile(name_or_path: str) -> dict:
    p = Path(name_or_path)
    if not p.exists():
        p = PROFILES_DIR / f"{name_or_path}.toml"
    with open(p, "rb") as f:
        prof = tomllib.load(f)
    guard.check_text(p.read_text(), f"profile {p}")
    prof["_path"] = str(p)
    return prof


def profile_value(profile: dict, role: str, dotted: str, default=None):
    cur = profile.get(role, {})
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _set(d: dict, dotted: str, value):
    parts = dotted.split(".")
    cur = d
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    cur[parts[-1]] = value


def apply_overrides(cfg: dict, overrides: list[str] | None):
    """--set section.key=value (value parsed as TOML scalar/array)."""
    for ov in overrides or []:
        k, v = ov.split("=", 1)
        parsed = tomllib.loads(f"x = {v}")["x"] if v else ""
        _set(cfg, k.strip(), parsed)


def build_arc_config(profile: dict, name: str, proc_dir: Path, port: int, s3_endpoint: str,
                     bucket: str, overrides: list[str] | None = None) -> dict:
    role = ROLE_OF[name]
    cfg = copy.deepcopy(profile.get(role, {}))
    guard.check_endpoint(s3_endpoint, f"S3 endpoint for {name}")
    d = str(proc_dir)
    managed = {
        "server.host": "127.0.0.1",
        "server.port": port,
        "log.level": "info",
        "log.format": "json",
        "storage.backend": "s3",
        "storage.s3_bucket": bucket,
        "storage.s3_endpoint": s3_endpoint,
        "storage.s3_region": "us-east-1",
        "storage.s3_access_key": MINIO_USER,
        "storage.s3_secret_key": MINIO_PASSWORD,
        "storage.s3_use_ssl": False,
        "storage.s3_path_style": True,
        "storage.s3_prefix": "",
        "storage.local_path": f"{d}/data/arc",
        "database.temp_directory": f"{d}/duckdb-tmp",
        "ingest.late_stager_directory": f"{d}/late-stager",
        "ingest.fresh_stager_directory": f"{d}/fresh-stager",
        "wal.directory": f"{d}/wal",
        "compaction.temp_directory": f"{d}/compaction-tmp",
        "reorg.temp_directory": f"{d}/reorg-tmp",
        "retention.db_path": f"{d}/data/arc.db",
        "continuous_query.db_path": f"{d}/data/arc.db",
        "backup.local_path": f"{d}/data/backups",
        "telemetry.enabled": False,
        "license.enabled": False,
        "auth.enabled": False,
        "mqtt.enabled": False,
    }
    for k, v in managed.items():
        _set(cfg, k, v)
    apply_overrides(cfg, overrides)
    # Re-assert the safety-critical keys after user overrides.
    guard.check_endpoint(cfg["storage"]["s3_endpoint"], f"S3 endpoint for {name}")
    return cfg


# ---------------------------------------------------------------- TOML writer

def _fmt(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(v, list):
        return "[" + ", ".join(_fmt(x) for x in v) + "]"
    raise TypeError(f"cannot format {type(v)}")


def toml_dumps(d: dict, prefix: str = "") -> str:
    lines = []
    scalars = {k: v for k, v in d.items()
               if not isinstance(v, dict) and not (isinstance(v, list) and v and all(isinstance(x, dict) for x in v))}
    tables = {k: v for k, v in d.items() if isinstance(v, dict)}
    arrays = {k: v for k, v in d.items() if isinstance(v, list) and v and all(isinstance(x, dict) for x in v)}
    for k, v in scalars.items():
        lines.append(f"{k} = {_fmt(v)}")
    for k, v in tables.items():
        name = f"{prefix}{k}"
        lines.append("")
        lines.append(f"[{name}]")
        body = toml_dumps(v, name + ".")
        lines.append(body)
    for k, items in arrays.items():
        name = f"{prefix}{k}"
        for it in items:
            lines.append("")
            lines.append(f"[[{name}]]")
            lines.append(toml_dumps(it, name + "."))
    return "\n".join(l for l in lines if l is not None).strip("\n") + "\n"


def write_arc_toml(cfg: dict, path: Path):
    text = "# Generated by the Arc E2E harness - do not edit\n" + toml_dumps(cfg)
    guard.check_text(text.replace(MINIO_PASSWORD, "***"), f"rendered config {path}")
    tomllib.loads(text)  # round-trip sanity check
    path.write_text(text)
    os.chmod(path, 0o600)
