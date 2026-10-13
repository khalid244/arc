"""Test environment: MinIO + fault proxy + Arc processes + load generator."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from . import guard, logscan
from .config import HARNESS_DIR, Ports, load_profile, profile_value
from .infra import FaultProxy, Minio, preflight_ports
from .supervisor import ArcProc, Sampler, Timeline

# Which proxy listener each role talks through (faults can be scoped per role).
PROXY_SCOPE = {"ingest": "ingest", "query": "query", "compactor": "compactor", "rollup": "rollup"}


def sha256_file(p: str) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class Env:
    def __init__(self, scenario: str, args):
        self.args = args
        self.scenario = scenario
        stripped = guard.check_environment()
        self.ports = Ports()
        self.ports.validate()
        ts = time.strftime("%Y%m%d-%H%M%S")
        root = Path(args.runs_dir).resolve()
        self.run_dir = root / f"{scenario}-{ts}"
        (self.run_dir / "logs").mkdir(parents=True, exist_ok=True)
        (self.run_dir / "procs").mkdir(parents=True, exist_ok=True)
        self.timeline = Timeline(self.run_dir / "timeline.jsonl")
        self.bucket = f"arc-e2e-{scenario.lower()}-{ts}".replace("_", "-")[:63]
        self.binary = str(Path(args.binary).resolve())
        if not os.access(self.binary, os.X_OK):
            raise SystemExit(f"binary not executable: {self.binary}")
        self.profile = load_profile(args.profile)
        self.minio = Minio(self.ports)
        self.proxy = FaultProxy(self.ports, self.run_dir)
        self.procs: dict[str, ArcProc] = {}
        self.sampler: Sampler | None = None
        self.loadgen: subprocess.Popen | None = None
        self.ledger_path = self.run_dir / "ledger.sqlite"
        self.lg_stats_path = self.run_dir / "loadgen-stats.json"
        self.stop_file = self.run_dir / "STOP_LOADGEN"
        self.meta = {"scenario": scenario, "run_dir": str(self.run_dir), "bucket": self.bucket,
                     "binary": self.binary, "binary_sha256": sha256_file(self.binary),
                     "profile": self.profile["_path"], "overrides": args.set or [],
                     "stripped_env": stripped, "started": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
        print(f"== {scenario}: run dir {self.run_dir}", flush=True)

    # ------------------------------------------------------------ bring-up
    def up(self, roles=("ingest1", "ingest2", "query"), extra_overrides: dict | None = None):
        preflight_ports([self.ports.arc[r] for r in roles])
        self.minio.ensure_running()
        self.minio.create_bucket(self.bucket)
        self.timeline.mark("bucket_created", bucket=self.bucket)
        self.proxy.start()
        self.timeline.mark("proxy_started")
        for name in roles:
            self.add_proc(name, extra_overrides)
        # Stagger starts: prod pods never start in the same millisecond, and
        # phase-aligned stager tickers make cross-process key collisions far
        # likelier than in prod (use --stagger-ms 0 to provoke them).
        for i, p in enumerate(self.procs.values()):
            if i and p.role == "ingest" and self.args.stagger_ms:
                time.sleep(self.args.stagger_ms / 1000)
            p.start()
        for p in self.procs.values():
            h = p.wait_healthy(timeout=self.args.startup_timeout)
            if h.get("health_s") is None:
                raise RuntimeError(f"{p.name} did not become healthy; see {p.log_path}")
        self.sampler = Sampler(self.procs, self.run_dir / "samples.csv", self.timeline)
        self.sampler.start()

    def add_proc(self, name: str, extra_overrides: dict | None = None) -> ArcProc:
        from .config import ROLE_OF
        role = ROLE_OF[name]
        ov = list(self.args.set or [])
        for k, v in (extra_overrides or {}).get(role, {}).items():
            ov.append(f"{k}={v}")
        if role == "ingest":
            # Each prod pod has its own HOSTNAME (the default stager pod id);
            # processes on one host would share it and refuse each other's
            # handoff as their own (409), so name each process explicitly.
            ov.append(f'ingest.stager_pod_id="e2e-{name}"')
        if role == "ingest" and getattr(self.args, "handoff", False):
            peers = [n for n in ("ingest1", "ingest2", "ingest3") if n != name and n in self.ports.arc]
            peer = peers[0]  # ingest1 <-> ingest2: the Service routes to a live peer
            ov.append(f'ingest.stager_handoff_url="http://127.0.0.1:{self.ports.arc[peer]}"')
            ov.append("ingest.stager_import_enabled=true")
        p = ArcProc(name, self.binary, self.run_dir, self.ports.arc[name],
                    self.proxy.endpoint(PROXY_SCOPE[role]), self.bucket, self.profile, ov, self.timeline)
        p.tz = getattr(self.args, "arc_tz", "UTC") or "UTC"
        self.procs[name] = p
        return p

    def ingest_procs(self) -> list[ArcProc]:
        return [p for p in self.procs.values() if p.role == "ingest"]

    def cfg(self, role: str, dotted: str, default=None):
        for p in self.procs.values():
            if p.role == role:
                cur = p.cfg
                for part in dotted.split("."):
                    if not isinstance(cur, dict) or part not in cur:
                        return default
                    cur = cur[part]
                return cur
        return profile_value(self.profile, role, dotted, default)

    # ------------------------------------------------------------ load
    def start_load(self, rate: float | None = None, extra: list[str] | None = None):
        targets = ",".join(p.url for p in self.ingest_procs())
        argv = [sys.executable, "-m", "lib.loadgen", "--targets", targets,
                "--ledger", str(self.ledger_path), "--stats-file", str(self.lg_stats_path),
                "--stop-file", str(self.stop_file), "--rate", str(rate or self.args.rate),
                "--late-window", str(self.cfg("ingest", "ingest.late_window_seconds", 7200)),
                "--future-skew", str(self.cfg("ingest", "ingest.future_skew_seconds", 300)),
                "--drain-timeout", str(self.args.client_drain_timeout),
                "--seed", str(self.args.seed), "--tag", f"e2e-harness={HARNESS_DIR}"]
        argv += extra or []
        log = open(self.run_dir / "logs" / "loadgen.log", "ab")
        self.loadgen = subprocess.Popen(argv, cwd=HARNESS_DIR, stdout=log, stderr=subprocess.STDOUT,
                                        env=guard.child_env())
        self.timeline.mark("load_start", rate=rate or self.args.rate)

    def lg_stats(self) -> dict:
        try:
            return json.loads(self.lg_stats_path.read_text())
        except (OSError, json.JSONDecodeError):
            return {}

    def stop_load(self) -> dict:
        if not self.loadgen:
            return {}
        self.stop_file.touch()
        self.timeline.mark("load_stop_requested")
        try:
            self.loadgen.wait(timeout=self.args.client_drain_timeout + 120)
        except subprocess.TimeoutExpired:
            self.loadgen.terminate()
            self.loadgen.wait(timeout=30)
        st = self.lg_stats()
        self.timeline.mark("load_stopped", acked_rows=st.get("rows_acked"),
                           never_acked_rows=st.get("rows_never_acked", 0))
        return st

    # ------------------------------------------------------------ waiting
    def sleep(self, seconds: float, label: str = "", poll: float = 10):
        """Sleep while printing a short progress line."""
        end = time.time() + seconds
        while True:
            left = end - time.time()
            if left <= 0:
                break
            time.sleep(min(poll, left))
            st = self.lg_stats()
            loc = {p.name: p.local_state() for p in self.ingest_procs() if p.alive()}
            brief = {n: f"stage={v['fresh_files']}+{v['late_files']}f/{(v['fresh_bytes'] + v['late_bytes']) // 1_000_000}MB wal={v['wal_bytes'] // 1_000_000}MB"
                     for n, v in loc.items()}
            print(f"   [{label}] {max(0, end - time.time()):5.0f}s left | acked={st.get('rows_acked', 0)} "
                  f"rate10s={st.get('acked_rows_per_s_10s')} inflight={st.get('inflight')} "
                  f"err={sum(st.get('errors', {}).values())} | {brief}", flush=True)

    def local_backlog(self) -> dict:
        out = {}
        for p in self.ingest_procs():
            if not p.alive():
                out[p.name] = {"alive": False, **p.local_state()}
                continue
            bs = p.buffer_stats() or {}
            ls = p.local_state()
            out[p.name] = {"alive": True, "active_buffers": bs.get("active_buffers"),
                           "flush_queue_depth": bs.get("flush_queue_depth"), **ls}
        return out

    def is_drained(self) -> tuple[bool, dict]:
        bl = self.local_backlog()
        ok = all(v.get("alive") and v.get("active_buffers") == 0 and v.get("flush_queue_depth") == 0
                 and v["fresh_files"] == 0 and v["late_files"] == 0 for v in bl.values())
        return ok, bl

    def wait_drained(self, timeout: float, since: float | None = None, stable_polls: int = 2) -> dict:
        """Wait until every ingest process has empty buffers and empty stager dirs."""
        t0 = since or time.time()
        deadline = time.time() + timeout
        ok_streak = 0
        last = {}
        while time.time() < deadline:
            ok, last = self.is_drained()
            ok_streak = ok_streak + 1 if ok else 0
            if ok_streak >= stable_polls:
                d = round(time.time() - t0, 1)
                self.timeline.mark("drained", seconds_since_restore=d)
                return {"drained": True, "drain_s": d, "backlog": last}
            time.sleep(2)
        self.timeline.mark("drain_timeout", backlog=last)
        return {"drained": False, "drain_s": None, "backlog": last}

    # ------------------------------------------------------------ teardown
    def logs_summary(self) -> dict:
        out = {name: logscan.scan(p.log_path) for name, p in self.procs.items()}
        return out

    def down(self):
        if self.loadgen and self.loadgen.poll() is None:
            self.loadgen.terminate()
        if self.sampler:
            self.sampler.stop()
        try:
            self.proxy.normal()
        except Exception:  # noqa: BLE001
            pass
        for p in self.procs.values():
            if p.alive():
                p.sigterm(timeout=max(30, p.cfg.get("server", {}).get("shutdown_timeout", 30) + 15))
        self.proxy.stop()
        self.timeline.mark("env_down")
